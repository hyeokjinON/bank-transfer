# P5-1 일마감 대사 · 은행 점검 시간 · 알림 — 설계

> 상태: 설계 승인 (2026-10-29) · 다음: 구현 계획 · 결정 기록: ADR-0010 (구현과 함께 작성)
> 앞 단계: [P3 설계](2026-10-19-p3-transfer-design.md) (결과조회 · 정리 작업 · 재발행), [P4-1 설계](2026-10-26-p4-1-wallet-balance-design.md) (잔고 · 원장 · `fail()`)

## 1. 목적과 범위
낮 동안 출금은 한 건씩 은행 응답 · 결과조회로 상태가 정해진다. 그래도 우리 기록이 은행과 어긋날 길이 남아 있다: 결과조회 10번에도 모르는 `UNKNOWN`, 실패로 확정 · 환불한 뒤 늦게 온 "처리됨"(CONFLICT 경고), 미전송으로 판단해 다시 보냈는데 실제로는 간 건, 사람 · 버그가 고친 행. 이런 건은 우리 DB 안에서는 멀쩡해 보이고, **은행이라는 독립된 기록과 맞춰 봐야** 드러난다.

P5-1 은 하루치 출금을 **은행이 보내 준 대사 파일**과 맞춰 보고(일마감 대사), 결과를 몰라 잔고를 묶어 둔 건만 자동 확정하고, 나머지 어긋남은 기록 · 알린다. 날짜 경계를 깔끔하게 하려고 **자정 전후 은행 점검 시간**을 둔다. 알림은 텔레그램(선택)과 로그.

**범위 밖 (P5-2 이후)**: 보존 배치(발행된 `outbox_event`, 만료된 `account_verification` 정리), 원장 파티셔닝 · 보관, 불일치 해결 API(지금은 SQL 로 `resolved_at` 기록), 알림 재전송 큐, 관리 API 인증, 별도 배치 서버(P8).

## 2. 결정 요약
| 결정 | 내용 | 이유 |
|---|---|---|
| 은행 쪽 기록 | 모의 은행이 만든 **일자별 대사 파일**(고정길이 EUC-KR, 헤더 · 상세 · 트레일러) | 실제 은행 · 금결원 대사 파일과 같은 모양. Spring Batch 의 파일 읽기 · 청크 · 재시작을 그대로 쓴다. 전문으로 내역을 페이징해 받는 것은 실제와 거리가 있다 |
| 자동 보정 범위 | **결과를 모르는 출금(`UNKNOWN` · `SENDING`)만** 파일로 확정 (있으면 `COMPLETED`, 없으면 `FAILED E002` + 환불) | 묶인 잔고를 풀어 준다. 이미 확정된 건을 배치가 뒤집으면 보정이 새 사고가 된다(환불한 돈을 다시 빼려는데 잔고 부족 등) → 기록 + 알림 |
| 실행 위치 | **API 앱 안의 Spring Batch 잡** | 확정 · 환불에 기존 `TransferStore` 경로를 그대로 쓴다. 별도 배치 앱은 저장소 코드 이전이 대사보다 큰 일이라 P8 에서 |
| 중복 실행 · 재시작 | Spring Batch JDBC 잡 저장소, 잡 파라미터 `reconDate` | 날짜마다 인스턴스 하나: 끝난 날짜는 거부, 실패하면 멈춘 곳부터, 여러 대가 동시에 띄워도 하나만 |
| 날짜 경계 | **은행 점검 시간 23:50 ~ 00:10 (KST)**: 은행을 건드리는 API 거절 + 은행 송신 보류 | 점검 중에는 아무것도 은행에 가지 않으므로 "D일에 보낸 출금" ↔ "D일 파일" 이 같은 범위가 된다 |
| 알림 | `AlertNotifier` + 텔레그램 Bot API 어댑터(토큰이 있을 때만) / 로그 | 봇이 없는 PC · CI 에서도 그대로 돈다. 알림 실패가 대사를 되돌리지 않는다 |
| 이름 | 일마감 대사 = `dailyrecon` (잡 `dailyReconciliationJob`) | 10초 주기 "정리 작업"(`reconcile` 패키지)과 구분 |

## 3. 대사 파일과 모의 은행
### 3-1. 파일 형식 (전문 규격 v0.3 에 "대사 파일" 절로 추가)
필드 규칙은 전문과 같다 (AN 왼쪽 정렬 · 공백 채움, N 오른쪽 정렬 · 0 채움, EUC-KR). 한 줄 = 한 레코드, 줄바꿈 `\n`. 레코드 길이는 구분자 1자리를 포함해 모두 같게 공백으로 채운다.

| 레코드 | 필드 (길이) |
|---|---|
| 헤더 `H` | 구분 `H`(1) · 처리일 `yyyyMMdd`(8) · 은행코드(3) |
| 상세 `D` | 구분 `D`(1) · 이용기관 거래번호 AN(20) · 은행코드 AN(3) · 계좌번호 AN(16) · 금액 N(13) · 처리시각 `HHmmss`(6) |
| 트레일러 `T` | 구분 `T`(1) · 상세 건수 N(9) · 금액 합계 N(18) |

- 상세에는 **은행이 실제로 처리(입금)한 이체만** 넣는다. 계좌가 없어 거절한 건은 돈이 움직이지 않았으므로 없다.
- 1원인증 송금(거래번호 `V…`)도 들어간다. 우리는 출금 행으로 저장하지 않으므로 대조하지 않고 건수만 보고한다.
- 형식 코드(쓰기 · 읽기 · 검증)는 `modules/bank-protocol` 에 두고 모의 은행과 API 가 같이 쓴다.

### 3-2. 모의 은행
- 이체를 처리할 때마다(지금 메모리에 넣는 그 순간) **일자별 거래 기록** `{dir}/journal/yyyyMMdd.log` 에 상세 레코드 한 줄을 덧붙인다 (날짜 = 처리 시각의 KST 날짜).
- 기동할 때 기록 파일들을 읽어 처리한 거래번호를 복원한다 → 재기동해도 결과조회가 맞게 답한다.
- **일마감**: 매일 00:05 (KST) 에 전날 기록으로 `{dir}/recon/RECON_yyyyMMdd.dat` 를 만든다. 기록이 없는 날도 헤더 + 0건 트레일러인 빈 파일을 만든다. 임시 파일에 쓰고 이름을 바꿔(같은 디렉터리 안 원자적 이동) 반쯤 쓴 파일이 보이지 않게 한다. 이미 있으면 덮어쓰지 않는다.
- `--close-day=yyyyMMdd` 인자: 서버를 띄우지 않고 그 날짜 파일만 만들고 끝난다 (손 확인 · 재생성용).
- 디렉터리: 모의 은행 `--bank-files=…` 인자 / API `daily-recon.file-dir`, 기본 둘 다 `./bank-files` (작업 디렉터리 = 저장소 루트, `.gitignore`). 실제로는 은행 SFTP 에서 받아 오는 자리다.

## 4. 은행 점검 시간
- `bank.maintenance.start` / `end` (기본 `23:50` / `00:10`, `Asia/Seoul`), `bank.maintenance.enabled` (기본 `true`). 시작 포함 · 끝 제외, 자정을 넘는 구간을 지원한다.
- 순수 객체 `BankMaintenanceWindow(start, end, zone)`: `isActive(now)`, `remaining(now)`. 시계는 새 `Clock` 빈 하나를 주입받는다 (지금 곳곳의 `Clock.systemUTC()` 중 점검 판단이 닿는 곳을 이 빈으로 바꾼다 — 테스트가 시각을 고정할 수 있게).
- **웹**: 출금 접수, 1원인증 요청 · 확인 → 503 `BANK_MAINTENANCE` + `Retry-After: <점검 종료까지 남은 초, 올림>`. 조회 · 지갑 충전 · 관리 API 는 그대로.
- **은행 호출 어댑터**: 점검 중에는 이체 · 결과조회 전문을 보내지 않고 **"연결 없음"** 으로 답한다 → 기존 경로 그대로: 이체는 `RECEIVED` 로 돌아가 재시도 토픽 · 재발행, 결과조회는 시도 횟수를 세지 않고 다음 주기. 1원인증의 은행 호출도 막는다(웹에서 먼저 막히지만 어댑터가 최종 보장).
- 포기(`E001`)까지 약 26분 > 점검 20분 → 점검 때문에 포기하는 건이 없다. 점검 길이는 이 한도보다 짧게 둔다(설정 주석 · OPERATION 에 명시).
- 이 덕분에 **점검 시간에는 아무것도 은행에 가지 않는다** → 대사일 D 의 우리 쪽 대상은 `sending_at` 이 D일(KST)인 출금이다.

## 5. 대사 잡
### 5-1. 단계 (`dailyReconciliationJob`, 파라미터 `reconDate`)
| 단계 | 방식 | 하는 일 |
|---|---|---|
| ① `validateFile` | 태스클릿 | `RECON_D.dat` 존재, 헤더 날짜 = D, 트레일러 건수 · 합계 = 상세. 없으면 `FILE_NOT_READY` 로 실패, 틀리면 `FILE_INVALID` 로 실패 |
| ② `loadStatement` | 청크 500 | 상세를 `bank_statement_line` 에 적재 (`(bank_date, line_no)` 유니크, 다시 넣으면 무시) |
| ③ `matchTransfers` | 청크 100 | `sending_at` 이 D일인 출금을 id 순으로 페이징 → 거래번호로 **적재된 모든 날짜의 줄**을 찾아 판정(§6) → 자동 확정 또는 `recon_issue` |
| ④ `matchBankLines` | 청크 100 | 파일 D 의 `T` 줄 중 ③ 에서 다루지 않은 것: 우리에게 없는 거래번호(⑨), 다른 날 보낸 출금(같은 판정 — 예: 늦게 처리돼 오늘 파일에 나온 건) |
| ⑤ `checkLedger` | 태스클릿 | 잔고 ≠ 원장 합인 지갑 (⑩) |
| ⑥ `report` | 태스클릿 | 단계별 건수 · `recon_issue` 로 요약 → `AlertNotifier` |

- 판정은 **순수 함수** `StatementMatcher`(`modules/transfer`): (우리 상태 · 금액 · 계좌, 은행 줄 목록) → 판정. 배치 코드는 읽기 · 쓰기 · 연결만.
- 자동 확정은 기존 조건부 전이(`UNKNOWN`/`SENDING` → `COMPLETED`)와 `TransferStore.fail(id, from, at, "E002")` 를 쓴다. 0건이면(그 사이 정리 작업 등이 바꿈) 다시 읽어 바뀐 상태로 판정한다.

### 5-2. 실행
- 스케줄: `daily-recon.schedule.enabled` (기본 `true`), **01:00 ~ 06:00 매시 정각**에 전날 날짜로 실행. 파일이 아직 없으면(`FILE_NOT_READY`) 다음 정각에 재시작. 이미 완료면 조용히 넘어간다(INFO). 06:00 회차까지 완료하지 못하면 "대사 미완료" 긴급 알림.
- 수동: `POST /admin/reconciliations?date=yyyy-MM-dd` → 200 `{status, summary}` (실패해도 200 + `status=FAILED` + 이유). 이미 완료 409 `RECON_ALREADY_COMPLETED`, 실행 중 409 `RECON_RUNNING`, 내일 이후 날짜 400 (오늘은 손 확인용으로 허용).
  - **(마감 후 수정)** 오늘도 기본은 400 `RECON_DATE_NOT_CLOSED` — 마감된 날(어제 이전, KST)만 받는다. 오늘을 대사하면 아직 파일에 없는 방금 보낸 출금을 `E002` 환불 → 은행이 그 뒤 처리하면 이중 지급. 손 확인은 `daily-recon.allow-today=true`(실행 구성 6)일 때만 오늘을 허용 ([ADR-0010](../../adr/0010-daily-reconciliation.md) 결정 7).
- 끝난 날짜 재실행 거부 · 실패 시 이어서 · 동시 실행 하나만 — 모두 Spring Batch 잡 저장소가 보장한다. 이어서 할 때 이미 확정된 건은 조건부 전이 0건, 이슈는 유니크로 무시 → **어디서 다시 시작해도 결과가 같다.**
- 부하 측정 실행 구성(`5. API (부하 측정)`) · 다른 테스트는 `daily-recon.schedule.enabled=false`.

## 6. 판정 (거래번호로 짝지은 결과)
| # | 우리 상태 | 은행 줄 | 판정 | 배치가 하는 일 |
|---|---|---|---|---|
| 1 | COMPLETED | 있음, 금액 · 계좌 같음 | 일치 | 건수만 |
| 2 | FAILED | 없음 | 일치 | 건수만 |
| 3 | UNKNOWN · SENDING | 있음, 같음 | 자동 완료 | `COMPLETED` 전이 + 이슈 `AUTO_COMPLETED` (감사 기록) |
| 4 | UNKNOWN · SENDING | 없음 | 자동 실패 | `fail(…, "E002")` + 환불 + 이슈 `AUTO_FAILED` |
| 5 | 어느 상태든 | 있음, **금액이나 계좌가 다름** | 불일치 `AMOUNT_OR_ACCOUNT_MISMATCH` | 알림만 (3 이어도 확정하지 않음) |
| 6 | COMPLETED | 없음 | 불일치 `COMPLETED_NOT_AT_BANK` | 알림만 |
| 7 | FAILED | 있음 | 불일치 `REFUNDED_BUT_PAID` | 알림만 (환불한 돈이 나감) |
| 8 | RECEIVED | 있음 | 불일치 `PAID_BUT_RECEIVED` (긴급) | 알림만 (재발행되면 이중 지급) |
| — | RECEIVED | 없음 | 넘어감 | 재시도 중 (재발행 · `E001` 이 맡는다) |
| 9 | 없음 | `T` 줄 | 불일치 `UNKNOWN_REFERENCE` | 알림만 |
| 10 | — | — | 불일치 `BALANCE_LEDGER_MISMATCH` (지갑) | 알림만 |
| 11 | 어느 상태든 | **같은 거래번호 2줄 이상** | 불일치 `PAID_TWICE` (긴급) | 알림만 |

- `V` 줄은 대조하지 않고 건수만.
- `E002` = "일마감 대사로 은행 미처리 확정" (`E001` 재발행 포기와 구분). 조회 응답 · OPERATION 표에 추가.

## 7. 데이터 (Flyway `V4__daily_reconciliation.sql`)
- Spring Batch 메타데이터 표 (`BATCH_*`, Spring Batch 6 의 MySQL 스키마 그대로). 스키마는 Flyway 로만 만든다 (`spring.batch.jdbc.initialize-schema=never`).
- `bank_statement_line`: `bank_date` DATE, `line_no` INT, `reference` CHAR(20) ascii_bin, `bank_code`, `account_no`, `amount` BIGINT, `processed_at` DATETIME(6), `loaded_at`. PK `(bank_date, line_no)`, 인덱스 `reference`.
- `recon_issue`: `id`, `recon_date` DATE, `type` VARCHAR(40), `reference` CHAR(20) NULL, `transfer_id` CHAR(36) NULL, `user_id` NULL, `our_value` · `bank_value` VARCHAR(200) NULL, `created_at`, `resolved_at` NULL, `resolution_note` NULL. 유니크 `(recon_date, type, ref_key)` — `ref_key` 는 거래번호 또는 지갑이면 `user_id` (생성 컬럼 또는 NOT NULL 값으로 채움).
- 둘 다 append-only 기록 (`resolved_at` · `resolution_note` 갱신만).

## 8. 알림
- `AlertNotifier.notify(Alert(level = INFO | WARN | CRITICAL, title, lines))`. 구현: `TelegramNotifier`(토큰 · chat id 가 둘 다 있을 때) / `LogNotifier`(그 밖).
- 설정: `alert.telegram.bot-token` · `chat-id` · `base-url`(기본 `https://api.telegram.org`), 환경변수 `ALERT_TELEGRAM_BOT_TOKEN` · `ALERT_TELEGRAM_CHAT_ID`. 토큰은 저장소 · 로그에 남기지 않는다.
- 전송: `POST {base-url}/bot{token}/sendMessage`, 일반 텍스트(파싱 모드 없음 — 이스케이프 실수 방지). 연결 3초 · 응답 5초. 실패하면 ERROR 로그만 남기고 삼킨다 — **알림 실패가 대사를 실패시키거나 되돌리지 않는다.** 기록의 원본은 `recon_issue`.
- 내용: 계좌번호 가운데 가림(`1002****6789`), 사용자 id · 예금주명 넣지 않음, 불일치 목록 최대 20건 + "외 n건", 4,096자 이하로 자른다.
- 보내는 때:
  - 대사 완료: 불일치 0 이면 INFO, 있으면 CRITICAL (요약 + 목록).
  - 대사 실패: CRITICAL. 단 `FILE_NOT_READY` 는 06:00 마지막 회차에서만.
  - 기존 경고 3곳(P3 ~ P4 에서 "알림은 P5" 로 미룬 것) → WARN: ① 늦은 응답으로 확정이 어긋남(컨슈머 CONFLICT) ② DLT 에 은행 장애가 아닌 이유로 들어감 ③ 결과조회 한도(10회) 소진.

## 9. 설정 (기본값)
`bank.maintenance.{enabled=true,start=23:50,end=00:10}` · `daily-recon.{file-dir=./bank-files, schedule.enabled=true, schedule.cron=0 0 1-6 * * *, schedule.zone=Asia/Seoul, chunk-size=100}` · `alert.telegram.{bot-token=, chat-id=, base-url=https://api.telegram.org}` · `spring.batch.job.enabled=false` (기동 때 잡을 자동 실행하지 않는다) · `spring.batch.jdbc.initialize-schema=never`.

## 10. 테스트
**단위**
- `StatementMatcher`: §6 의 모든 줄을 표 그대로. 뮤테이션 확인(금액 비교를 지우면 실패하는지 등) 결과를 커밋 본문에.
- `BankMaintenanceWindow`: 시작 포함 · 끝 제외, 자정 넘김, `remaining` 올림, 꺼짐. **시계는 UTC 로 주고 KST 로 판정하는지** (UTC 14:50 = KST 23:50 → 점검 중, UTC 23:50 = KST 08:50 → 점검 아님 — `LocalTime.now(clock)` 을 그대로 쓰는 실수를 잡는다).
- 대사 파일 형식: 쓰기 → 읽기 왕복(EUC-KR), 트레일러 건수 · 합계 불일치 거부, 헤더 날짜 불일치 거부, 길이 틀린 줄 거부.
- `TelegramNotifier`: 가짜 HTTP 서버로 경로 · 본문, 가림 · 자르기, 500 · 시간 초과에도 예외 없음.

**통합 (Testcontainers MySQL, 실제 Spring Batch)**
- 전 칸: §6 의 각 경우를 심은 출금 + 만든 파일 → 잡 → 전이 · 환불(원장 `REFUND`) · 이슈 종류별 건수 · 가짜 알림이 받은 요약.
- 재실행 거부: 완료한 날짜를 다시 → 거부, 환불 · 이슈가 늘지 않음.
- 재시작: ③ 중간 청크에서 한 번 예외 → 잡 실패 → 재시작 → 완료, 결과가 한 번에 끝낸 것과 같음(이중 환불 없음).
- 동시 실행: 같은 날짜로 두 스레드 → 하나만 실행.
- 경합: 판정 직전에 상태가 바뀌면 다시 읽어 판정.
- `FILE_NOT_READY` → 실패, 파일을 놓고 재시작 → 완료.
- 점검 시간: 고정 시계로 출금 · 1원인증 503 + `Retry-After`, 조회 · 충전 200. 점검 전에 접수된 출금은 은행에 가지 않고 `RECEIVED` → 점검이 끝나면 `COMPLETED` (흐름 테스트, 시계를 옮긴다).
- 모의 은행: 재기동 후 결과조회가 처리 기록대로, `--close-day` 파일이 형식대로.
- 공유 DB 테스트는 테스트마다 다른 대사일을 쓴다 (잡 인스턴스가 날짜로 구분되므로).

**손 확인 (Postman 6 폴더 · `http/daily-recon.http`)**
1. 점검 시간: 지금 시각을 포함하도록 `--bank.maintenance.start/end` 를 주고 띄운다 → 출금 503 + `Retry-After`, 잔고 조회 200.
2. 대사: `transfer.reconcile.enabled=false` 로 띄워 결과를 모르는 출금을 남긴다 → 오늘 출금(정상 · 없는 계좌 · 처리 후 무응답 계좌) → 모의 은행 `--close-day=오늘` → `POST /admin/reconciliations?date=오늘` (마감 후 수정: API 는 실행 구성 6 = `allow-today=true` 로 띄워야 오늘을 받는다) → 요약 · DB(자동 완료 · 자동 실패 · 환불) → 다시 보내면 409. 봇을 설정했으면 휴대폰으로 요약.
3. 주의: 오늘을 미리 마감하는 것은 손 확인용이다 (그 뒤 출금은 파일에 없다).

## 11. 위험과 한계
- 점검 시작 직전에 보낸 출금을 은행이 20분 넘게 붙잡았다가 처리하면, 그날 회차는 자동 실패(`E002` + 환불)로 확정하고 다음 날 회차가 ④ 에서 `REFUNDED_BUT_PAID` 로 잡아 알린다. 자동 복구는 없다.
- 확정된 건의 어긋남(5 ~ 11)은 사람이 SQL 로 처리하고 `resolved_at` 을 남긴다 (해결 API 는 범위 밖).
- 개발 PC 에서는 스케줄이 PC 가 켜져 있을 때만 돈다. 모의 은행이 00:05 에 떠 있지 않았으면 파일이 없어 06:00 에 "대사 미완료" 알림. P5-1 이전 날짜에는 은행 기록이 없어 대사 대상이 아니다.
- 관리 API(`/admin/…`)에 인증이 없다 — 다른 API 와 같이 범위 밖으로 둔다.
- 텔레그램은 외부 서비스다: 메시지에 가린 계좌 · 금액 · 거래번호가 나간다.
- 같은 프로세스에서 배치가 돈다 — 하루 한 번 · 수 초 규모라 운영 API 영향은 작다고 본다 (측정은 하지 않는다).

## 12. 결정 기록 · 문서
- ADR-0010 일마감 대사(대사 파일 · 미확정만 자동 확정 · 점검 시간 · 배치 위치).
- `docs/protocol.md` v0.3 대사 파일 절, `docs/database.md` 새 표 · 운영 쿼리, OPERATION(점검 시간 · 대사 실행 · 알림 설정 · 이슈 처리 · `E002`), SETUP(텔레그램 봇 만들기 · 환경변수), ARCHITECTURE(대사 흐름), README, HANDOVER, CHANGELOG, Postman 6 폴더, velog #9.
