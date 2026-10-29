# ADR-0010 일마감 대사는 은행 대사 파일과 맞추고 결과를 모르는 출금만 자동 확정한다 — 점검 시간으로 날짜 경계를 맞추고 API 앱 안의 Spring Batch 로 돌린다

- 상태: 제안 (2026-10-29) — 머지 때 채택
- 단계: P5-1 · 설계: [P5-1 설계](../superpowers/specs/2026-10-29-p5-1-daily-reconciliation-design.md) · [ADR-0006](0006-inquire-instead-of-resend.md) (결과조회) · [ADR-0008](0008-wallet-debit-on-intake-refund-on-failure.md) (`fail()` 환불) · [database.md](../database.md) §4-6 ~ §4-8 · [protocol.md](../protocol.md) "대사 파일"

## 맥락
- 낮 동안 출금은 한 건씩 은행 응답 · 결과조회로 상태가 정해진다. 그래도 우리 기록이 은행과 어긋날 길이 네 갈래 남는다: 결과조회 10번에도 모르는 `UNKNOWN`, 확정 · 환불 뒤 늦게 온 "처리됨"(`CONFLICT`), 미전송으로 보고 다시 보냈는데 실제로는 간 건, 사람 · 버그가 고친 행.
- 이 넷은 우리 DB 안에서는 멀쩡해 보인다 → **은행이라는 독립된 기록과 맞춰야** 드러난다.
- 날짜 경계가 흐리면 "D일 출금" 과 "D일 파일" 이 어긋난다. 자정 직전에 보내 자정 뒤에 처리된 건이 두 날짜를 넘나들면 매일 어긋남이 생긴다.

## 결정
1. **은행 쪽 기록 = 일자별 대사 파일** (고정길이 EUC-KR 헤더 · 상세 · 트레일러, 규격 v0.3, [protocol.md §7](../protocol.md)). 형식 코드는 `bank-protocol` 하나(`ReconFileFormat`)에 두어 모의 은행이 쓰고 API 가 읽는다.
2. **자동 보정은 `UNKNOWN` · `SENDING` 만.** 파일에 있으면 `COMPLETED`(`0000`) + 이슈 `AUTO_COMPLETED`, 없으면 `fail(E002)` + 환불 + 이슈 `AUTO_FAILED`. **이미 확정된 건의 어긋남**(금액 · 계좌 불일치, `COMPLETED` 인데 은행에 없음, `FAILED` 인데 은행에 있음, `RECEIVED` 인데 은행에 있음, 우리에게 없는 거래번호, 이중 지급, 잔고 ≠ 원장 합)은 `recon_issue` 에 남기고 알리기만 한다. 금액 · 계좌가 다르면 `UNKNOWN` 이어도 확정하지 않는다.
3. **판정은 순수 함수 `StatementMatcher`, 확정은 `StatementSettler`.** 확정은 기존 조건부 전이(→ `COMPLETED`)와 `TransferStore.fail()`(→ `FAILED` `E002` + 환불, 한 트랜잭션)를 그대로 쓴다. 조건부 전이가 0건이면(그 사이 정리 작업 · 컨슈머가 바꿈) **다시 읽어 바뀐 상태로 다시 판정, 최대 3회**, 넘으면 예외 → 단계 실패 → 재시작으로 이어서.
4. **API 앱 안의 Spring Batch 6**, JDBC 잡 저장소(`spring-boot-starter-batch-jdbc`, 메타 표는 Flyway `V4`). 잡 `dailyReconciliationJob` 은 6단계: ① `validateFile` ② `loadStatement`(청크 500) ③ `matchTransfers`(청크 100) ④ `matchBankLines`(청크 100) ⑤ `checkLedger` ⑥ `report`.
   - ③ 은 `sending_at` 이 D일(KST)인 출금 전부를 거래번호로 파일 줄과 짝짓는다(**적재된 모든 날짜의** 줄을 본다 — 이중 지급 · 늦은 처리를 잡으려고).
   - ④ 는 파일 D 의 줄 중 **1원인증(`V…`) 줄이 아니고 ③ 이 다루지 않은 것 전부**를 판정한다. `T…` 만이 아니라 낯선 접두의 거래번호도 `UNKNOWN_REFERENCE` 가 된다 — 은행이 내보낸 돈이 접두 때문에 보고되지 않는 구멍을 두지 않는다. 같은 거래번호의 여러 줄은 첫 줄 하나만 읽고 판정은 그 거래번호의 모든 줄로 한 번 한다(이중 지급을 두 번 세지 않는다). `V…` 줄은 적재하되 대조하지 않고 건수만 보고한다.
   - **단계 트랜잭션 설정은 회귀 테스트로 고정했다.** Spring Batch 6 의 `chunk(size)` 는 트랜잭션 관리자를 넘기지 않으면 조용히 `ResourcelessTransactionManager`(트랜잭션 없음)로 돌아, 확정 · 환불이 청크와 함께 롤백되지 않는다. 그래서 여섯 단계 모두 JPA 트랜잭션 관리자를 명시하고, ③ ④ 는 `READ COMMITTED` 다. 이 설정은 `DailyReconJobTest` 가 리플렉션으로 단계마다 단언한다.
   - **③ ④ 가 `READ COMMITTED` 인 이유**: MySQL 기본 `REPEATABLE READ` 에서는 청크의 첫 읽기(리더)가 스냅숏을 잡아, 조건부 UPDATE 가 0건일 때의 "다시 읽기" 가 바뀐 상태를 보지 못한다. `StatementSettlerIsolationTest` 가 둘을 나란히 보인다 — `READ COMMITTED` 는 다시 읽어 일치, `REPEATABLE READ` 는 스냅숏 때문에 3회 뒤 포기.
5. **날짜마다 잡 인스턴스 하나** (식별 파라미터 `reconDate`): 끝난 날짜는 거부(409 `RECON_ALREADY_COMPLETED`), 실패하면 멈춘 단계 · 마지막 커밋 청크부터, 동시 실행은 하나만.
   - Spring Batch 6.0.5 의 시작은 "인스턴스 조회 → 돌고 있는 실행 확인 → 실행 생성" 을 서로 다른 트랜잭션으로 한다. 그래서 스펙이 기대한 "잡 저장소가 하나만 보장" 은 절반만 맞다. 처음 시작하는 날짜는 인스턴스 UNIQUE 가 막지만 진 쪽은 `DuplicateKeyException`(직렬화 격리라 데드락일 수도)을 받고, 실패한 날짜를 두 곳이 동시에 재시작하면 둘 다 확인을 통과할 수 있다.
   - 해결: 런처가 그 예외들을 "실행 중"(409 `RECON_RUNNING`)으로 바꾸고, 잡 리스너 **`SingleRunGuard`** 가 같은 인스턴스에 먼저 만든 실행이 아직 돌고 있으면 나중 실행을 멈춘다. 인스턴스만 만들어지고 실행이 아직 없는 짧은 순간에 온 쪽은 잠깐 기다려 한 번 더 시도한다. 결과가 같다는 보장(조건부 전이 · 유니크)은 그대로라, 이 가드가 막는 것은 알림 중복이다.
6. **점검 시간 23:50 ~ 00:10 (KST)로 날짜 경계를 맞춘다.** 점검 중에는 아무것도 은행에 가지 않으므로 "D일에 보낸 출금" 과 "D일 파일" 이 같은 범위가 된다. 웹은 503 `BANK_MAINTENANCE` + `Retry-After`(출금 접수 · 1원인증 요청 · 확인), 은행 호출은 `BankGatewayClient.send()` 의 `SendGate` 한 곳에서 "연결 없음" 과 같은 길로 보류한다(이체는 `RECEIVED` 로 돌아가 재시도 · 재발행, 결과조회는 횟수를 세지 않음). 판단은 `Clock` 빈 하나를 `Asia/Seoul` 로 바꿔서 한다.
7. **스케줄 01 ~ 06시 매시 정각**(`0 0 1-6 * * *`, `Asia/Seoul`)에 전날 대사. 파일이 아직 없으면(`FILE_NOT_READY`) 다음 정각에 멈춘 곳부터 다시. **마지막 회차**(같은 cron 의 다음 실행이 다음 날인 회차)는 이유와 무관하게 끝내지 못했으면 "대사 미완료" CRITICAL 을 보낸다 — 실패일 때뿐 아니라 **실행이 아직 돌고 있는 것으로 끝났을 때(이유 `RUNNING`)도**. 대사 도중 JVM 이 죽어 실행이 `STARTED` 로 남으면 회차마다 "실행 중" 만 돌아와 아무도 모르게 되기 때문이다(계획의 "실행 중은 알리지 않음" 은 마지막 회차에 한해 바꿨다). 수동 `POST /admin/reconciliations?date=` 는 200(`COMPLETED` / `FAILED` + 이유) · 409 · 400(`RECON_DATE_IN_FUTURE`, 오늘은 손 확인용으로 허용).
8. **알림** `AlertNotifier`: 텔레그램 토큰 · chat id 가 둘 다 있으면 `TelegramNotifier`(일반 텍스트, 로그에도 먼저 남김), 아니면 `LogNotifier`. 알림 실패는 삼키고(ERROR 로그만) 대사를 실패시키거나 되돌리지 않는다 — 기록의 원본은 `recon_issue`. 계좌번호는 가운데를 가리고, 사용자 id 는 싣지 않고, 목록은 20건 · 전체 4,096자까지. 기존 경고 3곳(`CONFLICT` · DLT · 결과조회 한도 소진)도 WARN 으로 같은 길을 쓴다.

### 가정과 알려진 모양
- **은행 파일은 한 번 보내면 바뀌지 않는다.** 적재(`bank_statement_line`)는 PK `(bank_date, line_no)` 의 중복 INSERT 를 무시(no-op)하므로, 같은 줄 번호에 다른 내용이 다시 오면 조용히 무시된다. 모의 은행도 이미 있는 파일을 덮어쓰지 않는다. 다시 받으려면 파일을 지우고 `--close-day`, 그리고 적재 줄도 사람이 지워야 한다.
- 거래번호가 ASCII 가 아닌 파일은 ② 적재에서 **ERROR 로 실패**한다(열이 `ascii_bin`). 우리가 만드는 거래번호는 항상 ASCII 라 정상 흐름에서는 나오지 않는다.
- `E002` = 일마감 대사로 "은행이 처리하지 않았다" 를 확정한 것(돈은 움직이지 않았고 환불됨). `E001`(재발행 포기)과 구분한다.
- **Spring Batch 메타 표(`BATCH_*`)의 시각은 KST 가 아니다.** Spring Batch 가 `LocalDateTime.now()` 로 쓰므로 JVM 기본 시간대다. `TimeZone.setDefault` 는 쓰지 않는다는 원칙([database.md §2](../database.md))을 지키려 그대로 두고, 대사 결과의 기준은 업무 표(`bank_statement_line` · `recon_issue`, KST)로 한다.
- `bank.maintenance.start` · `end` 는 yml 에서 따옴표가 필수다 — YAML 1.1 은 `23:50` 을 60진수 정수로 읽는다. 기본 설정 확인 테스트가 이 함정을 잡는다.

## 결과
- 증명 테스트:
  - **판정** (`StatementMatcherTest`): 설계 §6 표를 줄마다 — 1 ~ 11 + `RECEIVED` 있음 / 없음 + 순서 규칙(이중 지급이 금액 비교보다 먼저).
  - **확정** (`StatementSettlerTest`): 미확정만 확정하고 확정된 건은 판정만 돌려준다 / 경합(확정이 0건)이면 다시 읽어 바뀐 상태로 판정 / 3회 넘게 바뀌면 포기. `StatementSettlerIsolationTest`: `READ COMMITTED` 는 다시 읽어 일치, `REPEATABLE READ` 는 스냅숏 때문에 포기 — 격리 수준을 낮춘 근거.
  - **잡** (`DailyReconJobTest`): 전 칸 — 전이 · 환불 · 이슈 종류별 · 알림 요약, 여섯 단계의 트랜잭션 관리자 · 격리 수준(리플렉션).
  - **실행** (`DailyReconLauncherTest`): 끝난 날짜 재실행 거부 / ③ 중간 청크 실패 → 재시작 → 이중 환불 없음 / 동시 실행 하나 / `SingleRunGuard` / `FILE_NOT_READY` / 마지막 회차 알림 / `FILE_INVALID`. `DailyReconSchedulerTest`(회차 · 마지막 회차 판정), `ReconciliationApiTest`(200 / 409 / 400).
  - **파일 · 모의 은행**: `ReconFileFormatTest`(왕복 · 건수 · 합계 · 날짜 · 길이), `BankJournalTest` · `MockBankRestartTest`(기록 · 재기동 복원 · 일마감 · 덮어쓰지 않음).
  - **점검 시간**: `BankMaintenanceWindowTest`(포함 · 제외 · 자정 넘김 · 올림 · 꺼짐 · UTC 시계 → KST 판정), `BankMaintenanceApiTest`(503 + `Retry-After`, 조회 · 충전 · 관리 API 는 그대로) · `BankMaintenanceFlowTest`(송신 보류 → 점검 뒤 처리).
  - **알림**: `TelegramNotifierTest`(가짜 서버 · 500 · 시간 초과 · 토큰이 로그에 없음).
- **테스트가 실제로 잡아내는지 확인** (일회성 변이 검사, 커밋하지 않음 — Task 8 커밋 `52e24e9` 본문의 결과):
  - 금액 비교 제거 → `5 어느 상태든 금액이 다르면 불일치` 실패: `RECEIVED ==> expected AMOUNT_OR_ACCOUNT_MISMATCH but was PAID_BUT_RECEIVED`
  - 계좌번호 비교 제거 → `5 계좌번호가 다르면 불일치` 실패: `RECEIVED ==> expected AMOUNT_OR_ACCOUNT_MISMATCH but was PAID_BUT_RECEIVED`
  - 줄 수 `>= 2` → `> 2` → `11 같은 거래번호 2줄 이상` 실패: `RECEIVED ==> expected PAID_TWICE but was RETRYING`
  - 줄 없음 `RECEIVED` → `AUTO_FAIL` → `RECEIVED + 없음 = 재시도 중` 실패: `expected RETRYING but was AUTO_FAIL`
  - 다시 읽기(`transfer = read(transferId)`) 제거 → `경합 - 확정이 0건이면 … 다시 읽어 바뀐 상태로 판정한다` · `경합 - … REFUNDED_BUT_PAID` 실패: `IllegalStateException` (3회 안에 확정 못 함)
- 한계:
  - 점검 시작 직전에 보낸 출금을 은행이 20분 넘게 붙잡았다가 처리하면, 그날 회차는 자동 실패(`E002` + 환불)로 확정하고 다음 날 회차의 ④ 가 `REFUNDED_BUT_PAID` 로 잡아 알린다. 자동 복구는 없다. 그래서 점검 길이는 재발행 포기(`E001`)까지의 약 26분보다 짧게 두는 것을 설정 주석 · OPERATION 에 적었다.
  - 확정된 건의 어긋남은 사람이 SQL 로 처리하고 `resolved_at` 을 남긴다(해결 API 는 범위 밖).
  - 개발 PC 에서는 스케줄이 PC 가 켜져 있을 때만 돈다. 모의 은행이 00:05 에 떠 있지 않았으면 파일이 없어 06시에 "대사 미완료" 알림이 온다.
  - 관리 API(`/admin/…`)에 인증이 없다(다른 API 와 같이 범위 밖).
  - 텔레그램은 외부 서비스다: 가린 계좌 · 금액 · 거래번호가 나간다. 호출은 호출한 스레드에서 최대 약 8초(연결 3초 + 응답 5초) 걸릴 수 있다.
  - 같은 프로세스에서 배치가 돈다 — 하루 한 번 · 수 초 규모라 운영 API 영향은 작다고 보고 측정하지 않았다.
  - ③ 은 `sending_at` 범위를 전체 훑는다(단독 인덱스 없음 — 하루 한 번이라 두지 않았다). ⑤ 는 지갑 전체를 훑는다.
  - `bank_statement_line` · `recon_issue` 는 매일 늘어난다(보존 · 파티셔닝은 P5-2).

## 검토한 다른 선택지
| 선택지 | 버린 이유 |
|---|---|
| 전문으로 거래내역을 페이징해 받기 | 실제 은행 · 금결원의 대사 방식과 거리가 멀고, Spring Batch 의 파일 읽기 · 재시작을 쓰지 못한다 |
| 확정된 건도 자동 보정 | 환불한 돈을 다시 빼려다 잔고가 모자라는 등 보정이 새 사고가 된다 |
| 별도 배치 앱 | 저장소 코드 이전이 대사보다 큰 일이다 → P8 |
| Spring Batch 없이 `@Scheduled` 만 | 재시작 지점 · 날짜별 중복 거부 · 동시 실행 방지를 직접 만들어야 한다 |
| `spring-boot-starter-batch` (메모리 잡 저장소) | Boot 4 에서는 `ResourcelessJobRepository` 라 재시작 · 중복 거부가 프로세스를 넘지 못한다 |
| 점검 시간 없이 `processed_at` 으로 날짜를 맞추기 | 자정 직전 송신 · 자정 뒤 처리 건이 날짜를 넘나들어 매일 어긋남이 생긴다 |
| 점검을 웹에서만 막기 | 컨슈머 · 정리 작업 · 재발행은 웹을 거치지 않는다 |
| `LocalTime.now(clock)` 로 점검 판단 | 시계의 시간대(서버는 UTC)를 써서 9시간 어긋난다 → `Instant` 를 KST 로 바꿔 판단 |
| 텔레그램 `parse_mode=MarkdownV2` | 이스케이프 실수 하나로 메시지가 거절된다 → 일반 텍스트 |
| 알림 재전송 큐 | 범위 밖(P5-2). 기록의 원본은 `recon_issue` 다 |
