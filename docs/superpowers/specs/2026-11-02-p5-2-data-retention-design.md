# P5-2 보존 배치 — 설계

> 상태: 설계 승인 (2026-11-02) · 다음: 구현 계획 · 결정 기록: ADR-0011 (구현과 함께 작성)
> 앞 단계: [P5-1 설계](2026-10-29-p5-1-daily-reconciliation-design.md) (Spring Batch 잡 · 실행기 · 알림), [P3 설계](2026-10-19-p3-transfer-design.md) (아웃박스 · 발행기)

## 1. 목적과 범위
`outbox_event` 는 출금마다 한 행씩 늘고, 발행이 끝나면 읽는 코드가 없다. `account_verification` 은 끝나지 않은 인증도 예금주명 · 계좌번호를 계속 들고 있다. 둘 다 쌓아 둘 이유가 없다.

P5-2 는 이 **일회성 데이터 두 종류만** 매일 정해진 기간이 지나면 지우고, 나머지 표는 왜 지우지 않는지 정책표로 남긴다.

**범위 밖**: `transfer` · `wallet_ledger` 아카이빙, `bank_statement_line` · `recon_issue` 삭제 · 파티셔닝(금융 기록이라 지우지 않는다), `BATCH_*` 정리(하루 몇 행), 쓰이지 않는 `VERIFIED` 정리(인증에 만료 개념이 없다), 모의 은행 `bank-files/` 정리.

## 2. 결정 요약
| 결정 | 내용 | 이유 |
|---|---|---|
| 지울 대상 | 발행된 `outbox_event`, `VERIFIED` 가 아닌 만료 `account_verification` | 일회성 데이터. 금융 기록은 지우지 않는다 |
| 실행 방식 | Spring Batch 잡 `dataRetentionJob`, 표마다 tasklet 한 단계 | P5-1 의 저장소 · 실행기 · 알림을 그대로 쓴다. 날짜별 이력 · 삭제 건수가 `BATCH_*` 에 남는다 |
| 삭제 방식 | `DELETE … LIMIT n` 을 트랜잭션마다 한 번, `READ COMMITTED` | 읽었다가 지우는 reader/writer 는 낭비. 짧은 트랜잭션 · gap 락 없음으로 접수를 막지 않는다 |
| 기준 시각 | `retentionDate` 00:00(KST) − 보관 기간 | 날짜로만 정해져 재시작해도 같다 |
| 스케줄 | 매일 06:30 KST, 재시도 회차 없음 | 대사 마지막 회차(06:00) 뒤. 다음 날 실행이 밀린 행까지 지운다 |
| 알림 | 실패만 WARN, 성공은 로그 | 대사 요약이 이미 매일 간다. 삭제가 하루 밀려도 급하지 않다 |

## 3. 지우는 조건 (이 절이 이 단계의 핵심)
| 표 | 삭제 조건 | 기본 보관 | 지키는 것 |
|---|---|---|---|
| `outbox_event` | `published_at < :cutoff` | 7일 | 미발행(`published_at IS NULL`)은 아무리 오래돼도 남는다 — NULL 은 `<` 비교에 걸리지 않는다. 7일은 브로커 기본 보존과 같아 그동안 Kafka 메시지와 대조할 수 있다 |
| `account_verification` | `status IN ('SENT','EXPIRED','LOCKED') AND expires_at < :cutoff` | 30일 | **`VERIFIED` 는 절대 지우지 않는다.** |

두 함정을 조건으로 막는다.
- **`VERIFIED` 도 `expires_at` 이 지나 있다.** `expires_at` 은 코드 입력 시한이고, 출금 접수는 `existsVerified`(`status = VERIFIED` 행이 있는가)만 본다. `expires_at` 만으로 지우면 인증한 사용자 전부의 출금이 422 `ACCOUNT_NOT_VERIFIED` 가 된다.
- **코드를 입력하지 않고 시간만 지난 건은 상태가 `SENT` 그대로다.** `EXPIRED` 는 만료 뒤 확인을 시도했을 때만 된다. `EXPIRED` 만 지우면 이 행들이 영원히 남는다.

### 지우지 않는 표 (`docs/database.md` 정책표로 남긴다)
| 표 | 정책 | 이유 |
|---|---|---|
| `transfer` · `wallet_ledger` · `wallet` | 삭제하지 않음 | 전자금융거래 기록 · 잔고의 근거 |
| `bank_statement_line` · `recon_issue` | 삭제하지 않음 | 대사 근거 · 감사 기록. ③ 이 모든 날짜의 줄을 본다 |
| `account_verification` 의 `VERIFIED` | 삭제하지 않음 | 출금 인증의 근거 |
| `BATCH_*` | 삭제하지 않음 | 하루 몇 행, 실행 이력 |

법정 보존 기간은 전자금융거래법 시행령 제12조 기준 5년(1만 원 이하 거래 등은 1년)으로 2차 자료에서 확인했고, 조문 원문은 확인하지 않았다. 문서에는 "이 프로젝트의 가정, 실제 적용은 원문 · 금융위 고시 확인 필요"로 적는다. 7일 · 30일은 법이 아니라 이 프로젝트의 운영 판단이다.

## 4. 잡
`dataRetentionJob` (식별 파라미터 `retentionDate: LocalDate`) — ① `purgeOutbox` ② `purgeVerifications`.

- 각 단계는 tasklet 이다. 한 번 불릴 때 `DELETE … LIMIT batchSize` 한 문장을 실행하고 `contribution.incrementWriteCount(n)` 후, `n == batchSize` 면 `CONTINUABLE`, 아니면 `FINISHED`. 트랜잭션은 호출마다 하나다.
- 두 단계 모두 트랜잭션 관리자를 명시하고 격리 수준은 `READ COMMITTED` (P5-1 과 같은 함정 — 넘기지 않으면 트랜잭션 없이 돈다. 리플렉션 테스트로 고정).
- `ORDER BY` 는 두지 않는다. 어느 행부터 지워도 결과가 같고, MySQL 8 기본 `binlog_format=ROW` 라 `LIMIT` 의 비결정성이 복제에 문제가 되지 않는다.
- 기준 시각은 `RetentionPolicy` 순수 함수로 계산한다: `date.atStartOfDay(Asia/Seoul) − 보관 기간`. 시각 바인딩은 `ReconRepository` 와 같은 방식이고, 실제 `NOW(6)` 로 쓴 `published_at` 과 KST 로 맞는지 테스트한다.
- **인덱스**: `V5__data_retention.sql` 이 `idx_verification_purge (status, expires_at)` 를 추가한다. 없으면 묶음마다 표 전체를 훑는다. 아웃박스는 기존 `idx_outbox_unpublished (published_at, id)` 를 탄다. 실제 실행 계획은 시드 데이터로 `EXPLAIN` 해 손 확인 때 적는다 (행이 적으면 옵티마이저가 전체 훑기를 고를 수 있어 자동 테스트로 두지 않는다).

### 왜 `READ COMMITTED` 인가 (가설 — 테스트로 확인)
미발행 행은 `published_at` 이 NULL 이라 인덱스 맨 앞에 모인다. 새 미발행 행 `(NULL, 가장 큰 id)` 가 들어갈 자리는 "마지막 NULL 행과 첫 발행 행 사이" 다. `REPEATABLE READ` 의 범위 삭제는 첫 발행 행에 next-key 락을 잡으므로 바로 이 틈을 잠가, **출금 접수의 아웃박스 INSERT 가 삭제가 끝날 때까지 기다릴 수 있다.** 발행기를 `READ COMMITTED` 로 돌리는 이유(`MessagingConfiguration`)와 같은 뿌리다. `StatementSettlerIsolationTest` 처럼 두 격리 수준을 나란히 재현해 결과를 ADR 에 그대로 적는다. 가설과 다르게 나오면 그 결과를 적는다.

### 실행기 공용화
시작 경합 처리(`IllegalStateException` 한 번 재시도, `DuplicateKey` · `PessimisticLocking` → 실행 중, `SingleRunGuard`)는 까다로워 복사하지 않는다. `DailyReconLauncher` 에서 **날짜별 잡을 시작하고 결과 종류만 돌려주는** 공용 부분을 빼고(이름은 계획에서 정한다), 대사의 알림 · 실패 해석은 `DailyReconLauncher` 에 남긴다. `SingleRunGuard` 도 파라미터 이름 · 예외를 받도록 일반화한다. 기존 `DailyReconLauncherTest` 는 그대로 통과해야 한다 (대사 동작은 바뀌지 않는다).

### 실행과 실패
- **스케줄**: `0 30 6 * * *` `Asia/Seoul`, 오늘 날짜로 실행. 스케줄 풀 3 → **4** — 밀린 행이 많은 첫 실행이 길어져도 발행기 · 정리 작업 · 대사를 굶기지 않는다.
- **이미 끝난 날짜**: 스케줄은 조용히(INFO) 넘어간다.
- **실패**: 멈춘 단계 · 지운 만큼은 그대로 두고 텔레그램 WARN `보존 배치 {date} 실패` (예외 종류만, 내부 메시지는 로그). 자동 재시도 없음 — 다음 날 실행은 새 인스턴스이고 기준 시각이 뒤로 가 밀린 행까지 지운다.
- **도중에 JVM 이 죽으면**: 그 날짜의 실행은 `STARTED` 로 남지만 다음 날 인스턴스는 막지 않는다. 그날 날짜를 수동으로 다시 돌릴 때만 409 `RETENTION_RUNNING` 이고, 대사와 같은 SQL 복구를 OPERATION 에 적는다.
- **성공**: 표별 삭제 건수를 INFO 로그로. 텔레그램은 보내지 않는다.

### 수동 실행
`POST /admin/retention-runs?date=yyyy-MM-dd` — 요청 스레드에서 끝까지 돌린다 (인증은 다른 관리 API 와 같이 범위 밖).

| 응답 | 뜻 |
|---|---|
| 200 `{"date","status":"COMPLETED","reason":null,"deleted":{"outboxEvents":n,"verifications":m}}` | 끝났다. 건수는 이 날짜 인스턴스의 모든 실행을 더한 값 (재시작해도 맞다) |
| 200 `{"status":"FAILED","reason":{"code","message"},"deleted":null}` | 요청은 처리됐고 결과가 실패다 |
| 409 `RETENTION_ALREADY_COMPLETED` · `RETENTION_RUNNING` | 끝난 날짜 · 돌고 있음 |
| 400 `RETENTION_DATE_IN_FUTURE` | 내일 이후(KST). 기준 시각이 앞당겨져 정책보다 일찍 지우게 되므로 거부. 오늘과 지난 날은 받는다 (지난 날은 기준이 더 과거라 지우는 범위가 좁아질 뿐이다) |

## 5. 설정 (기본값)
```yaml
data-retention:
  outbox-retention: 7d
  verification-retention: 30d
  batch-size: 1000
  schedule:
    enabled: true          # 부하 측정 실행 구성 · 테스트는 false
    cron: "0 30 6 * * *"
    zone: Asia/Seoul
```
보관 기간이 1일 미만이거나 `batch-size` 가 1 ~ 10000 밖이면 **기동에 실패한다** — `0d` 같은 실수로 방금 발행한 행이나 막 만료된 인증을 지우는 일을 막는다.

## 6. 테스트
- **핵심 (`DataRetentionJobTest`)**: `expires_at` 이 지난 `VERIFIED` 는 남고, 잡 뒤에도 그 사용자의 `POST /transfers` 가 202 다.
- **삭제 조건 (`RetentionRepositoryTest`)**: `SENT` · `EXPIRED` · `LOCKED` 는 기준 전이면 지우고 후면 남긴다. 미발행 아웃박스는 오래돼도 남는다. 기준 시각 경계(기준 −1µs 삭제 · 기준 그대로 유지). `NOW(6)` 로 쓴 `published_at` 이 KST 로 비교된다.
- **순수 함수 (`RetentionPolicyTest`)**: 기준 시각, KST 경계(KST 00:30 = UTC 전날 15:30), 내일 거부 · 오늘 허용, 설정 하한.
- **잡**: 2,500행 + 묶음 1,000 → 3번 커밋 · 건수 2,500. ① 뒤 ② 실패 → 재시작하면 ② 부터 · 합계가 맞다. 단계 트랜잭션 관리자 · 격리 수준(리플렉션). 끝난 날짜 재실행 거부, 동시 실행 하나.
- **잠금 (`RetentionLockTest`)**: 삭제 트랜잭션을 열어 둔 채 아웃박스 INSERT — `READ COMMITTED` 는 바로 들어가고, `REPEATABLE READ` 와 나란히 비교한다.
- **API (`RetentionApiTest`)**: 200 · 409 · 400, KST 경계. **스케줄러**: 오늘 날짜로 실행, 실패 시 WARN 알림, 시작 실패에도 예외가 새지 않는다. **기본 설정**: yml 값이 바인딩된다.
- **변이 검사 (일회성, 결과를 ADR 에)**: `status` 조건 제거 → 핵심 테스트 실패, `published_at < :cutoff` 를 `published_at IS NULL OR …` 로 바꿈 → 미발행 보존 테스트 실패, 격리 수준 제거 → 리플렉션 · 잠금 테스트 실패.
- **손 확인 (`http/data-retention.http`, Postman 7 폴더)**: SQL 로 오래된 행 시드 → 수동 실행 → 건수 · 남은 행 확인 · `VERIFIED` 사용자 출금 202 · 다시 실행 409 · 내일 400 · `EXPLAIN`.

## 7. 위험과 한계
- 삭제는 되돌릴 수 없다. 조건은 테스트로 고정했지만 **첫 운영 실행 전에는 같은 조건의 `SELECT COUNT(*)` 로 대상을 확인**하라고 OPERATION 에 적는다 (건수 미리보기 기능은 만들지 않는다).
- 만료 후 30일이 지난 인증을 확인하면 "만료" 대신 404 `VERIFICATION_NOT_FOUND` 다.
- 한 번도 쓰지 않는 `VERIFIED` 는 영원히 남는다 — 인증에 만료 정책이 생길 때 다룬다.
- 묶음 사이에 쉬지 않는다. 지연 복제가 있는 운영이면 쉬는 시간이 필요하다 (이 프로젝트에는 복제가 없다).
- 삭제가 늘 돌아도 InnoDB 표 파일은 바로 줄지 않는다 — 공간은 재사용된다.

## 8. 결정 기록 · 문서
ADR-0011(조건 · 격리 재현 결과 · 변이 검사), `docs/database.md`(정책표 · 인덱스 · V5), OPERATION(§10 보존 배치 · §4 오류 코드), ARCHITECTURE, README(P5 완료), SETUP 변화 없음, HANDOVER, CHANGELOG, `http/data-retention.http`, Postman 7 폴더, `.run/5` 에 `--data-retention.schedule.enabled=false`, velog #10.
