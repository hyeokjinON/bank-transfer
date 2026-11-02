# ADR-0011 보존 배치는 일회성 데이터 두 종류만 READ COMMITTED 로 나눠 지운다 — 금융 기록은 지우지 않는다

- 상태: 제안 (2026-11-02)
- 단계: P5-2 · 설계: [P5-2 설계](../superpowers/specs/2026-11-02-p5-2-data-retention-design.md) · [ADR-0005](0005-kafka-delivery-and-consumer-idempotency.md) (발행기 READ COMMITTED) · [ADR-0010](0010-daily-reconciliation.md) (Spring Batch · 실행기) · [database.md](../database.md) §4-9

## 맥락
- `outbox_event` 는 출금마다 한 행씩 늘고, 발행이 끝나면 읽는 코드가 없다.
- `account_verification` 은 끝나지 않은 인증도 예금주명 · 계좌번호를 계속 들고 있다.
- 삭제는 되돌릴 수 없다. 그래서 **무엇을 지우고 무엇을 지우지 않는지(조건)가 이 단계의 전부**다. 나머지(잡 · 격리 수준 · 실행기)는 그 조건을 안전하게 돌리는 방법이다.

## 결정
1. **지울 대상과 조건** (설계 §3 표 그대로).

   | 표 | 삭제 조건 | 기본 보관 |
   |---|---|---|
   | `outbox_event` | `published_at < :cutoff` — 미발행(`NULL`)은 `<` 비교에 걸리지 않아 아무리 오래돼도 남는다 | 7일 |
   | `account_verification` | `status IN ('SENT','EXPIRED','LOCKED') AND expires_at < :cutoff` | 30일 |

   두 함정을 조건으로 막는다.
   - **`VERIFIED` 도 `expires_at` 이 지나 있다.** `expires_at` 은 코드 입력 시한이고 출금 접수는 `existsVerified`(`status = VERIFIED` 행이 있는가)만 본다. `expires_at` 만으로 지우면 인증한 사용자 전부의 출금이 422 `ACCOUNT_NOT_VERIFIED` 가 된다.
   - **코드를 입력하지 않고 시간만 지난 건은 상태가 `SENT` 그대로다.** `EXPIRED` 는 만료 뒤 확인을 시도했을 때만 된다. `EXPIRED` 만 지우면 이 행들이 영원히 남는다.
   - 상태는 "`VERIFIED` 가 아닌 것" 이 아니라 **목록**으로 적는다. 나중에 새 상태가 생겨도 묻지 않고 지우지 않게.
2. **지우지 않는 표** — `transfer` · `wallet_ledger` · `wallet` · `bank_statement_line` · `recon_issue` · `BATCH_*`, 그리고 `account_verification` 의 `VERIFIED`. 이유는 [database.md §4-9](../database.md) 정책표에 표마다 적었다.
3. **Spring Batch 잡 `dataRetentionJob`**, 표마다 tasklet 한 단계(① `purgeOutbox` ② `purgeVerifications`). tasklet 이 한 번 불릴 때 `DELETE … LIMIT n` 한 문장을 실행하고 `incrementWriteCount(n)` 한 뒤, `n == batchSize` 면 `CONTINUABLE` 아니면 `FINISHED`. 호출마다 = 트랜잭션 하나. `ORDER BY` 는 없다 — 어느 행부터 지워도 결과가 같고, MySQL 8 기본 `binlog_format=ROW` 라 `LIMIT` 의 비결정성이 복제에 문제가 되지 않는다.
4. **기준 시각 = 실행 날짜 00:00(KST) − 보관 기간** (`RetentionPolicy` 순수 함수). 시계를 보지 않고 날짜로만 정해 재시작해도 같다. 시각 바인딩은 `ReconRepository` 와 같이 `Timestamp.from(Instant)`.
5. **두 단계 모두 트랜잭션 관리자를 명시하고 `READ COMMITTED` + 모든 예외 롤백.** 이유는 아래 "격리 재현"(잠금)과 "스펙과 달라진 점"(`rollbackOn`). 단계 설정은 리플렉션 테스트로 고정했다 (P5-1 과 같은 함정 — 넘기지 않으면 트랜잭션 없이 돈다).
6. **시작 경합 처리를 `DatedJobLauncher` · `DatedJobSingleRunGuard` 로 뽑아 대사와 같이 쓴다** (`IllegalStateException` 한 번 재시도, `DuplicateKey` · `PessimisticLocking` → 실행 중, 가드). 대사의 관찰 가능한 동작은 바뀌지 않는다 — `DailyReconLauncherTest` 는 고치지 않고 통과한다. `Job` 빈이 둘이 되어 대사 쪽 두 곳의 주입에 `@Qualifier` 를 달았다 (`@Primary` 는 쓰지 않았다, 아래 선택지).
7. **스케줄** 매일 06:30 KST(대사 마지막 회차 06:00 뒤), 오늘 날짜로. **재시도 회차는 없다** — 다음 날 실행은 새 인스턴스이고 기준 시각이 뒤로 가 밀린 행까지 지운다. 스케줄 풀 3 → **4** (밀린 행이 많은 첫 실행이 길어져도 발행기 · 정리 작업 · 대사를 굶기지 않는다). 이미 끝난 날짜는 조용히(INFO) 넘어간다.
8. **알림은 실패만 WARN** (`보존 배치 {date} 실패`, 예외 종류만, 내부 메시지는 로그). 스케줄 · 수동 모두 보낸다. 성공은 표별 건수를 INFO 로그로만 — 대사 요약이 이미 매일 간다. 삭제가 하루 밀려도 급하지 않다.
9. **수동 `POST /admin/retention-runs?date=yyyy-MM-dd`**: 200 `COMPLETED` · 200 `FAILED` · 409 `RETENTION_ALREADY_COMPLETED` / `RETENTION_RUNNING` · 400 `RETENTION_DATE_IN_FUTURE`(내일 이후, KST. 기준 시각이 앞당겨져 정책보다 일찍 지우게 되므로. 오늘과 지난 날은 받는다). **건수는 그 날짜 인스턴스의 모든 실행의 합**이다 — 재시작하면 멈춘 단계에 건수 0 인 새 단계 실행이 생기므로, 마지막 실행만 보면 재시작 뒤 건수가 모자란다.
10. **설정 범위를 벗어나면 기동에 실패한다**: 보관 기간 1일 미만, `batch-size` 1 ~ 10000 밖. `0d` 같은 실수로 방금 발행한 행이나 막 만료된 인증을 지우는 일을 막는다.

### 설계 · 계획과 달라진 점
- **`rollbackOn` 을 `true` 로 고정한 트랜잭션 속성을 쓴다.** 설계는 `DefaultTransactionAttribute` 에 격리 수준만 얹는 모양이었다. 그런데 `TaskletStep` 의 기본 속성은 `rollbackOn` 이 언제나 true 인 익명 속성이다. 평범한 `DefaultTransactionAttribute` 는 검사 예외(Kotlin 은 막지 않는다)에 `rollbackOn` 이 false 라, 단계가 그 예외를 **삼키고 초깃값 `CONTINUABLE` 을 돌려 같은 호출이 끝없이 돈다** (Spring Batch 6.0.5 `TaskletStep.ChunkTransactionCallback` 소스로 확인). 리플렉션 테스트가 격리 수준과 함께 단언한다.
- **`RESTART_REFUSED` 의 메시지는 고정 문장이다.** 설계는 "예외 종류만" 이라고 했다. 대사는 `e.message` 를 그대로 싣지만(동작을 바꾸지 않는다) 보존 배치는 알림에 내부 메시지를 싣지 않는다. 원문은 ERROR 로그에만.
- **실패 WARN 은 스케줄 · 수동 모두 보낸다.** 설계는 둘을 가르지 않았다. 수동 실행자가 응답을 봐도 기록이 남아 해롭지 않다. `Running` · `AlreadyCompleted` 는 알리지 않는다 (대사의 "마지막 회차" 같은 개념이 없다).
- 테스트 지원: `Job` 이 둘이 되어 `DailyReconTestSupport` 의 주입에도 `@Qualifier` 한 줄이 필요했다. `DailyReconLauncherTest` · `DailyReconJobTest` 본문은 고치지 않았다.

## 결과
### 격리 재현 — 가설대로, 단 보인 범위는 좁다
저장소에 있는 `RetentionLockTest` 의 관찰 (이 테스트를 넣은 커밋 `8bc1822` 본문 그대로):

```
관찰: READ COMMITTED = INSERTED
관찰: REPEATABLE READ = LOCK_WAIT_TIMEOUT
```

설계 §4 의 가설("`REPEATABLE READ` 의 범위 삭제가 첫 발행 행의 next-key 락으로 새 미발행 행의 자리를 잠근다")은 **맞았다.** 삭제 트랜잭션을 열어 둔 채 미발행 아웃박스 행을 INSERT 하면, 잡의 실제 단계 속성(`READ COMMITTED`)에서는 바로 들어가고 `REPEATABLE READ` 에서는 2초 락 대기 한도에 걸려 끝난다.

**보인 것은 여기까지다.** `REPEATABLE READ` 는 삭제가 열려 있는 동안 INSERT 를 막고, `READ COMMITTED` 는 막지 않는다. 막히는 시간의 길이(실제 삭제 한 번이 얼마나 걸리는지)와 처리량에 미치는 효과는 **재지 않았다.** 테스트의 2초는 일부러 건 락 대기 한도이지 실제 대기 길이가 아니다.

테스트는 시간 초과로 끝나도 늦게 들어온 INSERT 가 공유 DB 에 미발행 행을 남기지 않게, INSERT 가 끝나기를 기다린 뒤에만 지우고 12초 안에 끝나지 않으면 실패한다 (커밋 `67bc833`, 구현 계획의 테스트 코드를 이 부분만 덮어썼다).

잠금의 모양 (일회성 확인, 아래 탐색의 작은 4가지 모양에서 `performance_schema.data_locks` 로 봄):
- 발행 행이 있으면 INSERT 는 **첫 발행 행**의 `INSERT_INTENTION` 에서 기다린다.
- 발행 행이 하나도 없으면(미발행만 있거나 빈 표) **supremum** 에서 기다린다. "첫 발행 행" 으로 일반화하면 틀린다.
- `READ COMMITTED` 에서는 삭제가 지운 행의 레코드 락뿐이고(지울 행이 없으면 표 IX 만), INSERT 가 기다린 락은 없었다.

**일회성 탐색 (재현할 수 없다):** 위 테스트와 별개로, 저장소에 넣지 않은 일회성 테스트로 데이터 모양 10가지(지울 행 0건 · 미발행 0 ~ 200건 · 빈 표 · 묵은 발행 행 3000건 포함)를 ANALYZE 없음 · 있음으로 두 번씩, 두 격리 수준으로 돌렸다 (40회). 모든 모양에서 결과가 같았다. 지울 행이 0건이어도, 표가 비어도 `REPEATABLE READ` 는 막혔다 — "지울 것이 있을 때만 막힌다" 가 아니다. 한계: 표는 많아야 약 5,000행, 동시 부하 없음, 실행 계획은 모두 `idx_outbox_unpublished` 범위 훑기였다 (전체 훑기 계획은 한 번도 나오지 않아 그 경우는 재지 못했다). 이 탐색은 저장소의 테스트로 다시 돌릴 수 없고, 위 서술은 그때의 관찰이다. 시도하지 않은 모양에서 같다는 증명이 아니다.

`READ COMMITTED` 를 유지하는 근거로 남는 것은 이 한 가지다: 같은 비용(`DELETE … LIMIT` 한 문장)에서 삭제 트랜잭션이 열려 있어도 `RetentionLockTest` 의 INSERT 가 들어간다. 트랜잭션이 실제로 얼마나 짧은지는 재지 않았다. 발행기가 `READ COMMITTED` 인 것([ADR-0005](0005-kafka-delivery-and-consumer-idempotency.md))과 같은 뿌리다.

### 변이 검사 (일회성 — 커밋하지 않았고 모두 되돌렸다)
구현 계획대로 코드를 하나씩 망가뜨리고 테스트가 실제로 잡아내는지 확인했다. 변이마다 테스트를 돌려 실패를 읽고 `git restore` 로 되돌린 뒤 작업 트리가 깨끗한 것을 확인했다. 모두 Testcontainers 안에서만 지웠다.

| # | 변이 | 실패한 테스트 | 메시지 |
|---|---|---|---|
| 1 | `RetentionRepository`: `status IN ('SENT','EXPIRED','LOCKED')` 조건 제거 (`expires_at < :cutoff` 만) | `DataRetentionJobTest > 핵심 - expires_at 이 지난 VERIFIED 는 남고, 잡 뒤에도 그 사용자의 출금이 202 다` | `actual value is null ==> expected: not <null>` (`VERIFIED` 행이 사라졌다) |
| | | `DataRetentionJobTest > 대상만 지운다 - …` | `expected: <3> but was: <4>` |
| | | `RetentionRepositoryTest > 1원인증 - VERIFIED 는 expires_at 이 아무리 지나도 지우지 않는다 (출금 인증의 근거)` | `expected: <0> but was: <1>` |
| 2 | `RetentionRepository`: `published_at < :cutoff` → `(published_at IS NULL OR published_at < :cutoff)` | `RetentionRepositoryTest > 아웃박스 - 미발행은 아무리 오래돼도 남는다 (NULL 은 비교에 걸리지 않는다)` | `expected: <0> but was: <1>` |
| | | `DataRetentionJobTest > 대상만 지운다 - …` | `expected: <1> but was: <2>` |
| | | `RetentionRepositoryTest > 아웃박스 - 한 번에 limit 행까지만 지운다` | `expected: <1> but was: <2>` (기대한 둘 밖의 추가 실패) |
| 3 | `DataRetentionJobConfiguration`: `.transactionAttribute(purgeTransaction())` 줄 삭제 | `DataRetentionJobTest > 단계 트랜잭션 - 두 단계 모두 JPA 트랜잭션 관리자 · READ COMMITTED · 모든 예외에 롤백` | `purgeOutbox ==> expected: <2> but was: <-1>` (격리 수준 기본값) |
| | | `RetentionLockTest > READ COMMITTED (잡의 실제 설정) - 삭제를 열어 둔 채 미발행 아웃박스 INSERT 가 들어간다` | `expected: <INSERTED> but was: <LOCK_WAIT_TIMEOUT>` |

세 변이 모두 기대한 테스트가 실패했다. 변이 1 의 핵심 테스트는 출금이 422 가 되기 전에 인증 행 조회에서 이미 null 이다 (행이 지워졌다). 변이 1 · 2 는 기대한 테스트 말고도 `대상만 지운다` 가 함께 실패했다 — 잡 단계에서도 같은 조건이 잡힌다는 뜻이다. 변이 3 의 잠금 테스트 실패는 테스트가 자기 격리 수준을 적지 않고 **잡의 실제 단계 속성**으로 트랜잭션을 열기 때문에 성립한다 (격리 수준을 테스트가 직접 들면 잡 설정을 지워도 통과한다).

### 증명 테스트
- `RetentionRepositoryTest`: 삭제 조건 · 경계(기준 −1µs 삭제 · 기준 그대로 유지) · `NOW(6)` 로 쓴 `published_at` 의 KST 비교.
- `DataRetentionJobTest`: 핵심(`expires_at` 이 지난 `VERIFIED` 는 남고 잡 뒤에도 출금이 202) · 대상만 지운다 · 2,500행 + 묶음 1,000 → 3번 커밋 · 묶음의 배수 · 단계 설정(트랜잭션 관리자 · `READ COMMITTED` · 모든 예외 롤백, 리플렉션).
- `RetentionLockTest`: `READ COMMITTED` 는 들어가고 `REPEATABLE READ` 는 락 대기 (위 관찰).
- `DataRetentionLauncherTest`: 건수 합 · 끝난 날짜 재실행 거부 · ② 실패 뒤 재시작 · 동시 실행 하나 · 실패한 날짜의 동시 재시작을 막는 가드 · `STARTED` 로 남은 날짜 · 잡 인스턴스만 남은 경우.
- `DataRetentionSchedulerTest`: 오늘(KST) 날짜 · 시작 실패에도 예외가 새지 않음 · 풀 4.
- `RetentionPolicyTest`: 기준 시각 · KST 경계 · 내일 거부 · 오늘 허용 · 설정 하한 · yml 바인딩.
- `RetentionApiTest`: 200 · 409 · 400 · KST 경계.
- 대사 쪽 `DailyReconLauncherTest` 는 수정 없이 통과한다 (실행기 공용화의 회귀 검사).

### 한계
- **삭제는 되돌릴 수 없다.** 조건은 테스트로 고정했지만 **첫 운영 실행 전에는 같은 조건의 `SELECT COUNT(*)` 로 대상을 확인한다** ([OPERATION §10](../../OPERATION.md), [database.md §9](../database.md)). 건수 미리보기 기능은 만들지 않았다.
- 만료 후 30일이 지난 인증을 확인하면 "만료"(410) 대신 404 `VERIFICATION_NOT_FOUND` 다. 잠긴(`LOCKED`) 인증도 같다 — "잠김"(423 `VERIFICATION_LOCKED`) 대신 404.
- 한 번도 쓰지 않는 `VERIFIED` 는 영원히 남는다 — 인증에 만료 정책이 생길 때 다룬다.
- 묶음 사이에 쉬지 않는다. 지연 복제가 있는 운영이면 쉬는 시간이 필요하다 (이 프로젝트에는 복제가 없다).
- 삭제가 늘 돌아도 InnoDB 표 파일은 바로 줄지 않는다 — 공간은 재사용된다.
- **실행 계획은 자동 테스트로 두지 않았다.** 행이 적으면 옵티마이저가 전체 훑기를 고를 수 있다. `idx_verification_purge (status, expires_at)` 와 `idx_outbox_unpublished` 를 타는지는 손 확인 때 대표 데이터(1만 행)로 `EXPLAIN` 한다 (`http/data-retention.http` 0-2 · database.md §9). 위 탐색의 `range` 계획은 아웃박스 쪽 일회성 관찰일 뿐이다.
- 실행 이력이 `STARTED` 로 남는 JVM 사망은 그 날짜만 막는다(409). 푸는 SQL 은 OPERATION §10-4.
- **법정 보존 기간은 확인되지 않은 가정이다.** 전자금융거래법 시행령 제12조 기준 5년(1만 원 이하 거래 등은 1년)이라고 2차 자료에서 확인했고 **조문 원문은 확인하지 않았다.** 이 프로젝트의 가정이며, 실제로 쓰려면 원문과 금융위 고시를 확인해야 한다. 7일 · 30일은 법이 아니라 이 프로젝트의 운영 판단이다. 이 단계는 금융 기록을 지우지 않으므로 법정 기간이 코드에 들어가 있지는 않다.

## 검토한 다른 선택지
| 선택지 | 버린 이유 |
|---|---|
| `READER` / `WRITER` 청크 (읽었다가 지우기) | 표마다 읽기가 낭비고 트랜잭션도 길어진다. 지울 행의 내용은 필요 없다 |
| `@Scheduled` + SQL 만 | 날짜별 이력 · 삭제 건수 · 재시작 · 동시 실행 방지를 직접 만들어야 한다 |
| `expires_at` 만으로 삭제 | 인증한 사용자 전부가 422 `ACCOUNT_NOT_VERIFIED` |
| `EXPIRED` 만 삭제 | 코드를 입력하지 않은 `SENT` 가 영원히 남는다 |
| 묶음마다 `ORDER BY` | 정렬 비용만 든다. 복제는 ROW 형식이라 필요 없다 |
| 일 단위 파티션 DROP | 미발행 행이 섞여 있고 스키마 변경이 크다 |
| `Job` 에 `@Primary` | 앞으로 `Job` 을 주입하는 코드가 조용히 대사 잡을 받는다 → `@Qualifier` 로 이름을 적는다 |
| 삭제 전 건수 미리보기 API | 범위 밖. OPERATION 의 `SELECT COUNT(*)` 로 충분하다 |
