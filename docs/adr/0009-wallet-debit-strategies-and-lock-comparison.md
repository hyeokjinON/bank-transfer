# ADR-0009 잔고 차감은 전략 인터페이스로 바꿔 끼운다 (조건부 · 비관적 · 낙관적 · Redis) — Redis 락은 DB 앞의 줄 세우기로만 쓰고 정확성은 DB 가 지킨다

- 상태: 제안 (2026-10-27)
- 단계: P4-2 · 설계: [P4-2 설계](../superpowers/specs/2026-10-27-p4-2-lock-comparison-design.md) · [ADR-0008](0008-wallet-debit-on-intake-refund-on-failure.md) (차감 위치 · 원장은 그대로) · [database.md](../database.md) §6 · [측정 결과](../perf/p4-2-lock-comparison.md)

## 맥락
- P4-1 의 차감은 조건부 UPDATE 한 문장이다. 같은 차감을 비관적 · 낙관적 · Redis 분산락으로 바꾸면 비용이 어디서 생기는지(행 락 대기 · 재시도 · 네트워크 왕복)를 같은 API · 같은 조건에서 숫자로 설명하고 싶다.
- 네 방식은 락이 걸리는 위치가 다르다. 안(트랜잭션 안): 조건부 · 비관적. 바깥(트랜잭션을 감쌈): 낙관적 재시도 · Redis 락. 그래서 "트랜잭션 안의 SQL 만 바꾸는" 이음새로는 부족하다.
- P4-1 의 한계: Hikari 기본 풀 10 (100건 동시 테스트가 10-way 경합), 락 대기 기본 50초, 같은 키 경합 데드락(1213) → 500.

## 결정
1. **이음새 `WalletDebit { around · withdraw }`**. `JpaTransferStore.saveWithEvent` 는 `debit.around(userId) { tx.execute { INSERT transfer → debit.withdraw → INSERT outbox_event } }` 모양이다. `around` 는 트랜잭션 바깥(기본은 그대로 실행), `withdraw` 는 트랜잭션 안(잔고가 모자라거나 지갑이 없으면 `false`, 성공하면 원장 `WITHDRAW` 까지). `saveWithEvent` 를 바깥 트랜잭션 안에서 부르면 `check` 로 거부한다 — 낙관적 재시도와 Redis 락이 트랜잭션 바깥이라는 전제를 문서가 아니라 코드가 지킨다.
2. **기동 설정 `wallet.debit-strategy` 로 하나만** (`conditional` 기본 · `pessimistic` · `optimistic` · `redis`). 모르는 이름이면 기동 실패. `RedissonClient` 빈은 `redis` 일 때만 만들고 연결은 lazy — 다른 전략은 Redis 없이 뜨고, `redis` 전략도 기동 때 Redis 에 붙지 않아 장애 중 기동해도 차감 요청만 503 이 된다. 지금 전략은 `GET /actuator/info`.
3. **전략별 규칙** (잔고 1만 원, 1만 원 출금 A · B 동시 기준. 설계 §3-2):

   | 전략 | 바깥 (`around`) | 안 (`withdraw`) | 진 쪽은 |
   |---|---|---|---|
   | `conditional` (기준선) | 그대로 | `UPDATE wallet SET balance = balance - :amt, version = version + 1 … WHERE user_id = :u AND balance >= :amt` → 0건이면 false | A 커밋까지 행 락 대기 → 바뀐 잔고 0 으로 조건 확인 → 422 |
   | `pessimistic` | 그대로 | `SELECT balance FROM wallet WHERE user_id = :u FOR UPDATE` → 없거나 모자라면 false → `UPDATE … SET balance = :newBalance, version = version + 1` | `FOR UPDATE` 에서 대기 → 잔고 0 을 읽음 → 422. 5초 넘게 기다리면 503 |
   | `optimistic` | 버전 충돌이면 트랜잭션 전체를 **처음부터** 다시. 최대 5회, 사이에 10~50ms 무작위 대기. 5회 모두 충돌 → 409 | `SELECT balance, version` (락 없음) → 모자라면 false → `UPDATE … SET balance = :newBalance, version = :v + 1 WHERE user_id = :u AND version = :v` → 0건이면 충돌 | 첫 시도는 충돌 → 다시 읽으면 잔고 0 → 422 |
   | `redis` | Redisson `getLock("lock:wallet:{userId}").tryLock(3초)` — 임대 시간은 watchdog 자동 연장. 못 잡으면 503. 트랜잭션 **커밋 뒤** `unlock` | **기준선과 같은 조건부 UPDATE** | Redis 에서 A 가 끝날 때까지 대기 → 조건 확인 → 422 |

4. **Redis 의 안쪽은 조건부 UPDATE** — 환불 · 충전은 Redis 락을 잡지 않으므로, 안쪽에서 잔고를 읽어 계산한 값을 쓰면 그 사이 들어온 환불이 덮어써져 사라진다(lost update). 그래서 Redis 락은 같은 사용자 요청을 DB 앞에서 줄 세우는 문으로만 쓰고 정확성은 DB(조건부 UPDATE + `CHECK`)가 지킨다. 락이 만료된 뒤 끝나는 작업도 안전하다. 비관적은 `FOR UPDATE` 가 환불의 UPDATE 도 기다리게 하고, 낙관적은 환불 · 충전이 `version` 을 올려 충돌로 잡히므로 "읽고-계산하고-쓰기"여도 안전하다.
5. **오류 응답**: 낙관적 재시도 소진 409 `CONCURRENT_UPDATE`, 락을 못 잡음 503 `WALLET_BUSY`, 둘 다 `Retry-After: 1` (잔고 부족 422 는 다시 보내도 같아 구분). MySQL 락 대기 초과(1205)와 데드락(1213)은 전략과 무관하게 `JpaTransferStore` 한 곳에서 `WalletBusy` 로 바꾼다 — P4-1 의 "같은 키 3건 이상 동시 + 잔고 부족 → 1213 → 500" 한계가 503 + `Retry-After` 가 된다(다시 보내면 멱등 응답). Redis 에 닿지 않으면 fail-closed: 락 없이 진행하지 않고 503 (로그는 ERROR 로 따로 — 클라이언트에게 락 경합과 장애를 구분해 알리지 않는다, 할 일이 같다).
6. **커넥션 풀 20 · `innodb_lock_wait_timeout` 5초를 모든 전략에 같게** (Hikari `connection-init-sql`). 풀 기본 10 은 "풀이 작다"를 재게 되고, 락 대기 기본 50초는 커넥션 · 톰캣 스레드를 오래 붙잡아 장애가 번진다.
7. **환불 · 충전은 전략과 무관** (원자적 UPDATE `balance = balance + :amt`). `CHECK (balance >= 0)` 유지(어느 전략이 틀려도 DB 가 음수를 막는다). **스키마 변경 없음** — `wallet.version` 은 V3 부터 있고 모든 UPDATE 가 이미 올린다.
8. **측정**: k6 `grafana/k6:2.3.0`, 핫스팟(1명 `load-hot`) · 분산(500명) 두 시나리오, 50 VU, 워밍업 10초 + 측정 60초, 조합마다 3회 중앙값(4 × 2 × 3 = 24회), 매 회차 정리 → 시드 → 정확성 확인. 정확성 확인(음수 잔고 0 · 잔고 = 원장 합 · k6 의 202 수 = 출금 행 수)을 통과하지 못한 회차는 숫자를 버린다. 절차는 [OPERATION §8](../../OPERATION.md#8-부하-측정-p4-2-k6).

## 결과
- 증명 테스트:
  - **전략별 정확성** (`WalletDebitConcurrencyTest`, 네 전략 × 100건 동시): 잔고 10만 원에 1만 원 출금 100건 → `성공 + 422 + 409 + 503 = 100`, 1 ≤ 성공 ≤ 10, 음수 없음, 잔고 = 원장 합, 출금 행 수 = 원장 `WITHDRAW` 수 = 성공 수. 조건부 · 비관적은 정확히 10건. 이 테스트는 풀 20 · 락 대기 5초 아래에서 돈다 (ADR-0008 의 10-way 한계 해소).
  - **비관적** (`PessimisticWalletDebitTest`): 다른 트랜잭션이 행을 잡고 놓지 않으면 락 대기 한도 뒤 `WalletBusy`, 출금 · 원장 · 아웃박스 없음 / 모든 커넥션의 락 대기 5초 · 풀 20.
  - **낙관적** (`OptimisticWalletDebitTest`): 한 번 충돌 → 처음부터 다시 해 성공 / 5번 충돌 → `ConcurrentUpdate`, 아무것도 남지 않음 / 잔고 부족은 재시도 없이 `InsufficientBalance`. 충돌은 훅으로 결정적으로 만들고 재시도 대기는 주입한 sleeper 로 처리해 테스트가 실제로 자지 않는다.
  - **Redis** (`RedisWalletDebitTest`): 다른 쪽이 락을 쥐고 있으면 대기 뒤 `WalletBusy` / `unlock` 시점에 접수 트랜잭션은 이미 커밋 / Redis 에 닿지 않으면 `WalletBusy`(fail-closed) · 아무것도 남지 않음.
  - **이음새** (`JpaTransferStoreTest`): 바깥 트랜잭션 안에서 부르면 거부 / `around` 가 트랜잭션 전체를 감쌈 / 락을 못 잡으면(1205 · 1213 계열) 전략과 무관하게 `WalletBusy`, 아무것도 남지 않음. 진짜 1205 가 `WalletBusy` 가 되는 것은 `PessimisticWalletDebitTest` 가 증명한다. 1213 은 같은 `catch` 가 매핑하지만 테스트로 일으키지는 않았다.
  - **설정** (`WalletDebitConfigurationTest` · `WalletDebitWiringTest`): 설정값별 전략 빈, `redis` 가 아니면 `RedissonClient` 없음, 모르는 이름이면 기동 실패, `redis` 도 기동 때 Redis 에 붙지 않음, `/actuator/info` 에 전략.
  - **API** (`ApiExceptionHandlerTest`): 409 `CONCURRENT_UPDATE` · 503 `WALLET_BUSY` + `Retry-After: 1`.
- **테스트가 실제로 잡아내는지 확인** (일회성 변이 검사, 커밋하지 않음): 비관적 전략에서 `FOR UPDATE` 를 빼자 `WalletDebitConcurrencyTest [pessimistic]` 이 `pessimistic: {OK=100}` 로 실패했다 — 100건이 모두 성공(상한 10건 단언 위반), 잔고를 서로 덮어쓰는 lost update. 되돌리자 통과했다.
- 낙관적 전략은 100건 동시 테스트에서 충돌을 재시도하다 잔고가 0 이 되면 422 로 끝나므로 409 가 0건일 수 있다. 재시도 소진(409)은 훅 기반 테스트가 증명한다.
- 측정 결과와 기본 전략 판단은 측정 뒤 이 절에 적는다 (docs/perf/p4-2-lock-comparison.md).
- 한계:
  - 한 PC 에서 앱 · MySQL · Redis · k6 가 CPU 를 나눠 쓴다 → 절대값은 의미가 없고 같은 조건의 상대 비교만 한다.
  - 서버가 한 대라 Redis 락의 진짜 이점(여러 인스턴스 사이 직렬화)은 재지 않는다.
  - Redis 락은 watchdog 연장이 끊기면(긴 GC 멈춤 등) 풀릴 수 있다. 안쪽이 조건부 UPDATE 라 잔고는 틀리지 않는다.
  - `tryLock` 응답이 시간 초과(명령 응답 대기 1초, 재시도 1번)로 끝났는데 Redis 는 실제로 락을 준 경우, 그 락은 Redisson 내부 임대(30초)가 만료될 때까지 남아 그 사용자의 출금이 최대 약 30초 503 일 수 있다. 실패 쪽으로 닫히는 동작(fail-closed)이라 잔고는 맞고, 다른 사용자에게는 영향이 없다.
  - Redis 가 대기 중에 죽으면 Redisson 의 `tryLock` 이 예외 대신 `false` 를 돌려줄 수 있다. 이때 우리 로그는 INFO `not acquired` 이고 ERROR 는 Redisson 이 따로 남긴다 (응답은 같은 503).
  - 낙관적 재시도는 톰캣 스레드에서 잔다 (최대 약 200ms).
  - 5초 락 대기는 발행기 · 정리 작업 트랜잭션에도 적용된다 (지금은 짧아 영향이 없다고 본다).

## 검토한 다른 선택지
| 선택지 | 버린 이유 |
|---|---|
| 요청마다 전략 선택 (헤더 · 파라미터) | 운영 API 에 실험 손잡이가 생기고, 네 빈이 자원(커넥션 · 스레드)을 나눠 써 측정이 흐려진다 |
| Redis 안쪽을 "읽고-비교-쓰기" | 락을 잡지 않는 환불 · 충전과 lost update |
| Redis 임대 시간 고정 | 작업이 임대보다 길면 먼저 만료된다 → watchdog 을 쓴다 |
| Redisson Spring Boot 스타터 | 빈 하나만 필요한데 자동 설정이 늘어난다 |
| Spring Data Redis + `SET NX` 직접 구현 | 재진입 · 원자적 해제 · 대기 알림을 직접 만들어야 한다 |
| Redis 장애 시 락 없이 진행 (fail-open) | 돈이 걸린 판단을 락 없이 하지 않는다 |
| `innodb_lock_wait_timeout` 기본 50초 | 커넥션 · 톰캣 스레드를 오래 붙잡아 장애가 번진다 |
| 낙관적 무한 재시도 | 핫스팟에서 스레드가 묶인다 |
| 낙관적 락을 JPA `@Version` 엔티티로 | 지갑은 JDBC 한 곳이고 SQL 이 드러나는 편이 비교에 낫다 |
| Hikari 기본 10 | "풀이 작다"를 재게 된다 |
