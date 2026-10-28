# P4-2 잔고 차감 락 전략 비교 — 설계

> 승인: 2026-10-27 사용자와 섹션별로 합의 (핫스팟 + 분산 두 시나리오, 접수 API 그대로 측정, Redis 는 Redisson RLock, A안 = 차감 전략 인터페이스 + 기동 시 설정으로 하나만 선택, 전략별 규칙, k6 측정 조건).
> 이전 단계: P4-1 (조건부 UPDATE 로 접수 때 차감, 실패 확정 때 환불 — ADR-0008).

## 1. 목적과 범위

P4-1 의 차감(조건부 UPDATE 한 문장)을 **비관적 락 · 낙관적 락 · Redis 분산락**으로 바꿔 끼울 수 있게 하고, 같은 접수 API 를 k6 로 두드려 처리량 · 지연 · 실패율을 비교한다.
목표는 "어느 방식이 빠른가"보다 **"각 방식의 비용이 어디서 생기고, 언제 무엇을 쓰는가"를 숫자로 설명할 수 있게** 되는 것이다.

정확성이 먼저다: 네 전략 모두 동시 출금 테스트를 통과해야 측정을 시작한다. 잔고는 어느 전략에서도 음수가 되지 않고, 잔고 = 원장 합이다.

**범위 밖**: 환불 · 충전의 락 전략(지금의 조건부 그대로), 다중 인스턴스 실측(Redis 락의 진짜 이점은 서버가 여러 대일 때지만 이번엔 한 대), 운영 기본 전략 변경(측정 결과로 ADR 에 판단만 남긴다), k6 의 CI 실행.

## 2. 결정 요약

| 결정 | 내용 | 이유 |
|---|---|---|
| 전략 선택 | `wallet.debit-strategy = conditional \| pessimistic \| optimistic \| redis` (기본 `conditional`). 기동 때 하나만 | 운영처럼 한 번에 한 전략. 요청마다 고르면 운영 API 에 실험 손잡이가 생기고 네 빈이 자원을 나눠 쓴다 |
| 이음새 | 차감 전략 인터페이스 하나에 **바깥(트랜잭션을 감쌈) · 안(트랜잭션 안에서 차감)** 두 지점 | 네 전략은 락이 걸리는 위치가 다르다. 낙관적 재시도와 Redis 락은 트랜잭션 **바깥**이어야 한다 |
| 실패 응답 | 낙관적 재시도 소진 409 `CONCURRENT_UPDATE`, 락을 못 잡음 503 `WALLET_BUSY`. 둘 다 `Retry-After: 1` | "잠시 뒤 다시 하면 될 수 있다"를 클라이언트에 알린다. 잔고 부족(422)과 구분 |
| 락 대기 한도 | MySQL `innodb_lock_wait_timeout` 을 커넥션마다 **5초** (기본 50초) | 50초 동안 커넥션 · 톰캣 스레드를 붙잡으면 장애가 번진다. 네 전략에 같은 조건 |
| 커넥션 풀 | Hikari `maximum-pool-size` **20** 명시 (`minimum-idle` 은 기본 = 최대) | P4-1 의 기본 10 은 "풀이 작다"를 잴 위험. 네 전략에 같은 조건 |
| Redis 장애 | `redis` 전략에서 Redis 에 닿지 않으면 503 (fail-closed) | 돈이 걸린 판단을 락 없이 진행하지 않는다 |
| 환불 · 충전 | 전략과 무관하게 지금의 원자적 UPDATE (`balance = balance + :amt`) | 더하기라 음수 위험이 없다. 비교 대상은 차감이다 |
| 안전망 | `CHECK (balance >= 0)` 유지 | 어느 전략이 틀려도 DB 가 음수를 막는다 |
| 스키마 | **바꾸지 않는다** — `wallet.version` 은 V3 부터 있고 모든 UPDATE 가 이미 올린다 | 낙관적 락이 바로 쓸 수 있다 |

## 3. 전략

### 3-1. 이음새

```kotlin
/** 잔고 차감 전략 (P4-2). 접수 트랜잭션의 바깥과 안, 두 지점에 끼운다. */
interface WalletDebit {
    /** 접수 트랜잭션 바깥. 기본은 그대로 실행. 낙관적 = 충돌하면 처음부터 다시, Redis = 사용자 락 안에서 실행하고 커밋 뒤 해제. */
    fun <T> around(userId: String, transaction: () -> T): T = transaction()

    /** 접수 트랜잭션 안. 잔고가 모자라거나 지갑이 없으면 false (아무것도 쓰지 않는다). 성공하면 원장 WITHDRAW 까지 쓴다. */
    fun withdraw(userId: String, transferId: UUID, amount: Long, at: Instant): Boolean
}
```

`JpaTransferStore.saveWithEvent` 는 `debit.around(userId) { tx.execute { 출금 INSERT → debit.withdraw(…) → 아웃박스 } }` 모양이 된다.
`saveWithEvent` 를 바깥 트랜잭션 안에서 부르면 안 된다는 P4-1 의 문서상 전제를 **`check` 로 강제**한다 — 낙관적 재시도와 Redis 락이 트랜잭션 바깥이라는 것도 이 검사가 지킨다.
`WalletLedger` 에는 충전 · 환불 · 조회와 원장 쓰기가 남고, 차감 SQL 은 전략 구현으로 옮긴다.

### 3-2. 전략별 규칙

잔고 1만 원, 1만 원 출금 A · B 가 동시에 오는 상황 기준.

| 전략 | 바깥 (`around`) | 안 (`withdraw`) | 진 쪽은 |
|---|---|---|---|
| `conditional` (기준선) | 그대로 | `UPDATE wallet SET balance = balance - :amt, version = version + 1 … WHERE user_id = :u AND balance >= :amt` → 0건이면 false | A 커밋까지 행 락 대기 → 바뀐 잔고 0 으로 조건 확인 → 422 |
| `pessimistic` | 그대로 | `SELECT balance FROM wallet WHERE user_id = :u FOR UPDATE` → 없거나 모자라면 false → `UPDATE … SET balance = :newBalance, version = version + 1` | `FOR UPDATE` 에서 대기 → 잔고 0 을 읽음 → 422. 5초 넘게 기다리면 503 |
| `optimistic` | 버전 충돌이면 트랜잭션 전체를 **처음부터** 다시. 최대 5회, 사이에 10~50ms 무작위 대기. 5회 모두 충돌 → 409 | `SELECT balance, version` (락 없음) → 모자라면 false → `UPDATE … SET balance = :newBalance, version = :v + 1 WHERE user_id = :u AND version = :v` → 0건이면 충돌 | 첫 시도는 충돌 → 다시 읽으면 잔고 0 → 422 |
| `redis` | Redisson `getLock("lock:wallet:{userId}").tryLock(3초)` — 임대 시간은 watchdog 자동 연장. 못 잡으면 503. 트랜잭션 **커밋 뒤** `unlock` | **기준선과 같은 조건부 UPDATE** | Redis 에서 A 가 끝날 때까지 대기 → 조건 확인 → 422 |

- **Redis 전략의 안쪽이 "읽고 → 비교 → 값 쓰기"가 아닌 이유**: 환불 · 충전은 Redis 락을 잡지 않는다. 안쪽에서 잔고를 읽어 계산한 값을 쓰면, 그 사이 들어온 환불이 덮어써져 사라진다(lost update). 그래서 Redis 락은 **DB 앞에서 같은 사용자 요청을 줄 세우는 문**으로만 쓰고, 정확성은 DB(조건부 UPDATE + CHECK)가 지킨다. 락이 만료된 뒤 작업이 끝나는 경우에도 안전하다.
- 비관적 락의 "읽고 → 비교 → 값 쓰기"는 안전하다: `FOR UPDATE` 가 환불의 UPDATE 도 기다리게 한다. 낙관적 락도 안전하다: 환불 · 충전이 `version` 을 올리므로 그 사이 바뀌면 충돌로 잡힌다.
- 낙관적 재시도는 출금 INSERT 까지 롤백된 뒤 같은 출금(같은 id · 거래번호)으로 다시 시작한다. 재시도 중 같은 키 UNIQUE 충돌이 나면 기존 멱등 응답 경로를 그대로 탄다 (재시도 대상은 버전 충돌뿐).
- 원장 `balance_after` 는 모든 전략에서 이 트랜잭션이 X 락을 쥔 행의 값이다 (P4-1 과 같다).

## 4. 오류와 응답

| 상황 | 도메인 (`StoreConflict` → `TransferException`) | HTTP |
|---|---|---|
| 잔고 부족 · 지갑 없음 | `InsufficientBalance` (기존) | 422 `INSUFFICIENT_BALANCE` |
| 낙관적 재시도 5회 소진 | `ConcurrentUpdate` (신규) | 409 `CONCURRENT_UPDATE` + `Retry-After: 1` |
| MySQL 락 대기 초과(1205) · 데드락(1213) — Spring 의 `PessimisticLockingFailureException` 계열 | `WalletBusy` (신규) | 503 `WALLET_BUSY` + `Retry-After: 1` |
| Redis 락 3초 안에 못 잡음 | `WalletBusy` | 503 `WALLET_BUSY` + `Retry-After: 1` |
| Redis 에 닿지 않음 | `WalletBusy` (로그는 ERROR 로 따로) | 503 `WALLET_BUSY` + `Retry-After: 1` |

- 1205 · 1213 매핑은 **전략과 무관**하게 `JpaTransferStore` 한 곳에서 한다. 덕분에 P4-1 의 알려진 한계("같은 키 3건 이상 동시 + 잔고 부족 → 1213 → 500")도 503 + `Retry-After` 로 바뀐다 (다시 보내면 멱등 응답을 받는다).
- 클라이언트에게 Redis 장애와 락 경합을 구분해 알리지 않는다 — 할 일(잠시 뒤 다시)이 같다. 운영자는 로그 수준으로 구분한다.
- 도메인 모듈은 여전히 락을 모른다: "저장소가 지금 잔고를 잡지 못했다 / 동시에 바뀌어 포기했다"만 안다.

## 5. 설정

```yaml
spring.datasource.hikari:
  maximum-pool-size: 20                                       # 명시. minimum-idle 기본값 = 최대 → 측정 중 커넥션을 새로 만들지 않는다
  connection-init-sql: SET SESSION innodb_lock_wait_timeout = 5
wallet:
  debit-strategy: conditional
  optimistic: { max-attempts: 5, backoff-min: 10ms, backoff-max: 50ms }
  lock: { wait-time: 3s, redis-address: "redis://${REDIS_HOST:localhost}:${REDIS_PORT:6379}" }
```

- 잘못된 전략 이름은 기동 실패 (enum 바인딩).
- `RedissonClient` 빈은 `redis` 전략일 때만 만든다. 다른 전략으로 띄우면 Redis 가 없어도 기동된다. Redis 명령 응답 대기는 1초, 재시도 1번으로 줄여 장애 때 수 초 안에 503 이 나게 한다.
- 의존성: `org.redisson:redisson` (Spring Boot 스타터는 쓰지 않는다 — Boot 4.1 지원 여부와 무관하게 빈 하나만 필요). 정확한 버전은 계획 단계에서 Maven Central 로 확인해 고정한다.

## 6. 테스트

| 층 | 증명 |
|---|---|
| **전략별 정확성 (MySQL · Redis)** | 네 전략 각각 `JpaTransferStore` 를 그 전략으로 만들어, 잔고 10만 원에 1만 원 출금 **100건 동시** → `성공 + 잔고 부족 + 409 + 503 = 100`, **1 ≤ 성공 ≤ 10**, 잔고 = 10만 − 1만 × 성공 수 ≥ 0, 원장 합 = 잔고, 출금 행 수 = 원장 WITHDRAW 수 = 성공 수. `conditional` · `pessimistic` 은 정확히 10건 성공 |
| 낙관적 | 첫 시도 사이에 다른 트랜잭션이 버전을 올리면 재시도로 성공 (훅으로 결정적으로) / 5번 모두 충돌 → `ConcurrentUpdate`, 아무것도 남지 않음 / 재시도 대기는 주입한 sleeper 로 (테스트가 실제로 자지 않게) |
| 비관적 | 다른 트랜잭션이 행을 잡고 놓지 않으면 5초 뒤 `WalletBusy`, 출금 · 원장 없음 |
| Redis | 다른 쪽이 락을 쥐고 있으면 3초 뒤 `WalletBusy` / **`unlock` 시점에 트랜잭션이 이미 끝나 있다** / Redis 주소가 닿지 않으면 `WalletBusy` |
| 이음새 | `saveWithEvent` 를 바깥 트랜잭션 안에서 부르면 거부 / 설정값별로 맞는 전략 빈이 선택되고 `redis` 가 아니면 `RedissonClient` 가 없다 (`ApplicationContextRunner`) |
| API | 409 · 503 응답에 `code` 와 `Retry-After` |
| 기존 | `WalletConcurrencyTest` (API 수준 100건 · 같은 키 20건)는 기본 전략으로 그대로 통과 |

- 전략별 테스트는 **캐시된 컨텍스트 하나** 안에서 전략 객체를 직접 만들어 돌린다 (전략마다 컨텍스트를 띄우면 MySQL · Kafka 컨테이너가 전략 수만큼 뜬다). Redis 는 Testcontainers 로 `redis:7.4-alpine` 을 함께 띄운다.
- 동시성 테스트는 확률적이다 — 실제로 경합이 났는지는 "성공 ≤ 10"이 깨지는 변형(예: 비관적 전략에서 `FOR UPDATE` 를 뺌)으로 한 번 확인하고, 그 확인 결과를 PR 에 적는다.

## 7. 부하 측정

### 7-1. 시나리오 (전략마다 같은 조건)

| 시나리오 | 사용자 | 부하 | 보는 것 |
|---|---|---|---|
| ① 핫스팟 `load/hotspot.js` | 1명 `load-hot` | 50 VU, 60초 동안 쉬지 않고 1원 출금 | 한 행을 50명이 다툼: 비관적 = 대기열, 낙관적 = 재시도 폭주, Redis = 왕복 + 대기 |
| ② 분산 `load/spread.js` | 500명 `load-u001`~`load-u500` | 50 VU, 60초, 요청마다 사용자 무작위 | 경합이 거의 없음 → 방식 자체의 고정 비용 |

- VU = 요청 → 응답 대기 → 바로 다음 요청을 반복하는 가상 사용자 (닫힌 모델). 동시에 처리 중인 요청이 최대 50건.
- 앞 10초는 워밍업 시나리오로 따로 돌리고, **측정 시나리오 태그로만 집계**한다 (JIT · 커넥션 풀 데우기).
- 요청마다 새 멱등키. 금액 1원, 잔고 10억 원 → 측정 중 422 가 나지 않는다.
- 조합(전략 × 시나리오)마다 **3회**, 중앙값을 쓴다. 4 × 2 × 3 = 24회.

### 7-2. 조건 (모두 고정하고 결과 문서에 적는다)

- **인프라**: `infra/docker-compose.yml` 의 MySQL · Kafka · Redis. 모의 은행도 띄워 둔다 (조건 고정, 은행으로 가는 요청은 없다).
- **앱**: `bootJar` 로 만든 jar 를 같은 JVM 옵션(`-Xms1g -Xmx1g`)으로 띄우고, 전략이 바뀔 때마다 다시 띄운다. 인자:
  - `--wallet.debit-strategy=<전략>`
  - `--transfer.outbox.relay.enabled=false --transfer.reconcile.enabled=false` — 같은 Hikari 풀 · DB 를 쓰는 백그라운드 작업을 끈다
  - `--spring.kafka.listener.auto-startup=false` — 컨슈머가 출금마다 상태를 바꾸고 은행을 부르지 않게
  - `--server.tomcat.threads.max=200` (기본값을 명시) · Hikari 20 (§5)
  - `--logging.level.io.github.hyeokjinon.banktransfer=WARN` — 요청마다 찍히는 INFO 로그를 줄인다
  - 같은 인자를 담은 IntelliJ 실행 구성 `5. API (부하 측정)` 을 편의용으로 둔다.
- **k6**: compose 의 `load` 프로필에 `grafana/k6` (정확한 태그는 계획에서 고정). 스크립트는 `load/`, 대상은 `host.docker.internal:8080`, 요약 JSON 은 `docs/perf/raw/` 로.
- **데이터**: 매 회차 **전에** `load/cleanup.sql` → `load/seed.sql` 로 같은 출발점을 만든다 (회차가 쌓일수록 테이블이 커져 뒤 회차가 불리해지지 않게). 시드 = 부하용 사용자 501명의 지갑(10억 원) · 충전 원장 · VERIFIED 인증 행. 계좌는 모의 은행에 없는 번호.
- **정확성 확인**: 매 회차 **뒤에** `load/verify.sql` — 부하용 사용자 중 음수 잔고 0명, 잔고 ≠ 원장 합 0명. 하나라도 어긋나면 그 회차 숫자는 버리고 원인을 찾는다.
- **실행 보조**: `load/run.sh <전략> <시나리오> <회차>` 가 cleanup → seed → k6 → verify 를 한 번에 하고 결과 파일 이름을 맞춘다. 앱 재기동은 전략이 바뀔 때만.
- **PC 환경 기록**: CPU, 메모리, Docker Desktop 자원 설정, JDK · MySQL 버전. 숫자는 **이 PC 기준의 상대 비교**다 (앱 · MySQL · k6 가 한 PC 의 CPU 를 나눠 쓴다).
- 측정 24회는 Claude 가 이 PC 에서 돌린다. 측정 중에는 PC 에서 다른 무거운 작업을 하지 않는다.

### 7-3. 지표와 결과

- 초당 처리량, 지연 p50 · p95 · p99, 응답 코드별 건수 (202 / 409 / 503 / 그 외 5xx).
- `docs/perf/p4-2-lock-comparison.md`: 조건 · PC 환경, 시나리오별 표(전략 4줄 × 지표), 해석, "언제 무엇을 쓰나" 결론, 아래 예상과의 비교. 원본 요약 JSON 은 `docs/perf/raw/`.
- 측정 뒤 `load/cleanup.sql` 은 반드시 돌린다 — 발행기를 다시 켜면 쌓인 수만 건의 아웃박스가 은행으로 나간다. 문서와 스크립트 머리말에 경고로 남긴다.
- k6 는 CI 에서 돌리지 않는다. CI 는 §6 의 정확성 테스트만.

## 8. 예상 (측정 전에 적어 두고 결과와 비교한다)

- **핫스팟**: `conditional` ≥ `pessimistic` > `redis` ≫ `optimistic`.
  - 비관적은 같은 행 락을 기다리는 건 조건부와 같고, 왕복이 하나(`SELECT … FOR UPDATE`) 더 있다.
  - Redis 는 DB 앞에서 줄을 세워 커넥션을 덜 잡지만, 요청마다 Redis 왕복 2번(락 · 해제)과 대기 알림 지연이 더해진다.
  - 낙관적은 50명이 한 버전을 다퉈 대부분 재시도하고, 상당수가 409 로 끝난다.
- **분산**: 네 전략이 비슷하다. Redis 만 왕복 비용만큼 지연이 조금 높다.
- 예상과 다르면 그 이유를 찾아 결과 문서에 적는다 — 그게 이 단계에서 얻을 것이다.

## 9. 위험과 한계

- 한 PC 에서 앱 · MySQL · Redis · k6 를 함께 돌린다. 절대값은 의미가 없고 같은 조건의 상대 비교만 한다. 회차 간 편차가 크면(중앙값 대비 ±20% 이상) 조건을 다시 확인한다.
- 서버가 한 대라 Redis 락의 진짜 이점(여러 인스턴스 사이 직렬화)은 재지 않는다. 결과 문서에 명시한다.
- Redis 락은 watchdog 이 임대를 연장하지만, 앱이 GC 등으로 멈춰 연장이 끊기면 락이 풀릴 수 있다. 이번 설계는 안쪽을 조건부 UPDATE 로 두어 이 경우에도 잔고가 틀리지 않는다.
- 낙관적 재시도는 톰캣 스레드에서 잔다 (최대 약 200ms). 핫스팟에서 스레드를 오래 붙잡는 것도 측정 결과의 일부다.
- `innodb_lock_wait_timeout` 5초는 모든 트랜잭션에 적용된다 (발행기 · 정리 작업 포함). 지금 그쪽 트랜잭션은 짧아 영향이 없다고 보지만, HANDOVER 에 남긴다.

## 10. 결정 기록 · 문서

- **ADR-0009** 잔고 차감 락 전략: 네 전략을 설정으로 바꿔 끼울 수 있게 하고, 측정 결과를 근거로 기본 전략과 "언제 무엇을"을 판단한다. Redis 락은 DB 앞의 줄 세우기로만 쓰고 정확성은 DB 가 지킨다.
- 갱신: `docs/perf/p4-2-lock-comparison.md` (신규), `docs/database.md` (동시성 표: 네 전략 · 락 대기 5초 · `version` 사용), OPERATION (부하 측정 절차 · 409 · 503 대응 · Redis 전략의 fail-closed), SETUP (k6 실행), ARCHITECTURE, README (P4-2), HANDOVER (P4-1 의 1213 한계 해소 · 남은 한계), CHANGELOG, `http/*.http` · Postman 오류 코드.
