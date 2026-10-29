# OPERATION — 실행 · 상태 확인 · 장애 대응

> 로컬 개발과 CI 기준 운영 안내다. 실제 배포(K8s)는 P8 에서 이 문서에 추가한다.

## 1. 구성 요소와 포트
| 구성 요소 | 실행 | 포트 | 상태 확인 |
|---|---|---|---|
| API (`apps/api`) | `./gradlew :apps:api:bootRun` 또는 실행 구성 `2. API (8080)` | 8080 | `GET /actuator/health` · MySQL · Kafka 필요 (기동 시 Flyway 마이그레이션) |
| 모의 은행 (`apps/mock-bank`) | `./gradlew :apps:mock-bank:run --args="9090"` (작업 디렉터리 = 저장소 루트, 거래 기록 · 대사 파일은 `./bank-files`) 또는 `1. Mock Bank (9090)` | 9090 (TCP) | 콘솔 `Mock bank FEP started on port 9090` |
| MySQL 8.4 | `infra/docker-compose.yml` | 3306 | `docker compose ... ps` → healthy |
| Kafka 4.1 (KRaft) | 〃 | 9092 (PC) / 29092 (컨테이너) | 〃 |
| Redis 7.4 | 〃 | 6379 | 〃 |
| kafka-ui (선택) | `--profile tools up -d` | 8089 | http://localhost:8089 |

모의 은행 · API 는 `./bank-files` 를 같이 본다 (Gradle `run` · `bootRun` 이 저장소 루트에서 돈다). `git status` 에 보이지 않는다(`.gitignore`).

실행 구성 `0. Mock Bank + API` 와 `1. Mock Bank (9090)` 를 같이 띄우지 않는다: Windows 에서 모의 은행의 `SO_REUSEADDR` 가 같은 포트에 두 번 bind 하는 것을 허용해, 모의 은행이 둘 뜨고 어느 쪽에 붙을지 알 수 없다 (입금 내역이 갈린다).

API 는 기동 시 모의 은행에 접속한다. 은행이 없어도 API 는 뜨고, 게이트웨이가 1초→30초 간격으로 재접속을 시도한다(그동안 은행 호출은 503).

## 2. 설정 (`apps/api/src/main/resources/application.yml`)
| 키 | 기본값 | 의미 |
|---|---|---|
| `spring.datasource.url` | `jdbc:mysql://localhost:3306/bank_transfer?...` | `MYSQL_HOST` · `MYSQL_PORT` · `MYSQL_DATABASE` 환경변수로 덮어쓴다 (시각은 KST, [database.md §2](docs/database.md)) |
| `spring.datasource.username` / `password` | `bank` / 로컬 개발용 값 (`infra/.env.example` 와 같다) | `MYSQL_USER` · `MYSQL_PASSWORD` 환경변수 |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | `KAFKA_BOOTSTRAP_SERVERS` 환경변수로 덮어쓴다 (컨테이너에서는 `kafka:29092`) |
| `spring.kafka.consumer.group-id` | `transfer-processor` | 리스너 그룹. 재시도 · DLT 그룹은 여기에 `-retry-0/1/2`, `-dlt` 접미사가 붙는다 |
| `spring.kafka.listener.concurrency` | `3` | 컨테이너마다 컨슈머 3개 (파티션 3). 재시도 · DLT 컨테이너도 같다. 그룹 · 동시성은 yml 에서만 정한다 |
| `transfer.retry.attempts` | `4` | 시도 횟수 (원래 1 + 재시도 3) |
| `transfer.retry.delay-ms` | `5000` | 첫 재시도 간격 (밀리초) |
| `transfer.retry.multiplier` | `6.0` | 간격 배수. 5초 → 30초 → 60초 |
| `transfer.retry.max-delay-ms` | `60000` | 간격 상한 |
| `transfer.outbox.relay.enabled` | `true` | 폴링 퍼블리셔 스케줄 on/off (테스트는 끄고 직접 호출) |
| `transfer.outbox.relay.interval-ms` | `500` | 미발행 아웃박스 폴링 간격 (이전 실행이 끝난 뒤부터 잰다) |
| `transfer.outbox.relay.batch-size` | `100` | 한 번에 발행하는 최대 건수 |
| `transfer.reconcile.enabled` | `true` | 정리 작업 스케줄 on/off (테스트는 끄고 직접 호출) |
| `transfer.reconcile.interval-ms` | `10000` | 정리 작업 주기 (이전 실행이 끝난 뒤부터 잰다) |
| `transfer.reconcile.sending-timeout` | `30s` | 이보다 오래 `SENDING` 이면 `UNKNOWN` 으로 보고 결과조회 |
| `transfer.reconcile.max-inquiries` | `10` | 결과조회도 모르면 이 횟수에서 멈추고 경고 (일마감 대사 대상) |
| `transfer.reconcile.batch-size` | `100` | 한 번에 처리하는 최대 건수 |
| `transfer.reconcile.republish-after` | `5m` | 마지막 변경 후 이만큼 지난 `RECEIVED` 를 재발행한다 (재시도 토픽 체인 약 95초가 끝난 뒤) |
| `transfer.reconcile.max-republish` | `3` | 재발행 한도. 다 쓰고도 은행에 못 보내면 `FAILED` `E001` |
| `spring.task.scheduling.pool.size` | `3` | 스케줄러 스레드. 발행기(0.5초) · 정리 작업(10초) · 일마감 대사(01 ~ 06시)까지 셋이 서로 막지 않게 한다 (P5-1 에서 2 → 3) |
| `spring.datasource.hikari.maximum-pool-size` | `20` | 커넥션 풀 (P4-2 에서 명시, `minimum-idle` = 최대) |
| `spring.datasource.hikari.connection-init-sql` | `SET SESSION innodb_lock_wait_timeout = 5` | 행 락 대기 한도 5초 (MySQL 기본 50초). 넘으면 503 `WALLET_BUSY`. 풀의 모든 커넥션에 적용 — 발행기 · 정리 작업 트랜잭션도 (지금은 짧다) |
| `wallet.debit-strategy` | `conditional` | 잔고 차감 전략: `conditional` · `pessimistic` · `optimistic` · `redis` ([ADR-0009](docs/adr/0009-wallet-debit-strategies-and-lock-comparison.md)). 기동 때 하나, 모르는 이름이면 기동 실패. 지금 값은 `GET /actuator/info` |
| `wallet.optimistic.max-attempts` / `backoff-min` / `backoff-max` | `5` / `10ms` / `50ms` | `optimistic`: 충돌 재시도 횟수 · 사이 대기. 다 쓰면 409 |
| `wallet.lock.wait-time` | `3s` | `redis`: 사용자 락 대기. 넘으면 503 |
| `wallet.lock.redis-address` | `redis://${REDIS_HOST:localhost}:${REDIS_PORT:6379}` | `redis` 전략일 때만 연결 (첫 요청 때) |
| `bank.gateway.host` / `port` | `localhost` / `9090` | 은행 FEP 주소 |
| `bank.gateway.response-timeout` | `3s` | 응답 대기. 넘으면 504 `BANK_TIMEOUT` |
| `bank.gateway.poll-interval` | `30s` | 쓰기 유휴 시 폴링 주기. ×3 동안 수신이 없으면 연결 종료 |
| `bank.maintenance.enabled` / `start` / `end` | `true` / `"23:50"` / `"00:10"` | 은행 점검 시간 (KST, 시작 포함 · 끝 제외, 자정 넘김 가능). yml 에서는 따옴표 필수. 길이는 재발행 포기(`E001`)까지의 약 26분보다 짧게 |
| `account.verification.ttl` | `5m` | 1원인증 유효시간 |
| `account.verification.max-attempts` | `5` | 코드 실패 허용 횟수 |
| `management.endpoints.web.exposure.include` | `health,info` | 노출 actuator |
| `daily-recon.file-dir` | `./bank-files` | 은행 대사 파일 디렉터리 (`recon/RECON_yyyyMMdd.dat`) |
| `daily-recon.chunk-size` | `100` | ③ 출금 대조 · ④ 은행 줄 대조 청크 (② 적재는 500 고정) |
| `daily-recon.schedule.enabled` / `cron` / `zone` | `true` / `0 0 1-6 * * *` / `Asia/Seoul` | 매시 정각 전날 대사. 부하 측정 · 테스트는 `false` |
| `alert.telegram.bot-token` / `chat-id` | (비어 있음) | 환경변수 `ALERT_TELEGRAM_BOT_TOKEN` · `ALERT_TELEGRAM_CHAT_ID`. 둘 다 있을 때만 텔레그램, 아니면 로그(`ALERT` 로거) |
| `alert.telegram.base-url` | `https://api.telegram.org` | Bot API 주소 |
| `spring.batch.job.enabled` / `spring.batch.jdbc.initialize-schema` | `false` / `never` | 기동 때 잡을 돌리지 않는다 / `BATCH_*` 표는 Flyway V4 |

환경변수로 덮어쓰기: `BANK_GATEWAY_PORT=9191 ./gradlew :apps:api:bootRun`.

### Kafka
| 항목 | 값 |
|---|---|
| 토픽 | `transfer.requested` — 파티션 3, 복제 1 (로컬). 앱이 기동할 때 선언한다 (브로커 자동 생성은 꺼져 있다). 재시도 · DLT 토픽 `transfer.requested-retry-0/1/2`, `transfer.requested-dlt` 도 같은 파티션 3 으로 기동할 때 `@RetryableTopic` 이 만든다 |
| 메시지 | key = 이체 id, value = `{"transferId", "occurredAt", "eventId"}` ([database.md §4-3](docs/database.md)) |
| 컨슈머 그룹 | 5개: `transfer-processor`, `transfer-processor-retry-0`, `-retry-1`, `-retry-2`, `transfer-processor-dlt` (각 컨슈머 3개, 수동 ack — 처리를 끝낸 뒤에만 오프셋 커밋) |
| 프로듀서 | `acks=all`, 멱등, `max.block.ms=5000` — 브로커에 닿지 않으면 `send()` 가 5초(기본 60초 아님) 뒤 실패하고 발행기가 다음 주기에 다시 보낸다 |
| 컨슈머 폴링 | `max-poll-records=50` — 한 건이 은행 호출로 최대 3초 막히므로 한 번의 poll 이 `max.poll.interval.ms`(5분)를 넘기지 않게 한다 |
| 재시도 | 은행에 보내지 못하면 재시도 토픽으로 넘긴다: 5초 → 30초 → 60초 뒤 다시 처리, 그래도 안 되면 DLT. 원래 토픽의 뒤 메시지는 기다리지 않는다. 이체는 `RECEIVED` 로 남고 정리 작업이 재발행한다 ([ADR-0007](docs/adr/0007-retry-topic-and-bounded-republish.md)) |

보기: kafka-ui — `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile tools up -d` 후 http://localhost:8089 에서 토픽의 메시지와 그룹 `transfer-processor` 의 랙(밀린 메시지 수)을 본다.

### 재시도 토픽 · DLT 보는 법 (P3-3b)
- kafka-ui 에서 메시지가 `transfer.requested` → `transfer.requested-retry-0` → `-retry-1` → `-retry-2` → `transfer.requested-dlt` 로 옮겨 가는 것이 보인다 (각 단계 사이가 5초 · 30초 · 60초). 그룹별 랙(`transfer-processor`, `-retry-0/1/2`, `-dlt`)은 메시지가 지나가면 0 으로 돌아온다.
- DLT 레코드 헤더: `kafka_original-topic` / `kafka_original-partition` / `kafka_original-offset` (원래 위치), `kafka_exception-cause-fqcn` (예외 클래스), `kafka_exception-message`.
- DLT 에 온 이유에 따라 이체의 다음이 다르다. 로그는 한 줄이고 수준이 갈린다 (계좌번호 · 메시지 값은 남기지 않는다):

| DLT 로 온 이유 (`kafka_exception-cause-fqcn`) | 로그 | 이체는 | 할 일 |
|---|---|---|---|
| `RetryLaterException` (은행 연결 없음) | WARN `transfer … moved to DLT: bank not connected. stays RECEIVED, reconcile will republish it` | `RECEIVED`, `sending_at` 있음 → 5분 뒤 정리 작업이 재발행한다 (아래 순서 0) | 없음. 은행 복구를 기다린다 |
| `InvalidTransferKeyException` (key 가 UUID 아님) | WARN `message moved to DLT: key is not a transfer id (UUID), no transfer is affected. key=…` (키는 제어문자를 `?` 로 바꿔 64자까지) | 이체가 없다 | 보낸 쪽(발행기 · 다른 프로듀서)을 확인한다 |
| 그 밖 (DB 장애 등) | **ERROR** `transfer … moved to DLT and is not retried automatically - check transfer status` (이체 id · 예외 클래스 · 원인 메시지 200자 · 원래 토픽 · 파티션 · 오프셋) | `RECEIVED` 이고 `sending_at` 이 NULL → 정리 작업이 **다시 발행하지 않는다** (재발행은 은행 연결 없음 한 가지만 다룬다) | 수동 확인 · 재처리: [database.md §9](docs/database.md)의 "발행 · 소비가 안 돼 오래 `RECEIVED`" 쿼리로 찾고, 원인(DB 등)을 고친 뒤 이체 상태를 보고 다시 처리한다 |

### 정리 작업 (재발행 · 포기, 결과를 모르는 이체 확정, P3-3a · P3-3b)
10초마다(이전 실행이 끝난 뒤부터) 한 번에 최대 100건씩 돈다. 은행에 **다시 보내지 않고 묻기만 한다** ([ADR-0006](docs/adr/0006-inquire-instead-of-resend.md)).

| 순서 | 대상 | 동작 |
|---|---|---|
| 0 | `status = RECEIVED`, `updated_at` 이 5분보다 오래됨, `sending_at` 이 있고 마지막 재발행 이후(`republished_at` 없음 또는 `sending_at > republished_at`) | 아웃박스로 다시 발행한다 (`republish_count + 1`). 3회를 다 썼으면 `FAILED` `E001` 로 포기한다 |
| 1 | `status = SENDING` 이고 `sending_at` 이 30초보다 오래된 건 | `UNKNOWN` 으로 바꾼다 (컨슈머가 죽었거나 은행 응답 뒤 DB 기록이 실패한 경우) |
| 2 | `UNKNOWN` 이고 `sending_at` 이 30초보다 오래된 건, `inquiry_attempts < 10` | 결과조회(`INQ`): 처리됨(`0000`) → `COMPLETED`, 원거래 없음(`2001`) → `FAILED`, 모름 → `inquiry_attempts + 1` |

- 30초 유예는 컨슈머가 3초 타임아웃으로 적은 `UNKNOWN` 에도 똑같이 적용된다(은행이 조금 느릴 뿐이면 아직 처리 중이라 `2001` 을 믿을 수 없다). 그래서 `UNKNOWN` 은 접수 후 몇 초 만에 보이지만 `COMPLETED` / `FAILED` 는 접수로부터 대략 30~40초 뒤(유예 30초 + 최대 10초 주기)에 확정된다. 단계 1 이 방금 `UNKNOWN` 으로 바꾼 건은 이미 30초가 지났으므로 같은 주기에 바로 묻는다.
- 10번째에도 모르면 더 묻지 않고 경고를 남긴다 → 일마감 대사(P5-1) 대상. 대상 조회: [database.md §9](docs/database.md).
- 로그: `reconcile: ReconcileResult(…)` (한 일이 있을 때만, INFO) · `still UNKNOWN after 10 inquiries, leave it to daily reconciliation: …` (한도 도달, WARN) · `bank inquiry inconclusive: reference=… reason=…` (조회가 답을 못 얻음, WARN) · `transfer abandoned after 3 republishes, the bank was never reached (FAILED E001): …` (재발행 한도를 다 쓰고 포기, WARN) · `bank not connected, inquiries paused until next round` (결과조회 중 연결 없음, WARN) · `reconcile failed, retrying next round` (DB 장애 등, 다음 주기에 다시).
- 정리 작업이 먼저 `UNKNOWN` 으로 바꾼 뒤 컨슈머가 은행의 확정 결과를 받으면 컨슈머가 `UNKNOWN → COMPLETED/FAILED` 로 기록한다. 이미 반대 결과로 확정돼 있으면 `conflict: bank result differs …` 경고가 남는다 → 사람이 확인한다.
- 은행 연결이 없으면 결과조회는 그 주기에서 멈추고 횟수를 세지 않는다 (P3-3b). 긴 은행 점검에도 `UNKNOWN` 이 한도에 닿지 않는다.
- 순서 0 의 기준은 은행에 보내려 한 흔적이다: `sending_at` 이 없는 `RECEIVED`(발행 · 소비가 안 된 건)는 재발행하지 않는다 — Kafka · 컨슈머가 복구되면 밀린 메시지가 처리된다. 은행 장애가 약 26분((재시도 약 95초 + 대기 5분) × 4 ≈ 26분, 정리 작업 주기 최대 10초 별도)보다 길면 그 사이 접수된 출금은 `FAILED` `E001` 이 된다. 대상 조회: [database.md §9](docs/database.md).
- `FAILED` 로 확정하는 곳(순서 0 포기 · 순서 2 의 `2001`)은 `TransferStore.fail()` 이라 같은 트랜잭션에서 잔고도 돌려준다 ([ADR-0008](docs/adr/0008-wallet-debit-on-intake-refund-on-failure.md)).

### 지갑 (P4-1)
- 모든 출금 · 1원인증 · 지갑 API 는 `X-User-Id` 헤더가 필요하다 (영숫자 · 하이픈 1~36자, 대소문자 구분). 인증은 범위 밖이라 **값을 그대로 믿는다**. 없거나 형식이 틀리면 400 `USER_ID_REQUIRED`.
- `POST /api/v1/wallet/deposits {"amount": n}` — **관리 · 테스트용** 충전 (1 ~ 1억 원, 지갑이 없으면 만든다). 인증 · 멱등키가 없어 같은 요청을 두 번 보내면 두 번 충전된다. 응답 200 `{userId, balance, pending}`.
- `GET /api/v1/wallet` → `{userId, balance, pending}`. 지갑이 없으면 `balance = 0`. `pending` = 이 사용자의 출금 중 `RECEIVED` · `SENDING` · `UNKNOWN` 금액 합.
- 출금은 접수할 때 잔고에서 빠진다 (모자라면 422 `INSUFFICIENT_BALANCE`). 결과를 기다리는 동안(`RECEIVED` · `SENDING` · `UNKNOWN`) `pending` 에 보이고, `COMPLETED` 면 빠진 채, `FAILED` 면 같은 트랜잭션에서 돌아온다. `CONFLICT` 는 잔고를 건드리지 않고 경고만 남긴다.

```bash
curl -X POST localhost:8080/api/v1/wallet/deposits -H 'Content-Type: application/json' -H 'X-User-Id: user-1' -d '{"amount":100000}'
curl localhost:8080/api/v1/wallet -H 'X-User-Id: user-1'
```

### 잔고 차감 전략 (P4-2)
접수 때 잔고를 빼는 방식을 기동 설정으로 하나 고른다 ([ADR-0009](docs/adr/0009-wallet-debit-strategies-and-lock-comparison.md), 비용 비교는 [docs/perf/p4-2-lock-comparison.md](docs/perf/p4-2-lock-comparison.md)).
- `conditional` (기본): 조건부 UPDATE 한 문장. 같은 사용자의 동시 출금은 행 락에서 줄을 서고, 진 쪽은 바뀐 잔고로 조건을 다시 봐 422.
- `pessimistic`: `SELECT … FOR UPDATE` 로 행 락을 먼저 잡고 계산해 쓴다. 왕복이 하나 더 있고, 락을 쥔 동안 환불 · 충전도 기다린다. 5초 넘게 기다리면 503.
- `optimistic`: 락 없이 읽고 `version` 이 그대로일 때만 쓴다. 부딪히면 트랜잭션 전체를 처음부터 다시 (최대 5회, 10~50ms 쉬고). 다 부딪히면 409. 같은 사용자 요청이 몰리면 재시도가 늘어난다.
- `redis`: Redisson 사용자 락(`lock:wallet:{userId}`)을 트랜잭션 바깥에서 잡고 안쪽은 `conditional` 과 같다. 락은 DB 앞의 줄 세우기일 뿐 정확성은 DB 가 지킨다. 3초 안에 못 잡으면 503.
- 바꾸는 법: `--wallet.debit-strategy=redis` 인자로 다시 띄운다 (환경변수로 주려면 Boot 의 정식 이름 `WALLET_DEBITSTRATEGY=redis` — `WALLET_DEBIT_STRATEGY` 가 아니다). 확인: `curl localhost:8080/actuator/info` → `{"wallet":{"debit-strategy":"…"}}`.
- `redis` 전략은 Redis 가 없어도 기동된다. 차감 요청만 503 (fail-closed — 락 없이 진행하지 않는다, 로그 `redis unavailable for wallet lock` ERROR). 환불 · 충전은 전략과 무관하게 항상 같다 (원자적 UPDATE).

## 3. 모의 은행 시나리오
| 계좌번호 | 예금주 | 동작 |
|---|---|---|
| `1002123456789` | 홍길동 | 정상. 이체 시 콘솔에 `[통장] … 입금 1원 적요='BT1234'` |
| `3333012345678` | 김카뱅 | 정상 (이름을 다르게 보내면 422 불일치 확인용) |
| `1101234567890` | 이페이 | 정상 |
| `9990000000002` | 무응답 | **처리하고 응답만 안 한다.** 출금 이체(2원 이상)는 입금을 기록하고 응답하지 않는다 → 타임아웃 후 `UNKNOWN`, 결과조회 `INQ` 는 `0000`. **1원 이체는 정상 응답**하므로 1원인증으로 이 계좌를 VERIFIED 로 만들 수 있다 |
| `9990000000001` | – | 테스트 전용: `MockBankServer(silentAccounts = setOf(…))` 로 지정했을 때 **처리도 응답도 안 한다** (결과조회 → `2001`). 모의 은행을 직접 띄울 때는 기본 계좌가 아니다 |
| 그 외 | – | 성명조회 `1001` → 404 `ACCOUNT_NOT_FOUND` |

처리한 이체는 이용기관 거래번호로 기억하고, 결과조회는 기억한 번호면 `0000`, 아니면 `2001` 로 답한다.
테스트 코드에서는 `MockBankServer(silentAccounts = …)` 로 무응답(타임아웃), `responseDelay` 로 지연을 만든다.
손 확인 절차: [`http/transfer.http`](http/transfer.http) 의 "손 확인: 처리 후 무응답" 절.

### 거래 기록 · 대사 파일 (P5-1)
- 모의 은행은 이체를 처리할 때마다 `bank-files/journal/yyyyMMdd.log` 에 한 줄을 덧붙인다 (날짜 = 처리 시각의 KST 날짜). 계좌가 없어 거절한 건은 돈이 움직이지 않았으므로 기록하지 않는다.
- 재기동하면 기록으로 처리 목록을 되살린다 — 결과조회(`INQ`)가 재기동 뒤에도 처리 기록대로 답한다.
- 매일 00:05 (KST) 에 전날 기록으로 `bank-files/recon/RECON_yyyyMMdd.dat` 를 만든다 (형식: [protocol.md §7](docs/protocol.md)). 이미 있으면 덮어쓰지 않는다.
- `./gradlew :apps:mock-bank:run --args="--close-day=20261029"` = 서버를 띄우지 않고 그 날짜 파일만 만들고 끝낸다 (콘솔 `Recon file for …`). 다시 만들려면 파일을 지우고 다시 실행한다.
- `--bank-files=경로` 로 디렉터리를 바꾼다 (API 는 `daily-recon.file-dir` 로 같은 곳을 가리켜야 한다).
- **기동이 `parseJournal` 의 `ProtocolException`("file must end with a newline (partially written?)")으로 실패하면** 기록을 쓰는 도중 죽어 마지막 줄이 잘린 것이다 — `bank-files/journal/yyyyMMdd.log` 의 잘린 마지막 줄(마지막 줄바꿈 뒤의 끝부분)만 지우고 다시 띄운다. 기록이 "처리함" 의 기준이고 응답도 나가지 않았으므로 그 한 건은 처리되지 않은 것이 된다 (API 쪽은 결과조회 `2001` 로 확정한다).
- 손 확인 절차: [`http/daily-recon.http`](http/daily-recon.http).

## 4. API 오류 코드
| 상황 | HTTP | code |
|---|---|---|
| 요청 형식 오류 (본문 JSON 깨짐 · 검증 실패 · 경로 변수 타입 오류 …) | 400 | `INVALID_REQUEST` |
| 예금주명 불일치 | 422 | `HOLDER_NAME_MISMATCH` |
| 없는 계좌 | 404 | `ACCOUNT_NOT_FOUND` |
| 틀린 코드 | 400 | `WRONG_CODE` (+ `remainingAttempts`) |
| 5회 실패 | 423 | `VERIFICATION_LOCKED` |
| 유효시간 경과 | 410 | `VERIFICATION_EXPIRED` |
| 은행 무응답 | 504 | `BANK_TIMEOUT` |
| 은행 연결 없음 | 503 | `BANK_UNAVAILABLE` |
| `Idempotency-Key` 헤더 없음 · 1~64자 보이는 ASCII 아님 | 400 | `IDEMPOTENCY_KEY_REQUIRED` |
| 금액이 1 ~ 10,000,000 원 밖 | 400 | `INVALID_AMOUNT` |
| 금액이 정수가 아님 (소수 · 문자열) | 400 | `INVALID_REQUEST` |
| 적요가 EUC-KR 20바이트 초과 | 400 | `INVALID_MEMO` |
| 은행코드·계좌번호 형식 오류 | 400 | `INVALID_REQUEST` (요청 검증). 도메인 검사는 `INVALID_ACCOUNT` |
| `X-User-Id` 헤더 없음 · 형식 오류 | 400 | `USER_ID_REQUIRED` |
| 충전 금액이 1 ~ 100,000,000 원 밖 | 400 | `INVALID_REQUEST` |
| 이 사용자가 1원인증을 마치지 않은 계좌 (남이 인증한 계좌 포함) | 422 | `ACCOUNT_NOT_VERIFIED` |
| 잔고가 출금액보다 적음 (지갑이 없어도) | 422 | `INSUFFICIENT_BALANCE` |
| 잔고를 지금 잡지 못함 — 락 대기 5초 초과 · 데드락 · Redis 락 3초 초과 · Redis 장애 | 503 | `WALLET_BUSY` (+ `Retry-After: 1`) |
| 동시 변경이 이어져 포기 (`optimistic` 5회) | 409 | `CONCURRENT_UPDATE` (+ `Retry-After: 1`) |
| 같은 키에 다른 내용 | 422 | `IDEMPOTENCY_KEY_REUSED` |
| 없는 이체 id · 남의 출금 | 404 | `TRANSFER_NOT_FOUND` |
| 남의 1원인증 건 확인 | 404 | `VERIFICATION_NOT_FOUND` |
| 은행 점검 시간 (23:50 ~ 00:10) 의 출금 접수 · 1원인증 요청 · 확인 | 503 | `BANK_MAINTENANCE` (+ `Retry-After`: 점검 종료까지 남은 초) |
| 대사: 이미 완료한 날짜 | 409 | `RECON_ALREADY_COMPLETED` |
| 대사: 같은 날짜가 실행 중 | 409 | `RECON_RUNNING` |
| 대사: 아직 마감되지 않은 날짜 (오늘 이후, KST) | 400 | `RECON_DATE_NOT_CLOSED` |
| 허용되지 않는 상태 전이 | 409 | `ILLEGAL_TRANSITION` |
| 그 밖의 표준 Spring 오류 (405 · 415 …) | 해당 상태 | HTTP 상태 이름 (예 `METHOD_NOT_ALLOWED`) |

409 · 503 은 아무것도 남기지 않는다 — 같은 요청(같은 `Idempotency-Key`)을 잠시 뒤 다시 보내면 된다.

조회 응답의 `bankResponseCode = "E001"` 은 은행 코드가 아니라 "은행에 보내지 못하고 포기" 를 뜻한다 (돈은 움직이지 않았다). `E002` 는 일마감 대사가 은행 대사 파일로 "은행이 처리하지 않았다" 를 확정한 것이다 (돈은 움직이지 않았고 환불됐다).

## 5. 인프라 일상 명령
```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait   # 시작 (healthy 까지 대기)
bash infra/verify.sh                                                            # 읽기/쓰기 확인
docker compose -f infra/docker-compose.yml --env-file infra/.env ps             # 상태
docker compose -f infra/docker-compose.yml --env-file infra/.env logs -f kafka  # 로그
docker compose -f infra/docker-compose.yml --env-file infra/.env down           # 멈춤 (데이터 유지)
docker compose -f infra/docker-compose.yml --env-file infra/.env down -v        # 데이터까지 삭제 (초기화)
```
직접 접속 예시는 [infra/README.md](infra/README.md#직접-만져-보기).

## 6. 장애 대응
| 증상 | 먼저 볼 것 | 원인 · 조치 |
|---|---|---|
| 은행 호출이 계속 503 | API 로그의 재접속 메시지, 모의 은행 실행 여부 | 은행 미기동/포트 불일치 → 9090 확인. 재접속은 자동 (최대 30초 간격) |
| 504 `BANK_TIMEOUT` 이 잦음 | 은행 응답 지연, 폴링 로그 | 3초 안에 응답 없음. **이체는 은행에서 처리됐을 수 있다** → P3 부터는 결과조회로 확정 (재전송 금지) |
| 이체가 `UNKNOWN` 으로 남아 있음 | `transfer` 의 `status` · `inquiry_attempts`, 정리 작업 로그(`reconcile:`) | 응답 시간 초과 · 보낸 뒤 연결 끊김 · 포트 예외. 은행이 처리했을 수 있다 → **정리 작업이 `sending_at` 30초 유예 뒤부터 10초마다 결과조회(그 전에는 `inquiry_attempts = 0` 이 정상), 10번 후에도 모르면 경고(`still UNKNOWN after`) → 일마감 대사.** 재전송하지 않는다. 계속 `UNKNOWN` 이면 은행 연결(503)과 `bank inquiry inconclusive` 로그를 본다 |
| 이체가 `SENDING` 에서 안 넘어감 | `sending_at` ([database.md §9](docs/database.md)) | 은행이 응답한 뒤 DB 기록이 실패했거나, `SENDING` 커밋 뒤 또는 은행 호출 도중 앱이 죽은 경우. 둘 다 30초 뒤 정리 작업이 `UNKNOWN` 으로 바꾸고 결과조회로 확정한다 |
| 이체가 `RECEIVED` 에서 안 넘어감 | 릴레이 로그(`outbox relay failed`) · `outbox_event.published_at` · 컨슈머 랙 · 앱 로그 · `moved to DLT` 로그 · `sending_at` | 은행 연결 없음 → 재시도 토픽(약 95초) → DLT → 5분 뒤 재발행(최대 3회) → 그래도 안 되면 `FAILED` `E001`. `sending_at` 이 없으면 은행에 시도도 못 한 것(발행 · 소비 문제: 컨슈머 중단 · 랙, 첫 상태 전환 DB 실패 등) → 릴레이 로그 · `published_at`(NULL 이면 Kafka 접속부터) · 컨슈머 랙을 본다 (재발행하지 않는다, [database.md §9](docs/database.md)) |
| DLT 에 메시지가 쌓임 | 로그 `moved to DLT`(WARN / ERROR), 헤더의 예외 | `RetryLaterException` 이면 은행 장애 — 재발행을 기다린다. `InvalidTransferKeyException` 이면 보낸 쪽 버그. **그 밖의 예외(예: DB 장애)는 ERROR 로그** — `sending_at` 이 NULL 이라 정리 작업이 다시 발행하지 않는다 → 수동 확인 · 재처리 ([database.md §9](docs/database.md)) |
| `FAILED` 인데 `bank_response_code = E001` | `republish_count = 3` | 은행에 보내지 못하고 포기 — 돈은 움직이지 않았다. 사용자가 다시 요청하면 된다. 은행 장애가 26분 넘게 이어졌는지 본다 |
| `late or unknown response tranNo=…` 경고 | 전문번호 | 타임아웃 후 도착한 응답. 로그만 남긴다 (게이트웨이). 결과는 정리 작업의 결과조회로 확정한다. **남는 위험**: 은행이 30초 유예보다 더 늦게 처리하면 조회가 `2001` 을 답해 `FAILED` 로 확정되고, 이후 도착한 이체 응답은 이 로그만 남는다(돈은 나갔는데 `FAILED`) → 일마감 대사(P5-1)가 다음 날 `REFUNDED_BUT_PAID` 로 기록 · 알린다 (자동 복구는 없다). 이 로그에는 거래번호가 없어 어느 건인지 바로 알 수 없다 (범위 밖으로 남김, [HANDOVER](HANDOVER.md)) |
| 503 `WALLET_BUSY` 가 잦음 | 로그 `is busy (lock wait timeout or deadlock)`(WARN) · `not acquired within`(INFO) · `redis unavailable`(ERROR), `/actuator/info` 의 전략 | 같은 사용자 출금이 몰려 락 대기가 한도를 넘는다(핫스팟) 또는 `redis` 전략에서 Redis 장애. Redis 장애면 Redis 를 살리거나 `conditional` 로 재기동. `redis` 전략에서 락 요청의 응답이 시간 초과로 끝났는데 Redis 는 락을 줬다면 그 사용자만 임대가 만료될 때까지(최대 약 30초) 503 일 수 있다 (잔고는 맞다) |
| 409 `CONCURRENT_UPDATE` 가 잦음 | 전략이 `optimistic` 인지 | 같은 사용자에 동시 요청이 많으면 낙관적 락은 재시도가 폭주한다 → `conditional` 이나 `pessimistic` 으로 (측정 결과 [docs/perf](docs/perf/p4-2-lock-comparison.md)) |
| 부하 측정 뒤 `RECEIVED` 출금 수만 건 · 미발행 아웃박스 | `user_id` 가 `load-` 로 시작하는지 | 측정 데이터다. `bash load/run.sh cleanup` 으로 지운다 — 지우지 않고 평소 설정으로 앱을 띄우면 모두 은행으로 나간다 |
| 422 `INSUFFICIENT_BALANCE` 가 잦음 | `GET /api/v1/wallet` 의 `balance` · `pending` | 잔고 부족. `pending` 이 크면 결과를 기다리는 출금(`UNKNOWN` 등)이 돈을 묶고 있다 → 정리 작업 · 결과조회 로그를 본다 |
| 잔고와 원장 합이 다름 | [database.md §9](docs/database.md) "잔고 = 원장 합 검증" | 정상이면 0행. 원장은 수정하지 않는다 — 원인(직접 수정한 SQL 등)을 찾고 보정 기록을 남긴다 (일마감 대사가 매일 `BALANCE_LEDGER_MISMATCH` 로 알린다) |
| `FAILED` 인데 잔고가 안 돌아옴 | [database.md §9](docs/database.md) "환불 누락 확인", 해당 출금의 `user_id` | `user_id` 가 NULL 이면 P3 시절 출금이라 차감도 환불도 없다 (정상). 그 밖이면 버그 — `fail()` 을 거치지 않은 상태 변경이 있었는지 본다 |
| `CONFLICT` 경고 (`wallet balance left as is`) | 출금 상태 · 원장 · 은행 거래내역 | 잔고를 자동으로 고치지 않는다. 예: `FAILED` 확정 · 환불 뒤 은행이 완료 → 환불된 돈이 실제로 나갔다 → 수동 확인 (일마감 대사가 다음 날 `REFUNDED_BUT_PAID` 등으로 알린다) |
| "대사 미완료" 긴급 알림 (06시) | 알림의 이유 코드, `bank-files/recon/` 에 그날 파일, 모의 은행 로그 `[일마감]` | `FILE_NOT_READY`: 모의 은행이 00:05 에 떠 있지 않았다 → `--close-day=그날` 로 만들고 `POST /admin/reconciliations?date=그날`. `FILE_INVALID`: 파일을 확인(헤더 날짜 · 트레일러) — 은행에 재전송 요청(모의는 지우고 `--close-day`). `ERROR`: API 로그의 예외 → 고치고 같은 요청으로 멈춘 곳부터. `RUNNING`: 다른 실행이 아직 돌고 있다 — 곧 끝나면 완료 알림이 따로 온다, 오지 않으면 아래 `RECON_RUNNING` 행 (§9-6) |
| "일마감 대사 … 불일치 N건" 긴급 알림 | `recon_issue` ([database.md §9](docs/database.md) 열린 이슈 쿼리) | 종류별: `PAID_TWICE` · `PAID_BUT_RECEIVED` 는 긴급 — 은행에 확인 · 회수 요청, `RECEIVED` 면 재발행 전에 손으로 `COMPLETED` 처리. `REFUNDED_BUT_PAID` 는 환불한 돈이 나갔다. `COMPLETED_NOT_AT_BANK` 는 은행에 확인. `BALANCE_LEDGER_MISMATCH` 는 원장을 고치지 말고 원인을 찾는다. 처리 뒤 `resolved_at` · `resolution_note` 를 SQL 로 남긴다 ([database.md §9](docs/database.md)) |
| 409 `RECON_RUNNING` 이 계속 (앱이 대사 도중 죽음) | 대사 잡 실행 이력 쿼리의 `STATUS = STARTED` · `END_TIME` NULL | 앱이 죽어 실행이 '돌고 있음' 으로 남았다. 다른 인스턴스가 정말 돌고 있지 않은지 확인한 뒤 FAILED 로 바꾸고 다시 실행한다 — SQL 은 §9-6 |
| 503 `BANK_MAINTENANCE` | 지금 시각(KST), `bank.maintenance.*` | 점검 시간이다. `Retry-After` 초 뒤 다시 (같은 `Idempotency-Key` 의 재요청도 점검 중에는 503). 점검 중 접수된 건은 없다(웹이 거절), 점검 전 접수된 건은 끝나면 재시도 · 재발행으로 처리된다 |
| 길이 필드 오류로 연결이 끊김 | `CorruptedFrameException` | 스트림 경계를 잃음 → 연결 종료 후 재접속이 정상 동작 |
| Kafka 접속은 되는데 메시지가 안 감 | `advertised.listeners` | PC 앱은 `localhost:9092`, 컨테이너는 `kafka:29092` |
| Kafka `Invalid cluster.id` | `.env` 의 `KAFKA_CLUSTER_ID` 변경 여부 | `down -v` 후 다시 `up` |
| `port is already allocated` | PC 에 이미 떠 있는 MySQL/Redis | `infra/.env` 포트 변경 |
| 기동 시 `Communications link failure` | MySQL 실행 여부 | infra 먼저 `up -d --wait` |
| 기동 시 `Schema-validation` / Flyway 체크섬 오류 | 엔티티 · 마이그레이션 변경 | 적용된 V 파일을 고치지 않았는지 확인. 로컬은 `down -v` 로 초기화 |

### 자주 막히는 곳
- PowerShell 에서 `bash …` 실패 → Git Bash 로 실행 ([SETUP §3](SETUP.md#3-windows-에서-알아둘-것)).
- Git Bash 에서 `/opt/...` 경로 깨짐 → `MSYS_NO_PATHCONV=1` (`verify.sh` 는 처리돼 있음).

## 7. CI (GitHub Actions `.github/workflows/ci.yml`)
| 잡 | 하는 일 | 실패 시 |
|---|---|---|
| `build` | JDK 21 → `./gradlew build` (테스트 전체) | 테스트 리포트 아티팩트(`test-reports`) 업로드 |
| `infra-smoke` | Compose 로 인프라 기동(`--wait`, 240초) → `verify.sh` → 항상 `down -v` | 컨테이너 로그 출력 |

`main` push 와 모든 PR 에서 실행된다. PR 은 두 잡이 모두 초록이어야 머지한다.

## 8. 부하 측정 (P4-2, k6)
네 차감 전략의 처리량 · 지연 · 실패율을 같은 조건에서 잰다 (핫스팟 `load-hot` 1명 / 분산 `load-u001` ~ `load-u500`, 50 VU, 워밍업 10초 + 측정 60초). 결과는 [docs/perf/p4-2-lock-comparison.md](docs/perf/p4-2-lock-comparison.md).

**준비** (이 PC 에서 앱 · MySQL · Redis · k6 가 CPU 를 나눠 쓰므로 측정 중에는 다른 무거운 작업을 하지 않는다. 다른 API 인스턴스(예: IntelliJ 로 띄운 것)는 모두 끈다 — 같은 DB 를 쓰는 발행기와 컨슈머가 측정용 출금을 은행으로 보내 버린다):
```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait
./gradlew :apps:mock-bank:run --args="9090"        # 별도 터미널
./gradlew :apps:api:bootJar
```

**앱 실행** (전략이 바뀔 때마다 다시 띄운다. 백그라운드 작업(발행기 · 정리 작업 · 일마감 대사 스케줄)을 끄고 로그를 줄인다 — 또는 IntelliJ 실행 구성 `5. API (부하 측정)`):
```bash
java -Xms1g -Xmx1g -jar apps/api/build/libs/api-0.1.0-SNAPSHOT.jar   --wallet.debit-strategy=conditional   --transfer.outbox.relay.enabled=false --transfer.reconcile.enabled=false   --spring.kafka.listener.auto-startup=false   --server.tomcat.threads.max=200   --logging.level.io.github.hyeokjinon.banktransfer=WARN   --daily-recon.schedule.enabled=false
```

**회차 실행** (Git Bash): `bash load/run.sh <전략> <hotspot|spread> <회차>` — 정리 → 시드 → InnoDB 정리 대기 → k6 → 정확성 확인(음수 잔고 · 잔고 ≠ 원장 합 · k6 202 수 ≠ 출금 행 수 · RECEIVED 가 아닌 출금 · 발행된 아웃박스 — 뒤 둘은 다른 인스턴스나 끄지 않은 발행기 · 컨슈머가 부하 출금을 건드렸는지 잡는다)을 한 번에 한다. 떠 있는 앱의 전략(`/actuator/info`)이 인자와 다르면 거부한다. 확인을 통과하지 못했거나 중단된 회차의 요약은 `.json.invalid` 로 바뀌어 표에서 빠진다. 같은 회차 번호를 다시 돌리면 그 회차의 이전 결과를 지우고 새로 만든다. 결과 원본은 `docs/perf/raw/`.
```bash
bash load/run.sh conditional hotspot 1
WARMUP=2s MEASURE=5s VUS=5 bash load/run.sh conditional hotspot 1    # 짧은 시험 (이 숫자는 쓰지 않는다)
bash load/summarize.sh                                               # 시나리오 · 전략별 중앙값 표
```

**지연은 성공만 따로 본다.** 409 · 503 은 일찍 실패해 빠르므로, 거절을 많이 하는 전략은 전체 요청 백분위(`p50` · `p95` · `p99`)가 더 빨라 보일 수 있다. 요약 JSON 에는 접수 성공(202)만 센 `p50_202` · `p95_202` · `p99_202` 가 따로 있고 표의 `p95 성공만` 열이 그것이다 — 전략 사이 지연은 이 값으로 비교한다 (실패 비율은 `202` · `409` · `503` 열).

> **끝나면 반드시** `bash load/run.sh cleanup`. 측정용 출금 수만 건이 `RECEIVED` + 미발행 아웃박스로 남아 있어, 지우지 않고 평소 설정(발행기 켬)으로 앱을 띄우면 모두 은행으로 나간다. 이 명령은 부하용 사용자 정확히 501명(`load-hot`, `load-u001` ~ `load-u500`)의 데이터만 지운다.

- k6 는 Docker 이미지(`grafana/k6:2.3.0`)로 돌고, CI 에서는 돌리지 않는다 (CI 는 전략별 정확성 테스트만).
- DB 계정 정보는 `infra/.env` 에서 mysql 컨테이너 안으로만 전달된다 (호스트 명령행에 남지 않는다).

## 9. 일마감 대사 · 점검 시간 · 알림 (P5-1)
근거: [ADR-0010](docs/adr/0010-daily-reconciliation.md). 테이블: [database.md §4-6 ~ §4-8](docs/database.md), 파일 형식: [protocol.md §7](docs/protocol.md).

### 9-1. 점검 시간 (23:50 ~ 00:10 KST)
- **언제**: 매일 `bank.maintenance.start` ~ `end` (기본 23:50 ~ 00:10, 시작 포함 · 끝 제외, 자정을 넘는다). 이 시간에는 아무것도 은행에 가지 않으므로 "D일에 보낸 출금" 과 "D일 대사 파일" 이 같은 범위가 된다.
- **막히는 것**:
  - 웹 세 곳 — 출금 접수, 1원인증 요청 · 확인 → 503 `BANK_MAINTENANCE` + `Retry-After`(점검 끝까지 남은 초). **같은 `Idempotency-Key` 의 재요청(재생)도 점검 중에는 503** 이다 (이미 접수된 건의 200 재생이 아니다) — `Retry-After` 뒤에 다시 보낸다.
  - 은행으로 나가는 전문 — 컨슈머 · 정리 작업 · 재발행 모두. 게이트웨이의 `send()` 한 곳이 이체 · 결과조회 전문을 보내지 않고 "연결 없음" 과 같은 길로 답한다: 이체는 `RECEIVED` 로 돌아가 재시도 토픽 · 재발행, 결과조회는 시도 횟수를 세지 않고 다음 주기. 폴링(링크 체크)은 계속 돌아 연결은 유지된다.
- **안 막히는 것**: 조회(`GET`), 지갑 충전, 관리 API(`POST /admin/reconciliations` 포함).
- **길이 한도**: 재발행 포기(`E001`)까지 약 26분이다. 점검을 이보다 길게 잡으면 점검 전에 접수된 건이 `E001` 로 포기될 수 있다. 기본은 20분.
- 시계는 `Clock` 빈 하나이고 판단은 `Instant` 를 KST 로 바꿔서 한다 (서버 시계가 UTC 여도 KST 로 맞다).

### 9-2. 대사 실행
- **자동**: 매일 01:00 ~ 06:00 매시 정각(KST)에 **전날** 대사. 파일이 아직 없으면(`FILE_NOT_READY`) 다음 정각에 멈춘 곳부터 다시, 이미 끝났으면 조용히(INFO) 넘어간다. 06:00 이 마지막 회차다.
- **수동**: `date` 는 `yyyy-MM-dd` (KST). **마감된 날(어제 이전)만** 받는다 — 오늘 이후는 400 `RECON_DATE_NOT_CLOSED`. 오늘을 대사하면 방금 보내 아직 은행 파일에 없는 출금을 "은행이 처리하지 않았다" 로 보고 `E002` 환불하는데, 은행이 그 출금을 그 뒤 처리하면 **이중 지급**이다 ([ADR-0010](docs/adr/0010-daily-reconciliation.md) 결정 7).
  - 로컬 손 확인에서만 `daily-recon.allow-today=true`(실행 구성 `6. API (대사 손 확인)`)로 오늘을 허용한다(내일 이후는 그래도 거부). **운영에서는 절대 켜지 않는다.** 스케줄은 늘 전날이라 이 설정과 무관하다.
  ```bash
  curl -X POST "http://localhost:8080/admin/reconciliations?date=2026-10-28"
  # {"date":"2026-10-28","status":"COMPLETED","summary":{"transfers":…,"bankLines":…,"autoCompleted":…,"autoFailed":…,"mismatches":{…},…}}
  ```
  | 응답 | 뜻 |
  |---|---|
  | 200 `status = COMPLETED` + `summary` | 끝났다. `summary` = 출금 수 · 은행 줄 수 · 일치 · 자동 완료 · 자동 실패 · 재시도 중 · 종류별 불일치 · `resentLater`(다른 날 재송신 — 아래 9-3) |
  | 200 `status = FAILED` + `reason {code, message}` | 요청은 처리됐고 결과가 실패다. `code` = `FILE_NOT_READY` · `FILE_INVALID` · `RESTART_REFUSED` · `ERROR`(예외 클래스 이름만) |
  | 409 `RECON_ALREADY_COMPLETED` | 끝난 날짜는 다시 돌리지 않는다 |
  | 409 `RECON_RUNNING` | 같은 날짜가 실행 중이다 |
  | 400 `RECON_DATE_NOT_CLOSED` | 아직 마감되지 않은 날(오늘 이후, KST). 지난 날만 대사할 수 있다 (`allow-today=true` 면 오늘은 허용) |
  | 400 `INVALID_REQUEST` | `date` 가 없거나 형식 오류 |
- **실패하면 멈춘 곳부터**: 같은 날짜를 다시 돌리면 끝난 단계는 건너뛰고 멈춘 단계는 마지막 커밋 청크부터 잇는다. 결과는 한 번에 끝낸 것과 같고 환불은 두 번 되지 않는다.
- 동시에 같은 날짜를 두 곳에서 돌려도 하나만 돈다 (나머지는 `RECON_RUNNING`).

### 9-3. 무엇을 맞추고 무엇을 자동으로 고치나
- **대상**: `sending_at` 이 D일(KST)인 출금 전부(③) + 파일 D 의 줄 중 1원인증이 아니고 ③ 이 다루지 않은 것(④ — 다른 날 보낸 출금, 우리에게 없는 거래번호, `T` 가 아닌 낯선 접두의 거래번호도). 거래번호로 짝지을 때 적재된 모든 날짜의 줄을 본다. 잔고 ≠ 원장 합(⑤)은 대사일과 무관하게 지금 전체를 본다.
- **1원인증 줄 = `V…` 이면서 금액 1원인 줄**만이다. 대조하지 않고 건수만 센다. `V…` 여도 1원이 아니면 1원인증이 아니므로 ④ 가 대조해 `UNKNOWN_REFERENCE` 로 기록 · 알린다 (`V` 거래번호로 큰돈이 나가도 조용히 빠지지 않게).
- **자동 확정은 `UNKNOWN` · `SENDING` 만**: 은행 줄이 있고 금액 · 은행 · 계좌가 같으면 `COMPLETED`(`0000`) + 이슈 `AUTO_COMPLETED`, 줄이 없으면 `FAILED` `E002` + 환불 + 이슈 `AUTO_FAILED`. **그 밖의 어긋남은 고치지 않고** `recon_issue` 에 남기고 알린다 — 확정된 건을 뒤집으면 보정이 새 사고가 된다.
- **확정은 "`sending_at` 이 아직 D일 안" 일 때만** — 상태와 같은 조건부 UPDATE 의 조건이다. ③ 이 읽은 뒤 그 출금이 다시 보내지면(`RECEIVED → SENDING`, `sending_at` = 지금) 상태는 다시 `SENDING` 이지만 은행이 지금 처리 중일 수 있어, `E002` 로 환불하면 이중 지급이 된다. 이런 건은 확정하지 않고 요약의 `resentLater`(알림 첫 줄 "다른 날 재송신(확정 안 함) n", 있을 때만)로만 센다. 이슈는 남기지 않는다 — 새 `sending_at` 의 날짜 대사가 그 출금을 맡고, D일 파일의 줄은 ④ 가 본다. 평소에는 0 이고, Kafka · 컨슈머 장애가 자정을 넘겨 01시 무렵 복구될 때 나올 수 있다.

### 9-4. 이슈 처리
- 열린 이슈 보기 · 처리 기록 남기기: [database.md §9](docs/database.md) "일마감 대사". `recon_issue` 는 append-only 라 사람이 `resolved_at` · `resolution_note` 만 SQL 로 채운다.
- 확정된 건의 어긋남은 사람이 SQL 로 처리한다 (해결 API 는 범위 밖).

| `type` | 뜻 | 할 일 |
|---|---|---|
| `AUTO_COMPLETED` · `AUTO_FAILED` | 자동 확정의 감사 기록 (어긋남이 아님) | 없음 |
| `PAID_TWICE` (긴급) | 같은 거래번호가 은행 파일에 2줄 이상 — 돈이 두 번 나갔다 | 은행에 확인 · 회수 요청 |
| `PAID_BUT_RECEIVED` (긴급) | 우리는 `RECEIVED` 인데 은행이 처리했다 — 재발행되면 이중 지급 | 재발행 전에 손으로 `COMPLETED` 처리 |
| `REFUNDED_BUT_PAID` | 우리는 `FAILED`(환불) 인데 은행이 처리했다 — 환불한 돈이 나갔다 | 은행 확인 후 잔고 보정 방법 결정 |
| `COMPLETED_NOT_AT_BANK` | 우리는 `COMPLETED` 인데 은행 파일에 없다 | 은행에 확인 |
| `AMOUNT_OR_ACCOUNT_MISMATCH` | 금액 · 은행코드 · 계좌가 다르다 (`UNKNOWN` 이어도 확정하지 않는다) | 은행 기록과 대조 |
| `UNKNOWN_REFERENCE` | 우리에게 없는 거래번호의 줄 (1원인증 = `V…` 이면서 1원인 줄 제외 — 1원이 아닌 `V…` 줄도 여기) | 은행에 확인 |
| `BALANCE_LEDGER_MISMATCH` | 잔고 ≠ 원장 합. 고칠 때까지 날마다 다시 나온다 | 원장을 고치지 말고 원인을 찾는다 |

### 9-5. 알림
| 언제 | 수준 | 내용 |
|---|---|---|
| 대사 완료, 불일치 0 | INFO | 요약 (출금 N건: 일치 · 자동 완료 · 자동 실패 · 재시도 중 [· 다른 날 재송신] / 은행 줄 N건) |
| 대사 완료, 불일치 있음 | CRITICAL | 요약 + 불일치 목록(긴급 먼저, 최대 20건 + "외 n건"). 자동 확정은 요약의 건수로만 |
| 대사 실패 (`FILE_NOT_READY` 가 아닌 이유) | CRITICAL | 이유 + 멈춘 단계부터 다시 돌리는 방법 |
| 06시 마지막 회차에도 끝내지 못함 | CRITICAL | "대사 미완료" + 이유. 실패뿐 아니라 **실행이 아직 돌고 있는 것으로 끝난 경우(`RUNNING`)도** — 대사 도중 JVM 이 죽어 `STARTED` 로 남으면 회차마다 "실행 중" 만 돌아오므로 이 알림이 유일한 신호다 |
| 대사를 시작하지 못함 (DB 장애 등) | CRITICAL | 예외 클래스 |
| 기존 경고 3곳 — `CONFLICT`, 자동 재처리가 없는 DLT(그 밖의 예외), 결과조회 한도 소진 | WARN | 이체 id · 이유 (계좌번호 가림). 호출이 컨슈머 · 정리 작업 스레드에서 일어나 텔레그램이 느리면 최대 약 8초 붙잡을 수 있다 |

- 마지막 회차 전의 `FILE_NOT_READY` 와 수동 실행의 `FILE_NOT_READY` 는 알리지 않는다 (INFO 로그) — 은행 파일은 보통 00:05 에 오고, 다음 정각에 다시 한다. "이미 완료" 도 알리지 않는다.
- 로그만 볼 때(텔레그램을 켜지 않았을 때)는 로거 `ALERT` 를 grep 한다 (CRITICAL 은 ERROR 수준). 텔레그램을 켜도 로그에 먼저 남는다. 텔레그램 켜는 법: [SETUP 2-6](SETUP.md#2-6-텔레그램-알림-선택-p5-1).
- 알림이 실패해도(텔레그램 장애 · 시간 초과) 대사에는 영향이 없다 — ERROR 로그만 남기고 삼킨다. 기록의 원본은 `recon_issue` 다.
- 본문에는 계좌번호 가운데를 가린 값(`1002****6789`)만 나가고, 사용자 id · 예금주명은 싣지 않는다. 길이는 4,096자까지다.

### 9-6. 복구
**실행이 STARTED 로 남음** — 대사 도중 JVM 이 죽으면 그 실행은 "돌고 있음" 으로 남아 같은 날짜가 409 `RECON_RUNNING` 이 된다 (06시 "대사 미완료" `RUNNING` 알림이 같은 신호다).
1. 실행 이력 쿼리([database.md §9](docs/database.md))로 `STATUS = STARTED` · `END_TIME` NULL 인 `JOB_EXECUTION_ID` 를 찾는다.
2. 다른 API 인스턴스가 정말 돌고 있지 않은지 확인한다 (돌고 있는 대사를 끊지 않는다).
3. FAILED 로 바꾼다:
   ```sql
   UPDATE BATCH_STEP_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=? AND STATUS IN ('STARTING','STARTED');
   UPDATE BATCH_JOB_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=?;
   ```
   `NOW(6)` 은 KST 이지만 `BATCH_*` 의 시각은 JVM 기본 시간대로 쓰인다 ([database.md §2](docs/database.md)) — 몇 시간 어긋나 보일 수 있다. 재시작 판단은 `STATUS` 로 하므로 기록용일 뿐이다.
4. 같은 날짜를 다시 실행하면 멈춘 곳부터 이어진다 (`POST /admin/reconciliations?date=…`).

**잡 인스턴스만 있고 실행이 없음** — 처음 시작하는 날짜를 두 곳이 동시에 시작하면 인스턴스와 실행이 서로 다른 트랜잭션에서 만들어져, 짧은 순간 "실행 없는 인스턴스" 가 보인다. 런처는 이때 잠깐 기다려 한 번 더 시도한다. 그래도 실행이 없으면(만들던 쪽이 그 사이 죽음) 200 `FAILED` + `reason.code = ERROR`(`IllegalStateException`) + 긴급 알림이다. 그 인스턴스는 사람이 지워야 그 날짜가 다시 돈다:
```sql
-- 실행이 하나도 없는 대사 잡 인스턴스 (보통 하나)
SELECT i.JOB_INSTANCE_ID FROM BATCH_JOB_INSTANCE i
LEFT JOIN BATCH_JOB_EXECUTION e ON e.JOB_INSTANCE_ID = i.JOB_INSTANCE_ID
WHERE i.JOB_NAME = 'dailyReconciliationJob' AND e.JOB_EXECUTION_ID IS NULL;
-- 지우고 같은 날짜를 다시 실행
DELETE FROM BATCH_JOB_INSTANCE WHERE JOB_INSTANCE_ID = ? AND NOT EXISTS (SELECT 1 FROM BATCH_JOB_EXECUTION e WHERE e.JOB_INSTANCE_ID = ?);
```

**`RESTART_REFUSED`** — 지난 실행이 `UNKNOWN`(롤백할 수 없는 실패)으로 끝나 Spring Batch 가 재시작을 거부한 것이다. `BATCH_JOB_EXECUTION` 의 `STATUS` 를 확인하고, 원인을 확인한 뒤 위 STARTED 와 같은 방법으로 `FAILED` 로 바꿔 다시 실행한다.

**동시 시작 때 ERROR 스택은 정상이다** — 실패한 날짜를 두 곳이 동시에 다시 돌리면 나중 실행을 `SingleRunGuard` 가 거부하는데, 이때 Spring Batch 가 "Encountered fatal error executing job" 이라는 **ERROR 로그와 예외 스택**(`ReconRunningException`)을 남긴다. 요청자는 409 `RECON_RUNNING` 을 받고, 먼저 시작한 실행은 계속 돈다. 동시 시작이 의도된 상황이면 무시한다.

### 9-7. 손 확인할 때 주의
- 손 확인은 [`http/daily-recon.http`](http/daily-recon.http), 실행 구성 `6. API (대사 손 확인)` (정리 작업을 꺼서 결과를 모르는 출금이 `UNKNOWN` 으로 남는다).
- **오늘을 대사하려면 API 를 반드시 실행 구성 6 으로 띄운다** — `--daily-recon.allow-today=true` 가 켜져 있어야 오늘 날짜를 받는다. 다른 구성(`2. API (8080)` 등)으로 띄우면 400 `RECON_DATE_NOT_CLOSED` 다. 명령줄로는 `./gradlew :apps:api:bootRun --args="--transfer.reconcile.enabled=false --daily-recon.allow-today=true"`.
- **손으로 오늘을 마감하는 것(`--close-day=오늘`)은 손 확인용이다.** 그 뒤에 처리된 출금은 이미 만들어진 오늘 파일에 없다. 오늘 대사를 아직 돌리지 않았다면 내일 01시 회차가 그 파일로 돌아, 그 출금들이 은행에 없는 것으로 보인다 (`UNKNOWN` 은 `E002` 로 확정 · 환불, `COMPLETED` 는 `COMPLETED_NOT_AT_BANK` 이슈). 손 확인을 끝낸 뒤에는 `bank-files/recon/RECON_오늘.dat` 를 지워 두면 00:05 에 모의 은행이 정상 파일을 만든다 (이미 오늘 대사를 끝냈다면 그 날짜는 "이미 완료" 로 조용히 넘어간다).
