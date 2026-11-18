# OPERATION — 실행 · 상태 확인 · 장애 대응

> 로컬 개발과 CI 기준 운영 안내다. 컨테이너 실행은 §14 (Compose), Kubernetes(kind) 실행은 §15.

## 1. 구성 요소와 포트
| 구성 요소 | 실행 | 포트 | 상태 확인 |
|---|---|---|---|
| API (`apps/api`) | `./gradlew :apps:api:bootRun` 또는 실행 구성 `2. API (8080)` | 8080 | `GET /actuator/health` · MySQL · Kafka 필요 (기동 시 Flyway 마이그레이션) |
| 은행 게이트웨이 (`apps/bank-gateway`, P8-1) | `./gradlew :apps:bank-gateway:bootRun` 또는 `11. Bank Gateway (8082)` (기본 http 모드에서 api 가 부르는 곳 — 모의 은행보다 나중, api 보다 먼저) | 8082 | `GET /actuator/health` (`bankConnection` 포함 — `/actuator/health/bankConnection` → `UP` 이면 은행에 붙어 있다) |
| 모의 은행 (`apps/mock-bank`) | `./gradlew :apps:mock-bank:run --args="9090"` (작업 디렉터리 = 저장소 루트, 거래 기록 · 대사 파일은 `./bank-files`) 또는 `1. Mock Bank (9090)` | 9090 (TCP) | 콘솔 `Mock bank FEP started on port 9090` |
| MySQL 8.4 | `infra/docker-compose.yml` | 3306 | `docker compose ... ps` → healthy |
| Kafka 4.1 (KRaft) | 〃 | 9092 (PC) / 29092 (컨테이너) | 〃 |
| Redis 7.4 | 〃 | 6379 | 〃 |
| kafka-ui (선택) | `--profile tools up -d` | 8089 | http://localhost:8089 |
| 컨테이너 실행 (`app` 프로필, P8-2) | `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app up -d --build --wait` (§14) | 8000 (nginx — 뒤에 api-1 · api-2 · 게이트웨이 · 모의 은행) | `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app ps` → 5개 healthy, `curl localhost:8000/actuator/info` |
| Kubernetes 실행 (kind, P8-3a) | `bash infra/k8s/up.sh` (§15, [infra/k8s/README.md](infra/k8s/README.md)) | 8000 (Envoy → api Pod ×2 → 게이트웨이 → 모의 은행, Compose `app` 프로필과 동시에 못 띄움) | `bash infra/k8s/smoke.sh` · `kubectl -n bank-transfer get pods -o wide` |

모의 은행 · API 는 `./bank-files` 를 같이 본다 (Gradle `run` · `bootRun` 이 저장소 루트에서 돈다). `git status` 에 보이지 않는다(`.gitignore`).

실행 구성 `0. Mock Bank + API` 와 `1. Mock Bank (9090)` 를 같이 띄우지 않는다: Windows 에서 모의 은행의 `SO_REUSEADDR` 가 같은 포트에 두 번 bind 하는 것을 허용해, 모의 은행이 둘 뜨고 어느 쪽에 붙을지 알 수 없다 (입금 내역이 갈린다).

기본(`bank.gateway.mode=http`, P8-2)에서 API 는 은행에 직접 붙지 않는다: api → 은행 게이트웨이 앱(8082) → 은행이다. 게이트웨이가 기동 시 모의 은행에 접속하고, 은행이 없어도 둘 다 뜨며 게이트웨이가 1초→30초 간격으로 재접속을 시도한다(그동안 은행 호출은 503). `direct` 모드에서만 API 가 기동 시 모의 은행에 직접 접속한다(같은 재접속 규칙).

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
| `transfer.consumer.mode` | `blocking` | 출금 컨슈머 (P6-1, [ADR-0012](docs/adr/0012-consumer-coroutines.md)): `blocking` · `coroutine` (소문자 그대로). 그 밖의 값은 기동 실패. 지금 값은 `GET /actuator/info` (§11) |
| `transfer.consumer.coroutine.max-in-flight-bank-calls` | `64` | `coroutine` 에서 은행에 동시에 보내는 최대 수. 1 ~ 1024 밖이면 기동 실패 |
| `spring.task.scheduling.pool.size` | `4` | 스케줄러 스레드. 발행기(0.5초) · 정리 작업(10초) · 일마감 대사(01 ~ 06시) · 보존 배치(06:30)가 서로 막지 않게 한다 (P5-2 에서 3 → 4) |
| `spring.datasource.hikari.maximum-pool-size` | `20` | 커넥션 풀 (P4-2 에서 명시, `minimum-idle` = 최대) |
| `spring.datasource.hikari.connection-init-sql` | `SET SESSION innodb_lock_wait_timeout = 5` | 행 락 대기 한도 5초 (MySQL 기본 50초). 넘으면 503 `WALLET_BUSY`. 풀의 모든 커넥션에 적용 — 발행기 · 정리 작업 트랜잭션도 (지금은 짧다) |
| `wallet.debit-strategy` | `conditional` | 잔고 차감 전략: `conditional` · `pessimistic` · `optimistic` · `redis` ([ADR-0009](docs/adr/0009-wallet-debit-strategies-and-lock-comparison.md)). 기동 때 하나, 모르는 이름이면 기동 실패. 지금 값은 `GET /actuator/info` |
| `wallet.optimistic.max-attempts` / `backoff-min` / `backoff-max` | `5` / `10ms` / `50ms` | `optimistic`: 충돌 재시도 횟수 · 사이 대기. 다 쓰면 409 |
| `wallet.lock.wait-time` | `3s` | `redis`: 사용자 락 대기. 넘으면 503 |
| `wallet.lock.redis-address` | `redis://${REDIS_HOST:localhost}:${REDIS_PORT:6379}` | `redis` 전략일 때만 연결 (첫 요청 때) |
| `bank.gateway.mode` | `http` | `http`(기본, P8-2) = 게이트웨이 앱(`apps/bank-gateway`)을 HTTP 로 부른다 · `direct` = 이 프로세스가 Netty 로 은행에 직결 (테스트 · 측정 기준선은 명시) |
| `bank.gateway.http.base-url` | `http://localhost:8082` | `http` 일 때만. 게이트웨이 주소 |
| `bank.gateway.http.client` | `webclient` | `http` 일 때만. `webclient` · `jdk` (P8-1 비교) |
| `bank.gateway.http.max-connections` | `500` | `http` 일 때만 (WebClient). 연결 풀 상한 |
| `bank.gateway.http.connect-timeout` / `pending-acquire-timeout` | `1s` / `1s` | `http` 일 때만. 연결 · 풀 대기 |
| `bank.gateway.http.response-timeout` | `5s` | `http` 일 때만. 게이트웨이 응답 대기 (게이트웨이의 은행 응답 3s 보다 길게) |
| `bank.gateway.host` / `port` | `localhost` / `9090` | `direct` 일 때만. 은행 FEP 주소 |
| `bank.gateway.response-timeout` | `3s` | `direct` 일 때만. 응답 대기. 넘으면 504 `BANK_TIMEOUT` |
| `bank.gateway.poll-interval` | `30s` | `direct` 일 때만. 쓰기 유휴 시 폴링 주기. ×3 동안 수신이 없으면 연결 종료 |
| `server.shutdown` | `graceful` | 내려갈 때 새 연결을 받지 않고 처리 중인 요청을 끝낸다 (P8-2) |
| `spring.lifecycle.timeout-per-shutdown-phase` | `20s` | 종료 **단계당** 상한 (Compose `stop_grace_period` 30s, §14-3) |
| `info.instance` | `${INFO_INSTANCE:local}` | `GET /actuator/info` 의 어느 api 인가. Compose 가 api-1 · api-2 를 넣는다 |
| `bank.maintenance.enabled` / `start` / `end` | `true` / `"23:50"` / `"00:10"` | 은행 점검 시간 (KST, 시작 포함 · 끝 제외, 자정 넘김 가능). yml 에서는 따옴표 필수. 길이는 재발행 포기(`E001`)까지의 약 26분보다 짧게 |
| `account.verification.ttl` | `5m` | 1원인증 유효시간 |
| `account.verification.max-attempts` | `5` | 코드 실패 허용 횟수 |
| `management.endpoints.web.exposure.include` | `health,info` | 노출 actuator |
| `daily-recon.file-dir` | `./bank-files` | 은행 대사 파일 디렉터리 (`recon/RECON_yyyyMMdd.dat`) |
| `daily-recon.chunk-size` | `100` | ③ 출금 대조 · ④ 은행 줄 대조 청크 (② 적재는 500 고정) |
| `daily-recon.schedule.enabled` / `cron` / `zone` | `true` / `0 0 1-6 * * *` / `Asia/Seoul` | 매시 정각 전날 대사. 부하 측정 · 테스트는 `false` |
| `data-retention.outbox-retention` / `verification-retention` | `7d` / `30d` | 발행 · 만료 뒤 이만큼 지난 행을 지운다 (§10). 1일 미만이면 기동 실패 |
| `data-retention.batch-size` | `1000` | 트랜잭션 하나에 지우는 최대 행 수. 1 ~ 10000 밖이면 기동 실패 |
| `data-retention.schedule.enabled` / `cron` / `zone` | `true` / `0 30 6 * * *` / `Asia/Seoul` | 매일 06:30 오늘 날짜로. 부하 측정 · 테스트는 `false` |
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
| 보존 배치: 이미 완료한 날짜 | 409 | `RETENTION_ALREADY_COMPLETED` |
| 보존 배치: 같은 날짜가 실행 중 | 409 | `RETENTION_RUNNING` |
| 보존 배치: 내일 이후 날짜 (KST) | 400 | `RETENTION_DATE_IN_FUTURE` |
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

### 게이트웨이 장애 때 (P8-1, `mode=http` — P8-2 부터 기본)
api 가 은행이 아니라 게이트웨이(`apps/bank-gateway`, 8082)를 부른다. 순서대로 본다.
1. **게이트웨이 상태**: `GET :8082/actuator/health/bankConnection` — `UP` 이면 게이트웨이가 은행에 붙어 있다, `DOWN` 이면 은행 쪽 문제(모의 은행 · 포트 · 재접속 로그). 응답이 아예 없으면 게이트웨이 프로세스가 내려가 있다.
2. **api 로그로 "안 보냄" 과 "모름" 을 가른다**:
   - `bank transfer not sent` (INFO) — 은행에 확실히 안 갔다 (게이트웨이 연결 실패 · 연결 풀 대기 초과 · 대기열 초과 · 503 `BANK_NOT_SENT`). 출금은 **재시도 토픽**으로 간다.
   - `bank transfer result unknown` (WARN) — 보냈을 수 있다 (응답 시간 초과 · 응답 전 끊김 · 502 · 504). **결과조회**로 확정한다 (다시 보내지 않는다). 이 로그가 늘면 게이트웨이 ↔ 은행 구간 지연부터 본다.
3. **게이트웨이 로그**: `bank call failed: request=… status=… code=… cause=…` — 상태 · 코드(`BANK_NOT_SENT` · `BANK_TIMEOUT` · `BANK_UNAVAILABLE` · `INVALID_REQUEST` · `INTERNAL`)로 어느 분류였는지 본다.
4. **게이트웨이를 내린 채 출금**하면 재시도 토픽 → DLT → 재발행 한도(약 26분) 뒤 `FAILED E001` 이다. 그 안에 게이트웨이를 올리면 완료된다. 1원인증은 그동안 오류 응답이다 (단일 장애점, [ADR-0015](docs/adr/0015-bank-gateway-service.md)).
5. **점검 설정 두 곳을 같게**: `bank.maintenance.*` 는 api(접수 503)와 게이트웨이(송신 보류) 양쪽에 있다. 어긋나면 api 는 접수하고 게이트웨이는 보류하거나 그 반대가 된다 — 돈은 안전하지만(보류는 "안 보냄") 동작이 헷갈린다.
6. **연결 풀**: `bank.gateway.http.max-connections`(기본 500)를 줄여 둔 경우 동시 호출이 풀 × 3(사용 중 풀 + 대기열 풀 × 2)을 넘으면 즉시, 대기열 안에서도 1초(`pending-acquire-timeout`)를 넘으면 "안 보냄" 이 되어 재시도로 넘어간다 (측정 D, [결과](docs/perf/p8-1-gateway-split.md)).
7. 게이트웨이 포트(8082)는 **인증이 없다** — 내부망 밖에 열지 않는다.

### 자주 막히는 곳
- PowerShell 에서 `bash …` 실패 → Git Bash 로 실행 ([SETUP §3](SETUP.md#3-windows-에서-알아둘-것)).
- Git Bash 에서 `/opt/...` 경로 깨짐 → `MSYS_NO_PATHCONV=1` (`verify.sh` 는 처리돼 있음).

## 7. CI (GitHub Actions `.github/workflows/ci.yml`)
| 잡 | 하는 일 | 실패 시 |
|---|---|---|
| `build` | JDK 21 → `./gradlew build` (테스트 전체) | 테스트 리포트 아티팩트(`test-reports`) 업로드 |
| `infra-smoke` | Compose 로 인프라 기동(`--wait`, 240초) → `verify.sh` → 항상 `down -v` | 컨테이너 로그 출력 |
| `app-smoke` (P8-2, 15분 제한) | 이미지 3개 빌드(GitHub Actions 캐시) → `--profile app up -d --wait` → `infra/app-smoke.sh` (nginx 로 1원인증 · 출금 `COMPLETED` · 분배 · 대사 파일 바인딩) → 항상 `down -v` | 컨테이너 로그 출력 |

`main` push 와 모든 PR 에서 실행된다. PR 은 세 잡이 모두 초록이어야 머지한다.

## 8. 부하 측정 (P4-2, k6)
네 차감 전략의 처리량 · 지연 · 실패율을 같은 조건에서 잰다 (핫스팟 `load-hot` 1명 / 분산 `load-u001` ~ `load-u500`, 50 VU, 워밍업 10초 + 측정 60초). 결과는 [docs/perf/p4-2-lock-comparison.md](docs/perf/p4-2-lock-comparison.md). 컨슈머 처리량 측정(P6-1)은 [§11](#11-컨슈머-모드--코루틴-측정-p6-1).

**준비** (이 PC 에서 앱 · MySQL · Redis · k6 가 CPU 를 나눠 쓰므로 측정 중에는 다른 무거운 작업을 하지 않는다. 다른 API 인스턴스(예: IntelliJ 로 띄운 것)는 모두 끈다 — 같은 DB 를 쓰는 발행기와 컨슈머가 측정용 출금을 은행으로 보내 버린다):
```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait
./gradlew :apps:mock-bank:run --args="9090"        # 별도 터미널
./gradlew :apps:api:bootJar
```

**앱 실행** (전략이 바뀔 때마다 다시 띄운다. 백그라운드 작업(발행기 · 정리 작업 · 일마감 대사 스케줄)을 끄고 로그를 줄인다 — 또는 IntelliJ 실행 구성 `5. API (부하 측정)`):
```bash
java -Xms1g -Xmx1g -jar apps/api/build/libs/api-0.1.0-SNAPSHOT.jar --bank.gateway.mode=direct   --wallet.debit-strategy=conditional   --transfer.outbox.relay.enabled=false --transfer.reconcile.enabled=false   --spring.kafka.listener.auto-startup=false   --server.tomcat.threads.max=200   --logging.level.io.github.hyeokjinon.banktransfer=WARN   --daily-recon.schedule.enabled=false --data-retention.schedule.enabled=false
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

**`RESTART_REFUSED`** — 지난 실행이 `UNKNOWN`(롤백할 수 없는 실패)으로 끝나 Spring Batch 가 재시작을 거부한 것이다. `BATCH_JOB_EXECUTION` 의 `STATUS` 를 확인하고, 원인을 확인한 뒤 `FAILED` 로 바꿔 다시 실행한다. 위 STARTED 의 SQL 을 그대로 쓰면 안 된다 — 그 스텝 UPDATE 는 `STARTING` · `STARTED` 만 바꿔서 `UNKNOWN` 스텝이 그대로 남고, 재시작이 `JobRestartException`("Cannot restart step … from UNKNOWN status") 으로 다시 거부된다. 스텝 조건에 `'UNKNOWN'` 을 넣는다:
   ```sql
   UPDATE BATCH_STEP_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=? AND STATUS IN ('STARTING','STARTED','UNKNOWN');
   UPDATE BATCH_JOB_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=?;
   ```

**동시 시작 때 ERROR 스택은 정상이다** — 실패한 날짜를 두 곳이 동시에 다시 돌리면 나중 실행을 `SingleRunGuard` 가 거부하는데, 이때 Spring Batch 가 "Encountered fatal error executing job" 이라는 **ERROR 로그와 예외 스택**(`ReconRunningException`)을 남긴다. 요청자는 409 `RECON_RUNNING` 을 받고, 먼저 시작한 실행은 계속 돈다. 동시 시작이 의도된 상황이면 무시한다.

### 9-7. 손 확인할 때 주의
- 손 확인은 [`http/daily-recon.http`](http/daily-recon.http), 실행 구성 `6. API (대사 손 확인)` (정리 작업을 꺼서 결과를 모르는 출금이 `UNKNOWN` 으로 남는다).
- **오늘을 대사하려면 API 를 반드시 실행 구성 6 으로 띄운다** — `--daily-recon.allow-today=true` 가 켜져 있어야 오늘 날짜를 받는다. 다른 구성(`2. API (8080)` 등)으로 띄우면 400 `RECON_DATE_NOT_CLOSED` 다. 명령줄로는 `./gradlew :apps:api:bootRun --args="--transfer.reconcile.enabled=false --daily-recon.allow-today=true"`.
- **손으로 오늘을 마감하는 것(`--close-day=오늘`)은 손 확인용이다.** 그 뒤에 처리된 출금은 이미 만들어진 오늘 파일에 없다. 오늘 대사를 아직 돌리지 않았다면 내일 01시 회차가 그 파일로 돌아, 그 출금들이 은행에 없는 것으로 보인다 (`UNKNOWN` 은 `E002` 로 확정 · 환불, `COMPLETED` 는 `COMPLETED_NOT_AT_BANK` 이슈). 손 확인을 끝낸 뒤에는 `bank-files/recon/RECON_오늘.dat` 를 지워 두면 00:05 에 모의 은행이 정상 파일을 만든다 (이미 오늘 대사를 끝냈다면 그 날짜는 "이미 완료" 로 조용히 넘어간다).

## 10. 보존 배치 (P5-2)
근거: [ADR-0011](docs/adr/0011-data-retention.md). 정책표: [database.md §4-9](docs/database.md).

### 10-1. 무엇을 지우나
| 표 | 조건 | 기본 보관 |
|---|---|---|
| `outbox_event` | `published_at < 기준` — 미발행은 남는다 | 7일 |
| `account_verification` | `status IN ('SENT','EXPIRED','LOCKED') AND expires_at < 기준` — **`VERIFIED` 는 지우지 않는다** | 30일 |

기준 = 실행 날짜 00:00(KST) − 보관 기간. 금융 기록(`transfer` · `wallet_ledger` · `wallet` · `bank_statement_line` · `recon_issue`)은 지우지 않는다.

**삭제는 되돌릴 수 없다. 첫 운영 실행 전에는 같은 조건의 `SELECT COUNT(*)` 로 대상을 확인한다** ([database.md §9](docs/database.md)). 건수 미리보기 기능은 없다.

**기본 설정으로 띄운 API 는 아무도 누르지 않아도 매일 06:30(KST) 에 수동 실행과 똑같이 되돌릴 수 없는 삭제를 한다 — 그 API 가 연결된 DB 에, 개발자의 개발 DB 도 포함해서.** 위 `SELECT COUNT(*)` 로 대상을 확인하기 전에는 `--data-retention.schedule.enabled=false` 를 넘겨 스케줄을 끈 채로 띄운다.

### 10-2. 실행
- **스케줄**: 매일 06:30 (KST), 오늘 날짜로. 재시도 회차는 없다 — 다음 날 실행은 새 인스턴스이고 기준이 뒤로 가 밀린 행까지 지운다. 끝난 날짜는 조용히(INFO) 넘어간다.
- **수동**: `POST /admin/retention-runs?date=yyyy-MM-dd` — 요청 스레드에서 끝까지 돈다.

| 응답 | 뜻 |
|---|---|
| 200 `{"date","status":"COMPLETED","reason":null,"deleted":{"outboxEvents":n,"verifications":m}}` | 끝났다. 건수는 이 날짜 인스턴스의 모든 실행을 더한 값 (재시작해도 맞다) |
| 200 `{"date","status":"FAILED","reason":{"code","message"},"deleted":null}` | 요청은 처리됐고 결과가 실패다 |
| 409 `RETENTION_ALREADY_COMPLETED` · `RETENTION_RUNNING` | 끝난 날짜 · 돌고 있음 |
| 400 `RETENTION_DATE_IN_FUTURE` | 내일 이후(KST). 기준 시각이 앞당겨져 정책보다 일찍 지우게 되므로 거부. 오늘과 지난 날은 받는다 (지난 날은 기준이 더 과거라 범위가 좁아질 뿐이다) |

묶음 사이에 쉬지 않는다. 지연 복제가 있는 운영이면 쉬는 시간이 필요하다 (이 프로젝트에는 복제가 없다). 삭제가 늘 돌아도 InnoDB 표 파일은 바로 줄지 않는다 — 공간은 재사용된다.

### 10-3. 알림과 결과 확인
| 언제 | 수준 | 내용 |
|---|---|---|
| 실패 (스케줄 · 수동) | WARN | `보존 배치 {date} 실패` + 이유(예외 종류만, 내부 메시지는 로그) |
| 시작하지 못함 (DB 장애 등) | WARN | 같은 제목 + `시작하지 못함: 예외 클래스` |
| 성공 | 없음 | 표별 건수를 INFO 로그로 (`data retention … completed`) — 대사 요약이 이미 매일 간다 |

실행 이력 · 표별 건수는 [database.md §9](docs/database.md) "보존 배치" 쿼리.

### 10-4. 복구
**실패**: 멈춘 단계 · 지운 만큼은 그대로다. 삭제가 하루 밀려도 급하지 않으니 다음 날 실행에 맡겨도 된다. 그날 날짜를 바로 돌리면 멈춘 단계부터 잇는다.

**실행이 STARTED 로 남음** — 도중에 JVM 이 죽으면 그 실행은 "돌고 있음" 으로 남는다. **다음 날 인스턴스는 막지 않는다** — 그날 날짜를 수동으로 다시 돌릴 때만 409 `RETENTION_RUNNING` 이다. 그 날짜를 꼭 다시 돌려야 할 때만 §9-6 과 같은 SQL 로 푼다:
1. 실행 이력 쿼리로 `STATUS = STARTED` · `END_TIME` NULL 인 `JOB_EXECUTION_ID` 를 찾는다.
2. 다른 API 인스턴스가 정말 돌고 있지 않은지 확인한다.
3. FAILED 로 바꾼다:
   ```sql
   UPDATE BATCH_STEP_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=? AND STATUS IN ('STARTING','STARTED');
   UPDATE BATCH_JOB_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=?;
   ```
4. 같은 날짜를 다시 실행하면 멈춘 곳부터 잇는다.

**`reason.code = ERROR`(`IllegalStateException`)** — 처음 시작하다 죽어 잡 인스턴스만 남았다. §9-6 의 "잡 인스턴스만 있고 실행이 없음" 과 같은 SQL 로 지운다 (`JOB_NAME = 'dataRetentionJob'`).

**`RESTART_REFUSED`** — 지난 실행이 `UNKNOWN` 으로 끝났다. `BATCH_JOB_EXECUTION` 의 `STATUS` 를 확인하고 `FAILED` 로 바꿔 다시 실행한다. 위 STARTED 의 SQL 을 그대로 쓰면 안 된다 — 그 스텝 UPDATE 는 `STARTING` · `STARTED` 만 바꿔서 `UNKNOWN` 스텝이 그대로 남고, 재시작이 `JobRestartException`("Cannot restart step … from UNKNOWN status") 으로 다시 거부된다. 스텝 조건에 `'UNKNOWN'` 을 넣는다:
```sql
UPDATE BATCH_STEP_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=? AND STATUS IN ('STARTING','STARTED','UNKNOWN');
UPDATE BATCH_JOB_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=?;
```

**동시 시작 때 ERROR 스택은 정상이다** — §9-6 과 같다 (`RetentionRunningException`).

### 10-5. 함께 알아 둘 것
- 만료 후 30일이 지난 인증을 확인하면 "만료"(410) 대신 404 `VERIFICATION_NOT_FOUND` 다. 잠긴(`LOCKED`) 인증도 같다 — "잠김"(423 `VERIFICATION_LOCKED`) 대신 404.
- 한 번도 쓰지 않는 `VERIFIED` 는 영원히 남는다 — 인증에 만료 정책이 생길 때 다룬다.
- 손 확인은 [`http/data-retention.http`](http/data-retention.http), Postman 7 폴더. **손 확인의 실행(1) · 7-1)도 시드만이 아니라 그 DB 의 정책에 맞는 모든 행을 영구히 지운다.** 먼저 §10-1 의 `SELECT COUNT(*)` 로 대상을 보고, 옛 행이 소중하면 버려도 되는 별도 DB 에서 한다. 1-1) · 7-2 는 앞 실행이 끝난 뒤에만 보낸다.

## 11. 컨슈머 모드 · 코루틴 측정 (P6-1)
출금 컨슈머는 두 판이 있다 ([ADR-0012](docs/adr/0012-consumer-coroutines.md)). 결과는 [docs/perf/p6-1-consumer-coroutines.md](docs/perf/p6-1-consumer-coroutines.md).

### 11-1. 모드 전환
- `blocking` (기본): 컨슈머 스레드 3개가 은행 응답을 기다린다 — 동시에 기다리는 출금 ≤ 3.
- `coroutine`: suspend 리스너. 은행은 `await`, DB 는 IO 디스패처. 은행에 동시에 보내는 수 ≤ `max-in-flight-bank-calls`(64), 처리 중 메시지 ≤ 컨슈머 3 × `max-poll-records` 50.
- 띄우기: IntelliJ 실행 구성 `7. API (코루틴 컨슈머)` 또는 `./gradlew :apps:api:bootRun --args="--transfer.consumer.mode=coroutine"`. 확인: `curl -s localhost:8080/actuator/info` → `"consumer":{"mode":"coroutine", …}` (`max-in-flight-bank-calls` · `datasource.maximum-pool-size` 도 함께 나온다).
- 알아 둘 것: 같은 파티션 안 처리 순서를 지키지 않는다 (출금은 독립이라 무해). 재시도 토픽 · DLT · 알림 · 로그 문구는 블로킹과 같고, 로그 이름만 `SuspendingTransferRequestedListener` 다. 두 판을 동시에 띄우지 않는다 — 같은 그룹이면 메시지를 나눠 가진다.
- 은행 응답을 기다리는 중 앱을 내리면 그 출금은 `SENDING` 또는 `UNKNOWN` 으로 남고(내릴 때 게이트웨이가 대기 중인 요청을 `ResultUnknown` 으로 실패시키면 `UNKNOWN`) 정리 작업이 30초 뒤 `UNKNOWN` → 결과조회로 확정한다 (다시 보내지 않는다).

### 11-2. 측정 절차
"밀린 N건 비우기" 를 한 회씩 잰다: 정리 · 시드 → 측정 그룹 오프셋을 끝으로 → 발행만(리스너 끔) → 처리만(그 모드 · 그 은행 지연) → DB 시각으로 처리량 · p50 · p95 → 정확성 확인. 스크립트가 API 와 모의 은행을 직접 띄우고 내린다.

**준비** (측정 중에는 빌드 · 테스트 · IDE 인덱싱 같은 무거운 작업을 하지 않는다. 개발용 API · 모의 은행(9090) · IntelliJ 실행은 모두 끈다):
```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait
./gradlew :apps:api:bootJar :apps:mock-bank:installDist
```

**회차 실행** (Git Bash): `bash load/consumer/run.sh <blocking|coroutine> <지연ms 0~2999> <회차> [상한=64] [hikari=20]`
```bash
bash load/consumer/run.sh blocking 100 1
bash load/consumer/run.sh coroutine 100 1             # 상한 64 · Hikari 20
bash load/consumer/run.sh coroutine 100 1 3           # 대조 실험: 상한 3
N=20000 bash load/consumer/run.sh coroutine 100 sweep1 128 20   # 용량 탐색
bash load/consumer/summarize.sh                       # 비교 · 용량 탐색 중앙값 표
```
- N 기본값: 5,000, blocking 100ms 1,500, blocking 500ms 400 (대조 실험은 blocking 과 같은 값). 처리 시간이 20초보다 짧으면 스크립트가 알린다 — 그 칸은 `N=…` 을 늘려 다시 잰다.
- 측정 API 는 발행기(처리 단계) · 정리 작업 · 일마감 대사 · 보존 배치 스케줄 · 은행 점검 시간을 끄고, 측정 전용 컨슈머 그룹 `transfer-processor-p61` 을 쓴다 (개발용 그룹 `transfer-processor` 의 오프셋은 건드리지 않는다). 모의 은행은 포트 9190, 회차마다 새 거래 기록 디렉터리(`build/load-p61/<이름>/bank-files`).
- 정확성: N건 모두 `COMPLETED`, 모의 은행 거래 기록에 거래번호마다 정확히 한 번, 환불 0, 잔고 = 원장 합, 미발행 아웃박스 0. 하나라도 어긋나면 결과가 `.json.invalid` 로 바뀌어 표에서 빠진다. 원본은 `docs/perf/raw/p6-1/`, 로그 · CPU 표본은 `build/load-p61/<이름>/`.
- 스크립트는 띄운 앱의 모드 · 상한 · 풀 크기를 `/actuator/info` 로 확인한 뒤에만 잰다. 8080 · 9190 이 이미 쓰이거나 개발용 그룹에 붙은 컨슈머가 있으면 시작하지 않는다.
- 이전 회차에서 죽은 API 의 컨슈머 그룹 멤버가 남아 있으면 새 API 가 파티션을 배정받기까지 최대 약 45초 걸린다 — 스크립트가 기다린다.
- **P8-1 게이트웨이 측정** (환경 변수, 안 주면 위와 똑같다 — 원자료 이름도 같다): `GATEWAY=http` 면 처리 단계 API 가 별도 게이트웨이 앱(`apps/bank-gateway`, 포트 **18082**)을 HTTP 로 부르고, 게이트웨이가 측정용 모의 은행(9190)에 붙는다. `SERVER=reactive|servlet`(게이트웨이 서버 판) · `CLIENT=webclient|jdk`(API 의 HTTP 클라이언트) · `GW_POOL=n`(WebClient 최대 연결) · `NET_DELAY=ms`(응답 방향 지연, Toxiproxy). 원자료 이름 끝에 `-gw-<server>-<client>[-p<풀>][-n<ms>ms]` 가 붙으므로 `RAW_SUB=p8-1` 과 함께 쓴다. 먼저 `./gradlew :apps:bank-gateway:bootJar`, `NET_DELAY` 면 `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile chaos up -d toxiproxy` (제어 8474, API 는 프록시 **28082** → 게이트웨이 18082 로 간다). 끝나면 `... --profile chaos stop toxiproxy`. 스크립트는 회차가 끝나면 프록시(`p81-gateway`)를 지우고, 남았는지는 `curl -s localhost:8474/proxies` (`{}` 이어야 한다). 결과 JSON 에 `gateway` · `gw_cpu_avg` · `gw_cpu_max` · `gw_threads_max` 가 더해지고, 게이트웨이 스레드 덤프는 `build/load-p61/<이름>/gw-threads.txt` (servlet 판은 `http-nio-18082-exec-*`, WebFlux 판은 `reactor-http-nio-*`). `RAW_SUB=p8-1 bash load/consumer/summarize.sh` 가 게이트웨이 열이 붙은 표를 만든다. 처리 구간이 짧으면(N 이 작으면) `gw_cpu_*` 는 `null` 일 수 있다 — N 을 늘린다.

> **끝나면 반드시** `bash load/consumer/run.sh cleanup`. 측정 출금은 `COMPLETED` 로 남는데, 개발용 은행 대사 파일에는 없어 다음 날 개발용 API 의 일마감 대사가 어긋남으로 잡는다. 이 명령은 측정 사용자 정확히 100명(`load-p61-u001` ~ `u100`)의 데이터만 지운다.
>
> **개발용 그룹의 밀린 메시지**: 측정이 발행한 메시지는 측정 그룹뿐 아니라 개발용 그룹 `transfer-processor` 에도 밀린 메시지(lag)로 쌓인다. 다음에 개발용 API 를 띄우면 그걸 다 읽어(사용자가 없어 `NOT_FOUND` 로 지나간다) 수십만 건이면 새 개발 출금이 `RECEIVED` 로 한참 멈춰 보인다. `cleanup` 은 이 그룹을 건드리지 않고, 밀린 게 있으면 안내만 한다. 그때 **개발용 API 를 끈 채로 사람이 직접** `bash load/consumer/run.sh reset-dev-group` — 읽기 전용 사전 확인(측정 사용자 밖의 미발행 아웃박스 0 · `RECEIVED`/`SENDING` 출금 0 · 8080 비어 있음 · 그룹에 컨슈머 없음)을 통과해야만 `transfer-processor` 를 `transfer.requested` 의 끝으로 옮긴다(재시도 · DLT 토픽은 그 그룹이 오프셋을 가졌을 때만). 묻지 않고, 조건이 안 맞으면 아무것도 바꾸지 않고 멈춘다. 전후 lag 를 출력한다.

> **사고 기록 (첫 시험 실행)**: 발행 단계의 발행기는 측정 사용자의 행만이 아니라 **미발행 아웃박스 전부**를 릴레이한다. 첫 시험 실행에서 개발 DB 에 남아 있던 손 확인 출금 6건(`user-retention`)이 함께 발행돼 측정용 모의 은행으로 처리됐다. 지금은 `run.sh` 가 시작 전에 읽기 전용으로 검사해서, `load-p61-*` 가 아닌 미발행 아웃박스 행이나 `RECEIVED` · `SENDING` 인 다른 출금이 있으면 시작하지 않고 멈춘다. 멈추면 그 행을 먼저 정리한 뒤 다시 한다 — 검사를 우회하려고 스크립트를 고치지 않는다. 그 6건은 측정용 모의 은행으로 처리돼 개발용 은행 거래 기록에 없다 → 개발용 API 의 2026-11-04 일마감 대사가 `COMPLETED_NOT_AT_BANK` 6건을 보고할 수 있다 (시험 데이터라 예상된 결과, 조치 불필요).

- 시험 실행(N=300, 지연 20ms, 워밍업 포함 짧은 실행이라 결과가 아니다): blocking 39.5 tps · p95 48ms · 은행 동시 대기 최댓값 3 / coroutine 상한 64 는 210.7 tps · p95 591ms · 최댓값 64. 스크립트가 끝까지 도는지 보는 용도다.
- CI 에서는 돌리지 않는다 (CI 는 두 모드의 정확성 테스트만).

## 12. 웹 1원인증 판 · WebFlux 측정 (P6-2)
1원인증 요청은 네 판이 있다 ([ADR-0013](docs/adr/0013-web-verification-webflux-comparison.md)). 결과는 [측정 결과](docs/perf/p6-2-web-reactive.md) (2026-11-05).

### 12-1. 판 고르기
- `apps/api` (8080): `--account.verification.web-mode=async` (기본, 서블릿 비동기) | `blocking` (Tomcat 스레드가 끝까지 기다리는 대조군 — **측정 전용, 운영에서 쓰지 않는다**). 실행 구성 `10. API (1원인증 블로킹)`.
- `apps/api-reactive` (8081): `--account.verification.web-mode=coroutine` (기본) | `reactor`. 실행 구성 `8. API 리액티브 (코루틴)` · `9. API 리액티브 (Reactor)`. 이 앱은 1원인증 요청 하나만 받는다 (확인 `/confirm` 은 `apps/api`, 같은 DB).
- 값은 소문자 그대로만 받고 그 밖은 기동 실패. 지금 값은 `curl -s localhost:8080/actuator/info` (`apps/api`: `web-mode` · Tomcat 최대 스레드) · `localhost:8081/actuator/info` (리액티브: `web-mode` · Hikari 최대 풀).
- 리액티브 앱은 스키마를 만들지 않는다 — `apps/api` 를 한 번 띄워 Flyway 가 돈 뒤에 띄운다. 두 앱의 Hikari 최대 풀은 같은 20.

### 12-2. 측정 한 회
열린 모델(k6 `constant-arrival-rate`): 워밍업 10초 + 측정 30초, 요청 시간 초과 10초. 스크립트가 측정용 모의 은행과 앱을 직접 띄우고 내린다.

**준비** (측정 중에는 빌드 · 테스트 · IDE 인덱싱 같은 무거운 작업을 하지 않는다):
```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait
./gradlew :apps:api:bootJar :apps:api-reactive:bootJar :apps:mock-bank:installDist
```

**회차 실행** (Git Bash, 저장소 루트): `bash load/verification/run.sh <blocking|async|coroutine|reactor> <지연ms 0~2999> <도착률 1~5000> <회차> [톰캣 최대 스레드=200]`
```bash
bash load/verification/run.sh async 500 400 1
bash load/verification/run.sh blocking 500 400 1 400     # 원인 확인: Tomcat 400
bash load/verification/summarize.sh                       # 중앙값 표
```
- 측정 포트: 앱 `18080`(`apps/api`) · `18081`(`apps/api-reactive`), 측정용 모의 은행 `9190`(회차마다 새 거래 기록 디렉터리). 개발용 API(8080) · 개발용 모의 은행(9090)은 켜 둬도 된다. 측정 포트가 이미 쓰이면 시작하지 않고, 스크립트가 띄우지 않은 포트의 프로세스는 종료하지 않는다 (코드 확인됨, 커밋 45f54d8).
- `apps/api` 측정 인스턴스는 Kafka 리스너 · 발행기 · 정리 작업 · 대사 · 보존 스케줄을 끈다 (개발용 컨슈머 그룹 메시지를 나눠 갖지 않게). 켜진 채로는 시작하지 않는다.
- 측정 사용자는 `p62-u001` ~ `u100` 뿐이고 계좌는 모의 은행 기본 계좌다. 시작 전 읽기 전용 사전 확인: 포트 · `/actuator/info` 의 `web-mode` · 풀이 인자와 같은지 · 측정 사용자 행이 비어 있는지.
- 결과: `docs/perf/raw/p6-2/<판>-d<지연>-r<도착률>[-t<톰캣>]-<회차>.json`, 로그 · 표본은 `build/load-p62/<이름>/`. 회차가 숫자면 본 측정 표, `probe` 로 시작하면 한계 탐색 표, 그 밖(`smoke`)은 표에서 빠진다.
- 표본: `jvm.threads.live` · CPU 는 1초 쉬고 지표 3개를 차례로 읽는다 — 앱이 포화되면 같은 서버를 거쳐 표본이 크게 성겨진다 (P6-2: 포화 칸 1 ~ 5개, 정상 칸은 약 1.4 ~ 1.5초 간격). curl 에는 `--max-time 1` 을 둔다. 표본이 10개 미만인 칸의 스레드 최대 · CPU 는 약하게 읽는다.
- **P8-1 게이트웨이 측정** (`async` 판만 — 묶음 E): `GATEWAY=http [SERVER=reactive|servlet] [CLIENT=webclient|jdk] [GW_POOL=n] [NET_DELAY=ms]` 를 앞에 붙이고 `RAW_SUB=p8-1` 을 함께 준다. 안 주면 위와 똑같다 (원자료 이름도). 먼저 `./gradlew :apps:bank-gateway:bootJar`, `NET_DELAY` 면 `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile chaos up -d toxiproxy` 를 띄우고 끝나면 `... --profile chaos stop toxiproxy`. 측정 포트에 게이트웨이 **18082** · Toxiproxy 프록시 **28082**(제어 8474)가 더해진다. 앱 지표와 별도로 게이트웨이 앱의 CPU · `jvm.threads.live` 를 같은 측정 구간으로 표본해 JSON 의 `gw_cpu_avg` · `gw_cpu_max` · `gw_threads_max` 에, 스레드 덤프는 `build/load-p62/<이름>/gw-threads.txt` 에 남긴다. 회차가 끝나면 프록시(`p81-gateway`)를 지운다 — 남았는지는 `curl -s localhost:8474/proxies` (`{}` 이어야 한다). `RAW_SUB=p8-1 bash load/verification/summarize.sh` 가 게이트웨이별 표를 만든다.
- `dropped_iterations` 는 워밍업을 포함한 실행 전체의 값이다. 앱이 느려 VU 가 모자란 경우(`vus_max` 가 `maxVUs` 에 닿음)와 부하 발생기 한계를 나눠 읽는다.
- **정확성**: 시간 초과(k6 10초)가 0 이면 `201 수 = 행 수 = 1원 이체 수`. 시간 초과 T > 0 이면 `0 ≤ 행 수 − 201 수 ≤ T` 이고 `0 ≤ 1원 수 − 행 수 ≤ T` 일 때만 유효하다 — 블로킹 판이 천장을 넘으면 Tomcat 큐 대기가 시간 초과를 넘는 것이 예상된 결과이고, 클라이언트가 끊어도 MVC 는 끝까지 진행해 행을 저장하고(201 < 행), WebFlux 는 취소돼 1원만 나갈 수 있기 때문이다(행 < 1원). 그 밖은 `.json.invalid` 로 바뀌어 표에서 빠진다. `timeouts` · `rows_minus_created` · `one_won_minus_rows` 는 결과 파일에 남는다 (끊김 비용).

> **끝나면 반드시** `bash load/verification/run.sh cleanup` — 측정 사용자 `p62-u001` ~ `u100` 의 1원인증 행만 지운다 (다른 행은 건드리지 않는다). 그대로 두면 30일 뒤 보존 배치 대상이다.

### 12-3. 측정 순서
- 본 측정 72회: 4판 × 은행 지연 100 · 500ms × 도착률 100 · 200 · 400건/s × 3회, 칸마다 중앙값.
- 원인 확인 3회: MVC 블로킹 · Tomcat 최대 스레드 400 · 지연 500 · 도착률 400 — 천장이 스레드 수를 따라 움직이는지.
- 한계 탐색: 비동기 세 판 · 지연 500 · 도착률 800 → 1600 → … 성공률 99% 미만 · 201 의 p99 3초 초과 · 시스템 CPU 80%(측정 구간 평균) 중 하나에 닿으면 멈춘다.
- 판단 기준: 비동기 세 판 처리량 차이 5% 안이면 "같다". 운영 기본은 MVC 비동기 그대로 (ADR-0013).
- CI 에서는 돌리지 않는다 (CI 는 판별 테스트와 오류 계약 테스트).

### 12-4. 끊김
리액티브 앱 로그에 `1원인증 요청 취소 (stage=transfer)` 경고가 보이면 1원 이체 응답을 기다리는 중 클라이언트가 끊은 것이다 (1원이 나갔을 수 있다, 계좌는 끝 4자리만 남는다). 할 일은 없다 — 사용자가 다시 요청한다 (최악에 1원 두 번). 자주 보이면 클라이언트 시간 초과가 은행 응답 시간(게이트웨이 3초)보다 짧은지 본다. 성명조회 중 끊김은 1원이 나가지 않아 경고가 없다 (끊김 테스트가 1원 없음을 확인).

**틈**: 1원 이체 응답을 받은 뒤 저장이 시작되기 전(코루틴 IO 디스패처 대기 · Reactor `publishOn` 큐)에 취소되면 1원은 나갔는데 행도 경고도 없다. 부하가 높으면 넓어진다 (추정), P6-2 측정에서는 관측 없음(`one_won == rows`). 일마감 대사도 1원인증 줄(V 거래번호)은 대조하지 않고 건수만 세므로 개별로 보이지 않는다 — 은행 V 줄 건수와 우리 행 수의 차이로만 짐작한다 (코드 확인됨 / 추정).

## 13. 가상 스레드 측정 (P7)
가상 스레드는 기본으로 꺼져 있다 ([ADR-0014](docs/adr/0014-virtual-threads.md) — Java 21 에서는 끈 채로). 결과는 [측정 결과](docs/perf/p7-virtual-threads.md) (2026-11-09).

### 13-1. 켜고 끄기
- 켜기: 앱 인자 `--spring.threads.virtual.enabled=true` (IntelliJ 실행 구성의 프로그램 인자 또는 `java -jar apps/api/build/libs/api-0.1.0-SNAPSHOT.jar --spring.threads.virtual.enabled=true`). 바뀌는 곳: Tomcat 요청 · 앱 실행기 · `@Scheduled` · Kafka 리스너 (실험 V1).
- 확인: `curl -s localhost:8080/actuator/info` → `"threads":{"virtual":"true"}`.
- **켠 채로 막 띄운 앱에 동시 요청을 곧바로 몰지 않는다** — Java 21 에서 첫 응답 직렬화기 생성(`synchronized`)에 가상 스레드가 몰려 앱이 통째로 멈출 수 있다 (냉시동 교착, 결과 문서 §6). 손 확인처럼 한 건씩 보내면 괜찮다 — 단 냉시동만의 문제가 아닐 수 있다: 처음 보는 타입을 직렬화하는 순간(예: 처음 보는 오류 응답이 몰릴 때) 동시 요청이 캐리어 수(논리 코어 수)를 넘으면 같은 모양이 날 수 있다 (추정). 멈췄으면 JFR(`jdk.VirtualThreadPinned` 는 `synchronized` 입장 대기를 세지 않는다)이 아니라 **스레드 덤프**로 본다: `jcmd <PID> Thread.dump_to_file -format=text <파일>` (가상 스레드 포함, PID 는 `jcmd -l`).

### 13-2. 측정 한 회
P6-2 · P6-1 도구(§12 · §11)에 환경변수 두 개가 더해졌다:
```bash
RAW_SUB=p7 VT=1 bash load/verification/run.sh blocking 500 400 1   # 웹, MVC 판(blocking · async)만
RAW_SUB=p7 VT=1 bash load/consumer/run.sh blocking 100 1           # 컨슈머, blocking 만
bash load/p7/summarize.sh                                           # 웹 · 컨슈머 표 두 개
```
- `VT=1`: 앱에 `--spring.threads.virtual.enabled=true`, JVM 에 `-XX:StartFlightRecording=name=p7,settings=default,filename=<작업 폴더>/app.jfr` (pinning 기준 20ms). 앱을 내리기 **전에** `jcmd <PID> JFR.dump` → `jfr summary` 로 `jdk.VirtualThreadPinned` 수를 세어 JSON `pinned_events` 에 넣고, 0 이 아니면 스택을 `<작업 폴더>/pinned.txt` 에 남긴다. 덤프 · 집계가 실패하면 0 으로 쓰지 않고 멈춘다. 그 밖의 판에 `VT=1` 이면 인자 오류(exit 2). 컨슈머는 처리 단계 앱에만 붙인다.
- `RAW_SUB`: 원자료 폴더 `docs/perf/raw/<RAW_SUB>` (기본: 웹 `p6-2` · 컨슈머 `p6-1`). P7 은 끈 판 · 켠 판 모두 `p7`.
- 결과 이름에 `-vt` (`blocking-vt-d500-r400-1`, `blocking-vt-d100-h20-1`), JSON 에 `virtual_threads` · `pinned_events`(끈 회차는 `null`) · 웹은 `warmup`.
- **웹 회차는 k6 전에 1원인증 3건을 순차로 보낸다** (모든 판, 냉시동 교착을 피해 정상 상태를 잰다). 정확성은 워밍업 뒤 기준값과의 차이로 본다.
- 포트 · PID 찾기는 `load/lib/proc.sh` 다 (`/dev/tcp` 접속 시험 · `jcmd -l`). 이 PC 의 `netstat` 은 0.0.0.0 대기 포트를 보여 주지 않는다 — `load/run.sh`(P4-2)는 아직 netstat 을 쓴다.
- Git Bash 함정: curl 본문은 파일로 (`--data-binary @파일`, 한글은 `\uXXXX`), `-o /dev/null` 대신 작업 폴더 파일, `taskkill /F` (`//F` 아님).

### 13-3. 측정 순서
```bash
export RAW_SUB=p7
for round in 1 2 3; do
  bash load/verification/run.sh blocking 500 400 $round
  VT=1 bash load/verification/run.sh blocking 500 400 $round
  bash load/verification/run.sh async 500 400 $round
  VT=1 bash load/verification/run.sh async 500 400 $round
  bash load/verification/run.sh coroutine 500 400 $round
  VT=1 bash load/verification/run.sh blocking 500 800 $round
  bash load/verification/run.sh async 500 800 $round
  VT=1 bash load/verification/run.sh async 500 800 $round
  bash load/verification/run.sh coroutine 500 800 $round
done
VT=1 bash load/verification/run.sh blocking 500 400 drift
bash load/verification/run.sh cleanup

# 컨슈머 — 개발용 API(8080)를 끈 뒤
for round in 1 2 3; do
  bash load/consumer/run.sh blocking 100 $round
  VT=1 bash load/consumer/run.sh blocking 100 $round
done
bash load/consumer/run.sh cleanup
```
- 웹 약 40분, 컨슈머 약 15분. 판단 기준(ADR-0014): pinning 0 · 웹 VT 판 ≥ async(끔) · 컨슈머 ±5%.

### 13-4. 끝난 뒤
- `cleanup` 두 개는 반드시 (§12 · §11 과 같다 — 측정 사용자 데이터만 지운다).
- 컨슈머 측정은 개발용 그룹 `transfer-processor` 에 밀린 메시지를 남긴다 — §11 의 `reset-dev-group` 안내를 따른다 (개발용 API 를 끈 채 사람이 직접).
- 회차별 JFR · 표본 · 로그는 `build/load-p62/<이름>/` · `build/load-p61/<이름>/` 에 남는다 (커밋하지 않음). `jfr print --events jdk.VirtualThreadPinned <app.jfr>` 로 다시 볼 수 있다.

## 14. 컨테이너 실행 (P8-2)
Compose `app` 프로필: nginx(8000) → api-1 · api-2 → bank-gateway → mock-bank. 이유 · 숫자는 [ADR-0016](docs/adr/0016-app-images-compose.md) · [측정 결과](docs/perf/p8-2-compose.md). 모든 명령은 저장소 루트의 Git Bash 에서.

### 14-1. 띄우기 · 확인 · 내리기
```bash
# 먼저 IntelliJ 의 API · 게이트웨이 · 모의 은행을 모두 끈다 (같은 DB · 같은 컨슈머 그룹 — 출금이 다른 모의 은행으로 흘러 대사가 어긋난다)
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app up -d --build --wait   # 이미지 빌드 + 기동, healthy 까지 대기
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app ps                      # 5개(mock-bank · bank-gateway · api-1 · api-2 · nginx)가 healthy
bash infra/app-smoke.sh                                                                  # 1원인증 · 출금 · 멱등 · 분배 · 포트 · 이미지 비밀값
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app stop nginx api-1 api-2 bank-gateway mock-bank   # 앱만 멈춤 (인프라 유지)
```
- **기동 순서** (healthy 기준): MySQL · Kafka · Redis → mock-bank → bank-gateway(은행 연결 UP) → api-1 · api-2(readiness) → nginx. 두 api 가 동시에 떠도 Flyway 는 DB 잠금으로 한 번만 적용한다.
- **IntelliJ 의 API 와 동시에 띄우지 않는다.** 컨테이너 api 도 **06:30 보존 배치를 개발 DB 에 돌린다** (되돌릴 수 없는 삭제 — HANDOVER §3 경고와 같다).
- PC 에 열리는 포트는 nginx 8000 하나이고 `127.0.0.1` 에만 열린다 (인증 없는 `/admin/*` 가 LAN 에 안 열리게). 게이트웨이 · 모의 은행 · api 는 내부망 전용이다.

### 14-2. 로그 · 안에서 보기
```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app logs -f api-1                                                    # 한 대의 로그
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app logs mock-bank                                                    # 1원인증 적요 코드
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app exec bank-gateway wget -qO- localhost:8082/actuator/health        # bankConnection 포함
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app exec api-1 wget -qO- localhost:8080/actuator/health/readiness
curl -s localhost:8000/actuator/info                                   # 두 번 이상 — "instance" 가 api-1 · api-2 로 번갈아
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app logs nginx --since 10m                    # nginx 접근 · 오류 로그 (-f 로 따라 보기)
```
- nginx 접근 로그 열 (`log_format upstream`): `$msec $status $request_method $uri $upstream_addr $upstream_status $request_time` = 받은 시각(초.밀리초) · 상태 · 메서드 · 경로 · 어느 api(IP:포트) · 그 api 의 상태 · 걸린 시간(초). 실패해 다른 서버로 넘어갔으면 `$upstream_addr` · `$upstream_status` 에 `, ` 로 두 값이 나온다. 접근 로그는 stdout, 오류 로그는 stderr 로 나가므로 같은 `logs nginx` 에 둘 다 섞여 나온다 (오류 줄은 `YYYY/MM/DD … [error]` 로 시작). `exec nginx cat /var/log/nginx/…` 는 파일이 아니라 장치로 연결돼 있어 아무것도 안 나온다.
- 응답 헤더 `X-Upstream` 도 어느 api 가 받았는지 알려 준다 (IP:포트, 이름은 `/actuator/info` 의 `instance`).
- `/nginx-health` 는 nginx 자신만 본다 — api 가 살았는지는 `ps` 의 healthy 나 `/actuator/info` 로 본다.

### 14-3. 롤링 재시작 (한 대씩)
```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app restart api-1
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app ps                       # api-1 이 healthy 가 될 때까지 (약 30초)
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app restart api-2
```
- 한 대씩, 앞의 것이 healthy 가 된 뒤에 다음. 내려가는 동안 nginx 가 다른 한 대로 보낸다 (E3: 첫 시도 5xx 0).
- **graceful**: 앱은 `server.shutdown=graceful` 로 처리 중인 요청을 끝내고 Kafka 리스너를 정리한 뒤 내려간다. `spring.lifecycle.timeout-per-shutdown-phase: 20s` 는 **종료 단계 하나당** 상한이고 Compose `stop_grace_period` 는 30s 다 — "20s < 30s" 는 단계 하나씩 봤을 때 성립한다 (단계들이 모두 상한까지 쓰는 최악에는 합이 30s 를 넘어 Docker 가 SIGKILL 할 수 있다).
- **게이트웨이 종료 순서 (P8-3b)**: Compose · IntelliJ (선출 꺼짐) 종료 순서가 바뀜: SIGTERM 에 웹 graceful 전에 새 요청 503 BANK_NOT_SENT → 처리 중 전문 최대 5초 비우기 → 끊기 (예산 5 + 20 < 30초 `stop_grace_period`).
- **설정을 바꿔 `up -d` 로 컨테이너를 다시 만들면 `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app restart nginx` 도 한다** — nginx 는 기동 때 upstream 이름(api-1 · api-2)을 한 번만 풀어서, 컨테이너 IP 가 바뀌면 옛 IP 로 보내 502 가 난다. (`load/compose/run.sh` 는 재시작 · 강제 종료 회차 뒤마다 nginx 를 재시작해 이름을 다시 풀게 한다.)

### 14-4. 컨테이너가 죽었을 때
- **nginx**: 연결이 안 되면 연결 시간 초과(1s) 뒤 다음 서버로 보낸다 (`max_fails=3 fail_timeout=10s` — 10초마다 한 번씩 다시 시험하고 그 요청만 1s 늦다). 연결 단계 실패만 다시 보내고, **이미 보낸 POST 는 다시 보내지 않는다** — 그 요청은 502 이고, 클라이언트가 **같은 `Idempotency-Key`** 로 다시 보내면 한 번만 접수된다 (E4 kill-3: 502 1건 → 1초 뒤 같은 키 재전송 → 접수 1).
- **Kafka**: 정상 종료(restart)는 그룹에서 정상 탈퇴해 1 ~ 3초 안에 다른 api 가 파티션을 넘겨받는다 (E3: 0.96s · 3.2s). **강제 종료(SIGKILL · OOM · 호스트 장애)는 브로커가 세션 시간 초과(`session.timeout.ms` 기본 45초)를 기다린 뒤에야 파티션을 넘긴다** (E4: 45.6s). 그동안 죽은 api 의 파티션(3개 중 1개 몫, 메시지의 약 1/3)은 처리가 늦다 — kill-3 에서 1201건 중 268건(22%)이 5s 넘게, 최대 45.04s. **잃는 것은 없다** (은행 누락 · 중복 0).
- **`SENDING` 으로 남은 건**은 정리 작업이 30초 뒤 결과조회로 확정한다 (재전송 없음 — ADR-0006).
- **자동 재시작**: `restart: unless-stopped` 는 실제 크래시 · OOM 이면 다시 띄운다. 그러나 `docker kill` 은 "사람이 멈춤" 으로 보아 **다시 띄우지 않는다** (E4 3회 모두) — 실험 뒤에는 `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app up -d` 로 올린다.
- api-1 이 `unhealthy` 로 멈추면 `logs api-1` (DB · Kafka 연결) 과 `docker stats` (메모리 한도 768m) 를 본다.

### 14-5. 하루 배치 락 (ShedLock)
대사(`daily-recon`)와 보존(`data-retention`)은 두 api 중 **한 대만** 돈다.
```sql
SELECT name, locked_by, locked_at, lock_until, lock_until > UTC_TIMESTAMP(3) AS held FROM shedlock;
```
- **값은 UTC** (KST 로 읽으려면 +9시간). `locked_by` 는 잡은 서버(api-1 · api-2, IntelliJ 는 `local`). `held = 1` 이면 아직 잡혀 있다.
- 락이 남아 배치가 안 돈다고 의심되면: **`lock_until` 이 지나면 저절로 풀린다** (대사 50분 · 보존 30분, 끝나면 최소 1분). 급하면 그 행의 `lock_until` 을 `UTC_TIMESTAMP(3)` 로 바꾼다 — **다른 서버가 실제로 돌고 있지 않은지 로그로 먼저 확인**한다 (락은 1차 방어일 뿐 대사 · 보존은 두 대가 돌아도 돈이 틀어지지 않게 멱등이지만, 일부러 겹치게 하지 않는다).
```sql
UPDATE shedlock SET lock_until = UTC_TIMESTAMP(3) WHERE name = 'daily-recon';
```

### 14-6. 실험 도구 (`load/compose/`)
api 2대에 출금을 일정 속도로 넣으면서 한 대를 재시작(E3) · 강제 종료(E4)한다. k6 컨테이너(`p82-k6-<회차>`)는 nginx 로 보낸다.
```bash
bash load/compose/run.sh steady 1      # 기준 회차 (아무것도 안 함)
bash load/compose/run.sh restart 1     # api-1 재시작
bash load/compose/run.sh kill 1        # api-1 강제 종료 (끝나면 run.sh 가 올린다)
bash load/compose/run.sh cleanup       # 끝나면 반드시 — load-p82-u001 ~ u020 의 행만 지운다
```
- 실험 · 스모크 뒤 컨테이너 모의 은행의 `bank-files` 볼륨에는 거래 기록이 남지만 DB 행은 지워진다 → 그 날짜로 `--close-day` · 대사를 하면 "거래번호 모름" 이슈가 대량으로 나온다. 그 날짜는 대사하지 않거나 볼륨을 지운다 (`docker compose … --profile app down -v` 는 DB 볼륨까지 지우므로 주의 — `bank-files` 만 지우려면 앱 컨테이너를 내린 뒤 `docker volume rm bank-transfer_bank-files`).
- 환경변수: `RATE`(초당 출금, 20) · `DURATION`(부하 시간, `60s`) · `ACTION_AT`(동작 시각 초, 20 — `DURATION` 보다 작아야 한다) · `SETTLE`(완료 대기 최대 초, 240).
- 정확성 확인(접수 키 > 0, 접수 + 포기 = 반복 수, 출금 행 = 거래번호 수, 은행 누락 · 중복 0, 잔고 = 원장 …)을 통과하지 못하면 요약이 `.json.invalid` 로 남는다.
- 결과: `docs/perf/raw/p8-2/<동작>-<회차>.json` (요약) · `.k6.json` · `.nginx-errors.txt` · `.rebalance.txt`, 작업 파일은 `build/load-p82/<동작>-<회차>/`. 먼저 앱을 띄우고 IntelliJ 의 API 는 끈다 (14-1).

## 15. Kubernetes 실행 (P8-3a)
> 게이트웨이는 P8-3b 부터 2대 active-standby 다 (§16, ADR-0018 채택) — 아래 §15 의 "게이트웨이 1대 · `Recreate`" 설명은 P8-3a 시점이며 이전 구성(H0)이다.

kind 클러스터(제어 1 + 워커 2): PC `127.0.0.1:8000` → 제어 노드 NodePort 30080 → Envoy → Service `api` → api Pod ×2(워커) → bank-gateway(워커) → mock-bank(제어 노드), 인프라(MySQL · Kafka · Redis)는 제어 노드. 이유 · 숫자는 [ADR-0017](docs/adr/0017-kubernetes-kind.md) · [측정 결과](docs/perf/p8-3a-k8s.md), 파일 · 막히는 곳은 [infra/k8s/README.md](infra/k8s/README.md). 모든 명령은 저장소 루트의 Git Bash 에서. 아래 `kubectl` 은 `--context kind-bank-transfer` 를 줄여 적었다 (여러 클러스터를 쓰면 붙인다).

### 15-1. 띄우기 · 확인 · 내리기
```bash
# 먼저 IntelliJ 의 API · 게이트웨이 · 모의 은행을 끄고, Compose app 프로필도 내린다 (둘 다 127.0.0.1:8000 — §14)
bash infra/k8s/up.sh                       # 클러스터 · Envoy Gateway · 이미지 빌드 · kind load · Secret · 매니페스트 · 준비 대기
bash infra/k8s/smoke.sh                    # [0] ~ [8]: 입구로 1원인증 · 출금 · 멱등 · 분배 · 분산 배치 · 게이트웨이 · 비root · NetworkPolicy
kubectl -n bank-transfer get pods -o wide  # 7개 Pod Running — api 두 개가 서로 다른 워커
curl -s localhost:8000/actuator/info       # 두 번 이상 — "instance" 가 api Pod 이름 두 개로 번갈아
bash infra/k8s/down.sh                     # 클러스터 삭제 (데이터까지)
```
- 클러스터 DB · Kafka 는 **개발 DB 와 별개**다 (클러스터 안 PV). IntelliJ 의 API 와 부딪치지 않지만, Postman 의 base URL 은 `http://127.0.0.1:8000` 이다.
- **Compose `app` 프로필과 동시에 띄우지 않는다** (같은 8000). 내리기: `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app down`.
- 앱 입구는 `127.0.0.1:8000` 하나다 (kind API 서버도 `127.0.0.1` 의 임의 포트에 열린다 — kubectl 용). 8080 · 8082 · 9090 은 연결 거부여야 한다 (Service 는 전부 ClusterIP).
- 다시 부르면(`up.sh`) 클러스터는 그대로 두고 이미지 · 매니페스트만 갱신한다. 비밀번호를 `infra/.env` 에서 바꿔도 MySQL PV 가 이미 초기화됐으면 DB 에 반영되지 않는다 — `down.sh` 부터.

### 15-2. 로그 · 안에서 보기
```bash
kubectl -n bank-transfer logs -l app=api --prefix --tail=100            # 두 api Pod 의 로그 (--prefix 로 Pod 이름 구분, -f 로 따라 보기)
kubectl -n bank-transfer logs deploy/mock-bank | grep BT                # 1원인증 적요 코드 (BT + 4자리)
kubectl -n bank-transfer exec deploy/bank-gateway -- wget -qO- http://127.0.0.1:8082/actuator/health/readiness   # readinessState + bankConnection
kubectl -n bank-transfer exec deploy/api -- wget -qO- http://127.0.0.1:8080/actuator/health/readiness
kubectl -n bank-transfer describe pod -l app=bank-gateway               # 프로브 실패 · 재시작 이유 (Events)
kubectl -n bank-transfer get events --sort-by=.lastTimestamp | tail -20
kubectl -n envoy-gateway-system logs deploy/envoy-gateway --tail=100    # 입구 컨트롤러
```
- 게이트웨이 readiness 응답이 `DOWN` 이면 은행 연결이 없는 것이다 (15-5). `/actuator/health/liveness` 는 은행과 무관하게 `UP`.
- 어느 api 가 받았는지는 `/actuator/info` 의 `instance` (= Pod 이름). Envoy 프록시 로그는 `kubectl -n envoy-gateway-system logs -l app.kubernetes.io/name=envoy --tail=50`.

### 15-3. 배포 (롤링 · 태그)
```bash
bash infra/k8s/up.sh                       # 코드를 고친 뒤 — 이미지를 다시 만들고 새 태그로 apply (api 롤링, 게이트웨이 · 모의 은행 Recreate)
kubectl -n bank-transfer rollout status deploy/api
kubectl -n bank-transfer get deploy api -o jsonpath='{.spec.template.spec.containers[0].image}'   # 지금 태그
```
- **태그가 바뀌어야 롤링이 일어난다.** 태그는 `dev-<짧은 커밋>` (이미지에 들어가는 경로가 수정 중이면 `dev-<커밋>-<시각>`). 같은 태그로 다시 apply 하면 `unchanged` 이고 옛 이미지가 그대로 돈다 (R3).
- **api 롤링**: `maxUnavailable 0 · maxSurge 1` — 새 Pod 가 Ready 가 된 뒤 옛 Pod 를 내린다. 내려가는 Pod 는 `preStop` 10초 동안 새 요청도 받아 준 뒤 SIGTERM → graceful(단계당 20초), 45초를 넘으면 SIGKILL. R1: 롤링 중 첫 시도 5xx **preStop 10초 0 · 0 / preStop 0초 4 · 9** (`503 UF` — 입구가 이미 포트를 닫은 옛 Pod 로 연결).
- **게이트웨이 · 모의 은행은 `Recreate`**: 옛 Pod 가 내려간 뒤 새 Pod 가 뜬다 — 그동안 게이트웨이가 없어 출금은 "안 보냄 → 재시도" 로 흐르고(15-5) 1원인증은 503 이다. 배포는 점검 시간(23:50 ~ 00:10)을 피한다.
- 롤링이 끝나면 `smoke.sh` — 두 Pod 가 서로 다른 워커에 있어야 한다 ([5]). 한 워커로 몰리면 `kubectl -n bank-transfer rollout restart deploy/api` (api 는 `matchLabelKeys: [pod-template-hash]` 로 같은 리비전끼리 세어 평소엔 나뉜다).
- 클라이언트는 5xx(연결 단계 실패)를 받으면 **같은 `Idempotency-Key`** 로 다시 보낸다 — 입구는 다시 보내지 않는다 (재시도 설정 없음). 1원인증에는 키가 없어 사용자가 다시 요청해야 한다.

### 15-4. Pod · 노드가 죽었을 때
- **Pod 삭제 · 크래시**: Deployment 가 replicas 를 맞추려 새 Pod 를 만든다 (자동 재생성 — Compose 의 `docker kill` 은 다시 안 띄웠다, §14-4). api 강제 삭제 3회: 첫 시도 5xx 0, 대체 Pod Ready 까지 **10.8 ~ 11.2초**, Kafka 파티션 넘김 1.5 ~ 3.6초 (kubelet 이 SIGTERM 을 먼저 보내 정상 탈퇴 — SIGKILL 이면 세션 시간 초과 45초가 겹친다, §14-4).
- **노드가 사라지면** (`kubectl get nodes` 에 `NotReady`):
  - 컨트롤 플레인이 알아채기까지 **약 50초** — 그동안 api Pod 는 Ready 로 남아 입구가 요청의 절반을 사라진 주소로 보내 Envoy 연결 시간 초과 약 10초(측정: k6 최대 10.0초 · Envoy 로그 duration 10000ms) 뒤 503 (D2: 첫 시도 5xx 512 · 같은 키 재시도 뒤에도 포기 144). 연결 단계 실패라 api 에 닿지 않았고 출금 행도 없다 — **돈은 정확**하다, 사용자는 실패를 받고 다시 시도하면 된다.
  - 그 뒤 기본 **300초(`tolerationSeconds`)** 가 지나야 Pod 를 다른 노드에 다시 만든다 — 대체 api Ready 는 **+363.1초**, 그동안 api 는 1대. 퇴거 대기를 30초로 줄이면(`overlays/experiment-fast-evict`) **+88.2초** (5xx 439 · 포기 113 — 감지 전 구간은 그대로). 대가: 짧은 네트워크 끊김에도 Pod 를 옮긴다.
  - 노드가 돌아오지 않으면 클러스터를 다시 만든다 (`down.sh` → `up.sh`).
- **제어 노드가 죽으면** MySQL · Kafka · Redis · 모의 은행 · DNS · 입구가 다 같이 선다 (PV 가 제어 노드에 묶여 다른 노드로 못 간다 — 추정, 이번 단계는 제어 노드를 실험하지 않았다).
- **drain** (`kubectl drain <워커> --ignore-daemonsets --delete-emptydir-data`, 끝나면 `uncordon`): D1 3회 5xx 0. api 는 요청을 받는 대상 기준 최소 1대(새 Pod Ready 까지 약 11초), Pod Ready 기준 2대(종료 중 Pod 포함)였다. PDB(api `minAvailable 1`)는 이번 배치(워커당 api 1대)에서 개입하지 않았다 — 두 api 가 한 워커에 있을 때 drain 을 막는 효과는 미검증. **주의 — 게이트웨이는 `Recreate` 인데도 퇴거 때는 새 Pod 가 먼저 떠서 4 ~ 7초간 두 대가 모두 Ready · 은행 연결**이다 (이중 송신 0 이었지만 은행이 기관당 세션 하나만 허용하면 문제 — P8-3b). 게이트웨이가 있는 워커를 비우는 일은 점검 시간 밖에서.
- **`SENDING` 으로 남은 건**은 정리 작업이 30초 뒤 결과조회로 확정한다 (재전송 없음 — ADR-0006).

### 15-5. 은행 장애 때 보이는 것
- 모의 은행(또는 은행 연결)이 끊기면 게이트웨이 **readiness 만 DOWN** (약 8 ~ 10초 안 — 5초 × 2회) → Service `bank-gateway` 의 대상이 0 이 되어 api 의 연결 시도는 **즉시 `Connection refused`**. 게이트웨이 `RESTARTS` 는 **0 그대로** (liveness 는 `bankConnection` 을 보지 않는다 — 은행 장애는 재시작 사유가 아니다).
```bash
kubectl -n bank-transfer get pods -l app=bank-gateway     # READY 0/1, RESTARTS 0
kubectl -n bank-transfer logs -l app=api --prefix --tail=50 | grep -i "not sent"   # "bank transfer not sent" — P8-1 분류: 연결 단계 실패 = 안 보냄
```
- **출금은 접수 그대로, 처리가 재시도 경로로**: 접수는 DB 만 쓰므로 영향이 없고 처리 컨슈머가 "안 보냄" → 재시도 토픽 (5 · 30 · 60초, 약 95초) → DLT → 정리 작업의 제한 재발행(ADR-0007). 은행이 돌아오면 게이트웨이가 다시 붙고 `COMPLETED` 로 끝난다 (B1 60 · 180초 모두 접수 = 완료 · 이중 0). **1원인증은 동기 경로라 은행이 없으면 503**.
- **복구가 늦을 수 있다**: 은행이 돌아온 뒤 게이트웨이가 다시 Ready 가 되기까지 6.7 ~ 34.0초 (B1) — 게이트웨이의 재접속 백오프(1초에서 2배씩, 상한 30초) + readiness 주기. 기다린다. 재시작해도 백오프가 초기화될 뿐이고 재시작은 처리 중 요청을 끊는다.
- liveness 에 `bankConnection` 을 넣는 것은 안티패턴이다 (B2: 60초 장애에 재시작 1, 180초에 2 — 은행 점검이 길면 재시작 루프).

### 15-6. NetworkPolicy 확인
정책 7개 (`infra/k8s/base/networkpolicy.yaml`): namespace 전체 ingress 기본 거부 + api ← Envoy, 게이트웨이 ← api, 모의 은행 ← 게이트웨이, MySQL · Redis ← api, Kafka ← api · Kafka. egress 는 막지 않는다.
```bash
kubectl -n bank-transfer get networkpolicy
bash -c '. infra/k8s/lib.sh; np_probe bank-gateway.bank-transfer.svc.cluster.local 8082; np_probe mysql.bank-transfer.svc.cluster.local 3306'      # 허용 목록 밖 임시 Pod 에서 연결 시도 — "<time_connect> <curl 종료 코드>". 막힘 = 0.000000 28, 0 보다 크면 연결됨
bash -c '. infra/k8s/lib.sh; np_probe kubernetes.default.svc.cluster.local 443'    # 양성 대조 — time_connect 가 0 보다 커야 probe 가 정상 (종료 코드는 28 이어도 된다)
```
- `np_probe` 는 같은 namespace 에 `curlimages/curl:8.23.0` 임시 Pod 를 띄워 `<time_connect 초> <curl 종료 코드>` 를 출력한다. **막힘 = `0.000000` 이면서 종료 코드 28(연결 시간 초과 — 정책이 버림)일 때만이다.** 6(이름 풀이 실패) · 7(연결 거부)은 막힘이 아니라 대상 오류이고, 짧은 이름이 없으면 풀이 시간 초과로 28 이 나올 수 있어(관측) 대상은 FQDN 으로 준다. **빈 값 · `0.000000` 을 곧바로 "막힘" 으로 읽지 않는다** — 양성 대조(API 서버 443 은 정책 밖이라 붙어야 한다)가 먼저다. `smoke.sh` [8] 이 이 순서로 자동 확인한다.
- 정책 전 연결됨(0.0011 · 0.0010초) → 정책 후 막힘 (`0.000000`), Pod 7개 Ready 유지 — 프로브(kubelet → Pod)는 막히지 않는다. kind 기본 CNI(kindnet)가 집행한다.
- 새 Pod 가 붙지 못하면: 허용 목록에 그 경로가 있는지 본다 (예: api 는 Envoy 프록시에서 오는 것만 받는다).

### 15-7. 실험 도구 (`load/k8s/`)
입구(Envoy)로 출금을 일정 속도로 넣으면서 동작 하나를 한다 (k6 는 도커 네트워크 `kind` 에서 제어 노드 NodePort 30080 으로).
```bash
bash infra/k8s/up.sh                       # 먼저
bash load/k8s/run.sh steady 1              # 기준 (아무것도 안 함)
bash load/k8s/run.sh rolling 1             # api 이미지 태그를 바꿔 롤링 (R1)  — preStop 은 지금 배포된 값
bash load/k8s/run.sh podkill 1             # api Pod 하나 강제 삭제 (R2)
bash load/k8s/run.sh tagtrap 1             # 같은 태그로 다시 apply → 롤링 안 일어남 (R3, 부하 없음)
bash load/k8s/run.sh bank 1                # 모의 은행 replicas 0 → BANK_DOWN_S 뒤 1 (B1 · B2 — 게이트웨이 liveness 설정은 지금 배포된 값)
bash load/k8s/run.sh drain 1               # 워커 drain → 부하 끝나면 uncordon (D1)
bash load/k8s/run.sh node 1                # 워커 노드 컨테이너 docker stop → NODE_DOWN_S 뒤 start (D2 · D2')
bash load/k8s/run.sh cleanup               # 끝나면 반드시 — load-p82-u001 ~ u020 의 행만 지운다
```
- 대조 실험은 overlay 로 바꾼 뒤 돌리고 끝나면 `local` 로 되돌린다: `bash -c '. infra/k8s/lib.sh; apply_overlay experiment-prestop0 "$(deployed_tag)"'` (R1 대조) · `experiment-liveness-bank` (B2) · `experiment-fast-evict` (D2'). 되돌리기는 `apply_overlay local "$(deployed_tag)"`.
- 환경변수: `RATE`(초당 출금, 20) · `DURATION`(60s, rolling 은 150s) · `ACTION_AT`(동작 시각 초, 20) · `SETTLE`(완료 대기 최대 초, 300) · `BANK_DOWN_S`(60) · `NODE_DOWN_S`(420) · `NODE`(drain · node 대상 워커) · `OVERLAY`(tagtrap, `local`).
- 정확성 확인(접수된 키 = 출금 행, 전부 `COMPLETED`, 은행 거래 기록 = 출금 행(같은 거래번호 두 번 0), 잔고 = 원장 합)을 통과하지 못하면 요약이 `.json.invalid` 로 남는다. 시간 지표는 kubectl 표본(1 ~ 2초 간격)이라 ±1.5초.
- 결과: `docs/perf/raw/p8-3a/<동작>-<회차>.json` (요약) · `.k6.json` · `.timeline.txt` · `.rebalance.txt`, 작업 파일은 `build/load-p83a/<동작>-<회차>/`. 실험 전에 api 두 Pod 가 서로 다른 워커에 Ready 여야 한다 (도구가 사전 확인하고 종료 중 Pod 는 세지 않는다).
- 클러스터 DB 에 쓰는 행은 `load-p82-u001 ~ u020` · `load-p83a-smoke-<시각>` 뿐이다 (스모크는 끝에서 지운다).

## 16. 게이트웨이 이중화 (P8-3b)
> [ADR-0018](docs/adr/0018-gateway-ha-leader-election.md) 채택 (2026-11-16, [#31](https://github.com/hyeokjinON/bank-transfer/pull/31)). 이유 · 숫자는 ADR 과 [측정 결과](docs/perf/p8-3b-gateway-ha.md), 규격은 [protocol.md](docs/protocol.md) v0.4.

게이트웨이가 2대(리더 1 · 대기 1)다. **은행에 붙는 것은 리더뿐**이다. 리더는 K8s Lease `bank-gateway-leader` 로 정해지고, 리더가 자기 Pod 에 라벨 `bank-transfer/gateway-role=leader` 를 붙이면 Service `bank-gateway` 가 그 Pod 로만 보낸다. 대기자도 Ready 다 (롤링 · `rollout status` 가 동작하게). 전문번호는 ConfigMap `bank-gateway-tran-no` 에서 1,000개씩 구간을 예약해 쓰므로 선출과 무관하게 겹치지 않는다. 선출은 **K8s 매니페스트에서만** 켜진다 (`bank.gateway.leader-election.enabled` 기본 false — IntelliJ · Compose 는 그대로 1대가 항상 리더). 모든 명령은 저장소 루트의 Git Bash 에서, `kubectl` 은 `--context kind-bank-transfer` 를 줄여 적었다.

### 16-1. 지금 리더 보기
```bash
kubectl -n bank-transfer get lease bank-gateway-leader -o jsonpath='{.spec.holderIdentity}'; echo   # Lease 보유자 = 리더 Pod 이름
kubectl -n bank-transfer get pods -l app=bank-gateway -L bank-transfer/gateway-role -o wide        # 리더 라벨이 붙은 Pod 정확히 1개 + 어느 노드인지
kubectl -n bank-transfer get endpointslice -l kubernetes.io/service-name=bank-gateway               # Service 대상 = 리더 Pod 하나
kubectl -n bank-transfer exec <게이트웨이 Pod 이름> -- wget -qO- http://127.0.0.1:8082/actuator/health/bankConnection   # role: leader | standby
kubectl -n bank-transfer logs <게이트웨이 Pod 이름> | grep 'leader election'
```
- **대기자 헬스**: `/actuator/health/bankConnection` 이 `UP` 이고 `role` 이 `standby` 이면 정상 대기자다 (은행 연결 없음, Ready). 리더는 `role: leader` 이고 은행 연결이 끊기면 `DOWN`(`reason: bank channel not connected` — P8-3a B1 그대로, 이때 라벨 Pod 는 Service 에서 빠진다).
- **로그 문구**: `became leader` (획득), 정상 교대는 `leader election: released leadership`, 비정상(갱신 실패 · 워치독 자기 차단 포함)은 `leader election: lost leadership`. 워치독이 끊었다면 같은 직전에 자기 차단 줄이 있다. 정상 교대는 시작할 때 `leader election: stepping down` 도 남긴다. WARN `leader election: lease release not observed within 2s — standby will wait for expiry` 는 Lease 반납이 확인되지 않았다는 뜻이다 → 대기자는 만료(15초)까지 기다린다.
- 스모크 [9] (`bash infra/k8s/smoke.sh`)가 위 셋(Ready 2/2 · 리더 라벨 1개 = Lease 보유자 · 엔드포인트 = 그 Pod)과 모의 은행 동시 세션 1 을 한 번에 확인한다.

### 16-2. 전문번호 예약 보기 — 손으로 고치지 않는다
```bash
kubectl -n bank-transfer get configmap bank-gateway-tran-no -o yaml     # data.date (KST yyyyMMdd) · data.reservedTo (여기까지 예약됨)
kubectl -n bank-transfer logs deploy/mock-bank | grep -c 'duplicate tran no'   # 3001 (중복) 수 — 늘면 아래 16-4
```
- `reservedTo` 는 "이 번호까지 누군가 예약했다" 는 뜻이다. **줄이면 이미 쓴 번호가 다시 나가 은행이 `3001` 로 거절한다.** 지금 값 · 날짜를 보기만 한다 (`kubectl edit` · `patch` · `delete` 금지 — 지우면 처음 예약자가 1부터 다시 만든다).
- 구간은 1,000개씩 예약하고 남는 번호는 건너뛴다 (재시작하면 구간 하나가 통째로 비는 것이 정상). 날짜가 바뀌면 점검 시간(23:50 ~ 00:10) 뒤 첫 송신이 새 날짜 구간을 1부터 예약한다.
- **저장된 날짜가 오늘보다 새로우면(시계가 늦은 Pod) 예약하지 않고, 기록이 깨졌으면 덮어쓰지 않고, 둘 다 송신을 멈춘다** (`503 BANK_NOT_SENT` — fail-closed). 16-4 로.

### 16-3. 두 Pod 는 반드시 서로 다른 노드 (P8-3c 필수 분산)
**두 복제본이 한 노드에 있으면 이중화가 아니다** (P8-3b H4′ 변형 — 두 대가 한 노드에서 노드가 죽으면 새 리더 **353.4초**). P8-3b 의 `ScheduleAnyway` 분산은 노드가 빠진 동안 남은 워커로 몰린 Pod 를 노드가 돌아와도 다시 퍼뜨리지 않았다. **P8-3c 부터 `DoNotSchedule`** ([ADR-0019](docs/adr/0019-gateway-required-spread.md)) — 한 노드에 놓일 수 없다.
```bash
kubectl -n bank-transfer get pods -l app=bank-gateway -o wide -L bank-transfer/gateway-role   # NODE 열이 서로 다르다 (스모크 [9] 도 확인)
```
- **워커 하나가 빠지면(drain · 장애) 대체 게이트웨이는 `Pending` 이 정상이다.** 남은 워커로 가지 않고 기다리다가, 그 워커가 돌아오면(uncordon · 노드 Ready) 그 워커에 뜬다 — 나뉜 상태가 저절로 복구된다 (S2 drain: Pending 42초 → uncordon 뒤 원래 워커). 그동안 게이트웨이는 1대(리더)라 **대기자가 없다** — 남은 워커까지 죽으면 게이트웨이 0 (몰려 있었어도 같았다).
- `Pending` 사유 확인 — 사유만 바로 본다 (`describe … | tail` 은 실행 순간 Events 칸이 없거나 길면 사유가 잘린다, 2026-11-17 사람 확인에서 겪음):
  ```bash
  kubectl -n bank-transfer get events --field-selector involvedObject.name=<Pending Pod>,reason=FailedScheduling
  ```
  `didn't match pod topology spread constraints` (분산 규칙이 막은 것) 가 있으면 장애가 아니다. 나머지 문구로 워커가 왜 빠졌는지도 보인다 — drain(cordon)이면 `were unschedulable`, 노드 장애(`unreachable` taint)면 `had untolerated taint(s)` (제어 노드도 늘 taint 1개로 센다). `Insufficient cpu` · `Insufficient memory` 같은 다른 사유면 그것을 고친다.
- 워커가 오래 못 돌아오고 대기자가 꼭 필요하면 (운영 판단): 새 워커를 붙인다. 분산 규칙을 풀어 같은 노드에 띄우는 것은 권하지 않는다 (노드 장애에는 어차피 함께 죽는다).
- 롤링 업데이트는 그대로 된다 — 분산 규칙이 같은 리비전끼리만 세서(`matchLabelKeys: [pod-template-hash]`) 새 Pod 가 옛 Pod 옆에 잠깐 뜰 수 있고, 끝나면 나뉜다 (S1). 필수 pod anti-affinity 는 옛 Pod 까지 세어 워커 2개에서 롤링이 멈춘다 — 고르지 않은 이유.
- 아직 안 한 것: descheduler, 노드 도달 불가 `tolerationSeconds` 단축 (P8-3a D2' 88초 — 짧은 끊김에도 Pod 를 옮기는 대가). 워커가 3개 이상이면 빠진 워커 대신 다른 워커에 뜰 수 있다 (`maxSkew 1` 안에서 — 추정, 이 클러스터는 워커 2).

### 16-4. 장애 대응 표
| 보이는 것 | 뜻 · 확인 | 조치 |
|---|---|---|
| 리더 라벨 Pod 가 **둘** | API 서버와 단절된 옛 리더가 라벨을 못 뗐다. **은행 연결은 여전히 1개** (라벨이 아니라 연결이 기준), 옛 리더로 간 요청은 `503 BANK_NOT_SENT` 로 재시도된다 | 기다린다 — 단절이 풀리면 옛 리더가 정리한다. `get lease` 의 보유자가 진짜 리더 |
| 리더 라벨 Pod **없음**, Lease 보유자도 없음 | 선출이 안 돈다 | `kubectl -n bank-transfer logs <Pod> \| grep -i 'leader election'`, `kubectl -n bank-transfer auth can-i update lease/bank-gateway-leader --as=system:serviceaccount:bank-transfer:bank-gateway` (RBAC `rbac-gateway.yaml`), API 서버 도달 여부. 대기자가 Ready 가 아니면 `describe pod` |
| Lease 보유자는 있는데 라벨 Pod 없음 | 새 리더가 접속 · 번호 예약 중이거나(접속 뒤 라벨) 못 하는 중 | 몇 초 기다린 뒤 로그: 은행 연결 · 번호 예약 오류, Pod patch 권한(RBAC `rbac-gateway.yaml`) |
| 모의 은행 `3001`(`duplicate tran no`) 증가, 출금이 `BANK_NOT_SENT` 로 재시도 | 전문번호가 겹쳤다 — 번호 저장소(ConfigMap)가 되돌려졌거나 기록 삭제 | 16-2 로 `date` · `reservedTo` 확인. **`reservedTo` 를 은행에서 이미 쓴 최대 번호보다 크게 올리는 방향으로만** 고친다 (줄이지 않는다). 기록이 없으면 처음 예약자가 1부터 만든다 — 같은 날이면 겹친다 |
| 게이트웨이 로그에 번호 예약 실패 · 저장된 날짜가 더 새롭다 · 기록이 깨졌다, `503 BANK_NOT_SENT` 지속 | fail-closed — 번호 저장소를 믿을 수 없어 송신을 멈춘 것 | 아래 "깨진 번호 기록 고치기" |
| 게이트웨이 1대가 `Pending` | 워커 하나가 drain · 장애로 빠졌다 — 필수 분산(P8-3c)이 남은 워커로 보내지 않고 기다리게 한 것 (16-3) | 정상. 워커가 돌아오면 그 워커에 뜬다. `get events --field-selector involvedObject.name=<Pod>,reason=FailedScheduling` 의 사유가 분산 규칙이 아니면 그 사유를 고친다 (16-3) |
| Lease 가 **15초 넘게** 안 넘어간다 | 옛 리더 노드가 죽었고 만료 대기 중(정상 15초) / 대기자가 안 뜨는 중(대기자가 `Pending` 이면 남은 워커가 리더뿐 — 16-3) | `kubectl get nodes`, `get pods -o wide`. 노드째 죽었으면 정상 — 15초 뒤 획득 (H4′ 14.6초). 길게 가면 대기자 Pod 상태 · 한 노드 몰림 |
| 리더가 은행에 못 붙고 readiness DOWN | 은행 장애 (P8-3a B1 — §15-5) | 은행 복구를 기다린다. 재시작은 백오프만 초기화하고 처리 중 요청을 끊는다 |
| 롤링이 끝나지 않는다 (새 Pod 가 NotReady) | 옛 구성(선출 끔) Pod 가 은행 세션을 쥐고 있다 — 첫 전환 교착 | 16-5 (scale 0 → 적용) |

**깨진 번호 기록 고치기** (송신이 멈췄을 때): ① 먼저 모의 은행(또는 은행)에서 **오늘 이미 쓴 가장 큰 전문번호**를 확인한다. ② ConfigMap 의 `date` 를 오늘(KST `yyyyMMdd`)로, `reservedTo` 를 ①보다 **충분히 크게** 둔다 — 날짜 · `reservedTo` 는 **앞으로만** 고친다, 줄이지 않는다. ③ 고친 뒤 새 게이트웨이가 알아서 다음 예약을 한다 (필요하면 `rollout restart deploy/bank-gateway`). 은행 쪽 최대 번호를 알 수 없으면 임의로 고치지 말고 은행 쪽과 확인한다. 송신이 멈춰 있는 동안 출금은 접수만 되고 "안 보냄 → 재시도 → DLT → 제한 재발행" 으로 흐른다 (§15-5, 돈은 정확).

### 16-5. 도입 · 되돌리기 절차
**이전 구성(게이트웨이 1대 · 선출 끔) → 이중화 첫 적용은 롤링으로 하지 않는다.** 옛 Pod 는 선출에 참여하지 않고 은행 세션을 쥔 채 놓지 않아, 새 Pod 가 리더가 되어도 은행이 거부해 NotReady 가 되고 `maxUnavailable 0` 롤링이 영원히 진행되지 않는다 (교착). 옛 Pod 를 먼저 내린다 — 그동안 게이트웨이가 없어 출금은 "안 보냄 → 재시도", 1원인증은 503 (점검 시간 23:50 ~ 00:10 을 피한다).
```bash
# 이전 구성 → 이중화 (처음 한 번)
kubectl -n bank-transfer scale deploy/bank-gateway --replicas=0        # 옛 Pod 가 세션을 놓게
bash -c '. infra/k8s/lib.sh; apply_overlay local "$(deployed_tag)"'    # 또는 bash infra/k8s/up.sh — replicas 2 · RollingUpdate · RBAC 적용 (새 태그가 필요하면 up.sh)
kubectl -n bank-transfer rollout status deploy/bank-gateway
bash infra/k8s/smoke.sh                                                  # [9] 리더 라벨 1개 = Lease 보유자
```
- 이중화 → 이중화(코드 · 이미지 교체)는 보통 롤링이다 (H2: 세션 최대 1 · 거부 0 · 중복 0, 은행 공백 1.1 ~ 3.4초). 새 Pod 가 대기자로 Ready 가 된 뒤(약 12초) 옛 리더가 내려간다.
- **H0 대조 overlay(`experiment-gateway-single`)로 갈 때와 돌아올 때**:
```bash
# 이중화 → H0 (게이트웨이 1대 · Recreate · 선출 끔)
kubectl -n bank-transfer delete pdb bank-gateway                        # PDB 를 지운다 — apply 는 지워진 리소스를 치우지 않는다. 1대짜리에 minAvailable 1 이면 drain 이 안 끝난다
bash -c '. infra/k8s/lib.sh; apply_overlay experiment-gateway-single "$(deployed_tag)"'
# H0 → 이중화 (되돌리기) — 위 첫 전환과 같다
kubectl -n bank-transfer scale deploy/bank-gateway --replicas=0
bash -c '. infra/k8s/lib.sh; apply_overlay local "$(deployed_tag)"'     # PDB · RBAC 가 다시 만들어진다
```
- Compose · IntelliJ 는 선출이 꺼져 있어 이 절차와 무관하다 (게이트웨이 1대 = 항상 리더).
- 이번 단계의 `docker stop <노드>` 는 노드 안 Pod 에 SIGTERM 이라 **정상 교대**와 같다. 노드가 갑자기 사라지는 상황은 `docker kill` (아래 `NODE_KILL=1`).

### 16-6. 실험 재현 (`load/k8s/run.sh`, `PHASE=p8-3b`)
같은 부하(출금 20건/s)로 동작 하나를 하고 판정한다 (사용법 · 환경변수는 §15-7). 먼저 `bash infra/k8s/up.sh` · 게이트웨이 2대가 서로 다른 워커에 Ready (도구가 사전 확인하고, 한 노드면 시작하지 않는다 — 16-3). 결과는 `docs/perf/raw/p8-3b/<동작>-<회차>.json`. 숫자는 [측정 결과](docs/perf/p8-3b-gateway-ha.md).
| 실험 | 명령 | 볼 것 |
|---|---|---|
| H0 이전 구성 대조 | 16-5 로 `experiment-gateway-single` 적용 → `PHASE=p8-3b bash load/k8s/run.sh drain 1` → 끝나면 되돌리기 | 세션 거부 7 · 번호 중복 591 (깨지는 것이 목적 — 판정은 돈 정확성) |
| H1 리더 노드 drain | `PHASE=p8-3b bash load/k8s/run.sh drain 1` (기본 대상 = 리더 워커) | 세션 최대 1 · 거부 0 · 중복 0 · 결과 모름 0, 은행 공백 |
| H2 게이트웨이 롤링 | `PHASE=p8-3b bash load/k8s/run.sh gwroll 1` | 위와 같음 (이미지 태그를 바꿔 롤링) |
| H3 리더 강제 삭제 | `PHASE=p8-3b bash load/k8s/run.sh gwkill 1` | `--force --grace-period=0` (측정: 두 회차 모두 옛 리더 로그에 `stepping down` — `--force` 도 SIGTERM 을 보내 정상 교대 (P8-3a R2 와 같다)) |
| H4 리더 노드 정지 | `PHASE=p8-3b bash load/k8s/run.sh gwnode 1` | `docker stop` = SIGTERM 이라 정상 교대처럼 (노드 장애가 아니다) |
| H4′ 리더 노드 SIGKILL | `NODE_KILL=1 PHASE=p8-3b bash load/k8s/run.sh gwnode 1` | `docker kill` — 새 리더 약 15초 (Lease 만료) |
| H5 API 서버 단절 | `PHASE=p8-3b bash load/k8s/run.sh gwcut 1` (`CUT_S` 기본 40초) | **kind 에서는 재현되지 않는다** (kindnet 이 API 서버 egress 를 안 막는다) → `GatewayLeaderElectionTest` 로 대체 |
**P8-3c 필수 분산** (`PHASE=p8-3c`, 결과 `docs/perf/raw/p8-3c/`, 숫자는 [측정 결과](docs/perf/p8-3c-gateway-spread.md)):
| 실험 | 명령 | 볼 것 (요약 JSON) |
|---|---|---|
| S1 롤링 | `PHASE=p8-3c bash load/k8s/run.sh gwroll 1` | 롤링이 끝나는가, `gw_colocated_s` 0 · `gw_nodes_end` 2 |
| S2 리더 워커 drain | `PHASE=p8-3c bash load/k8s/run.sh drain 1` | `gw_pending_s` > 0 (남은 워커로 안 감), uncordon 뒤 원래 워커 · `gw_nodes_end` 2 |
| S3 대기자 워커 SIGKILL | `NODE=<대기자 워커> NODE_KILL=1 PHASE=p8-3c bash load/k8s/run.sh gwnode 1` | 리더 교대 없음, 축출(약 350초) 뒤 Pending → 복구 뒤 원래 워커 |
| S4 이어서 리더 워커 SIGKILL | `NODE_KILL=1 PHASE=p8-3c bash load/k8s/run.sh gwnode 2` | 새 리더 약 15초 (P8-3b 의 몰린 상태 353초와 비교) |
```bash
bash load/k8s/run.sh cleanup      # 끝나면 반드시 — 측정 사용자 행만 지운다 (§15-7)
```
- P8-3c 도구 요소: `PHASE=p8-3c`, 요약 JSON `gw_pending_s` · `gw_colocated_s` · `gw_nodes_end`, drain · 노드 동작 뒤 게이트웨이 Ready 2/2 까지 최대 300초 표본.
- 새 도구 요소: `PHASE=p8-3b`(결과 폴더), 동작 `gwroll` · `gwkill` · `gwnode` · `gwcut`, `NODE_KILL=1`(노드를 SIGKILL), 게이트웨이 분산 사전 확인, 요약 JSON 의 `bank_sessions_*` · `bank_dup_tran_no` · `unknown_results` · `gw_acquired_s` · 은행 공백(모의 은행 로그의 `session closed` → 다음 `session opened`). 은행 공백이 가장 정확한 "은행에 붙은 게이트웨이가 없던 시간" 이다 — 표본 기반 `gw_leaderless_s` 는 노드 손실에 쓰지 않는다 (죽은 노드의 Pod 는 노드가 NotReady 가 될 때까지 Ready · 라벨 그대로).
- 모의 은행 제약 3개(`--single-session` · `--session-idle-timeout=60s` · `--unique-tran-no`)는 K8s 매니페스트에서만 켠다. 모의 은행 readiness 는 `tcpSocket` 이 아니라 `/proc/net/tcp` LISTEN 을 보는 exec 프로브다 — TCP 프로브가 세션 하나를 먹어서 게이트웨이를 거부시켰다 (측정: 세션 거부 13).
- 한계: kind 한 호스트(노드 간 시계 차 없음, 노드 손실 때 TCP 끊김이 바로 전달) · 모의 은행 제약은 흉내 (실무 은행의 세션 정책 · 번호 규칙은 기관마다 다름 — 추정).

## 17. 관측 — 지표 · 대시보드 · 알림 (P9a)
> [ADR-0020](docs/adr/0020-observability-metrics-alerts.md) (제안, 2026-11-17). 설계 [스펙](docs/superpowers/specs/2026-11-17-p9a-metrics-design.md), 실험 숫자 [측정 결과](docs/perf/p9a-metrics.md). kind(K8s) 에만 있다 — Compose · IntelliJ 실행에는 수집기가 없다. 관측 4개(Prometheus · Alertmanager · alert-sink · Grafana)는 `monitoring` namespace 의 제어 노드에서 돈다 (`bash infra/k8s/up.sh` 가 같이 올린다).

### 17-1. 열기
```bash
kubectl --context kind-bank-transfer -n monitoring port-forward svc/grafana 3000:3000        # http://127.0.0.1:3000 (익명 보기)
kubectl --context kind-bank-transfer -n monitoring port-forward svc/prometheus 9090:9090     # Targets · Alerts · Graph
kubectl --context kind-bank-transfer -n monitoring port-forward svc/alertmanager 9093:9093   # 울리는 알림 · 억제 · 침묵
kubectl --context kind-bank-transfer -n monitoring logs deploy/alert-sink --timestamps        # 받은 알림 (운영이면 Slack 자리)
kubectl --context kind-bank-transfer -n monitoring get secret grafana-admin -o jsonpath='{.data.password}' | base64 -d   # 관리자 비밀번호 (화면에만)
```
입구(`127.0.0.1:8000`)로는 `/actuator/prometheus` 가 404 — 지표는 Prometheus 만 Pod 에서 직접 읽는다 (입구는 `/api/v1` · `/admin` · `/actuator/health` · `/actuator/info` 만 넘긴다).

### 17-2. 대시보드 읽는 순서
대시보드 두 장: `bank-transfer ① 출금 흐름` · `bank-transfer ② 게이트웨이 · 은행 · JVM` (기본 15분 · 새로고침 5초).
- ① 1행(돈이 흐르나) → 2행을 왼쪽부터 오른쪽(아웃박스 → Kafka → 은행 → 정리 — **처음 막힌 칸이 원인 쪽**) → 3행(사용자가 느끼는 시간).
- 은행 칸이 이상하면 ② 1 · 2행(리더 · 연결 · 안 보냄 사유), 자원이 의심되면 ② 3행.
- **카운터는 근사치, 돈의 정답은 DB** (`docs/database.md` 운영 쿼리). Pod 재시작 · Kafka 최소 한 번 전달로 건수가 어긋난다 (측정: 노드 kill 뒤 단순 차이 −420, `increase()` 로도 약 1% 어긋남).
- **복구는 대시보드에 늦게 보일 수 있다**: 수집 5초 + 그래프 격자 5초라 은행 연결 복구가 실제보다 최대 약 8초 늦게 보인다 (Prometheus 원 표본으로는 2.4 ~ 4.5초 안). 5초보다 짧은 끊김은 아예 안 보인다. 몇 초 차이로 원인을 단정하지 않는다.

### 17-3. 알림별 할 일
알림이 가는 곳은 alert-sink 로그뿐이다 (운영이면 Slack · 텔레그램 자리). 울림 · 해제(`resolved`) 둘 다 온다. 도착은 계산값 약 50초 (수집 5 + 평가 5 + `for` 30 + 묶기 10) — 측정 47 ~ 50초.

| 알림 | 뜻 | 먼저 볼 것 | 할 일 |
|---|---|---|---|
| GatewayNoLeader | 은행 연결 주인이 30초 넘게 없다 — 은행 송신 멈춤 | `kubectl -n bank-transfer get pods -l app=bank-gateway -L bank-transfer/gateway-role`, Lease holder | §16 장애 대응 표 (선출 · RBAC · 노드) |
| GatewayMultipleLeaders | 리더 둘 | 두 Pod 의 `leader election` 로그, 모의 은행 세션 수 | 보통 API 서버 단절 직후 — 기다리면 정리 (§16). 1분 넘으면 옛 리더 Pod 삭제 |
| BankDisconnected | 은행 회선 30초 넘게 없음 | ② 연결 패널, 게이트웨이 로그 `connect to bank failed` | 은행 · 망 확인. 출금은 RECEIVED 로 재시도 · 재발행 (돈 안전) |
| TransferPipelineStalled | RECEIVED 15분(마지막 상태 변화 뒤 — 재발행도 멈춤) · SENDING 2분 넘게 | ① 2행 — 아웃박스 · lag · 은행 결과 | Kafka · 컨슈머 · 정리 작업 스케줄 확인, `database.md` 운영 쿼리로 해당 건 |
| TransferGaveUp | FAILED E001 생김 | `SELECT … WHERE bank_response_code = 'E001'` | 사용자에게 다시 요청 안내 (돈은 환불됨) |
| TransferUnknownExhausted | 결과조회 소진 UNKNOWN | 해당 건 · 결과조회 로그 | 일마감 대사가 맞춘다 (§10). 1시간마다 다시 울린다 |
| OutboxBacklog | 미발행 60초 넘게 | 발행기 로그 `outbox relay failed`, Kafka Pod | Kafka 복구 — 복구되면 밀린 행이 나간다 |
| TransferDeadLettered | 사람 확인 DLT (other · invalid_key) | api 로그 `moved to DLT` | §6 DLT 처리 |
| ApiHigh5xx | 5xx 5% 넘게 2분 | ① HTTP 5xx 경로별, api 로그 | 경로 · 코드별 원인 (WALLET_BUSY · DB) |
| ScrapeTargetDown | 읽던 앱 Pod 를 못 읽음 | Prometheus Targets, Pod 상태 · 노드 | 노드 장애면 §15. 다른 규칙이 거짓 정상일 수 있다 |
| MetricsMissing | 상태 Gauge 가 NaN · 없음 | ② 조회 실패 패널, api 로그 `state metrics query failed` | DB 연결 확인. 이 동안 "쌓임" 알림이 눈을 감는다 |

- **억제**: `GatewayNoLeader` 가 울리는 동안 `BankDisconnected` 는 Alertmanager 가 억제한다 (원인 하나 · 알림 하나). Prometheus 화면(Alerts)에는 둘 다 보이고 alert-sink 에는 하나만 온다 — 억제 여부는 Alertmanager 화면 (`suppressed`).
- 노드 장애는 지표가 가장 늦게 본다: ScrapeTargetDown 은 장애 뒤 약 1분 남짓 (`for 1m`, 측정 +64 · +69초).

### 17-4. 지표 이름 (Prometheus 이름)
| 이름 | 종류 · 태그 | 앱 |
|---|---|---|
| `banktransfer_transfer_accepted_total` | Counter `result=created\|replayed` | api |
| `banktransfer_transfer_bank_result_total` | Counter `outcome=ok\|rejected\|not_sent\|unknown\|maintenance` | api |
| `banktransfer_transfer_completed_seconds_{count,sum,bucket,max}` | Timer `status=COMPLETED\|FAILED` (접수 → 확정) | api |
| `banktransfer_transfer_gave_up_total` · `banktransfer_transfer_republished_total` | Counter | api |
| `banktransfer_transfer_dlt_total` | Counter `kind=bank_not_connected\|invalid_key\|other` | api |
| `banktransfer_transfer_reconciled_total` | Counter `result=completed\|failed\|still_unknown` | api |
| `banktransfer_outbox_published_total` | Counter `result=ok\|error` | api |
| `banktransfer_transfer_open` · `banktransfer_transfer_open_oldest_seconds` | Gauge `status=RECEIVED\|SENDING\|UNKNOWN` (10초마다 DB 조회, 나이는 SQL). 나이는 RECEIVED = **마지막 상태 변화 뒤**(`updated_at` — 재발행도 갱신), SENDING · UNKNOWN = `sending_at` 뒤 | api |
| `banktransfer_transfer_unknown_exhausted` | Gauge | api |
| `banktransfer_outbox_unpublished` · `banktransfer_outbox_unpublished_oldest_seconds` | Gauge | api |
| `banktransfer_metrics_db_query_seconds_*` | Timer `result=ok\|error` — 위 조회 자체 | api |
| `banktransfer_bank_maintenance` | Gauge 0/1 (점검 창) | api · 게이트웨이 |
| `banktransfer_gateway_bank_request_seconds_*` | Timer `type=transfer\|inquiry\|name`, `outcome=ok\|rejected\|not_sent\|unknown` | 게이트웨이 |
| `banktransfer_gateway_not_sent_total` | Counter `reason=not_leader\|stepping_down\|no_tran_no\|disconnected\|duplicate_tran_no\|tran_no_store\|gateway_unreachable\|maintenance\|other` (`gateway_unreachable` 은 api 쪽 사유 — 게이트웨이 지표엔 늘 0, 나오지 않는다) | 게이트웨이 |
| `banktransfer_gateway_leader` · `banktransfer_gateway_bank_connected` | Gauge 0/1 | 게이트웨이 |
| `banktransfer_gateway_leader_terms_total` | Counter | 게이트웨이 |
| `banktransfer_gateway_tran_no_remaining` | Gauge (현재 + 예약 구간 남은 번호, 메모리 출처는 NaN) | 게이트웨이 |
| `banktransfer_gateway_tran_no_reserve_total` | Counter `result=ok\|conflict\|error` | 게이트웨이 |

공통 라벨 `application` `pod` `node` `app` `namespace`. 라벨에 사용자 id · 계좌 · 참조번호 · 금액은 없다. 기본 지표(JVM · `http_server_requests_seconds_*` · Hikari · Tomcat · Kafka 컨슈머 lag)도 같이 나온다. 상태 Gauge 는 api 2대가 같은 값을 내므로 `max()` 로 본다.

### 17-5. 점검 시간과 알림
점검 창은 앱 설정 한 곳(`app-config` 의 `BANK_MAINTENANCE_*`)이고, 규칙은 `banktransfer_bank_maintenance` 로 **점검 중 + 끝난 뒤 30분** 동안 TransferPipelineStalled · ApiHigh5xx 를 뺀다 (결정 D8). 점검 중 은행 결과는 `outcome=maintenance`, 안 보냄 사유는 `reason=maintenance`. 측정: 점검 창을 "지금" 으로 한 M5 에서 울린 알림 0건.

### 17-6. 실험 재현 (`load/k8s/run.sh`, `PHASE=p9a`)
같은 부하(출금 20건/s)로 동작 하나를 하고 판정한다 (사용법 · 환경변수는 §15-7). 먼저 `bash infra/k8s/up.sh`. 결과는 `docs/perf/raw/p9a/<동작>-<회차>.json`(판정) · `.p9a.json`(지표 요약) · `.prom-*.json` · `.alert-sink.txt` · `.alertmanager.txt` · `.mock-bank-sessions.txt`. 숫자는 [측정 결과](docs/perf/p9a-metrics.md).
| 실험 | 명령 | 볼 것 (요약 JSON) |
|---|---|---|
| M1a 리더 Pod 삭제 | `PHASE=p9a bash load/k8s/run.sh gwkill 1` | 지표에 공백이 안 보임 (공백 < 수집 5초) |
| M1b 리더 노드 SIGKILL | `NODE_KILL=1 PHASE=p9a bash load/k8s/run.sh gwnode 1` | `bank_gap_diff_s` · `bank_gap_diff_raw_s`, 리더 0 구간, 죽은 Pod 시계열 끝 |
| M2 모의 은행 0대 120초 | `BANK_DOWN_S=120 PHASE=p9a bash load/k8s/run.sh bank 1` | BankDisconnected 도착(약 50초) · 해제 |
| M3 게이트웨이 0대 60초 | `PHASE=p9a bash load/k8s/run.sh gwzero 1` | GatewayNoLeader 도착, BankDisconnected 억제 |
| M4 모의 은행 10분 끊김 | `BANK_DOWN_S=600 DURATION=640s SETTLE=1800 PHASE=p9a bash load/k8s/run.sh bank 4` (부하가 끊김보다 길어야 1만 2천 건이 쌓이고, 재발행까지 정산하려면 SETTLE 30분) | 상태 조회 시간(50ms 넘으면 다음 마이그레이션 인덱스) · 시계열 수 · 카운터 vs DB |
| M5 점검 창 = 지금 | `PHASE=p9a bash load/k8s/run.sh maint 1` | 울린 알림 없음 (`app-config` 를 고쳐 재시작 — 끝나면 도구가 되돌린다) |
- 카운터 차이는 `increase()` 로 잰다 (Pod 재시작이 카운터를 0 으로 되돌려 단순 차이는 음수가 나온다 — M1b 에서 −420).
- 은행 공백의 지표 값은 두 가지다: `bank_gap_diff_s`(5초 격자 — 대시보드가 보는 값, 5.0 ~ 8.2초) · `bank_gap_diff_raw_s`(Prometheus 원 표본, 2.4 ~ 4.5초). 성공 기준(차이 ≤ 5초)은 원 표본으로 판정한다.
- 끝나면 `bash load/k8s/run.sh cleanup` (§15-7). `MAINT_S` · `GW_DOWN_S` 등 환경변수는 `run.sh` 머리말.
- 알림 규칙 · 설정 검사는 PC 에서도 같은 명령: `bash infra/k8s/monitoring/check.sh` (`promtool test rules` · `amtool check-config` · 대시보드 검사 — 컨테이너로 돌아 설치할 것이 없다).

### 17-7. 막히는 곳
- Prometheus Targets 가 0 → RBAC: `kubectl --context kind-bank-transfer -n monitoring logs deploy/prometheus | grep forbidden`.
- `up == 0` + `context deadline exceeded` → NetworkPolicy `metrics-from-prometheus-*` (`kubectl --context kind-bank-transfer -n bank-transfer get networkpolicy`).
- 규칙이 0개 → ConfigMap · `bash infra/k8s/monitoring/check.sh` (`promtool check rules`).
- Grafana 비밀번호 분실 → Secret `grafana-admin` 을 지우고 `bash infra/k8s/up.sh` 다시 (새로 만든다), 이어서 `kubectl --context kind-bank-transfer -n monitoring rollout restart deploy/grafana` (Grafana 는 emptyDir 라 새 비밀번호로 다시 뜬다 — 재시작 없이는 옛 비밀번호 그대로).
- Prometheus 는 Recreate + emptyDir — 규칙 · 설정 ConfigMap 이 바뀌면 TSDB 가 비어 점검 뒤 30분 제외 창도 사라진다. 점검 창 끝 30분 안에는 감시 설정을 바꾸지 않는다.
- 알림이 alert-sink 에 안 온다 → ① Alertmanager 로그의 `notify` 오류 (`kubectl --context kind-bank-transfer -n monitoring logs deploy/alertmanager | grep -i notify`), ② `kubectl --context kind-bank-transfer -n monitoring get svc,pods` 에서 alert-sink Service · Pod 가 Ready 인지, ③ 웹훅 주소가 `http://alert-sink.monitoring.svc.cluster.local:8080/cgi-bin/sink` (경로 `/cgi-bin/sink`) 인지. 억제된 알림(`suppressed`)은 일부러 안 온다.
- 옛 `nc` 방식 alert-sink 는 본문을 잃었다 — 응답은 200 인데 알림 절반이 빈 줄이었다. 지금은 busybox `httpd` + CGI 로 고쳤다 ([측정 결과 B7](docs/perf/p9a-metrics.md)).

## 18. 분산 추적 — Tempo (P9b)
> [ADR-0021](docs/adr/0021-distributed-tracing.md) (제안, 2026-11-18). 설계 [스펙](docs/superpowers/specs/2026-11-18-p9b-tracing-design.md) (§14 계획 단계에서 고친 점), 실험 숫자 [측정 결과](docs/perf/p9b-tracing.md). kind(K8s) 에만 있다 — Compose · IntelliJ 실행은 trace 를 보내지 않는다 (구간 · 전파 · 로그의 traceId 는 어디서나 켜져 있다). 출금 한 건 = trace 하나, 앱이 Tempo 로 직접 보낸다 (5초 묶음 · Collector 없음).

### 18-1. 열기
```bash
kubectl --context kind-bank-transfer -n monitoring port-forward svc/grafana 3000:3000   # Explore → 데이터 소스 Tempo
kubectl --context kind-bank-transfer -n monitoring port-forward svc/tempo 3200:3200     # API: /api/search?q=… · /api/traces/<traceID>
```
요청 뒤 trace 가 검색에 나오기까지 약 8초 걸린다 (보내기 묶음 5초 + Tempo 수집). 검색 API 는 `start` · `end`(초 단위 시각)를 같이 준다.

### 18-2. 찾는 법 (TraceQL — Grafana Explore → Tempo → TraceQL)
| 무엇 | 질의 |
|---|---|
| 출금 한 건 (접수 · 정리 작업 trace 모두) | `{ span.transfer.id = "<출금 id>" }` |
| 게이트웨이까지 간 trace 만 | `{ span.transfer.id = "<id>" } && { resource.service.name = "bank-gateway" }` |
| 거래번호로 (은행 쪽 문의) | `{ span.transfer.reference = "<T…>" }` |
| 은행에 못 보낸 호출 · 사유 | `{ name = "bank.request" && span.bank.outcome = "not_sent" } \| select(span.bank.not_sent.reason)` |
| 재시도 토픽을 거친 출금 | `{ name =~ "transfer.requested-retry-.* process" }` |
| 1원인증 | `{ span.verification.id = "<id>" }` |
| 느린 은행 호출 | `{ name = "bank.request" && duration > 1s }` |

정리 작업 1회의 건수(`reconcile.republished` · `reconcile.completed` …)와 연 건별 구간 수(`reconcile.items`)는 숫자 속성이라 `{ name = "transfer.reconcile" && span.reconcile.republished > 0 }` 처럼 비교한다. 건수 · `reconcile.items` 가 모두 0 인 실행(할 일이 없던 10초 주기)은 Tempo 로 보내지 않는다 — 은행이 끊겨 결과조회만 하고 아무것도 못 바꾼 실행은 `items` 가 0 이 아니라 남는다.

### 18-3. trace 읽는 법
위에서 아래로, 아래로 갈수록 자식 구간이다.

| 구간 | 서비스 | 무엇 |
|---|---|---|
| `http post /api/v1/transfers` (SERVER) | api | 접수 — 속성 `transfer.id` (멱등 재응답도 같은 속성, 짧은 trace) |
| `outbox.publish` | api | 발행기가 행마다 연다 (부모 = 접수 때 저장한 `trace_parent`). 접수 구간과의 **틈 = 발행 주기 0.5초 근처면 정상**, 길면 발행기 · 락을 본다 |
| `transfer.requested send` (PRODUCER) | api | Kafka 발행 |
| `transfer.requested process` (CONSUMER) | api | 컨슈머. 재시도면 `transfer.requested-retry-N process` 가 **같은 trace** 에, DLT 는 `…-dlt process` |
| `http post` (CLIENT) | api | 게이트웨이로 HTTP |
| `http post /internal/v1/bank/{transfers,inquiries,name-inquiries}` (SERVER) | 게이트웨이 | 받음 |
| `bank.request` | 게이트웨이 | 은행 호출 — `bank.request.type` · `bank.outcome`(`ok`·`rejected`·`not_sent`·`unknown`) · `bank.tran_no` · `bank.not_sent.reason` · `transfer.reference` · `error.type` |

정리 작업은 `transfer.reconcile` root 의 **새 trace** (실행 1회 = trace 1개, 건별 `transfer.inquiry` · `transfer.republish`) 이고 `transfer.id` 로 처음 trace 와 같이 찾힌다. 한 trace 에 구간이 수백 개일 수 있다 (재발행 뒤 Kafka · 게이트웨이가 모두 자식). Boot 기본 로그 상관 패턴이 줄마다 `[서비스 이름] [스레드] [traceId-spanId]` 를 찍는다 (실제 api 로그 줄: `INFO 1 --- [bank-transfer] [ntainer#0-0-C-1] [<32자 traceId>-<16자 spanId>] …TransferRequestedListener : …`). 로그의 traceId 로 Tempo 에서 trace 를 연다 (`/api/traces/<traceId>`).

### 18-4. 막히는 곳
- trace 가 없다 → api · 게이트웨이의 `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` (`kubectl --context kind-bank-transfer -n bank-transfer get cm app-config`), 앱 로그의 `Failed to export` (`kubectl --context kind-bank-transfer -n bank-transfer logs deploy/api | grep -c "Failed to export"`), Tempo 로그 (`kubectl --context kind-bank-transfer -n monitoring logs deploy/tempo`). 요청 뒤 8초는 기다린다.
- 게이트웨이 구간이 따로 떨어진다 → traceparent 가 안 넘어간 것: api 의 WebClient 관측(`HttpBankGateway.create` 의 관측 인자) · 리액터 문맥 설정(`spring.reactor.context-propagation=auto`). traceparent 끝 플래그는 `-03`(샘플링 0 이면 `-02`)이라 `-01` 로 비교하지 않는다.
- 로그에 traceId 가 없다 → 로그 상관 패턴 (Boot 기본). 요청 밖(스케줄러) 로그는 비어 있는 게 정상.
- 구간에 민감값이 의심된다 → 지우개(`ScrubbingSpanExporter`)는 예외 메시지 · 스택 · 상태 설명만 지운다. 우리가 붙이는 속성(금액 · 계좌번호 · 예금주 · 코드 · Idempotency-Key · 사용자 id 는 금지)은 붙이는 코드를 고친다.
- Tempo 재시작 뒤 옛 trace 가 없다 → emptyDir 이라 정상 (보관 2일도 Pod 가 살아 있을 때만).
- Grafana 에서 Explore 가 안 열린다 → 익명 보기 사용자는 `GF_USERS_VIEWERS_CAN_EDIT=true` 가 있어야 연다 (kind 만, 저장은 못 한다).

### 18-5. 끄기 · 켜기
- 샘플링: `MANAGEMENT_TRACING_SAMPLING_PROBABILITY` (`app-config`, 앱 재시작 필요 — `kubectl --context kind-bank-transfer -n bank-transfer rollout restart deploy/api deploy/bank-gateway`). 0 이면 보내지 않고 구간 · 전파는 그대로 (비용 측정의 "끔").
- 보내기만 끄려면 `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` 를 지운다 (주소가 없으면 보내기 자체가 안 생긴다 — 구간 · 전파 · 로그 상관은 남는다). `management.tracing.export.enabled=false` 는 전파까지 끄므로 쓰지 않는다.
- 추적 실패는 돈 흐름에 영향이 없다 (Tempo 가 죽어도 접수 · 출금은 그대로, 앱 로그에 `Failed to export` 만).

### 18-6. 실험 재현 (T1 ~ T5, 도구 `PHASE=p9b`)
먼저 `bash infra/k8s/up.sh`. 결과는 `docs/perf/raw/p9b/<동작>-<회차>.json`(판정) · `.p9b.json`(추적 요약) · `.traces.txt`(표본 trace 20개 원문) · `.tempo-search-*.json`. 숫자 · 해석은 [측정 결과](docs/perf/p9b-tracing.md).

| 실험 | 명령 | 볼 것 |
|---|---|---|
| T1 추적 비용 (켬) | `PHASE=p9b bash load/k8s/run.sh steady 1` · `steady 2` / 40건/s 한 쌍: `PHASE=p9b RATE=40 DURATION=30s bash load/k8s/run.sh steady 5` | 접수 p95 · `completed_per_s` · CPU |
| T1 추적 비용 (끔) | `PHASE=p9b TRACING=off bash load/k8s/run.sh steady 3` · `steady 4` (**steady 에만**) / 40건/s: `PHASE=p9b TRACING=off RATE=40 DURATION=30s bash load/k8s/run.sh steady 6` | 켬과의 차이 (잡음 안인가) |
| T1d Tempo 0대 60초 | `PHASE=p9b TEMPO_DOWN_S=60 bash load/k8s/run.sh tempozero 1` (Tempo 의 emptyDir 이 비워져 이전 trace 가 사라진다) | 돈 · 접수 p95 그대로, 보내기 실패 로그 |
| T2 정상 출금 모양 | steady 회차의 `.p9b.json` | 표본 20개가 7구간 완전 · 구간 사이 틈 |
| T3 모의 은행 끊김 (재시도 · 재발행) | `PHASE=p9b BANK_DOWN_S=120 DURATION=180s SETTLE=900 bash load/k8s/run.sh bank 2` (끊김이 부하 안에 들어와야 하고, SETTLE 이 5분짜리 재발행까지 덮는다) | 재시도 trace 가 접수 trace 에 붙음 · 재발행의 정리 작업 trace |
| T4 리더 교대 | `PHASE=p9b bash load/k8s/run.sh gwkill 1` · `PHASE=p9b bash load/k8s/run.sh gwroll 1` | `not_sent` 사유 (`stepping_down`) |
| T5 exemplar | 켬 회차의 `.p9b.json` | exemplar 개수 · Tempo 에서 찾았나 |

- 도구는 인자를 먼저 검사한 뒤에 이전 출력을 지운다. 끝나면 `bash load/k8s/run.sh cleanup` (§15-7).
- `gwroll` 뒤에는 게이트웨이가 새 이미지 태그(`dev-…-r<epoch>`)에 남아 다음 실험의 사전 확인이 거부한다 (api ≠ 게이트웨이 태그) — 다음 실험 전에 `bash -c '. infra/k8s/lib.sh; apply_overlay local <api 태그>'` 를 먼저 돌린다.
- 스모크 [11]: `bash infra/k8s/smoke.sh` — Tempo Ready · 스모크 출금의 trace 가 Tempo 에서 검색되고 서비스 둘 · 구간 5개 이상 · api 로그에 그 traceId · `/actuator` 구간 0.
