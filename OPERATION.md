# OPERATION — 실행 · 상태 확인 · 장애 대응

> 로컬 개발과 CI 기준 운영 안내다. 실제 배포(K8s)는 P8 에서 이 문서에 추가한다.

## 1. 구성 요소와 포트
| 구성 요소 | 실행 | 포트 | 상태 확인 |
|---|---|---|---|
| API (`apps/api`) | `./gradlew :apps:api:bootRun` 또는 실행 구성 `2. API (8080)` | 8080 | `GET /actuator/health` · MySQL · Kafka 필요 (기동 시 Flyway 마이그레이션) |
| 모의 은행 (`apps/mock-bank`) | `./gradlew :apps:mock-bank:run --args="9090"` 또는 `1. Mock Bank (9090)` | 9090 (TCP) | 콘솔 `Mock bank FEP started on port 9090` |
| MySQL 8.4 | `infra/docker-compose.yml` | 3306 | `docker compose ... ps` → healthy |
| Kafka 4.1 (KRaft) | 〃 | 9092 (PC) / 29092 (컨테이너) | 〃 |
| Redis 7.4 | 〃 | 6379 | 〃 |
| kafka-ui (선택) | `--profile tools up -d` | 8089 | http://localhost:8089 |

API 는 기동 시 모의 은행에 접속한다. 은행이 없어도 API 는 뜨고, 게이트웨이가 1초→30초 간격으로 재접속을 시도한다(그동안 은행 호출은 503).

## 2. 설정 (`apps/api/src/main/resources/application.yml`)
| 키 | 기본값 | 의미 |
|---|---|---|
| `spring.datasource.url` | `jdbc:mysql://localhost:3306/bank_transfer?...` | `MYSQL_HOST` · `MYSQL_PORT` · `MYSQL_DATABASE` 환경변수로 덮어쓴다 (시각은 KST, [database.md §2](docs/database.md)) |
| `spring.datasource.username` / `password` | `bank` / 로컬 개발용 값 (`infra/.env.example` 와 같다) | `MYSQL_USER` · `MYSQL_PASSWORD` 환경변수 |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | `KAFKA_BOOTSTRAP_SERVERS` 환경변수로 덮어쓴다 (컨테이너에서는 `kafka:29092`) |
| `transfer.outbox.relay.enabled` | `true` | 폴링 퍼블리셔 스케줄 on/off (테스트는 끄고 직접 호출) |
| `transfer.outbox.relay.interval-ms` | `500` | 미발행 아웃박스 폴링 간격 (이전 실행이 끝난 뒤부터 잰다) |
| `transfer.outbox.relay.batch-size` | `100` | 한 번에 발행하는 최대 건수 |
| `transfer.reconcile.enabled` | `true` | 정리 작업 스케줄 on/off (테스트는 끄고 직접 호출) |
| `transfer.reconcile.interval-ms` | `10000` | 정리 작업 주기 (이전 실행이 끝난 뒤부터 잰다) |
| `transfer.reconcile.sending-timeout` | `30s` | 이보다 오래 `SENDING` 이면 `UNKNOWN` 으로 보고 결과조회 |
| `transfer.reconcile.max-inquiries` | `10` | 결과조회도 모르면 이 횟수에서 멈추고 경고 (일마감 대사 대상) |
| `transfer.reconcile.batch-size` | `100` | 한 번에 처리하는 최대 건수 |
| `spring.task.scheduling.pool.size` | `2` | 스케줄러 스레드. 발행기(0.5초)와 정리 작업(10초)이 서로 막지 않게 한다 |
| `bank.gateway.host` / `port` | `localhost` / `9090` | 은행 FEP 주소 |
| `bank.gateway.response-timeout` | `3s` | 응답 대기. 넘으면 504 `BANK_TIMEOUT` |
| `bank.gateway.poll-interval` | `30s` | 쓰기 유휴 시 폴링 주기. ×3 동안 수신이 없으면 연결 종료 |
| `account.verification.ttl` | `5m` | 1원인증 유효시간 |
| `account.verification.max-attempts` | `5` | 코드 실패 허용 횟수 |
| `management.endpoints.web.exposure.include` | `health,info` | 노출 actuator |

환경변수로 덮어쓰기: `BANK_GATEWAY_PORT=9191 ./gradlew :apps:api:bootRun`.

### Kafka
| 항목 | 값 |
|---|---|
| 토픽 | `transfer.requested` — 파티션 3, 복제 1 (로컬). 앱이 기동할 때 선언한다 (브로커 자동 생성은 꺼져 있다) |
| 메시지 | key = 이체 id, value = `{"transferId", "occurredAt", "eventId"}` ([database.md §4-3](docs/database.md)) |
| 컨슈머 그룹 | `transfer-processor` (컨슈머 3개, 수동 ack — 처리를 끝낸 뒤에만 오프셋 커밋) |
| 프로듀서 | `acks=all`, 멱등, `max.block.ms=5000` — 브로커에 닿지 않으면 `send()` 가 5초(기본 60초 아님) 뒤 실패하고 발행기가 다음 주기에 다시 보낸다 |
| 컨슈머 폴링 | `max-poll-records=50` — 한 건이 은행 호출로 최대 3초 막히므로 한 번의 poll 이 `max.poll.interval.ms`(5분)를 넘기지 않게 한다 |
| 재시도 | 은행에 보내지 못하면 1초부터 두 배씩(최대 10초) 5번 다시 시도한 뒤 로그를 남기고 넘어간다 ([ADR-0005](docs/adr/0005-kafka-delivery-and-consumer-idempotency.md)) |

보기: kafka-ui — `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile tools up -d` 후 http://localhost:8089 에서 토픽의 메시지와 그룹 `transfer-processor` 의 랙(밀린 메시지 수)을 본다.

### 정리 작업 (결과를 모르는 이체 확정, P3-3a)
10초마다(이전 실행이 끝난 뒤부터) 한 번에 최대 100건씩 돈다. 은행에 **다시 보내지 않고 묻기만 한다** ([ADR-0006](docs/adr/0006-inquire-instead-of-resend.md)).

| 순서 | 대상 | 동작 |
|---|---|---|
| 1 | `status = SENDING` 이고 `sending_at` 이 30초보다 오래된 건 | `UNKNOWN` 으로 바꾼다 (컨슈머가 죽었거나 은행 응답 뒤 DB 기록이 실패한 경우) |
| 2 | `UNKNOWN` 이고 `sending_at` 이 30초보다 오래된 건, `inquiry_attempts < 10` | 결과조회(`INQ`): 처리됨(`0000`) → `COMPLETED`, 원거래 없음(`2001`) → `FAILED`, 모름 → `inquiry_attempts + 1` |

- 30초 유예는 컨슈머가 3초 타임아웃으로 적은 `UNKNOWN` 에도 똑같이 적용된다(은행이 조금 느릴 뿐이면 아직 처리 중이라 `2001` 을 믿을 수 없다). 그래서 `UNKNOWN` 은 접수 후 몇 초 만에 보이지만 `COMPLETED` / `FAILED` 는 접수로부터 대략 30~40초 뒤(유예 30초 + 최대 10초 주기)에 확정된다. 단계 1 이 방금 `UNKNOWN` 으로 바꾼 건은 이미 30초가 지났으므로 같은 주기에 바로 묻는다.
- 10번째에도 모르면 더 묻지 않고 경고를 남긴다 → 일마감 대사(P5) 대상. 대상 조회: [database.md §9](docs/database.md).
- 로그: `reconcile: ReconcileResult(…)` (한 일이 있을 때만, INFO) · `still UNKNOWN after 10 inquiries, leave it to daily reconciliation: …` (한도 도달, WARN) · `bank inquiry inconclusive: reference=… reason=…` (조회가 답을 못 얻음, WARN) · `reconcile failed, retrying next round` (DB 장애 등, 다음 주기에 다시).
- 정리 작업이 먼저 `UNKNOWN` 으로 바꾼 뒤 컨슈머가 은행의 확정 결과를 받으면 컨슈머가 `UNKNOWN → COMPLETED/FAILED` 로 기록한다. 이미 반대 결과로 확정돼 있으면 `conflict: bank result differs …` 경고가 남는다 → 사람이 확인한다.
- 알려진 한계: 게이트웨이 연결이 없을 때의 조회도 시도 1회로 센다. 10초 × 10회 ≈ 100초보다 긴 은행 점검이면 그동안의 `UNKNOWN` 이 모두 한도에 닿아 대사로 넘어간다 (P3-3b 에서 정리, [HANDOVER](HANDOVER.md)).

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
| 1원인증을 마치지 않은 계좌 | 422 | `ACCOUNT_NOT_VERIFIED` |
| 같은 키에 다른 내용 | 422 | `IDEMPOTENCY_KEY_REUSED` |
| 없는 이체 id | 404 | `TRANSFER_NOT_FOUND` |
| 허용되지 않는 상태 전이 | 409 | `ILLEGAL_TRANSITION` |
| 그 밖의 표준 Spring 오류 (405 · 415 …) | 해당 상태 | HTTP 상태 이름 (예 `METHOD_NOT_ALLOWED`) |

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
| 이체가 `UNKNOWN` 으로 남아 있음 | `transfer` 의 `status` · `inquiry_attempts`, 정리 작업 로그(`reconcile:`) | 응답 시간 초과 · 보낸 뒤 연결 끊김 · 포트 예외. 은행이 처리했을 수 있다 → **정리 작업이 10초마다 결과조회, 10번 후에도 모르면 경고(`still UNKNOWN after`) → 일마감 대사.** 재전송하지 않는다. 계속 `UNKNOWN` 이면 은행 연결(503)과 `bank inquiry inconclusive` 로그를 본다 |
| 이체가 `SENDING` 에서 안 넘어감 | `sending_at` ([database.md §9](docs/database.md)) | 은행이 응답한 뒤 DB 기록이 실패했거나, `SENDING` 커밋 뒤 또는 은행 호출 도중 앱이 죽은 경우. 둘 다 30초 뒤 정리 작업이 `UNKNOWN` 으로 바꾸고 결과조회로 확정한다 |
| 이체가 `RECEIVED` 에서 안 넘어감 | 릴레이 로그(`outbox relay failed`) · `outbox_event.published_at` · 컨슈머 랙 · 앱 로그 | 흔한 원인: 은행 연결 없음(재시도 5회 뒤 로그만 남기고 넘어감), 컨슈머 중단 · 랙, 첫 상태 전환(`RECEIVED → SENDING`)의 DB 실패, 메시지 key 가 UUID 가 아님. 발행 전(`published_at` NULL)이면 Kafka 접속부터 확인한다. 어느 쪽이든 P3-3b 전까지는 자동으로 되살리지 않는다 (P3-3b 에서 재시도 토픽 · DLT · 오래된 `RECEIVED` 재발행) |
| `late or unknown response tranNo=…` 경고 | 전문번호 | 타임아웃 후 도착한 응답. 로그만 남긴다 (게이트웨이). 결과는 정리 작업의 결과조회로 확정한다. **남는 위험**: 은행이 30초 유예보다 더 늦게 처리하면 조회가 `2001` 을 답해 `FAILED` 로 확정되고, 이후 도착한 이체 응답은 이 로그만 남는다(돈은 나갔는데 `FAILED`) → 일마감 대사(P5)가 맞춘다. 이 로그에는 거래번호가 없어 어느 건인지 바로 알 수 없다 (P3-3b) |
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
