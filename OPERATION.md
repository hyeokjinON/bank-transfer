# OPERATION — 실행 · 상태 확인 · 장애 대응

> 로컬 개발과 CI 기준 운영 안내다. 실제 배포(K8s)는 P8 에서 이 문서에 추가한다.

## 1. 구성 요소와 포트
| 구성 요소 | 실행 | 포트 | 상태 확인 |
|---|---|---|---|
| API (`apps/api`) | `./gradlew :apps:api:bootRun` 또는 실행 구성 `2. API (8080)` | 8080 | `GET /actuator/health` · MySQL 필요 (기동 시 Flyway 마이그레이션) |
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
| `bank.gateway.host` / `port` | `localhost` / `9090` | 은행 FEP 주소 |
| `bank.gateway.response-timeout` | `3s` | 응답 대기. 넘으면 504 `BANK_TIMEOUT` |
| `bank.gateway.poll-interval` | `30s` | 쓰기 유휴 시 폴링 주기. ×3 동안 수신이 없으면 연결 종료 |
| `account.verification.ttl` | `5m` | 1원인증 유효시간 |
| `account.verification.max-attempts` | `5` | 코드 실패 허용 횟수 |
| `management.endpoints.web.exposure.include` | `health,info` | 노출 actuator |

환경변수로 덮어쓰기: `BANK_GATEWAY_PORT=9191 ./gradlew :apps:api:bootRun`.

## 3. 모의 은행 시나리오
| 계좌번호 | 예금주 | 동작 |
|---|---|---|
| `1002123456789` | 홍길동 | 정상. 이체 시 콘솔에 `[통장] … 입금 1원 적요='BT1234'` |
| `3333012345678` | 김카뱅 | 정상 (이름을 다르게 보내면 422 불일치 확인용) |
| `1101234567890` | 이페이 | 정상 |
| 그 외 | – | 성명조회 `1001` → 404 `ACCOUNT_NOT_FOUND` |

테스트 코드에서는 `MockBankServer(silentAccounts = …)` 로 무응답(타임아웃), `responseDelay` 로 지연을 만든다.

## 4. API 오류 코드
| 상황 | HTTP | code |
|---|---|---|
| 형식 오류 | 400 | ProblemDetail (검증 메시지) |
| 예금주명 불일치 | 422 | `HOLDER_NAME_MISMATCH` |
| 없는 계좌 | 404 | `ACCOUNT_NOT_FOUND` |
| 틀린 코드 | 400 | `WRONG_CODE` (+ `remainingAttempts`) |
| 5회 실패 | 423 | `VERIFICATION_LOCKED` |
| 유효시간 경과 | 410 | `VERIFICATION_EXPIRED` |
| 은행 무응답 | 504 | `BANK_TIMEOUT` |
| 은행 연결 없음 | 503 | `BANK_UNAVAILABLE` |
| `Idempotency-Key` 헤더 없음 · 1~64자 보이는 ASCII 아님 | 400 | `IDEMPOTENCY_KEY_REQUIRED` |
| 금액이 1 ~ 10,000,000 원 밖 | 400 | `INVALID_AMOUNT` |
| 적요가 EUC-KR 20바이트 초과 | 400 | `INVALID_MEMO` |
| 은행코드·계좌번호 형식 오류 | 400 | ProblemDetail (요청 검증). 도메인 검사는 `INVALID_ACCOUNT` |
| 1원인증을 마치지 않은 계좌 | 422 | `ACCOUNT_NOT_VERIFIED` |
| 같은 키에 다른 내용 | 422 | `IDEMPOTENCY_KEY_REUSED` |
| 없는 이체 id | 404 | `TRANSFER_NOT_FOUND` |

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
| `late or unknown response tranNo=…` 경고 | 전문번호 | 타임아웃 후 도착한 응답. 현재는 로그만 남긴다 |
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
