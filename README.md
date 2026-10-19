# bank-transfer — 은행 연동 송금 서비스 (Kotlin · Spring Boot · Netty)

은행 FEP 와 **고정길이 전문(TCP)** 으로 통신하는 송금·계좌인증 서비스를 처음부터 다시 만드는 프로젝트입니다.
예전에 가상자산 거래소와 은행을 잇는 입출금 미들웨어(Java 8, Spring Boot 1.5, Netty, Kafka)를 개발·운영하면서 겪은 문제를,
지금의 스택(Kotlin, Spring Boot 4, Netty 4.2, Java 21)으로 어떻게 다르게 풀 수 있는지 단계별로 검증합니다.

> 회사 코드는 한 줄도 쓰지 않았습니다. 전문 규격과 은행은 모두 이 프로젝트에서 새로 정의한 **모의(Mock)** 입니다.

## 문서

| 문서 | 언제 읽나 |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | 모듈 구조 · 의존 방향 · 런타임 흐름 · 설계 결정 색인 |
| [SETUP.md](SETUP.md) | 새 PC 에서 개발 환경 만들기 (JDK 21, IntelliJ, Docker, Git Bash) |
| [OPERATION.md](OPERATION.md) | 실행 · 포트 · 설정 · 모의 은행 시나리오 · 장애 대응 · CI |
| [HANDOVER.md](HANDOVER.md) | 지금 어디까지 왔고 다음에 무엇을 하는지 |
| [CHANGELOG.md](CHANGELOG.md) | 단계별 변경 이력 |
| [docs/database.md](docs/database.md) | DB 모델 · 테이블 정의 · 인덱스 · 시간대(KST) 정책 |
| [CLAUDE.md](CLAUDE.md) | 이 저장소의 작업 규칙 (AI 에이전트 · 기여자용) |

## 지금 되는 것

- **1원 계좌인증 API**: 예금주 성명조회 → 인증코드를 적요에 실어 1원 이체 → 사용자가 통장에서 본 코드로 확인
- **은행 게이트웨이(Netty 클라이언트)**: 길이헤더 프레이밍, 폴링 전문으로 회선 확인, 지수 백오프 재접속, 전문번호 기반 응답 매칭
- **모의 은행 FEP(Netty 서버)**: 같은 전문 규격으로 응답, 무응답 계좌로 타임아웃 재현
- **출금이체 접수 API (P3-1)**: `Idempotency-Key` 로 멱등 접수(202), 1원인증 계좌만 허용, 이체와 처리 신호(아웃박스)를 한 트랜잭션에 저장. MySQL(Flyway) 사용
- **테스트 67개**: 전문 직렬화, 쪼개진 TCP 패킷, 동시 요청 50건 매칭, 타임아웃, 재접속, API 통합(HTTP → TCP), MySQL 통합(Testcontainers: 같은 키 동시 10건, 아웃박스 원자성)

## 구조

```mermaid
flowchart LR
    U[클라이언트] -- "HTTP/JSON" --> API

    subgraph app ["apps/api (Spring Boot)"]
        API[AccountVerificationController] --> SVC[AccountVerificationService]
        SVC -- "BankPort (인터페이스)" --> ADP[BankGatewayAdapter]
    end

    ADP --> GW["modules/bank-gateway<br/>Netty 클라이언트"]
    GW -- "TCP 고정길이 전문<br/>4B 길이 + EUC-KR" --> BANK["apps/mock-bank<br/>Netty 서버"]
    GW -. "폴링 (유휴 시)" .-> BANK
```

| 모듈 | 역할 | 의존 |
|---|---|---|
| `modules/bank-protocol` | 전문 모델(sealed class), 고정길이 직렬화, Netty 프레임 디코더/인코더 | Netty |
| `modules/bank-gateway` | 은행 접속 클라이언트: 폴링, 재접속, 응답 매칭, 타임아웃 | bank-protocol |
| `modules/account` | 1원 계좌인증 도메인. **Spring·Netty 를 모른다** | 없음 |
| `modules/transfer` | 출금이체 도메인: 상태 전이, 멱등 접수, 거래번호. **Spring·JPA 를 모른다** | 없음 |
| `apps/api` | REST API, 조립(wiring), 예외 → HTTP 매핑 | account, transfer, bank-gateway |
| `apps/mock-bank` | 모의 은행 FEP 서버 (실행 가능, 테스트 픽스처) | bank-protocol |

`account` 는 `BankPort` 인터페이스만 알고, Netty 로 실제 연결하는 어댑터는 `apps/api` 에 있습니다.
도메인 규칙을 프레임워크 없이 테스트할 수 있고, 나중에 모듈을 서비스로 떼어낼 때 경계가 그대로 남습니다 (모듈러 모놀리스).

### 1원 계좌인증 흐름

```mermaid
sequenceDiagram
    participant C as 클라이언트
    participant A as API (Spring MVC)
    participant G as Gateway (Netty)
    participant B as 은행 FEP

    C->>A: POST /api/v1/account-verifications
    A->>G: inquireName() → CompletableFuture
    Note over A: 요청 스레드는 반납 (서블릿 비동기)
    G->>B: 성명조회 전문 (전문번호 N)
    B-->>G: 응답 (전문번호 N, 예금주명)
    G-->>A: future 완료 (전문번호로 매칭)
    A->>G: transfer(1원, 적요 "BT1234")
    G->>B: 이체 전문
    B-->>G: 응답 0000
    A-->>C: 201 {id, status: SENT}
    Note over C: 통장 거래내역에서 "BT1234" 확인
    C->>A: POST /{id}/confirm {code: "1234"}
    A-->>C: 200 {status: VERIFIED}
```

## 실행

### IntelliJ
> P3-1 부터 API 는 MySQL 이 필요하다. 먼저 `docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait`

1. **JDK 21 지정**: `Settings > Build, Execution, Deployment > Build Tools > Gradle > Gradle JVM` 을 21 로, `File > Project Structure > SDK` 도 21 로.
2. 오른쪽 위 실행 목록에 `.run/` 의 구성이 자동으로 뜹니다.
   - `0. Mock Bank + API` : 모의 은행(9090)과 API(8080)를 한 번에 실행
   - `1. Mock Bank (9090)` / `2. API (8080)` : 하나씩 실행 (벌레 아이콘으로 디버그)
   - `3. All Tests` : 테스트 67개
3. `http/account-verification.http` 를 열고 요청 왼쪽 ▶ 버튼으로 호출합니다.
   1번 요청이 응답의 `id` 를 저장하고, 모의 은행 콘솔의 `적요='BT1234'` 숫자를 2번 요청의 `code` 에 넣으면 `VERIFIED` 가 됩니다.

> 디버깅 추천 위치: `BankGatewayClient.send` (요청 등록), `ClientHandler.channelRead0` (전문번호로 응답 매칭), `AccountVerificationService.request` (성명조회 → 1원 이체 연결).

### 로컬 인프라 (P3 부터 필요)

Docker Desktop 이 있으면 MySQL · Kafka · Redis 를 한 번에 띄웁니다. Windows 는 **Git Bash** 에서 실행하세요 (PowerShell 의 `bash` 는 WSL 로 연결됩니다). 자세한 내용은 [infra/README.md](infra/README.md).

```bash
cp infra/.env.example infra/.env
docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait
bash infra/verify.sh
```

### 터미널

> P3-1 부터 API 는 MySQL 이 필요하다. 먼저 `docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait`

요구사항: JDK 21 (Gradle 은 래퍼가 내려받습니다)

```bash
./gradlew build                                   # 컴파일 + 테스트 67개
./gradlew :apps:mock-bank:run --args="9090"       # 터미널 1: 모의 은행
./gradlew :apps:api:bootRun                       # 터미널 2: API (localhost:8080)
```

```bash
# 1) 인증 요청 (Windows 셸은 한글 인코딩 문제로 파일로 보내는 게 안전)
echo '{"bankCode":"090","accountNo":"1002123456789","holderName":"홍길동"}' > req.json
curl -X POST localhost:8080/api/v1/account-verifications -H 'Content-Type: application/json' --data-binary @req.json
# {"id":"3bad7a91-...","status":"SENT","expiresAt":"...","verifiedAt":null}

# 2) 모의 은행 로그에서 통장 적요 확인
# [통장] 1002123456789 입금 1원 적요='BT6542'

# 3) 코드 확인
curl -X POST localhost:8080/api/v1/account-verifications/{id}/confirm -H 'Content-Type: application/json' -d '{"code":"6542"}'
# {"id":"3bad7a91-...","status":"VERIFIED",...}
```

| 상황 | HTTP | code |
|---|---|---|
| 예금주명 불일치 | 422 | `HOLDER_NAME_MISMATCH` |
| 없는 계좌 | 404 | `ACCOUNT_NOT_FOUND` |
| 틀린 코드 | 400 | `WRONG_CODE` (+ `remainingAttempts`) |
| 5회 실패 | 423 | `VERIFICATION_LOCKED` |
| 유효시간(5분) 경과 | 410 | `VERIFICATION_EXPIRED` |
| 은행 무응답 | 504 | `BANK_TIMEOUT` |
| 은행 연결 없음 | 503 | `BANK_UNAVAILABLE` |

## 예전 시스템과 무엇을 다르게 했나

| 주제 | 예전 (Java 8, Netty 4.1) | 이 프로젝트 |
|---|---|---|
| 프레이밍 | `ByteToMessageDecoder` 직접 구현, 길이 상태를 필드로 보관 | `LengthFieldBasedFrameDecoder` 상속, ASCII 길이 해석만 교체 |
| 링크 체크 | `java.util.Timer` 로 5분마다 폴링 | `IdleStateHandler`: 쓰기 유휴 시 폴링, 읽기 유휴 시 연결 종료 |
| 재접속 | 3초 고정, 무한 | 1초부터 두 배씩, 최대 30초 (지수 백오프) |
| 응답 대기 | 요청 스레드가 DB 를 1초마다 조회 (블로킹) | 전문번호 → `CompletableFuture` 맵, 응답이 오면 이벤트 루프가 완료 (논블로킹) |
| 인증코드 저장 | - | 원문 대신 SHA-256 해시, 시도 횟수 원자적 갱신 |
| 전문 모델 | 바이트 배열 필드 + 수동 오프셋 | Kotlin `sealed interface` + `when` 으로 누락을 컴파일 에러로 |

자세한 설계 이유: [docs/adr](docs/adr) · 전문 규격: [docs/protocol.md](docs/protocol.md)

## 로드맵

- [x] **P0** Gradle 멀티모듈, Kotlin, Spring Boot 4, CI
- [x] **P1** 1원 계좌인증 API (동기 계약, 논블로킹 대기)
- [x] **P2** Netty 모의 은행, 프레이밍, 폴링, 재접속, 응답 매칭
- [x] **P3-0** 로컬 인프라: Docker Compose 로 MySQL 8.4 · Kafka 4.1(KRaft) · Redis 7.4, CI 연기 테스트 → [infra/](infra/README.md) · [ADR-0003](docs/adr/0003-local-infra-docker-compose.md)
- [ ] **P3** 입출금 비동기 처리: Kafka + Transactional Outbox + 멱등키 + DLT
  - [x] P3-1 접수: Idempotency-Key · 아웃박스 · 1원인증 MySQL 이전 → [ADR-0004](docs/adr/0004-idempotency-key-and-transactional-outbox.md)
  - [ ] P3-2 처리: 폴링 퍼블리셔 → Kafka → 컨슈머 → 은행
  - [ ] P3-3 실패: 재시도 · DLT · 결과조회
- [ ] **P4** 잔액 동시성: 비관적 락 vs 낙관적 락 vs Redis 분산락, k6 부하테스트
- [ ] **P5** Spring Batch 일마감 대사 + 텔레그램 알림
- [ ] **P6** WebFlux / Coroutines 버전과 성능 비교
- [ ] **P7** Virtual Thread 적용 비교
- [ ] **P8** 모듈 분리(MSA), Docker Compose → Kubernetes
- [ ] **P9** 관측성: Micrometer, Prometheus, Grafana

## 기술 스택

Kotlin 2.3 · Java 21 · Spring Boot 4.1 (Spring MVC, Validation, Actuator) · Spring Data JPA (Hibernate 7) · Flyway · MySQL 8.4 · Testcontainers · Netty 4.2 · JUnit 5 · Gradle 9 (Kotlin DSL) · GitHub Actions
