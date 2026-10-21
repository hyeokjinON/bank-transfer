# P3 출금이체 비동기 처리 — 설계

- 상태: 검토 중 (섹션 1~5 승인 2026-10-19, 문서 검토 대기)
- 범위: 로드맵 P3 (입출금 비동기 처리: Kafka + Transactional Outbox + 멱등키 + DLT)
- 관련: [ADR-0001](../../adr/0001-modular-monolith-ports.md) 모듈 경계 · [ADR-0002](../../adr/0002-netty-framing-and-response-matching.md) 응답 매칭 · [ADR-0003](../../adr/0003-local-infra-docker-compose.md) 로컬 인프라 · [protocol.md](../../protocol.md)

## 0. 목표와 범위

**목표**: 서비스 모계좌에서 사용자 계좌로 돈을 보내는 출금이체를 **비동기**로 처리하되, 다음 세 가지를 코드와 테스트로 보장한다.
1. 같은 요청이 여러 번 와도 **한 번만** 접수된다 (API 멱등).
2. 접수된 요청은 **반드시** 처리 단계로 넘어간다 (DB 저장과 메시지 발행의 원자성).
3. 결과가 불확실하면 **다시 보내지 않고 은행에 물어서** 확정한다 (이중 출금 방지).

**포함**
- 출금이체 1종 (서비스 모계좌 → 1원인증을 마친 사용자 계좌)
- 1원인증 저장소의 메모리 → MySQL 이전 (수취 계좌 검증의 근거)

**제외 (다른 단계)**
- 사용자 잔액, 동시 차감, 락 비교 → P4
- 일마감 대사, 알림 채널 → P5
- 은행 → 우리 방향의 입금통지 전문 → 로드맵 밖 (필요하면 별도 단계)
- Debezium CDC → P8 에서 폴링 퍼블리셔와 비교

**분할**: 단계마다 브랜치 1개 · PR 1개 · velog 1편.

| 단계 | 브랜치 | 내용 |
|---|---|---|
| P3-1 접수 | `feat/p3-1-transfer-intake` | transfer 모듈, JPA/MySQL/Flyway, Idempotency-Key, 상태 모델, 아웃박스 테이블, 1원인증 JPA 이전 |
| P3-2 처리 | `feat/p3-2-transfer-processing` | 폴링 퍼블리셔 → Kafka → 컨슈머 → 은행 이체 전문, 수동 커밋, 상태 조건부 멱등 |
| P3-3 실패 | – | 아래 둘로 나눈다 (2026-10-21) |
| P3-3a 결과조회 | `feat/p3-3a-inquiry-reconcile` | 규격 v0.2(이용기관 거래번호 · 결과조회), 정리 작업, 모의 은행 시나리오, 컨슈머 경합 처리 → [ADR-0006](../../adr/0006-inquire-instead-of-resend.md) |
| P3-3b 재시도 | `feat/p3-3b-retry-dlt` | 재시도 토픽 · DLT, 오래된 `RECEIVED` 재발행 |

## 1. 모듈 경계

```
modules/
  transfer/        신규. 출금이체 도메인 — Spring · JPA · Kafka · Netty 의존 없음
  account/         기존. "인증된 계좌인가?" 조회 추가
  bank-gateway/ · bank-protocol/   P3-3 에서 결과조회 전문 추가
apps/api/
  transfer/        REST 컨트롤러, JPA 어댑터, 아웃박스 퍼블리셔, Kafka 컨슈머, 정리 작업
  account/         InMemory 저장소 → JPA 저장소
apps/mock-bank/    P3-3 에서 결과조회 응답 · 무응답 시나리오 추가
```

`modules/transfer` 의 포트:

| 포트 | 계약 | 구현 (apps/api) |
|---|---|---|
| `TransferStore` | `saveWithEvent(transfer, event)`: 이체와 이벤트를 **함께 성공하거나 함께 실패**하게 저장. `findById`, `findByIdempotencyKey`, `transition(id, from, to, ...)`: 현재 상태가 `from` 일 때만 바꾸고 성공 여부 반환 | JPA + 같은 트랜잭션의 `outbox_event` insert |
| `VerifiedAccounts` | `isVerified(bankCode, accountNo): Boolean` | account 의 JPA 저장소 조회 (transfer 모듈은 account 모듈을 import 하지 않는다) |
| `BankTransferPort` | `transfer(...)` | `BankGatewayClient` 어댑터 (`BankTransferAdapter`) |
| `BankInquiryPort` | `inquire(reference)`: 처리됨 · 처리 안 됨 · 모름 (P3-3a). 정리 작업은 이 포트로 **묻기만** 하고 `BankTransferPort` 를 쓰지 않는다 | `BankGatewayClient` 어댑터 (`BankInquiryAdapter`) |

도메인이 "저장과 이벤트는 원자적"이라는 **계약만** 알기 때문에, 아웃박스를 Debezium 이나 Kafka 트랜잭션으로 바꿔도 도메인 코드와 테스트는 바뀌지 않는다.

**새 의존성** (`apps/api`): `spring-boot-starter-data-jpa`, `mysql-connector-j`, `flyway-core` + `flyway-mysql`, `spring-kafka` (P3-2). 테스트: Testcontainers (`mysql`, `kafka`), Awaitility. 버전은 Spring Boot BOM 으로 맞춘다.

## 2. 데이터 모델

Flyway `apps/api/src/main/resources/db/migration/V1__init.sql` (P3-1), 이후 변경은 `V2__...` 로 추가. 컬럼별 설명 · 인덱스 용도 · 잠금 방식 · 운영 쿼리는 **[docs/database.md](../../database.md)** 가 기준 문서다.

```sql
CREATE TABLE account_verification (
  id              CHAR(36)     PRIMARY KEY,
  bank_code       VARCHAR(3)   NOT NULL,
  account_no      VARCHAR(16)  NOT NULL,
  holder_name     VARCHAR(20)  NOT NULL,
  code_hash       CHAR(64)     NOT NULL,
  status          VARCHAR(10)  NOT NULL,        -- SENT | VERIFIED | EXPIRED | LOCKED
  failed_attempts INT          NOT NULL,
  expires_at      DATETIME(6)  NOT NULL,
  verified_at     DATETIME(6)  NULL,
  created_at      DATETIME(6)  NOT NULL,
  INDEX idx_verification_account (bank_code, account_no, status)
);

CREATE TABLE transfer (
  id                 CHAR(36)     PRIMARY KEY,
  idempotency_key    VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  reference          CHAR(20)     CHARACTER SET ascii COLLATE ascii_bin NOT NULL,     -- 이용기관 거래번호 (은행에 보내는 업무 키, §6)
  request_hash       CHAR(64)     NOT NULL,     -- SHA-256(bankCode|accountNo|amount|memo)
  bank_code          VARCHAR(3)   NOT NULL,
  account_no         VARCHAR(16)  NOT NULL,
  amount             BIGINT       NOT NULL,     -- 원 단위
  memo               VARCHAR(20)  NOT NULL,
  status             VARCHAR(10)  NOT NULL,     -- RECEIVED | SENDING | COMPLETED | FAILED | UNKNOWN
  bank_response_code VARCHAR(4)   NULL,
  sending_at         DATETIME(6)  NULL,         -- 정리 작업이 '멈춘 SENDING' 을 찾는 기준
  inquiry_attempts   INT          NOT NULL DEFAULT 0,   -- P3-3a: 결과조회에도 답이 없던 횟수
  created_at         DATETIME(6)  NOT NULL,
  updated_at         DATETIME(6)  NOT NULL,
  version            BIGINT       NOT NULL,
  UNIQUE KEY uk_transfer_idempotency (idempotency_key),
  UNIQUE KEY uk_transfer_reference (reference),
  INDEX idx_transfer_status (status, sending_at)
);

CREATE TABLE outbox_event (
  id            BIGINT       AUTO_INCREMENT PRIMARY KEY,
  aggregate_id  CHAR(36)     NOT NULL,
  event_type    VARCHAR(50)  NOT NULL,          -- TransferRequested
  payload       JSON         NOT NULL,
  created_at    DATETIME(6)  NOT NULL,
  published_at  DATETIME(6)  NULL,
  INDEX idx_outbox_unpublished (published_at, id)
);
```

- 시각은 **한국시간(KST) 벽시계 값**으로 `DATETIME(6)` 에 저장한다. 도메인은 `Instant`, 변환은 JDBC(`connectionTimeZone=Asia/Seoul`)·Hibernate(`jdbc.time_zone=Asia/Seoul`) 설정에서만 한다. 이유와 계층별 설정은 [database.md §2](../../database.md#2-시간대-정책--한국시간kst으로-저장).
- `version` 은 JPA `@Version` (낙관적 락). 상태 전이는 별도로 조건부 UPDATE 를 쓰므로 이중 안전장치다.
- 발행된 아웃박스 행 삭제(보존 기간)는 P5 배치에서 다룬다.

## 3. 상태 모델

```
RECEIVED ──(컨슈머)──▶ SENDING ──▶ COMPLETED   은행 0000
   ▲                     ├──────▶ FAILED      은행 거절 (1001 등)
   │                     ├──────▶ UNKNOWN     은행 무응답 (타임아웃)
   └──(미전송, 재시도)───┘
UNKNOWN ──(결과조회)──▶ COMPLETED | FAILED
SENDING ──(30초 초과, 정리 작업)──▶ UNKNOWN
```

| 전이 | 조건 |
|---|---|
| `RECEIVED → SENDING` | 컨슈머가 집어감. `sending_at` 기록 |
| `SENDING → RECEIVED` | `BankNotSentException` (전문을 쓰기 전 실패 = 은행에 안 감) **만** 허용 |
| `SENDING → COMPLETED / FAILED / UNKNOWN` | 은행 응답 · 거절 · 타임아웃 |
| `SENDING → UNKNOWN` | 정리 작업: `sending_at` 이 30초보다 오래됨 |
| `UNKNOWN → COMPLETED / FAILED` | 결과조회 응답 `0000` / `2001` |

- `COMPLETED`, `FAILED` 는 종결 상태. 그 밖의 전이는 도메인에서 예외(`IllegalTransition`).
- **타임아웃은 실패가 아니다.** 은행이 처리했을 수 있으므로 `UNKNOWN` 으로 두고 재전송하지 않는다.

## 4. P3-1 접수 API

`POST /api/v1/transfers`

```http
Idempotency-Key: 8c1f6a2e-...          # 필수, 1~64자 보이는 ASCII
Content-Type: application/json

{"bankCode": "090", "accountNo": "1002123456789", "amount": 50000, "memo": "출금"}
```
→ `202 Accepted` · `Location: /api/v1/transfers/{id}`. 본문은 `{"id": "...", "status": "RECEIVED", "createdAt": "..."}` 의 상위 집합인 전체 `TransferResponse` (아래 GET 응답과 같은 필드).

`GET /api/v1/transfers/{id}` → `200` `{id, status, bankCode, accountNo, amount, memo, bankResponseCode, createdAt, updatedAt}`

| 상황 | HTTP | code |
|---|---|---|
| 키 헤더 없음 · 형식 오류 | 400 | `IDEMPOTENCY_KEY_REQUIRED` |
| 금액 ≤ 0 또는 > 10,000,000 | 400 | `INVALID_AMOUNT` |
| 형식 오류 (은행코드 3자리, 계좌 숫자 10~16자리, 적요 ≤ 20바이트 EUC-KR) | 400 | ProblemDetail |
| 인증(VERIFIED)되지 않은 계좌 | 422 | `ACCOUNT_NOT_VERIFIED` |
| 같은 키 + 같은 내용 재요청 | 200 | (기존 건 그대로) |
| 같은 키 + 다른 내용 | 422 | `IDEMPOTENCY_KEY_REUSED` |
| 없는 id 조회 | 404 | `TRANSFER_NOT_FOUND` |

**처리 순서 (저장 3 은 한 트랜잭션, 4 의 재조회는 새 트랜잭션)**
1. 같은 키가 있으면 → `request_hash` 비교 → 같으면 기존 건 반환, 다르면 422.
2. `VerifiedAccounts.isVerified` 가 false → 422.
3. `transfer` insert (`RECEIVED`) + `outbox_event` insert (`TransferRequested`) → 커밋.
4. 동시에 같은 키로 들어와 3에서 `uk_transfer_idempotency` 위반 → 트랜잭션 롤백 후 **새 트랜잭션에서** 기존 건을 읽어 1과 같이 응답.

중복 방어를 앱 메모리가 아니라 DB UNIQUE 로 하는 이유: 인스턴스가 여러 대여도 마지막 방어선이 하나다.

**1원인증 JPA 이전**: `JpaVerificationRepository` 가 `VerificationRepository` 를 구현. 원자적 `update` 는 `SELECT … FOR UPDATE` 로 행을 잠그고 바꾼다. 기존 테스트 29개는 그대로 통과해야 한다 (도메인 테스트는 InMemory 를 계속 사용).

## 5. P3-2 처리 흐름

```
OutboxRelay (0.5초 fixedDelay, 트랜잭션)
  SELECT * FROM outbox_event WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED
  → 각 행 kafkaTemplate.send("transfer.requested", key=aggregate_id, payload).get(5s)
  → published_at = now → 커밋
     (전송 실패 시 예외 → 롤백 → 다음 주기에 다시)

TransferConsumer (group transfer-processor, AckMode.MANUAL_IMMEDIATE)
  ① 트랜잭션: transition(id, RECEIVED → SENDING)   — 0건이면 이미 처리 중/완료 → ack 후 종료
  ② 트랜잭션 밖: BankTransferPort.transfer(...)  (최대 3초)
  ③ 트랜잭션: 결과 전이 (§3)
  ④ ack.acknowledge()
```

| 항목 | 값 | 이유 |
|---|---|---|
| 토픽 | `transfer.requested`, 파티션 3, 복제 1 (로컬) | `NewTopic` 빈으로 선언. 브로커 자동 생성은 꺼져 있다 |
| 메시지 key | 이체 id | 같은 이체의 메시지는 같은 파티션 → 순서 유지 |
| payload | `{"eventId": <outbox id>, "transferId": "...", "occurredAt": "..."}` | 컨슈머는 DB 에서 최신 상태를 읽는다 (메시지는 신호) |
| 프로듀서 | `acks=all`, `enable.idempotence=true` | 설정이 실제로 적용됐는지 테스트로 확인한다 |
| 컨슈머 | `enable.auto.commit=false`, 수동 ack, `concurrency=3` | 처리가 끝난 뒤에만 커밋 |
| 발행 보장 | 적어도 한 번 | 커밋 직전 장애 시 재발행 → 컨슈머 ① 이 중복을 걸러낸다 |
| 컨슈머 멱등 | 상태 조건부 UPDATE | 도메인 상태가 곧 멱등키. 별도 `processed_event` 테이블은 쓰지 않는다 |

은행 호출을 트랜잭션 밖에 두는 이유: 응답 대기 3초 동안 DB 커넥션을 잡지 않는다.
컨슈머 스레드는 은행 응답을 블로킹으로 기다린다. 처리량은 P4 에서 측정하고 P6(WebFlux/코루틴)·P7(Virtual Thread)에서 같은 지점을 비교한다.

## 6. P3-3 실패 처리 (P3-3a 결과조회 · 정리 작업 / P3-3b 재시도 토픽 · DLT)

| 실패 | 은행 도달 | 처리 | 상태 |
|---|---|---|---|
| 은행 거절 (≠ `0000`) | 도달, 거절 확정 | 재시도 없음 | `FAILED` |
| 연결 없음 (`BankNotSentException`, 전문을 쓰기 전 실패) | 안 됨 | `SENDING → RECEIVED` 후 예외 → 재시도 토픽 | 재시도 |
| `BankTimeoutException` | 모름 | **재전송 금지** | `UNKNOWN` → 결과조회 |
| 그 밖의 `BankUnavailableException` (쓰기 실패 · 보낸 뒤 연결 끊김) | 모름 | **재전송 금지** | `UNKNOWN` → 결과조회 |
| 그 밖의 예외 (DB 등) | 상황별 | 재시도 토픽 → 소진 시 DLT | 유지 + 경고 로그 |
| 역직렬화 실패 | – | 재시도 없이 DLT | – |

- 재시도: `@RetryableTopic(attempts = 4, backoff 1s → 10s → 60s)` → `transfer.requested-retry-*` → `transfer.requested-dlt`. 원래 토픽을 막지 않는 대신 재시도 메시지는 순서가 바뀐다 (이체당 메시지 1개라 무해).
- DLT 핸들러: 이체 id · 예외 · 원본 오프셋을 경고 로그로 남긴다. 알림 채널은 P5.

**규격 v0.2** (`docs/protocol.md` 갱신)
- 이체 `TRF` 개별부에 `이용기관 거래번호 AN(20)` 추가 (52 → 72바이트).
  - 출금이체: 접수 시(P3-1) `T` + KST `yyMMdd` + 무작위 13자(대문자·숫자, `SecureRandom`) = 20자를 발급해 `transfer.reference` 에 저장한다. `uk_transfer_reference` 가 충돌을 막는다 (충돌 시 재발급).
  - 1원인증 이체: 같은 형식에 접두 `V`. 결과조회 대상이 아니므로 저장하지 않는다.
- 신규 업무 `INQ` 결과조회: 요청 개별부 `이용기관 거래번호 AN(20)`, 응답 개별부 + `처리결과 AN(4)` (`0000` 처리됨 · `2001` 원거래 없음).
- 전문번호(8자리)는 세션 내 요청·응답 짝짓기용으로만 쓴다. 업무 키가 아니다.

**정리 작업 `TransferReconciler`** (10초 fixedDelay)
1. `status = SENDING AND sending_at < now - 30s` → `UNKNOWN`.
2. `status = UNKNOWN AND sending_at < now - 30s AND inquiry_attempts < 10` 각각 결과조회 (컨슈머가 3초 타임아웃으로 적은 `UNKNOWN` 도 같은 유예) → `0000` 이면 `COMPLETED`, `2001` 이면 `FAILED`, 무응답이면 `inquiry_attempts + 1` 후 다음 주기. 10회 초과 시 경고 로그 (대사 대상).

**모의 은행 시나리오 추가**: 무응답 + 처리 안 함(조회 → `2001`), 처리 후 무응답(조회 → `0000`). 처리한 이체는 이용기관 거래번호로 기억한다.

**남는 위험**: 원거래가 은행 안에서 아직 처리 중일 때 조회하면 `2001` 이 올 수 있다. `sending_at` 30초 유예(정리 작업이 바꾼 `UNKNOWN` 과 컨슈머가 적은 `UNKNOWN` 모두)로 줄인다. 은행이 30초보다 더 늦게 처리하면 `2001` → `FAILED` 로 확정된 뒤 늦은 이체 응답은 로그만 남는다. 최종 안전망은 P5 일마감 대사다.

**2026-10-21 결정 (P3-3a)**
- 1원인증 이체의 `V` 거래번호는 **API 어댑터**(`BankGatewayAdapter`)가 만들고 저장하지 않는다. 결과조회 대상이 아니라서 `BankPort` 계약과 `account` 모듈은 그대로 둔다.
- 오래된 `RECEIVED`(발행됐는데 컨슈머가 못 집어간 건)는 **P3-3b** 에서 횟수를 제한해 다시 발행한다. P3-3a 의 정리 작업은 `SENDING` · `UNKNOWN` 만 다룬다.
- 정리 작업의 대상 선택은 `status` 로만 한다 (`SENDING` + `sending_at < now - 30s`, `UNKNOWN` + `inquiry_attempts < 10`). `sending_at` 은 미전송으로 `RECEIVED` 에 되돌린 뒤에도 남는다. 한도(10회)에 닿으면 멈추고 경고 → 일마감 대사.
- 컨슈머와 경합: 정리 작업이 먼저 `UNKNOWN` 으로 바꾼 뒤 컨슈머가 은행의 확정 결과(완료 · 거절)를 받으면 `UNKNOWN → COMPLETED/FAILED` 로 기록한다. 그 밖에는 로그만 남기고 던지지 않는다. 이미 다른 확정이면 `CONFLICT` 로 알린다.
- 스케줄러 스레드는 2개(`spring.task.scheduling.pool.size: 2`).
- **모의 은행 시나리오**: `9990000000001` = 무응답 · 처리 안 함 (테스트가 `silentAccounts` 로 지정, 조회 → `2001`). `9990000000002` = 처리하고 응답만 안 함 (기본 계좌, 예금주 `무응답`, 조회 → `0000`). 후자도 **1원 이체는 정상 응답**한다 — 1원인증으로 수취 계좌를 등록(VERIFIED)할 수 있어야 출금을 접수할 수 있기 때문이다.

## 7. 테스트 전략

| 층 | 위치 | 도구 | 대상 |
|---|---|---|---|
| 도메인 단위 | `modules/transfer` | JUnit + 가짜 포트 | 상태 전이 허용/금지, 멱등 판단, 금액·적요 검증, 인증 계좌 확인 |
| 어댑터 통합 | `apps/api` | Testcontainers MySQL | Flyway, `saveWithEvent` 원자성, UNIQUE 동시성, `FOR UPDATE` |
| 흐름 통합 | `apps/api` | Testcontainers MySQL + Kafka + `MockBankServer` + Awaitility | 요청 → `COMPLETED` 끝에서 끝까지 |

컨테이너는 JVM 당 한 번 띄워 재사용한다. Compose 는 손으로 실행할 때만 쓴다.

증명 테스트:
- [P3-1] 같은 키 동시 10건 → 이체 1건 · 아웃박스 1건
- [P3-1] 아웃박스 insert 실패 유도 → 이체도 없음
- [P3-1] 1원인증 confirm 동시 요청 → 시도 횟수 정확, 재시작 후 유지
- [P3-1] `Instant` 저장 → 네이티브 SQL 로 읽은 `DATETIME` 이 KST 벽시계 시각과 같음 (시간대 설정 고정)
- [P3-1] 같은 키 다른 내용 → 422, 인증 안 된 계좌 → 422
- [P3-2] 퍼블리셔 2개 동시 → 이벤트당 발행 1회
- [P3-2] 같은 메시지 2번 소비 → 모의 은행 입금 1건
- [P3-2] 프로듀서 `acks=all` · `enable.idempotence=true` 적용 확인
- [P3-3a] 처리 후 무응답 → `UNKNOWN` → 조회 → `COMPLETED` (입금 1건); 미처리 무응답 → `FAILED`
- [P3-3a] 멈춘 `SENDING` → 정리 작업 → 확정; 한도(10회) 후 멈춤; 컨슈머와 경합
- [P3-3b] 은행 꺼짐 → 재시도 중 기동 → `COMPLETED`
- [P3-3b] 깨진 메시지 → DLT

## 8. 완료 기준

공통 (각 PR): CI `build` · `infra-smoke` 초록 · ADR(제안) · `http/transfer.http` · README / CHANGELOG / HANDOVER 갱신 · velog 초안.

| 단계 | 추가 기준 | ADR |
|---|---|---|
| P3-1 | 로컬 MySQL 로 POST/GET 동작, 재시작 후 이체·인증 기록 유지 | 0004 멱등키와 아웃박스 |
| P3-2 | 로컬에서 요청 후 약 1초 안에 `COMPLETED`, kafka-ui 에서 메시지 확인 | 0005 Kafka 발행·소비 보장 |
| P3-3a | 규격 v0.2, 모의 은행 시나리오 2개, 정리 작업 | 0006 불확실한 결과는 재전송하지 않고 조회한다 |
| P3-3b | 재시도 토픽 · DLT, 오래된 `RECEIVED` 재발행 | 새 ADR (제안) |
