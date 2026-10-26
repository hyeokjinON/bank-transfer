# P4-1 원화 잔고 — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 사용자별 원화 잔고(`wallet`)와 append-only 원장(`wallet_ledger`)을 두고, 출금 접수 트랜잭션에서 조건부 UPDATE 한 문장으로 차감하며(잔고 부족이면 422), 실패로 확정되면 같은 트랜잭션에서 환불한다. 같은 사용자의 출금이 동시에 몰려도 잔고는 음수가 되지 않는다. 출금 · 1원인증 · 멱등키는 `X-User-Id` 사용자 범위가 된다.

**Architecture:** 도메인(`modules/transfer` · `modules/account`)은 `userId` 와 소유 규칙만 안다 — `RequestTransfer.userId`, `TransferStore.findByIdempotencyKey(userId, key)`, 실패 확정 전용 포트 `TransferStore.fail(...)`(기존 `transition(…, FAILED)` 는 거부), `VerifiedAccounts.isVerified(userId, …)`, `StoreConflict.InsufficientBalance`. `apps/api` 의 `WalletLedger`(신규, JDBC)가 충전 · 차감 · 환불 SQL 을 한곳에 모으고, `JpaTransferStore.saveWithEvent` 가 같은 `TransactionTemplate` 안에서 출금 INSERT → 차감 + 원장 → 아웃박스 순으로 쓰며, `fail()` 은 조건부 전이 1건일 때만 원장의 WITHDRAW 금액을 돌려준다. 웹은 `RequestUserResolver` 한 곳에서 `X-User-Id` 를 꺼내 검사하고, `WalletController` 가 충전 · 잔고 · 보류를 보여 준다.

**Tech Stack:** Kotlin 2.3.21 · Spring Boot 4.1.1 (Spring 7.0.9 · `NamedParameterJdbcTemplate` · `TransactionTemplate` · `HandlerMethodArgumentResolver`) · Spring Data JPA (Hibernate 7) · Flyway · MySQL 8.4 (CHECK 제약) · MySQL Connector/J 9.7.0 · Kafka 4.1 · Testcontainers 2.0 · Awaitility

**Spec:** [docs/superpowers/specs/2026-10-26-p4-1-wallet-balance-design.md](../specs/2026-10-26-p4-1-wallet-balance-design.md) (상위: README 로드맵 P4)
2026-10-26 사용자 결정: 사용자 원화 잔고, P4 를 둘로 분할(P4-1 정합성 기준선 · P4-2 락 전략 비교), `X-User-Id` + 계좌 소유, A안 = 접수 트랜잭션에서 차감 · 실패 확정 시 환불.

## Global Constraints

- 도메인 모듈(`modules/account`, `modules/transfer`)은 Spring · JPA · Netty · Kafka 의존성 없음. 잔고 SQL 은 `apps/api` 의 `WalletLedger` 한 곳 — P4-2 에서 락 전략을 바꿔 끼우는 이음새다.
- **사용자 식별**: `X-User-Id` 헤더, 정규식 `[A-Za-z0-9-]{1,36}` (영숫자 · 하이픈 1~36자). 없거나 형식이 틀리면 **400 `USER_ID_REQUIRED`**. 꺼내고 검사하는 곳은 웹 계층의 `RequestUserResolver` 하나뿐이다(컨트롤러 인자 `RequestUser`). 값은 신뢰한다(인증은 범위 밖 — "원래는 인증 토큰에서 꺼내는 값"). DB 는 `ascii_bin` 이라 대소문자가 다르면 다른 사용자다.
- **차감 = 접수 트랜잭션** (A안). `JpaTransferStore.saveWithEvent` 의 `tx.execute { }` 하나 안에서 순서대로: ① `transfer` INSERT(`saveAndFlush`, user_id 채움) ② `UPDATE wallet SET balance = balance - :amount, version = version + 1, updated_at = :at WHERE user_id = :userId AND balance >= :amount` — **0건이면 `StoreConflict.InsufficientBalance` 를 던져 전체 롤백** (지갑이 없어도 0건 = 잔고 0) ③ 원장 `WITHDRAW`(amount, balance_after, transfer_id) ④ `outbox_event` INSERT. 별도 락 없음(행 락 한 번), `CHECK (balance >= 0)` 이 2차 방어선. 서비스는 `InsufficientBalance` 를 `TransferException.InsufficientBalance` → **422 `INSUFFICIENT_BALANCE`** 로 바꾼다.
- **환불 = 실패 확정 4경로 모두 `TransferStore.fail(id, from, at, bankResponseCode): Boolean`**. 한 트랜잭션에서 조건부 전이(`… WHERE id = ? AND status = :from`)가 1건일 때만 원장의 `WITHDRAW` 금액만큼 `balance += amount`(`version + 1`, `updated_at`) 와 원장 `REFUND` 1줄. 0건이면 아무것도 안 한다. `WITHDRAW` 가 없는 P3 시절 출금(user_id NULL)은 상태만 바꾼다. 기존 `transition(…, to = FAILED)` 는 `require` 로 거부(`IllegalArgumentException`). 이중 환불은 조건부 전이 + `uk_ledger_transfer_type` 두 겹으로 막는다.
- **상태별 잔고**: `RECEIVED` · `SENDING` · `UNKNOWN` = 빠진 채(보류, GET `/wallet` 의 `pending`), `COMPLETED` = 빠진 채(확정), `FAILED` = 복원, `CONFLICT` = 손대지 않고 경고(수동 확인 · P5 대사).
- **멱등키는 사용자 범위**: UNIQUE `uk_transfer_user_idempotency (user_id, idempotency_key)`. 저장소의 제약 이름 매핑을 새 이름으로 바꾼다 (옛 이름 `uk_transfer_idempotency` 는 새 이름의 부분 문자열이 **아니다** — 그대로 두면 동시 같은 키가 500 이 된다).
- **소유**: 1원인증에 `user_id`. 출금은 **내가 인증한 계좌만**(`isVerified(userId, bankCode, accountNo)`), 확인(confirm)은 본인만 — 남이면 404 `VERIFICATION_NOT_FOUND`(실패 횟수도 늘리지 않는다). `GET /transfers/{id}` 는 요청자 소유가 아니면 404 `TRANSFER_NOT_FOUND`(존재 여부도 숨김). user_id NULL 인 옛 행은 누구에게도 보이지 않는다.
- **지갑 API** (관리 · 테스트용 충전): `POST /api/v1/wallet/deposits {amount}` — 1회 1 ~ 100,000,000원(`@Min` · `@Max`, 밖이면 400 `INVALID_REQUEST`), 지갑이 없으면 만들고(`INSERT … AS new ON DUPLICATE KEY UPDATE`) 원장 `DEPOSIT`, 200 + `{userId, balance, pending}`. `GET /api/v1/wallet` → `{userId, balance, pending}`, 지갑이 없으면 `balance = 0`. `pending` = 내 출금 중 `RECEIVED` · `SENDING` · `UNKNOWN` 금액 합.
- **스키마**: `V3__wallet.sql` 은 스펙 §3 의 SQL 그대로(V1 · V2 는 고치지 않는다). user_id 컬럼은 모두 `VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin`.
- DB 시각 KST `DATETIME(6)`, 쓰기 전 `toDbPrecision()` (마이크로초 절사). JDBC 로 쓸 때는 `java.sql.Timestamp.from(instant)` 를 넘기고 KST 변환은 드라이버 설정(`connectionTimeZone=Asia/Seoul`)에 맡긴다 (database.md §2 "변환은 JDBC · Hibernate 설정 한 곳").
- 주석 · 커밋 한국어 "왜". 커밋 `type(scope): 요약` + 구현 모델 이름의 `Co-Authored-By:` 줄(아래 `<작성 모델>` 을 구현한 모델 이름으로 채운다). push 하지 않는다.
- 테스트: Docker Desktop 필요, Git Bash 에서 `export PATH="/c/Users/user/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"`. 통합 테스트는 `ApiIntegrationTest` 상속(컨텍스트 공유), 발행기 · 정리 작업 스케줄은 꺼 두고 직접 부른다.
- **공유 DB 규칙** (P3-3b 교훈, HANDOVER · `RetryTopicFlowTest` 주석): ① 테스트마다 **새 사용자 id**(`newUserId()` / `fundedUser()`, UUID 36자)를 쓴다 — `RepublishFlowTest` 의 재발행 대기 1ms 정리 작업이 조건에 맞는 모든 `RECEIVED` 를 훑어 재발행 · 포기(→ `fail()` → 환불)하므로, 다른 테스트의 잔고 단언이 흔들리지 않게 사용자로 격리한다. ② `RECEIVED` 를 남기는 테스트는 **모의 은행에 없는 계좌**(`unregisteredAccount()`, `55` + 11자리)를 쓴다 — 나중에 누가 처리해도 1001 거절 → `FAILED` → 환불로 끝나 공용 계좌(1002123456789)에 입금이 생기지 않는다. ③ 공용 계좌 입금은 **자기 거래번호 · 적요로** 센다.
- **Task 1 을 마치면 `apps/api` 는 컴파일되지 않는다** (포트 시그니처 변경). Task 1 은 `:modules:transfer:test :modules:account:test` 만. **Task 2 끝에서는 HTTP 를 거치는 테스트 5개 클래스**(`TransferApiTest`, `AccountVerificationApiTest`, `TransferProcessingFlowTest`, `ReconcileFlowTest`, `RepublishFlowTest`)가 `X-User-Id` 가 없어 400 으로 실패한다 — Task 3 에서 고친다. Task 2 는 지정한 테스트만 돌리고, 전체 `./gradlew build` 는 Task 3 부터.
- 범위 밖 (HANDOVER 에 남긴다): 인증, 은행 입금 통지로 잔고 채우기 · 충전 멱등, 락 전략 비교 · 부하 테스트(P4-2), 일마감 대사(P5), `CONFLICT` 자동 보정.

**라이브러리 · 코드 확인 결과 (계획 작성 때 소스 · jar · 실제 MySQL 8.4 컨테이너로 확인함 — 구현자는 다시 찾지 않아도 된다)**
1. **실패 확정 4경로 (지금 `transition(…, FAILED)` 를 부르는 곳)**:
   - 컨슈머 은행 거절 `SENDING → FAILED`: `TransferProcessor.kt:50` (`Rejected -> record(…, FAILED, …)`) → `record()` 의 `:66` `store.transition(id, SENDING, to, …)`.
   - 컨슈머 늦은 거절 `UNKNOWN → FAILED`: 같은 `record()` 의 `:67` `store.transition(id, UNKNOWN, to, …)`.
   - 정리 작업 ② 결과조회 2001 `UNKNOWN → FAILED`: `TransferReconciler.kt:74`.
   - 정리 작업 ⓪ 재발행 3회 소진 `RECEIVED → FAILED(E001)`: `TransferReconciler.kt:99`.
   - 그 밖에 `transition(…, FAILED)` 를 직접 부르는 곳은 테스트 `TransferProcessorTest.kt:201`(CONFLICT 재현) 하나 → `fail()` 로 바꾼다. 다른 호출자: `findByIdempotencyKey` = `TransferService.kt:34 · :56`, `TransferPersistence.kt:86 · :208`, 가짜 저장소 3개(`TransferServiceTest:43`, `TransferProcessorTest:21`, `TransferReconcilerTest:25`), `JpaTransferStoreTest:53`. `isVerified` = `TransferService.kt:35`, `AppConfiguration.kt:105`(람다), `TransferServiceTest:61`(람다). `existsVerified` = 포트 `AccountVerification.kt:47`, `InMemoryVerificationRepository`(`AccountVerificationService.kt` 끝), `AccountVerificationPersistence.kt:95`, 테스트 `AccountVerificationServiceTest:107~115`, `JpaVerificationRepositoryTest:74~81`. `saveWithEvent` 직접 호출 = `TransferService.kt:53`, 테스트 `JpaTransferStoreTest`(15곳), `OutboxRelayTest:45`, `RetryTopicFlowTest:57`, `ReconcileFlowTest:133`.
2. **`JpaTransferStore.saveWithEvent`** (`TransferPersistence.kt:188-204`): 생성자로 받은 `TransactionTemplate.execute { }` 안에서 `transfers.saveAndFlush` → `outbox.saveAndFlush` 하고, 바깥 `catch (e: DataIntegrityViolationException)` 에서 `e.mostSpecificCause.message` 에 **제약 이름이 들어 있는지**로 `DuplicateIdempotencyKey` · `DuplicateReference` 를 가른다. MySQL 8.4 실제 메시지: `Duplicate entry 'user-1-k1' for key 'transfer.uk_transfer_user_idempotency'` (컨테이너로 확인). `execute` 안에서 던진 `RuntimeException`(→ `StoreConflict.InsufficientBalance`)은 `TransactionTemplate` 이 롤백한 뒤 그대로 다시 던지고, `DataIntegrityViolationException` 이 아니라 catch 를 지나간다. Boot 의 `JpaTransactionManager` 는 EMF 의 `DataSource` 로 커넥션을 묶어 두므로 같은 트랜잭션 안의 `NamedParameterJdbcTemplate` 은 Hibernate 와 **같은 커넥션**을 쓴다(`saveAndFlush` 로 INSERT 가 먼저 나간 뒤 JDBC UPDATE). `NamedParameterJdbcTemplate` 빈은 Boot 가 만든다(`spring-boot-jdbc-4.1.1.jar` 의 `NamedParameterJdbcTemplateConfiguration`).
3. **MySQL 8.4 확인** (`mysql:8.4` = 8.4.11, Testcontainers 와 같은 명령행 옵션으로 V1 + V2 + P3 시절 행 2개 + V3 를 적용):
   - V3 의 `ALTER TABLE transfer ADD COLUMN … AFTER id, DROP INDEX uk_transfer_idempotency, ADD UNIQUE KEY uk_transfer_user_idempotency …, ADD INDEX …` 한 문장이 기존 행이 있어도 그대로 성공한다. 기존 행 user_id = NULL 끼리 충돌 없음, 새 `(user-1, k1)` · `(user-2, k1)` 도 공존, 같은 `(user-1, k1)` 만 1062.
   - CHECK 는 강제된다: `UPDATE wallet SET balance = -1` → **ERROR 3819 (SQLSTATE HY000)** `Check constraint 'ck_wallet_balance_non_negative' is violated.` Connector/J 는 일반 `java.sql.SQLException` 을 던지고, Spring 의 MySQL 오류 코드표(`sql-error-codes.xml`, spring-jdbc 7.0.9)에 3819 가 없어 **`UncategorizedSQLException`**(`DataAccessException` 하위)로 바뀐다 → 테스트는 `DataAccessException` + 메시지의 제약 이름으로 확인한다. `ck_ledger_amount_positive` 도 같은 3819.
   - 조건부 차감 `… AND balance >= :amount` 는 모자라면 `ROW_COUNT() = 0`. `INSERT … VALUES (…) AS new ON DUPLICATE KEY UPDATE balance = wallet.balance + new.balance …` 는 새 지갑 1, 기존 지갑 2 를 돌려준다(행 별칭 문법 — `VALUES()` 함수는 8.0.20 부터 경고).
   - `uk_ledger_transfer_type`: `transfer_id` NULL 인 `DEPOSIT` 여러 줄 허용, 같은 `(transfer_id, 'REFUND')` 두 번째는 1062 → Spring `DuplicateKeyException`.
   - 정렬: `user_id` 는 `ascii_bin` 이라 `'USER-1'` 로 `'user-1'` 이 안 찾아진다. `idempotency_key` 와 같은 `ascii_bin` 이라 복합 UNIQUE 의 비교 규칙이 일관된다.
   - 시각: JVM 시간대 UTC + URL `connectionTimeZone=Asia/Seoul&forceConnectionTimeZoneToSession=true` 에서 `Timestamp.from(2026-10-26T00:00:00.123456Z)` 와 `setObject(Instant)` 모두 `2026-10-26 09:00:00.123456` 으로 저장된다. 읽을 때 `getObject(…, Instant::class.java)` 는 Connector/J 9.7.0 이 지원하지 않는다 → `getTimestamp().toInstant()` 또는 `DATE_FORMAT` 으로 확인.
4. **웹 오류 매핑 · 헤더 읽기**: `ApiExceptionHandler.handle` 은 `@ExceptionHandler(CompletionException, AccountVerificationException, BankTimeoutException, BankUnavailableException, BankRejectedException, TransferException)` 목록 + `when (cause)` 매핑, 400 표준 오류는 `handleExceptionInternal` 이 `INVALID_REQUEST` 를 붙인다. `Idempotency-Key` 는 `@RequestHeader("Idempotency-Key", required = false)` 로 받아 빈 문자열을 도메인에 넘기고 **도메인이** 검사한다(`TransferController.kt:59-66`). `X-User-Id` 는 스펙대로 "웹 계층 한 곳"에서 검사해야 하므로 컨트롤러마다 `@RequestHeader` 를 두지 않고 `HandlerMethodArgumentResolver` 하나로 모은다: 애너테이션 없는 커스텀 타입 인자는 내장 애너테이션 리졸버 뒤 · 기본 `@ModelAttribute` 처리 앞에서 커스텀 리졸버가 받고, 인자 해석 중 던진 예외도 `@RestControllerAdvice` 로 간다. 인자는 선언 순서로 해석되므로 `RequestUser` 를 **첫 인자**로 두면 헤더 오류가 본문 검증보다 먼저 난다.
5. **`modules/account`**: `AccountVerification`(data class, id 다음에 필드 추가 가능 — 생성은 모두 이름 붙은 인자), `RequestVerification(bankCode, accountNo, holderName)`(위치 인자 생성: 컨트롤러 · 테스트), 포트 `VerificationRepository.update(id, change)` 는 JPA 에서 `SELECT … FOR UPDATE` 후 변경 감지(`AccountVerificationPersistence.kt:84-89`) — 소유 검사를 `change` 람다 안에서 하면(남의 건이면 그대로 돌려줌) 잠금 안에서 원자적으로 판단되고 실패 횟수도 늘지 않는다. `existsVerified` 는 파생 쿼리 `existsByBankCodeAndAccountNoAndStatus` → `existsByUserIdAndBankCodeAndAccountNoAndStatus` 로 바꾸고 V3 의 `idx_verification_user_account (user_id, bank_code, account_no, status)` 를 탄다. `applyChanges` 는 바뀌는 값만 옮기므로 user_id 는 만들 때만 쓴다.
6. **영향받는 테스트 · 손 확인 파일** (모두 `X-User-Id`, 출금은 충전된 지갑 + 그 사용자가 인증한 계좌 필요):
   - HTTP 로 출금 접수 · 조회: `TransferApiTest`, `TransferProcessingFlowTest`, `ReconcileFlowTest`, `RepublishFlowTest` (Task 3) · 1원인증: `AccountVerificationApiTest` (Task 3).
   - 도메인 객체를 직접 저장: `JpaTransferStoreTest`, `OutboxRelayTest`, `RetryTopicFlowTest`, `JpaVerificationRepositoryTest` (Task 2), `ReconcileFlowTest` 의 "30초 넘게 SENDING" (Task 3).
   - `http/account-verification.http` 1) ~ 5), `http/transfer.http` 1) · 2) · 2-1) · 3) ~ 6) · 7-1) ~ 7-5) · 8-1) ~ 8-3), Postman 19개 요청 파일 전부 (Task 3), README 의 curl 예시 (Task 5).
   - **공유 컨텍스트 함정**: `RepublishFlowTest` 의 정리 작업은 `republishAfter = 1ms` · `batchSize = 1_000` 이라 다른 클래스가 남긴 "`RECEIVED` + `sending_at > republished_at`" 을 모두 재발행하고, 3회를 쓴 건은 포기(`fail()` → 환불)한다. 그래서 위 공유 DB 규칙 ① ② 를 지킨다. `OutboxRelayTest` 의 `relay.publishPending()` 도 다른 테스트의 미발행 행을 모두 내보낸다(→ 처리되면 그 사용자 잔고가 바뀔 수 있다 — 사용자 격리로 무해).
7. 기존 `TransferRequestedListener` 는 `CONFLICT` 에 이미 경고만 남기고 ack 한다(`TransferRequestedListener.kt:55-58`) — 잔고를 건드리는 코드가 없다. 문구에 "잔고 그대로" 만 더한다.
8. 모의 은행(`MockBankServer.DEFAULT_ACCOUNTS`)에 없는 계좌는 성명조회가 1001 이라 **API 로는 1원인증을 마칠 수 없다** → "모르는 계좌 출금 → FAILED → 환불" 손 확인은 SQL 로 VERIFIED 행을 넣어 둔다 (Task 5). 자동 테스트는 `verifiedBy()` 로 DB 에 직접 넣는다.

## 파일 구조

| 파일 | 책임 |
|---|---|
| `modules/transfer/.../Transfer.kt` (수정) | `Transfer.userId`, `RequestTransfer.userId`, `TransferStore.findByIdempotencyKey(userId, key)` · `fail(…)`, `StoreConflict.InsufficientBalance`, `VerifiedAccounts.isVerified(userId, …)`, `TransferException.InsufficientBalance` |
| `modules/transfer/.../TransferService.kt` (수정) | 사용자 범위 멱등 조회 · 인증 계좌, 잔고 부족 → 422 예외, `get(userId, id)` 소유 확인 |
| `modules/transfer/.../TransferProcessor.kt`, `TransferReconciler.kt` (수정) | FAILED 로 가는 3곳(processor 2 · reconciler 2 = 4경로)을 `store.fail(…)` 로 |
| `modules/account/.../AccountVerification.kt`, `AccountVerificationService.kt` (수정) | `userId`, 확인은 본인만, `existsVerified(userId, …)` |
| `apps/api/src/main/resources/db/migration/V3__wallet.sql` (신규) | `wallet`, `wallet_ledger`, `transfer.user_id` + 사용자 범위 UNIQUE, `account_verification.user_id` |
| `apps/api/.../persistence/WalletLedger.kt` (신규) | 충전 · 차감 · 환불 · 잔고 · 보류 SQL 한 곳 (P4-2 이음새) |
| `apps/api/.../persistence/TransferPersistence.kt` (수정) | 엔티티 `userId`, 사용자 범위 조회, `saveWithEvent` 차감, `fail()` 환불, `transition` FAILED 거부, 새 제약 이름 매핑 |
| `apps/api/.../persistence/AccountVerificationPersistence.kt` (수정) | 엔티티 `userId`, 사용자 범위 `existsVerified` |
| `apps/api/.../config/AppConfiguration.kt` (수정) | `walletLedger` 빈, `JpaTransferStore(…, wallets)`, `VerifiedAccounts` 3인자 |
| `apps/api/.../web/RequestUser.kt` (신규) | `RequestUser`, `USER_ID_HEADER`, `UserIdRequiredException`, `RequestUserResolver`, `WebConfiguration` |
| `apps/api/.../web/{TransferController,AccountVerificationController,ApiExceptionHandler}.kt` (수정) | `RequestUser` 인자, 오류 코드 `USER_ID_REQUIRED` · `INSUFFICIENT_BALANCE` |
| `apps/api/.../web/WalletController.kt` (신규) | `POST /api/v1/wallet/deposits`, `GET /api/v1/wallet` |
| `apps/api/.../messaging/TransferRequestedListener.kt` (수정) | CONFLICT 경고 문구 |
| `apps/api/src/test/.../support/{ApiIntegrationTest.kt, Concurrently.kt(신규)}` | 사용자 · 충전 · 인증 계좌 · 원장 도우미, 동시 출발 도우미 |
| `apps/api/src/test/.../persistence/WalletLedgerTest.kt` · `UserIdHeaderApiTest.kt` · `WalletApiTest.kt` · `WalletConcurrencyTest.kt` (신규) | 저장소 · 헤더 · 지갑 API · 동시성 증명 |
| 기존 테스트 10개 클래스 · `http/*.http` · `postman/…` | 헤더 · 충전 · 소유 계좌 |
| 문서 (ADR-0008, database.md, OPERATION, ARCHITECTURE, README, SETUP, CHANGELOG, HANDOVER), `postman/…/5-wallet/` | 결정 기록 · 운영 · 손 확인 |

---

### Task 1: 도메인 — 사용자 범위 · 잔고 부족 · 실패 확정은 `fail()` 하나로

**Files:**
- Modify: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/Transfer.kt`
- Modify: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferService.kt`
- Modify: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferProcessor.kt`
- Modify: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferReconciler.kt`
- Modify: `modules/account/src/main/kotlin/io/github/hyeokjinon/banktransfer/account/AccountVerification.kt`
- Modify: `modules/account/src/main/kotlin/io/github/hyeokjinon/banktransfer/account/AccountVerificationService.kt`
- Test: `modules/transfer/src/test/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferServiceTest.kt` (4개 추가 · 가짜 저장소)
- Test: `modules/transfer/src/test/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferProcessorTest.kt` (1개 추가 · 가짜 저장소)
- Test: `modules/transfer/src/test/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferReconcilerTest.kt` (가짜 저장소 · 단언 2개)
- Test: `modules/account/src/test/kotlin/io/github/hyeokjinon/banktransfer/account/AccountVerificationServiceTest.kt` (2개 추가)

**Interfaces:**
- Produces:
  - `Transfer.userId: String? = null` (`id` 바로 다음 필드), `toString()` 에 `userId=` 포함
  - `data class RequestTransfer(userId: String, idempotencyKey: String, bankCode: String, accountNo: String, amount: Long, memo: String)` — **userId 가 첫 인자**
  - `TransferStore.findByIdempotencyKey(userId: String, key: String): Transfer?`
  - `TransferStore.fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String): Boolean`
  - `TransferStore.transition(…)` 계약: `to == FAILED` 면 `IllegalArgumentException` (구현이 `require`)
  - `StoreConflict.InsufficientBalance(userId: String, amount: Long)`
  - `fun interface VerifiedAccounts { fun isVerified(userId: String, bankCode: String, accountNo: String): Boolean }`
  - `TransferException.InsufficientBalance()` (인자 없음)
  - `TransferService.get(userId: String, id: UUID): Transfer`
  - `AccountVerification.userId: String? = null` (`id` 바로 다음), `data class RequestVerification(userId: String, bankCode: String, accountNo: String, holderName: String)`
  - `VerificationRepository.existsVerified(userId: String, bankCode: String, accountNo: String): Boolean`
  - `AccountVerificationService.confirm(userId: String, id: UUID, code: String): AccountVerification`

- [ ] **Step 1: 실패하는 테스트**

`TransferServiceTest.kt`:
1) import 에 `import kotlin.test.assertNotEquals` 추가.
2) `FakeStore` 클래스 전체와 그 아래 필드 · 도우미(현재 15~67행, `private class FakeStore` 부터 `private fun command(...)` 까지)를 다음으로 바꾼다:
```kotlin
    /** DB 대신 쓰는 가짜 저장소. UNIQUE 위반 상황(동시 요청 · 거래번호 충돌)과 잔고 부족을 흉내 낼 수 있다. */
    private class FakeStore : TransferStore {
        val transfers = linkedMapOf<UUID, Transfer>()
        val events = mutableListOf<TransferEvent>()

        /** 지정하면 다음 저장 직전에 "다른 요청이 먼저 같은 키로 저장한" 상황을 만든다. */
        var raceWinner: Transfer? = null

        /** 남은 횟수만큼 거래번호 충돌을 낸다. */
        var referenceConflicts = 0

        /** 지갑 잔고 (사용자 하나만 본다). 실제 저장소처럼 저장과 함께 빼고, 모자라면 아무것도 남기지 않고 InsufficientBalance. */
        var balance: Long = Long.MAX_VALUE

        override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer {
            raceWinner?.let { winner ->
                raceWinner = null
                transfers[winner.id] = winner
                throw StoreConflict.DuplicateIdempotencyKey(transfer.idempotencyKey)
            }
            if (referenceConflicts > 0) {
                referenceConflicts--
                throw StoreConflict.DuplicateReference(transfer.reference)
            }
            if (transfer.amount > balance) throw StoreConflict.InsufficientBalance(transfer.userId!!, transfer.amount)
            balance -= transfer.amount
            transfers[transfer.id] = transfer
            events += event
            return transfer
        }

        override fun findById(id: UUID) = transfers[id]

        override fun findByIdempotencyKey(userId: String, key: String) =
            transfers.values.firstOrNull { it.userId == userId && it.idempotencyKey == key }

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?) =
            throw UnsupportedOperationException("접수 테스트에서는 쓰지 않는다")

        override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String): Boolean = throw UnsupportedOperationException()
        override fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer> = throw UnsupportedOperationException()
        override fun findUnknownForInquiry(maxAttempts: Int, sendingBefore: Instant, limit: Int): List<Transfer> = throw UnsupportedOperationException()
        override fun recordInquiryAttempt(id: UUID, at: Instant): Boolean = throw UnsupportedOperationException()
        override fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer> = throw UnsupportedOperationException()
        override fun republish(id: UUID, expectedCount: Int, at: Instant): Boolean = throw UnsupportedOperationException()
    }

    private val store = FakeStore()
    private val verified = mutableSetOf(Triple(USER, "090", "1002123456789"))
    private var referenceSeq = 0
    private val clock = Clock.fixed(Instant.parse("2026-10-19T01:00:00Z"), ZoneOffset.UTC)
    private val service = TransferService(
        store = store,
        verifiedAccounts = { userId, bankCode, accountNo -> Triple(userId, bankCode, accountNo) in verified },
        referenceGenerator = { "T261019" + "%013d".format(++referenceSeq) },
        clock = clock,
    )

    private fun command(key: String = "key-1", amount: Long = 50_000, memo: String = "출금", userId: String = USER) =
        RequestTransfer(userId, key, "090", "1002123456789", amount, memo)
```
3) 클래스 맨 끝(마지막 `}` 바로 앞)에 상수를 둔다:
```kotlin

    private companion object {
        const val USER = "user-1"
        const val OTHER = "user-2"
    }
```
4) 기존 테스트를 고친다:
- `처음 요청은 RECEIVED 로 접수하고 이벤트를 함께 남긴다` 끝에 `assertEquals(USER, result.transfer.userId)` 추가.
- `은행코드는 숫자 3자리, 계좌번호는 숫자 10~16자리` 의 `invalid` 목록 5줄 모두 첫 인자로 `USER` 를 넣는다 (예: `RequestTransfer(USER, "b1", "09", "1002123456789", 50_000, "출금"),          // 은행코드 2자리`).
- `같은 키가 동시에 들어와 저장에서 지면 이긴 쪽 건을 돌려준다` 와 `동시 요청에서 졌는데 이긴 쪽의 내용이 다르면 IdempotencyKeyReused` 의 `Transfer(` 에 `userId = USER,` 를 `id = UUID.randomUUID(),` 다음에 넣는다 (이긴 쪽도 같은 사용자여야 다시 찾힌다).
- `없는 id 는 TransferNotFound`: `service.get(UUID.randomUUID())` → `service.get(USER, UUID.randomUUID())`.
5) 테스트 4개를 `companion object` 위에 추가:
```kotlin
    @Test
    fun `잔고가 모자라면 InsufficientBalance 이고 출금 기록 · 이벤트를 남기지 않는다`() {
        store.balance = 49_999

        assertFailsWith<TransferException.InsufficientBalance> { service.request(command(amount = 50_000)) }

        assertTrue(store.transfers.isEmpty())
        assertTrue(store.events.isEmpty())
        assertEquals(49_999L, store.balance)
    }

    @Test
    fun `멱등키는 사용자 범위 - 다른 사용자의 같은 키 · 같은 내용은 새 출금이다`() {
        verified += Triple(OTHER, "090", "1002123456789")
        val mine = service.request(command(userId = USER))

        val theirs = service.request(command(userId = OTHER))

        assertTrue(theirs.created)                              // 남의 출금 정보를 200 으로 돌려받지 않는다
        assertNotEquals(mine.transfer.id, theirs.transfer.id)
        assertEquals(OTHER, theirs.transfer.userId)
    }

    @Test
    fun `남이 인증한 계좌로는 출금할 수 없다`() {
        assertFailsWith<TransferException.AccountNotVerified> { service.request(command(userId = OTHER)) }
        assertTrue(store.transfers.isEmpty())
    }

    @Test
    fun `내 출금만 조회된다 - 남의 출금 · 사용자 없는 옛 출금은 TransferNotFound`() {
        val mine = service.request(command()).transfer
        val legacy = mine.copy(id = UUID.randomUUID(), userId = null, idempotencyKey = "legacy")   // P3 시절 행
        store.transfers[legacy.id] = legacy

        assertEquals(mine.id, service.get(USER, mine.id).id)
        assertFailsWith<TransferException.TransferNotFound> { service.get(OTHER, mine.id) }   // 존재 여부도 숨긴다
        assertFailsWith<TransferException.TransferNotFound> { service.get(USER, legacy.id) }
    }
```

`TransferProcessorTest.kt`:
1) `FakeStore` 클래스 전체(현재 15~40행)를 다음으로 바꾼다:
```kotlin
    /** 조건부 UPDATE 를 흉내 내는 가짜 저장소: 현재 상태가 from 일 때만 바꾼다. FAILED 는 실제 저장소처럼 fail() 로만 간다. */
    private class FakeStore : TransferStore {
        val transfers = linkedMapOf<UUID, Transfer>()

        /** 실제로 FAILED 로 바꾼 fail() 호출 (id, from, 은행 코드). 실제 저장소는 바로 이때 같은 트랜잭션에서 환불한다. */
        val failCalls = mutableListOf<Triple<UUID, TransferStatus, String>>()

        override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer = transfer.also { transfers[it.id] = it }
        override fun findById(id: UUID) = transfers[id]
        override fun findByIdempotencyKey(userId: String, key: String) =
            transfers.values.firstOrNull { it.userId == userId && it.idempotencyKey == key }

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean {
            require(to != TransferStatus.FAILED) { "FAILED 는 fail() 로만 간다 (환불을 빠뜨리지 않게)" }
            val current = transfers[id] ?: return false
            if (current.status != from) return false
            transfers[id] = current.copy(
                status = to,
                updatedAt = at,
                bankResponseCode = bankResponseCode,
                sendingAt = if (to == TransferStatus.SENDING) at else current.sendingAt,
            )
            return true
        }

        override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String): Boolean {
            val current = transfers[id] ?: return false
            if (current.status != from) return false
            transfers[id] = current.copy(status = TransferStatus.FAILED, updatedAt = at, bankResponseCode = bankResponseCode)
            failCalls += Triple(id, from, bankResponseCode)
            return true
        }

        override fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer> = emptyList()
        override fun findUnknownForInquiry(maxAttempts: Int, sendingBefore: Instant, limit: Int): List<Transfer> = emptyList()
        override fun recordInquiryAttempt(id: UUID, at: Instant) = false
        override fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer> = emptyList()
        override fun republish(id: UUID, expectedCount: Int, at: Instant) = false
    }
```
2) `은행이 거절하면 FAILED 와 응답코드` 끝에 추가:
```kotlin
        assertEquals(listOf(Triple(t.id, TransferStatus.SENDING, "1001")), store.failCalls)   // 환불 경로 (저장소가 같은 트랜잭션에서 돌려준다)
```
3) `이미 다른 결과로 확정된 건에 은행의 확정 결과가 오면 CONFLICT` 의 `racing` 안 줄
`store.transition(t.id, TransferStatus.UNKNOWN, TransferStatus.FAILED, now, "2001")   // 결과조회가 먼저 FAILED 로 확정`
을
`store.fail(t.id, TransferStatus.UNKNOWN, now, "2001")   // 결과조회가 먼저 FAILED 로 확정 (환불까지)`
로 바꾸고, 테스트 끝에 `assertEquals(1, store.failCalls.size)   // CONFLICT 는 잔고를 더 건드리지 않는다 — 환불은 결과조회의 한 번뿐` 을 추가.
4) 테스트 1개를 클래스 끝에 추가:
```kotlin
    @Test
    fun `정리 작업이 먼저 UNKNOWN 으로 바꾼 뒤 늦게 온 거절도 fail() 로 확정한다 (환불 경로)`() {
        val t = received()
        val racing = BankTransferPort { _, _, _, _, _ ->
            store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, now)   // 30초 넘게 늦은 응답 사이에 정리 작업이 끼어듦
            BankTransferOutcome.Rejected("1001")
        }

        assertEquals(ProcessResult.FAILED, TransferProcessor(store, racing, Clock.fixed(now, ZoneOffset.UTC)).process(t.id))
        assertEquals(TransferStatus.FAILED, store.transfers.getValue(t.id).status)
        // SENDING 에서 먼저 시도(0건) → UNKNOWN 에서 확정. 환불은 실제로 바뀐 한 번뿐이다
        assertEquals(listOf(Triple(t.id, TransferStatus.UNKNOWN, "1001")), store.failCalls)
    }
```

`TransferReconcilerTest.kt` 의 `FakeStore`:
1) `override fun findByIdempotencyKey(key: String) = transfers.values.firstOrNull { it.idempotencyKey == key }` 를 다음으로:
```kotlin
        override fun findByIdempotencyKey(userId: String, key: String) =
            transfers.values.firstOrNull { it.userId == userId && it.idempotencyKey == key }
```
2) `override fun transition(...)` 본문 첫 줄에 `require(to != FAILED) { "FAILED 는 fail() 로만 간다 (환불을 빠뜨리지 않게)" }` 를 넣는다.
3) `transition` 아래에 추가:
```kotlin

        /** 실제로 FAILED 로 바꾼 fail() 호출 (id, from, 은행 코드). 실제 저장소는 바로 이때 같은 트랜잭션에서 환불한다. */
        val failCalls = mutableListOf<Triple<UUID, TransferStatus, String>>()

        override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String): Boolean {
            val current = transfers[id] ?: return false
            if (current.status != from) return false
            transfers[id] = current.copy(status = FAILED, updatedAt = at, bankResponseCode = bankResponseCode)
            failCalls += Triple(id, from, bankResponseCode)
            return true
        }
```
4) `처리 안 됨이면 FAILED 와 은행 코드` 끝에 `assertEquals(listOf(Triple(t.id, UNKNOWN, "2001")), store.failCalls)   // 환불 경로` 추가.
5) `재발행 한도를 다 쓰고도 못 보냈으면 FAILED E001 로 포기하고 알린다` 끝에 `assertEquals(listOf(Triple(t.id, RECEIVED, "E001")), store.failCalls)   // 은행에 간 적 없음 → 차감액을 돌려준다` 추가.

`AccountVerificationServiceTest.kt`:
1) 도우미를 바꾼다:
```kotlin
    private fun request(name: String = "홍길동", accountNo: String = "1002123456789", userId: String = "user-1") =
        service.request(RequestVerification(userId, "090", accountNo, name))
```
2) `service.confirm(v.id, …)` 호출을 모두 `service.confirm("user-1", v.id, …)` 로, `service.confirm(UUID.randomUUID(), "1234")` 를 `service.confirm("user-1", UUID.randomUUID(), "1234")` 로 바꾼다.
3) `인증을 마친 계좌만 existsVerified 가 true` 의 `repository.existsVerified("090", …)` 4곳을 `repository.existsVerified("user-1", "090", …)` / `("user-1", "004", …)` 로 바꾸고 끝에 `assertEquals(false, repository.existsVerified("user-2", "090", "1002123456789"))   // 남이 인증한 계좌` 추가.
4) 테스트 2개를 클래스 끝에 추가:
```kotlin
    @Test
    fun `인증 요청에 요청한 사용자를 남긴다`() {
        assertEquals("user-1", request().get().userId)
    }

    @Test
    fun `남의 인증 건은 확인할 수 없다 - VerificationNotFound 이고 실패 횟수도 늘지 않는다`() {
        val v = request(userId = "user-1").get()

        assertFailsWith<AccountVerificationException.VerificationNotFound> { service.confirm("user-2", v.id, "1234") }
        assertFailsWith<AccountVerificationException.VerificationNotFound> { service.confirm("user-2", v.id, "0000") }

        val after = repository.findById(v.id)!!
        assertEquals(VerificationStatus.SENT, after.status)
        assertEquals(0, after.failedAttempts)                                     // 남이 시도 횟수를 써 버려 잠기게 할 수 없다
        assertEquals(VerificationStatus.VERIFIED, service.confirm("user-1", v.id, "1234").status)
    }
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:transfer:test :modules:account:test` → FAIL (컴파일: `'findByIdempotencyKey' overrides nothing`, `'fail' overrides nothing`, `Unresolved reference 'InsufficientBalance'`, `Too many arguments for RequestTransfer` / `RequestVerification`, `Unresolved reference 'userId'` …)

- [ ] **Step 3: 구현**

`Transfer.kt`:
1) `data class Transfer` 의 `val id: UUID,` 다음 줄에 추가:
```kotlin
    /** 출금한 사용자 (P4-1, `X-User-Id`). P3 시절 행은 null — 어떤 사용자에게도 보이지 않고 잔고와도 무관하다(차감한 적이 없다). */
    val userId: String? = null,
```
2) `toString()` 을 바꾼다:
```kotlin
    override fun toString(): String =
        "Transfer(id=$id, userId=$userId, reference=$reference, status=$status, bankCode=$bankCode, accountNo=${maskAccountNo(accountNo)}, " +
            "amount=$amount, bankResponseCode=$bankResponseCode)"
```
3) `RequestTransfer` 를 바꾼다:
```kotlin
data class RequestTransfer(
    /** 요청한 사용자. 웹 계층이 X-User-Id 에서 꺼내 형식을 검사한 값이다 (인증은 범위 밖, P4-1). */
    val userId: String,
    val idempotencyKey: String,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    val memo: String,
)
```
4) `interface TransferStore` 의 앞부분(`saveWithEvent` ~ `transition`)을 바꾸고 끝(`republish` 아래)에 `fail` 을 더한다:
```kotlin
interface TransferStore {
    /**
     * 이체 저장 · 잔고 차감 · 이벤트 기록은 함께 성공하거나 함께 실패한다 (P4-1: 차감 추가).
     * [transfer] 의 userId 지갑에서 amount 를 뺀다 — 잔고가 모자라면(지갑이 없어도 잔고 0) [StoreConflict.InsufficientBalance] 이고 아무것도 남지 않는다.
     * UNIQUE 위반이면 [StoreConflict] 를 던진다.
     */
    fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer

    fun findById(id: UUID): Transfer?

    /** 멱등키는 사용자 범위다 (P4-1): 다른 사용자의 같은 키는 다른 출금이다. */
    fun findByIdempotencyKey(userId: String, key: String): Transfer?

    /**
     * 현재 상태가 [from] 일 때만 [to] 로 바꾼다 (조건부 UPDATE). 바뀌었으면 true.
     * 같은 메시지를 두 번 받아도 두 번째는 false 라서 은행에 다시 보내지 않는다 — 도메인 상태가 곧 멱등키다.
     * [to] 가 SENDING 이면 sendingAt 도 [at] 으로 기록한다. bankResponseCode 는 주어진 값으로 덮어쓴다.
     * [to] 가 FAILED 면 거부한다(IllegalArgumentException) — 실패 확정은 환불과 한 트랜잭션인 [fail] 로만 한다 (P4-1).
     */
    fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String? = null): Boolean
```
`republish` 선언 아래에 추가:
```kotlin

    /**
     * 실패 확정 (P4-1). 현재 상태가 [from] 일 때만 FAILED([bankResponseCode]) 로 바꾸고, 같은 트랜잭션에서 차감했던 금액을 지갑에 돌려주며
     * 원장에 REFUND 를 남긴다. 바꿨으면 true. 0건(다른 쪽이 먼저 확정)이면 환불도 하지 않는다 → 두 번 불러도 환불은 한 번.
     * 차감 기록(WITHDRAW)이 없는 P3 시절 출금은 상태만 바꾼다. FAILED 로 가는 길은 이것 하나다 — 새 실패 경로가 생겨도 환불을 빠뜨릴 수 없게.
     */
    fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String): Boolean
}
```
5) `StoreConflict` 에 추가 (`DuplicateReference` 아래):
```kotlin
    /** 잔고가 출금액보다 적다 (지갑이 없으면 잔고 0). 저장소는 아무것도 남기지 않고 롤백한다 (P4-1). */
    class InsufficientBalance(userId: String, amount: Long) : StoreConflict("balance of '$userId' is less than $amount")
```
6) `VerifiedAccounts` 를 바꾼다:
```kotlin
/**
 * "이 사용자가 이 계좌의 1원인증을 마쳤나?" (P4-1: 사용자 범위 — 남이 인증한 계좌로는 출금할 수 없다).
 * transfer 모듈은 account 모듈을 import 하지 않는다. 연결은 apps/api 에서 한다.
 */
fun interface VerifiedAccounts {
    fun isVerified(userId: String, bankCode: String, accountNo: String): Boolean
}
```
7) `TransferException` 에 추가 (`AccountNotVerified` 아래):
```kotlin
    class InsufficientBalance : TransferException("wallet balance is not enough for this transfer")
```

`TransferService.kt`: 클래스 KDoc 과 `request` · `get` 을 다음으로 바꾼다 (`replay`, `validate`, `companion object`, `RandomReferenceGenerator` 는 그대로):
```kotlin
/**
 * 출금이체 접수.
 *
 * 1. 형식 검증 (키 · 금액 · 적요)
 * 2. 이 사용자의 같은 키가 이미 있으면 내용이 같을 때만 그 건을 돌려준다 (멱등, 사용자 범위 — P4-1). 차감은 없다
 * 3. 받는 계좌가 **이 사용자의** 1원인증을 마쳤는지 확인
 * 4. 이체(RECEIVED) · 잔고 차감 · TransferRequested 이벤트를 함께 저장 → 잔고가 모자라면 422 (출금 자체를 받지 않는다). 은행 처리는 컨슈머가 한다
 *
 * 2번의 "먼저 조회"는 빠른 길일 뿐이다. 동시에 들어온 같은 키는 DB UNIQUE 가 막고,
 * 진 쪽은 [StoreConflict.DuplicateIdempotencyKey] 를 받아 이긴 쪽 건을 돌려준다 (진 쪽의 차감도 함께 롤백된다).
 */
class TransferService(
    private val store: TransferStore,
    private val verifiedAccounts: VerifiedAccounts,
    private val referenceGenerator: ReferenceGenerator,
    private val clock: Clock = Clock.systemUTC(),
    private val maxReferenceAttempts: Int = 3,
) {

    fun request(command: RequestTransfer): TransferResult {
        validate(command)
        val hash = requestHash(command)
        store.findByIdempotencyKey(command.userId, command.idempotencyKey)?.let { return replay(it, hash) }
        if (!verifiedAccounts.isVerified(command.userId, command.bankCode, command.accountNo)) {
            throw TransferException.AccountNotVerified()
        }

        repeat(maxReferenceAttempts) {
            val now = clock.instant()
            val transfer = Transfer(
                id = UUID.randomUUID(),
                userId = command.userId,
                idempotencyKey = command.idempotencyKey,
                reference = referenceGenerator.next(),
                requestHash = hash,
                bankCode = command.bankCode,
                accountNo = command.accountNo,
                amount = command.amount,
                memo = command.memo,
                status = TransferStatus.RECEIVED,
                createdAt = now,
                updatedAt = now,
            )
            try {
                val saved = store.saveWithEvent(transfer, TransferEvent.TransferRequested(transfer.id, now))
                return TransferResult(saved, created = true)
            } catch (e: StoreConflict.DuplicateIdempotencyKey) {
                val existing = store.findByIdempotencyKey(command.userId, command.idempotencyKey) ?: throw e
                return replay(existing, hash)
            } catch (e: StoreConflict.InsufficientBalance) {
                // 잔고가 모자라면 출금 자체를 받지 않는다 (출금 기록 · 이벤트도 함께 롤백됐다). 다시 시도해도 같다
                throw TransferException.InsufficientBalance()
            } catch (conflict: StoreConflict.DuplicateReference) {
                // 무작위 거래번호가 우연히 겹쳤다. 새 번호로 다시 시도한다.
            }
        }
        error("could not issue a unique reference in $maxReferenceAttempts attempts")
    }

    /** 요청자의 출금만 보인다 (P4-1). 남의 출금 · 사용자 없는 P3 시절 출금은 없는 것과 같다 — 존재 여부도 숨긴다. */
    fun get(userId: String, id: UUID): Transfer =
        store.findById(id)?.takeIf { it.userId == userId } ?: throw TransferException.TransferNotFound(id)
```

`TransferProcessor.kt`:
1) import 에 `import java.time.Instant` 추가.
2) 클래스 KDoc 의 3번 줄 아래에 한 줄 추가: ` *    은행이 거절하면 FAILED 와 함께 차감한 금액을 돌려준다 — [TransferStore.fail] 이 한 트랜잭션에서 한다 (P4-1).`
3) `record` 를 바꾸고 `move` 를 더한다:
```kotlin
    private fun record(id: UUID, to: TransferStatus, bankResponseCode: String?, result: ProcessResult): ProcessResult {
        val now = clock.instant()
        if (move(id, SENDING, to, now, bankResponseCode)) return result
        if ((to == COMPLETED || to == FAILED) && move(id, UNKNOWN, to, now, bankResponseCode)) return result
        if (to == COMPLETED || to == FAILED) {
            val current = store.findById(id)?.status
            if ((current == COMPLETED || current == FAILED) && current != to) return ProcessResult.CONFLICT
        }
        return ProcessResult.ALREADY_HANDLED
    }

    /**
     * FAILED 는 환불과 한 트랜잭션이어야 하므로 [TransferStore.fail] 로만 간다 (P4-1, transition 은 FAILED 를 거부한다).
     * 거절(Rejected)은 늘 은행 응답코드가 있다.
     */
    private fun move(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean =
        if (to == FAILED) {
            store.fail(id, from, at, checkNotNull(bankResponseCode) { "FAILED needs a bank response code" })
        } else {
            store.transition(id, from, to, at, bankResponseCode)
        }
```

`TransferReconciler.kt`:
1) 클래스 KDoc 의 마지막 줄(` * 상태 변경은 모두 조건부라, …`) 위에 추가: ` * 실패 확정(② 처리 안 됨, ⓪ 포기)은 [TransferStore.fail] 로 한다 — 상태 전이와 환불이 한 트랜잭션이다 (P4-1).`
2) `is InquiryOutcome.NotProcessed ->` 아래 줄을 `if (store.fail(transfer.id, UNKNOWN, now, outcome.responseCode)) failed++` 로 바꾼다.
3) `republishOrAbandon` 의 `} else if (store.transition(transfer.id, RECEIVED, FAILED, now, NOT_SENT)) {` 를 `} else if (store.fail(transfer.id, RECEIVED, now, NOT_SENT)) {` 로 바꾼다.

`AccountVerification.kt`:
1) `data class AccountVerification` 의 `val id: UUID,` 다음 줄에 추가:
```kotlin
    /** 인증을 요청한 사용자 (P4-1). 출금은 내가 인증한 계좌로만, 확인도 본인만 한다. P3 시절 행은 null. */
    val userId: String? = null,
```
2) `RequestVerification` 을 `data class RequestVerification(val userId: String, val bankCode: String, val accountNo: String, val holderName: String)` 로 바꾼다.
3) `VerificationRepository.existsVerified` 를 바꾼다:
```kotlin
    /** 이 사용자가 이 계좌로 끝낸 인증(VERIFIED)이 있는가. 출금 받는 계좌 검증에 쓴다 (P4-1: 사용자 범위). */
    fun existsVerified(userId: String, bankCode: String, accountNo: String): Boolean
```

`AccountVerificationService.kt`:
1) `confirm` 을 바꾼다:
```kotlin
    /**
     * 코드 확인. 저장된 사용자와 [userId] 가 다르면 없는 건과 같다(VerificationNotFound) — 남의 인증을 확인하거나
     * 남의 실패 횟수를 써 버려 잠기게 하지 못하게 (P4-1). 판단은 저장소의 원자적 update 안에서 한다.
     */
    fun confirm(userId: String, id: UUID, code: String): AccountVerification {
        val now = clock.instant()
        val updated = repository.update(id) { v ->
            when {
                v.userId != userId -> v   // 남의 건: 아무것도 바꾸지 않는다
                v.status != VerificationStatus.SENT -> v
                !now.isBefore(v.expiresAt) -> v.copy(status = VerificationStatus.EXPIRED)
                hash(v.id, code) == v.codeHash -> v.copy(status = VerificationStatus.VERIFIED, verifiedAt = now)
                else -> {
                    val failed = v.failedAttempts + 1
                    v.copy(
                        failedAttempts = failed,
                        status = if (failed >= maxAttempts) VerificationStatus.LOCKED else VerificationStatus.SENT,
                    )
                }
            }
        } ?: throw AccountVerificationException.VerificationNotFound(id)
        if (updated.userId != userId) throw AccountVerificationException.VerificationNotFound(id)

        return when (updated.status) {
            VerificationStatus.VERIFIED -> updated
            VerificationStatus.EXPIRED -> throw AccountVerificationException.Expired()
            VerificationStatus.LOCKED -> throw AccountVerificationException.Locked()
            VerificationStatus.SENT -> throw AccountVerificationException.WrongCode(maxAttempts - updated.failedAttempts)
        }
    }
```
2) `sendOneWon` 의 `AccountVerification(` 에 `userId = command.userId,` 를 `id = id,` 다음에 넣는다.
3) `InMemoryVerificationRepository.existsVerified` 를 바꾼다:
```kotlin
    override fun existsVerified(userId: String, bankCode: String, accountNo: String): Boolean =
        store.values.any {
            it.userId == userId && it.bankCode == bankCode && it.accountNo == accountNo && it.status == VerificationStatus.VERIFIED
        }
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:transfer:test :modules:account:test` → PASS (transfer 43 + 5 = 48, account 9 + 2 = 11). `apps/api` 는 아직 컴파일되지 않는다 (Task 2 에서 해결).

- [ ] **Step 5: 커밋**

```bash
git add modules/transfer modules/account
git commit -m "feat(domain): 사용자 범위 출금 · 1원인증 소유, 잔고 부족 거절, 실패 확정은 fail() 하나로

멱등키 · 조회 · 받는 계좌 확인을 X-User-Id 사용자 범위로 바꾼다. 전역 범위면 남의 출금 정보를 200 으로
돌려받거나 남이 인증한 계좌로 출금할 수 있다. FAILED 로 가는 4경로를 TransferStore.fail() 하나로 모아,
새 실패 경로가 생겨도 환불을 빠뜨릴 수 없게 한다 (transition 은 FAILED 를 거부).

Co-Authored-By: <작성 모델>"
```

---

### Task 2: 스키마 V3 · `WalletLedger` · 저장소의 차감 · 환불 · `X-User-Id` 한 곳 — 컴파일 복구

**Files:**
- Create: `apps/api/src/main/resources/db/migration/V3__wallet.sql`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/WalletLedger.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/TransferPersistence.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/AccountVerificationPersistence.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/AppConfiguration.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/RequestUser.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/TransferController.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/AccountVerificationController.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/ApiExceptionHandler.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/TransferRequestedListener.kt` (경고 문구만)
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt` (도우미)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/DatabaseSetupTest.kt` (1개 추가)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/WalletLedgerTest.kt` (신규 7개)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/JpaTransferStoreTest.kt` (11개 추가 · 기존 고침)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/JpaVerificationRepositoryTest.kt` (1개 바꿈)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/UserIdHeaderApiTest.kt` (신규 2개)
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/OutboxRelayTest.kt`, `.../messaging/RetryTopicFlowTest.kt` (사용자 · 충전)

**Interfaces:**
- Consumes: Task 1 의 `TransferStore.findByIdempotencyKey(userId, key)` · `fail(id, from, at, bankResponseCode)`, `StoreConflict.InsufficientBalance(userId, amount)`, `Transfer.userId`, `RequestTransfer(userId, …)`, `TransferService.get(userId, id)`, `VerifiedAccounts.isVerified(userId, bankCode, accountNo)`, `AccountVerification.userId`, `RequestVerification(userId, …)`, `VerificationRepository.existsVerified(userId, …)`, `AccountVerificationService.confirm(userId, id, code)`, `TransferException.InsufficientBalance`
- Produces:
  - `enum class LedgerType { DEPOSIT, WITHDRAW, REFUND }`
  - `class WalletLedger(jdbc: NamedParameterJdbcTemplate, tx: TransactionTemplate, clock: Clock = Clock.systemUTC())` — `deposit(userId: String, amount: Long): Long`(충전 뒤 잔고, 자기 트랜잭션), `withdraw(userId: String, transferId: UUID, amount: Long, at: Instant): Boolean`(호출자 트랜잭션 필수), `refund(transferId: UUID, at: Instant): Boolean`(호출자 트랜잭션 필수), `balanceOf(userId: String): Long`, `pendingOf(userId: String): Long`
  - `JpaTransferStore(transfers, outbox, tx, wallets: WalletLedger, payloadOf = ::outboxPayload)` — **`wallets` 가 4번째 인자**
  - `TransferJpaRepository.findByUserIdAndIdempotencyKey(userId: String, idempotencyKey: String): TransferEntity?`
  - `AccountVerificationJpaRepository.existsByUserIdAndBankCodeAndAccountNoAndStatus(userId, bankCode, accountNo, status): Boolean`
  - `data class RequestUser(val id: String)`, `const val USER_ID_HEADER = "X-User-Id"`, `class UserIdRequiredException`, `class RequestUserResolver : HandlerMethodArgumentResolver`, `class WebConfiguration : WebMvcConfigurer`
  - 오류 코드: `UserIdRequiredException` → 400 `USER_ID_REQUIRED`, `TransferException.InsufficientBalance` → 422 `INSUFFICIENT_BALANCE`
  - 테스트 도우미 (`ApiIntegrationTest`): `protected lateinit var wallets: WalletLedger`, `newUserId(): String`, `fundedUser(amount: Long = 10_000_000): String`, `verifiedBy(userId: String, accountNo: String): String`, `unregisteredAccount(): String`, `ledgerTypes(transferId: UUID): List<String>`, `ledgerSum(userId: String): Long`

- [ ] **Step 1: 실패하는 테스트**

`ApiIntegrationTest.kt`:
1) import 추가:
```kotlin
import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.persistence.WalletLedger
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.util.UUID
import kotlin.random.Random
```
2) 클래스 KDoc 끝에 줄 추가: ` * 잔고 단언이 다른 테스트에 흔들리지 않게 테스트마다 새 사용자를 쓴다 (P4-1): 정리 작업(RepublishFlowTest 의 1ms 대기)이 남은 RECEIVED 를 모두 훑어 환불할 수 있다.`
3) `protected val client …` 아래에 추가:
```kotlin

    @Autowired
    protected lateinit var wallets: WalletLedger

    // 이름에 test 를 붙인다: 하위 클래스가 이미 private verifications · jdbc 를 갖고 있어 같은 이름이면 가려진다
    @Autowired
    private lateinit var testVerifications: VerificationRepository

    @Autowired
    private lateinit var testJdbc: JdbcTemplate

    /** 새 사용자 id (UUID 36자 = X-User-Id 상한). 테스트끼리 DB 를 공유하므로 매번 새로 만든다. */
    protected fun newUserId(): String = UUID.randomUUID().toString()

    /** 새 사용자에게 [amount] 원을 충전해 id 를 돌려준다 (충전 API 를 거치지 않고 WalletLedger 로). */
    protected fun fundedUser(amount: Long = 10_000_000): String = newUserId().also { wallets.deposit(it, amount) }

    /** [userId] 가 1원인증을 마친(VERIFIED) 계좌로 DB 에 남긴다. 받는 계좌 확인은 사용자 범위다 (P4-1). */
    protected fun verifiedBy(userId: String, accountNo: String): String {
        testVerifications.save(
            AccountVerification(
                id = UUID.randomUUID(), userId = userId, bankCode = "090", accountNo = accountNo, holderName = "홍길동",
                codeHash = "0".repeat(64), status = VerificationStatus.VERIFIED, failedAttempts = 0,
                expiresAt = Instant.now(), verifiedAt = Instant.now(),
            ),
        )
        return accountNo
    }

    /**
     * 모의 은행에 없는 계좌 (처리되면 1001 거절 → FAILED → 환불). RECEIVED 를 남기는 테스트는 이것을 쓴다:
     * 나중에 다른 테스트의 발행 · 정리 작업이 처리해도 공용 계좌(1002123456789)에 입금이 생기지 않는다.
     */
    protected fun unregisteredAccount(): String = "55" + Random.nextLong(10_000_000_000, 99_999_999_999)

    /** 이 출금의 원장 종류(WITHDRAW, REFUND …)를 기록 순서대로. */
    protected fun ledgerTypes(transferId: UUID): List<String> =
        testJdbc.queryForList("SELECT type FROM wallet_ledger WHERE transfer_id = ? ORDER BY id", String::class.java, transferId.toString())

    /** 원장 합 = 충전 - 출금 + 환불. 지갑 잔고와 같아야 한다 (database.md §9 운영 쿼리와 같은 식). */
    protected fun ledgerSum(userId: String): Long =
        testJdbc.queryForObject(
            "SELECT COALESCE(SUM(CASE type WHEN 'WITHDRAW' THEN -amount ELSE amount END), 0) FROM wallet_ledger WHERE user_id = ?",
            Long::class.java, userId,
        )!!
```

`DatabaseSetupTest.kt` 끝에 추가:
```kotlin
    @Test
    fun `V3 가 지갑 · 원장 테이블과 사용자 범위 멱등 UNIQUE 를 만들었다 (user_id 는 모두 ascii_bin)`() {
        val tables = jdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() " +
                "AND table_name IN ('wallet', 'wallet_ledger') ORDER BY table_name",
            String::class.java,
        )
        assertEquals(listOf("wallet", "wallet_ledger"), tables)

        val uniqueColumns = jdbc.queryForList(
            "SELECT column_name FROM information_schema.statistics WHERE table_schema = DATABASE() " +
                "AND table_name = 'transfer' AND index_name = 'uk_transfer_user_idempotency' ORDER BY seq_in_index",
            String::class.java,
        )
        assertEquals(listOf("user_id", "idempotency_key"), uniqueColumns)
        val oldUnique = jdbc.queryForObject(
            "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() " +
                "AND table_name = 'transfer' AND index_name = 'uk_transfer_idempotency'",
            Int::class.java,
        )
        assertEquals(0, oldUnique)   // 전역 UNIQUE 는 없어졌다 — 남의 키와 겹쳐도 된다

        for (table in listOf("wallet", "wallet_ledger", "transfer", "account_verification")) {
            val collation = jdbc.queryForObject(
                "SELECT collation_name FROM information_schema.columns " +
                    "WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'user_id'",
                String::class.java, table,
            )
            assertEquals("ascii_bin", collation, "$table.user_id")
        }
    }
```

`WalletLedgerTest.kt` (신규, `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/`):
```kotlin
package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.DataAccessException
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 지갑 · 원장 SQL (P4-1). 차감 · 환불은 호출자 트랜잭션 안에서만 부르므로 테스트도 tx.execute 로 감싼다. */
class WalletLedgerTest : ApiIntegrationTest() {

    @Autowired private lateinit var tx: TransactionTemplate
    @Autowired private lateinit var jdbc: JdbcTemplate

    private fun ledgerRows(userId: String) = jdbc.queryForList(
        "SELECT type, amount, balance_after, transfer_id FROM wallet_ledger WHERE user_id = ? ORDER BY id", userId,
    )

    @Test
    fun `충전 - 지갑이 없으면 만들고 있으면 더한다, 원장 DEPOSIT 에 충전 뒤 잔고`() {
        val user = newUserId()
        assertEquals(0L, wallets.balanceOf(user))   // 지갑이 없으면 0

        assertEquals(30_000L, wallets.deposit(user, 30_000))
        assertEquals(50_000L, wallets.deposit(user, 20_000))

        assertEquals(50_000L, wallets.balanceOf(user))
        val rows = ledgerRows(user)
        assertEquals(listOf("DEPOSIT", "DEPOSIT"), rows.map { it["type"] })
        assertEquals(listOf(30_000L, 50_000L), rows.map { (it["balance_after"] as Number).toLong() })
        assertEquals(listOf(null, null), rows.map { it["transfer_id"] })
    }

    @Test
    fun `차감 - 잔고가 충분하면 빼고 WITHDRAW, 모자라거나 지갑이 없으면 0건이고 아무것도 쓰지 않는다`() {
        val user = fundedUser(10_000)
        val first = UUID.randomUUID()
        val now = Instant.now()

        assertTrue(tx.execute { wallets.withdraw(user, first, 7_000, now) }!!)
        assertFalse(tx.execute { wallets.withdraw(user, UUID.randomUUID(), 3_001, now) }!!)
        assertFalse(tx.execute { wallets.withdraw(newUserId(), UUID.randomUUID(), 1, now) }!!)   // 지갑 없음 = 잔고 0

        assertEquals(3_000L, wallets.balanceOf(user))
        val rows = ledgerRows(user)
        assertEquals(listOf("DEPOSIT", "WITHDRAW"), rows.map { it["type"] })
        assertEquals(first.toString(), rows[1]["transfer_id"])
        assertEquals(3_000L, (rows[1]["balance_after"] as Number).toLong())
    }

    @Test
    fun `차감 · 환불은 호출자 트랜잭션 밖에서 부르면 거부한다`() {
        val user = fundedUser(10_000)

        assertFailsWith<IllegalStateException> { wallets.withdraw(user, UUID.randomUUID(), 1, Instant.now()) }
        assertFailsWith<IllegalStateException> { wallets.refund(UUID.randomUUID(), Instant.now()) }

        assertEquals(10_000L, wallets.balanceOf(user))
    }

    @Test
    fun `환불 - 원장의 WITHDRAW 금액을 돌려주고 REFUND, 차감 기록이 없으면 false`() {
        val user = fundedUser(10_000)
        val transferId = UUID.randomUUID()
        val now = Instant.now()
        tx.execute { wallets.withdraw(user, transferId, 4_000, now) }

        assertTrue(tx.execute { wallets.refund(transferId, now) }!!)
        assertFalse(tx.execute { wallets.refund(UUID.randomUUID(), now) }!!)   // P3 시절 출금: 뺀 적이 없으니 돌려줄 것도 없다

        assertEquals(10_000L, wallets.balanceOf(user))
        assertEquals(listOf("DEPOSIT", "WITHDRAW", "REFUND"), ledgerRows(user).map { it["type"] })
    }

    @Test
    fun `같은 출금에 REFUND 를 두 번 쓰면 UNIQUE 가 막고 그 트랜잭션의 잔고 증가도 롤백된다`() {
        val user = fundedUser(10_000)
        val transferId = UUID.randomUUID()
        val now = Instant.now()
        tx.execute { wallets.withdraw(user, transferId, 4_000, now) }
        tx.execute { wallets.refund(transferId, now) }

        assertFailsWith<DuplicateKeyException> { tx.execute { wallets.refund(transferId, now) } }

        assertEquals(10_000L, wallets.balanceOf(user))   // 이중 환불의 2차 방어선
    }

    @Test
    fun `CHECK 제약이 음수 잔고를 거부한다 (조건부 UPDATE 를 우회한 SQL 에 대한 2차 방어선)`() {
        val user = fundedUser(1_000)

        // MySQL 3819 (SQLSTATE HY000) 은 Spring 오류 코드표에 없어 UncategorizedSQLException 이 된다 → 상위 타입 + 제약 이름으로 본다
        val e = assertFailsWith<DataAccessException> {
            jdbc.update("UPDATE wallet SET balance = balance - 1001 WHERE user_id = ?", user)
        }

        assertTrue("ck_wallet_balance_non_negative" in e.mostSpecificCause.message.orEmpty(), e.toString())
        assertEquals(1_000L, wallets.balanceOf(user))
    }

    @Test
    fun `잔고 = 원장 합 (충전 - 출금 + 환불), 원장 시각은 KST 로 저장된다`() {
        val user = fundedUser(50_000)
        val refunded = UUID.randomUUID()
        val at = Instant.parse("2026-10-26T00:00:00Z")
        tx.execute {
            wallets.withdraw(user, UUID.randomUUID(), 20_000, at)
            wallets.withdraw(user, refunded, 5_000, at)
            wallets.refund(refunded, at)
        }

        assertEquals(30_000L, wallets.balanceOf(user))
        assertEquals(wallets.balanceOf(user), ledgerSum(user))
        val kst = jdbc.queryForObject(
            "SELECT DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') FROM wallet_ledger WHERE transfer_id = ? AND type = 'REFUND'",
            String::class.java, refunded.toString(),
        )
        assertEquals("2026-10-26 09:00:00", kst)   // 테스트 JVM 은 UTC — 변환은 JDBC 설정(connectionTimeZone)이 한다
    }
}
```

`JpaTransferStoreTest.kt`:
1) 필드 · 도우미를 바꾼다 (`private val references = …` 아래부터 `private fun requested(...)` 까지):
```kotlin
    private val references = RandomReferenceGenerator()

    /** 이 테스트의 사용자 (테스트마다 새로, 1천만 원 충전 — 출금마다 5만 원씩 빠진다). */
    private val user by lazy { fundedUser() }

    private fun newTransfer(
        key: String = UUID.randomUUID().toString(),
        reference: String = references.next(),
        userId: String = user,
    ): Transfer {
        val now = Instant.parse("2026-10-19T01:00:00.123456Z")
        return Transfer(
            id = UUID.randomUUID(), userId = userId, idempotencyKey = key, reference = reference, requestHash = "h".repeat(64),
            bankCode = "090", accountNo = "1002123456789", amount = 50_000, memo = "출금",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
    }

    private fun requested(t: Transfer) = TransferEvent.TransferRequested(t.id, t.createdAt)
```
2) `이체와 아웃박스 이벤트를 함께 저장한다` 의 `store.findByIdempotencyKey(transfer.idempotencyKey)` → `store.findByIdempotencyKey(user, transfer.idempotencyKey)`.
3) `JpaTransferStore(transfers, outbox, tx, payloadOf = { "not-json" })` 2곳 → `JpaTransferStore(transfers, outbox, tx, wallets, payloadOf = { "not-json" })`.
4) 테스트 11개를 클래스 끝에 추가:
```kotlin
    @Test
    fun `접수 - 출금 INSERT · 잔고 차감 · 원장 WITHDRAW · 아웃박스가 한 트랜잭션에 들어간다`() {
        val before = wallets.balanceOf(user)
        val t = newTransfer()

        store.saveWithEvent(t, requested(t))

        assertEquals(before - 50_000, wallets.balanceOf(user))
        assertEquals(listOf("WITHDRAW"), ledgerTypes(t.id))
        assertEquals(1, outbox.countByAggregateId(t.id.toString()))
    }

    @Test
    fun `접수 - 잔고가 모자라면 InsufficientBalance 이고 출금 · 원장 · 아웃박스 모두 남지 않는다`() {
        val poor = fundedUser(49_999)
        val t = newTransfer(userId = poor)

        assertFailsWith<StoreConflict.InsufficientBalance> { store.saveWithEvent(t, requested(t)) }

        assertNull(store.findById(t.id))
        assertEquals(49_999L, wallets.balanceOf(poor))
        assertEquals(emptyList(), ledgerTypes(t.id))
        assertEquals(0, outbox.countByAggregateId(t.id.toString()))
    }

    @Test
    fun `접수 - 지갑이 없으면 잔고 0 으로 보고 InsufficientBalance`() {
        val t = newTransfer(userId = newUserId())

        assertFailsWith<StoreConflict.InsufficientBalance> { store.saveWithEvent(t, requested(t)) }
        assertNull(store.findById(t.id))
    }

    @Test
    fun `접수 - 사용자가 없는 출금은 저장하지 않는다 (차감 없이 나가는 길을 막는다)`() {
        val t = newTransfer().copy(userId = null)

        assertFailsWith<IllegalArgumentException> { store.saveWithEvent(t, requested(t)) }
        assertNull(store.findById(t.id))
    }

    @Test
    fun `접수 - 아웃박스 INSERT 가 실패하면 차감도 롤백된다`() {
        val broken = JpaTransferStore(transfers, outbox, tx, wallets, payloadOf = { "not-json" })
        val before = wallets.balanceOf(user)
        val t = newTransfer()

        assertFails { broken.saveWithEvent(t, requested(t)) }

        assertEquals(before, wallets.balanceOf(user))   // 돈만 빠지고 출금은 없는 상태가 생기지 않는다
        assertEquals(emptyList(), ledgerTypes(t.id))
        assertNull(store.findById(t.id))
    }

    @Test
    fun `같은 키 · 다른 사용자는 별개 출금이다 (UNIQUE 는 user_id + idempotency_key)`() {
        val other = fundedUser()
        val mine = newTransfer()
        val theirs = newTransfer(key = mine.idempotencyKey, userId = other)

        store.saveWithEvent(mine, requested(mine))
        store.saveWithEvent(theirs, requested(theirs))

        assertEquals(mine.id, store.findByIdempotencyKey(user, mine.idempotencyKey)!!.id)
        assertEquals(theirs.id, store.findByIdempotencyKey(other, mine.idempotencyKey)!!.id)
    }

    @Test
    fun `transition 은 FAILED 를 거부한다 - 환불을 빠뜨리지 않게 fail() 로만`() {
        val t = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        store.transition(t.id, TransferStatus.RECEIVED, TransferStatus.SENDING, Instant.now())

        assertFailsWith<IllegalArgumentException> {
            store.transition(t.id, TransferStatus.SENDING, TransferStatus.FAILED, Instant.now(), "1001")
        }
        assertEquals(TransferStatus.SENDING, store.findById(t.id)!!.status)
    }

    @Test
    fun `fail - 조건부 전이가 1건이면 FAILED 와 코드, 차감액을 돌려주고 원장 REFUND`() {
        val before = wallets.balanceOf(user)
        val t = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        store.transition(t.id, TransferStatus.RECEIVED, TransferStatus.SENDING, Instant.now())
        val at = Instant.parse("2026-10-26T01:00:00.123456Z")

        assertTrue(store.fail(t.id, TransferStatus.SENDING, at, "1001"))

        val after = store.findById(t.id)!!
        assertEquals(TransferStatus.FAILED, after.status)
        assertEquals("1001", after.bankResponseCode)
        assertEquals(at, after.updatedAt)
        assertEquals(before, wallets.balanceOf(user))
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(t.id))
    }

    @Test
    fun `fail - 두 번 불러도 (이미 FAILED 면 0건) 환불은 한 번`() {
        val before = wallets.balanceOf(user)
        val t = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        store.transition(t.id, TransferStatus.RECEIVED, TransferStatus.SENDING, Instant.now())
        store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, Instant.now())

        assertTrue(store.fail(t.id, TransferStatus.UNKNOWN, Instant.now(), "2001"))
        assertFalse(store.fail(t.id, TransferStatus.UNKNOWN, Instant.now(), "2001"))   // 컨슈머 · 정리 작업이 겹친 상황

        assertEquals(before, wallets.balanceOf(user))
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(t.id))
    }

    @Test
    fun `fail - 환불이 실패하면 상태 전이도 롤백된다 (한 트랜잭션)`() {
        val t = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        // 이미 REFUND 가 있는 것처럼 만든다 → 환불의 원장 INSERT 가 uk_ledger_transfer_type 에 걸린다
        jdbc.update(
            "INSERT INTO wallet_ledger (user_id, type, amount, balance_after, transfer_id, created_at) VALUES (?, 'REFUND', 1, 0, ?, NOW(6))",
            user, t.id.toString(),
        )

        assertFails { store.fail(t.id, TransferStatus.RECEIVED, Instant.now(), "E001") }

        assertEquals(TransferStatus.RECEIVED, store.findById(t.id)!!.status)   // "FAILED 인데 돈은 안 돌아온" 상태가 생기지 않는다
    }

    @Test
    fun `fail - 차감 기록이 없는 P3 시절 출금(user_id NULL)은 상태만 바꾸고 환불하지 않는다`() {
        val id = UUID.randomUUID()
        jdbc.update(
            "INSERT INTO transfer (id, idempotency_key, reference, request_hash, bank_code, account_no, amount, memo, status, " +
                "created_at, updated_at, version) VALUES (?, ?, ?, ?, '090', '1002123456789', 1000, 'legacy', 'UNKNOWN', NOW(6), NOW(6), 0)",
            id.toString(), UUID.randomUUID().toString(), references.next(), "h".repeat(64),
        )

        assertTrue(store.fail(id, TransferStatus.UNKNOWN, Instant.now(), "2001"))

        val after = store.findById(id)!!
        assertEquals(TransferStatus.FAILED, after.status)
        assertNull(after.userId)
        assertEquals(emptyList(), ledgerTypes(id))
    }
```
`JpaVerificationRepositoryTest.kt`:
1) `newVerification` 에 `userId: String = "user-1",` 인자를 맨 앞에 더하고 `AccountVerification(` 의 `id = UUID.randomUUID(),` 다음에 `userId = userId,` 를 넣는다.
2) `저장한 값과 다시 읽은 값이 같다 (마이크로초 단위)` 끝에 `assertEquals("user-1", repository.findById(saved.id)!!.userId)` 추가.
3) `VERIFIED 가 된 계좌만 existsVerified` 를 다음으로 바꾼다:
```kotlin
    @Test
    fun `이 사용자가 VERIFIED 로 만든 계좌만 existsVerified (사용자 범위)`() {
        val saved = repository.save(newVerification(userId = "user-1"))
        assertFalse(repository.existsVerified("user-1", "090", saved.accountNo))

        repository.update(saved.id) { it.copy(status = VerificationStatus.VERIFIED, verifiedAt = Instant.now()) }

        assertTrue(repository.existsVerified("user-1", "090", saved.accountNo))
        assertFalse(repository.existsVerified("user-2", "090", saved.accountNo))   // 남이 인증한 계좌로는 출금할 수 없다
        assertFalse(repository.existsVerified("user-1", "004", saved.accountNo))
    }
```

`UserIdHeaderApiTest.kt` (신규, `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/`):
```kotlin
package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

/** X-User-Id 는 웹 계층 한 곳(RequestUserResolver)에서 꺼내고 검사한다 (P4-1). 은행 · DB 까지 가기 전에 400 이다. */
class UserIdHeaderApiTest : ApiIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun call(method: HttpMethod, path: String, userId: String?, body: Any? = null): Pair<Int, Any?> =
        client.method(method).uri(path)
            .apply { if (userId != null) header(USER_ID_HEADER, userId) }
            .apply { if (body != null) contentType(MediaType.APPLICATION_JSON).body(body) }
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .exchange { _, r -> r.statusCode.value() to r.bodyTo(mapType)?.get("code") }

    private val transferBody = mapOf("bankCode" to "090", "accountNo" to "1002123456789", "amount" to 1_000, "memo" to "헤더")
    private val verificationBody = mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동")

    @Test
    fun `X-User-Id 가 없으면 출금 접수 · 조회 · 1원인증 · 확인 모두 400 USER_ID_REQUIRED`() {
        val expected = 400 to "USER_ID_REQUIRED"
        assertEquals(expected, call(HttpMethod.POST, "/api/v1/transfers", null, transferBody))
        assertEquals(expected, call(HttpMethod.GET, "/api/v1/transfers/${UUID.randomUUID()}", null))
        assertEquals(expected, call(HttpMethod.POST, "/api/v1/account-verifications", null, verificationBody))
        assertEquals(expected, call(HttpMethod.POST, "/api/v1/account-verifications/${UUID.randomUUID()}/confirm", null, mapOf("code" to "1234")))
    }

    @Test
    fun `X-User-Id 형식이 틀리면 (공백 · 밑줄 · 37자 · 빈 값) 400 USER_ID_REQUIRED`() {
        for (userId in listOf("user 1", "user_1", "a".repeat(37), "")) {
            assertEquals(400 to "USER_ID_REQUIRED", call(HttpMethod.GET, "/api/v1/transfers/${UUID.randomUUID()}", userId), "[$userId]")
        }
    }
}
```

`OutboxRelayTest.kt`: `private val references = …` 아래에 `private val user by lazy { fundedUser() }` 를 넣고, `saveTransfer()` 의 `Transfer(` 에 `userId = user,` 를 `id = UUID.randomUUID(),` 다음에 넣는다. KDoc 첫 줄 끝에 ` 사용자는 테스트마다 새로 충전한다 (접수 = 차감, P4-1).` 를 붙인다.

`RetryTopicFlowTest.kt`: `private val references = …` 아래에 `private val user by lazy { fundedUser() }` 를 넣고, `receivedTransfer()` 의 `Transfer(` 에 `userId = user,` 를 `id = UUID.randomUUID(),` 다음에 넣는다.

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:compileTestKotlin` → FAIL (main 컴파일: `JpaTransferStore` 가 `fail` · `findByIdempotencyKey(String, String)` 를 구현하지 않음, `RequestTransfer` 인자 부족, `VerifiedAccounts` 람다 인자 수, `Unresolved reference 'WalletLedger'`, `'USER_ID_HEADER'` …)

- [ ] **Step 3: 구현 — 스키마 · 저장소 · 조립**

`V3__wallet.sql`:
```sql
-- P4-1 원화 잔고. 정의와 이유는 docs/database.md §4-4 · §4-5 와 ADR-0008. V1 · V2 는 고치지 않는다.
-- 기존(P3) 행의 user_id 는 NULL 로 남는다: 어떤 사용자에게도 보이지 않고, 차감한 적이 없어 실패해도 환불하지 않는다.
-- uk_transfer_user_idempotency 에서 NULL 은 서로 다른 값이라 옛 행끼리 충돌하지 않는다 (이미 전역 UNIQUE 로 들어온 값).

CREATE TABLE wallet (
  user_id    VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
  balance    BIGINT      NOT NULL,
  version    BIGINT      NOT NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  CONSTRAINT ck_wallet_balance_non_negative CHECK (balance >= 0)
);

CREATE TABLE wallet_ledger (
  id            BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id       VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  type          VARCHAR(10) NOT NULL,          -- DEPOSIT / WITHDRAW / REFUND
  amount        BIGINT      NOT NULL,          -- 항상 양수, 방향은 type
  balance_after BIGINT      NOT NULL,
  transfer_id   CHAR(36)    NULL,              -- DEPOSIT 은 NULL
  created_at    DATETIME(6) NOT NULL,
  UNIQUE KEY uk_ledger_transfer_type (transfer_id, type),   -- 같은 이체에 출금 · 환불이 두 번 기록될 수 없다 (NULL 은 여러 개 허용)
  INDEX idx_ledger_user (user_id, id),
  CONSTRAINT ck_ledger_amount_positive CHECK (amount > 0)
);

ALTER TABLE transfer
  ADD COLUMN user_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL AFTER id,
  DROP INDEX uk_transfer_idempotency,
  ADD UNIQUE KEY uk_transfer_user_idempotency (user_id, idempotency_key),
  ADD INDEX idx_transfer_user (user_id, created_at);

ALTER TABLE account_verification
  ADD COLUMN user_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL AFTER id,
  ADD INDEX idx_verification_user_account (user_id, bank_code, account_no, status);
```

`WalletLedger.kt` (신규, `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/`):
```kotlin
package io.github.hyeokjinon.banktransfer.api.persistence

import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import java.sql.Timestamp
import java.sql.Types
import java.time.Clock
import java.time.Instant
import java.util.UUID

/** 원장 한 줄의 종류. amount 는 항상 양수이고 방향은 종류가 정한다 (잔고 = DEPOSIT - WITHDRAW + REFUND). */
enum class LedgerType { DEPOSIT, WITHDRAW, REFUND }

/**
 * 사용자 원화 잔고(wallet)와 append-only 원장(wallet_ledger)의 돈 움직임 SQL 을 한곳에 모은다 (P4-1, ADR-0008).
 * P4-2 에서 차감 방식을 비관적 락 · 낙관적 락(version) · Redis 분산락으로 바꿔 끼울 이음새가 [withdraw] 다.
 *
 * - 차감은 조건부 UPDATE 한 문장(balance >= :amount)이라 행 락 한 번으로 원자적이다. 별도 락이 없다. 음수는 CHECK 제약이 2차로 막는다.
 * - [withdraw] · [refund] 는 호출자(JpaTransferStore)의 트랜잭션 안에서만 부른다: 출금 행 · 상태 전이 · 아웃박스와 함께 커밋되거나 함께 롤백돼야 한다.
 * - 원장의 balance_after 는 방금 바꾼 행(이 트랜잭션이 X 락을 쥔 행)을 다시 읽은 값이라 다른 트랜잭션과 섞이지 않는다.
 * - 시각은 Timestamp 로 넘기고 KST 변환은 JDBC 설정(connectionTimeZone)이 한다 (database.md §2).
 */
class WalletLedger(
    private val jdbc: NamedParameterJdbcTemplate,
    private val tx: TransactionTemplate,
    private val clock: Clock = Clock.systemUTC(),
) {

    /**
     * 충전 (관리 · 테스트용 — 실제로는 은행 입금 통지가 채우고 멱등 처리가 필요하다). 지갑이 없으면 만든다.
     * 자기 트랜잭션을 쓴다. 돌려주는 값 = 충전 뒤 잔고.
     */
    fun deposit(userId: String, amount: Long): Long {
        require(amount > 0) { "deposit amount must be positive, was $amount" }
        return tx.execute {
            val at = Timestamp.from(clock.instant().toDbPrecision())
            jdbc.update(
                "INSERT INTO wallet (user_id, balance, version, created_at, updated_at) VALUES (:userId, :amount, 0, :at, :at) AS new " +
                    "ON DUPLICATE KEY UPDATE balance = wallet.balance + new.balance, version = wallet.version + 1, updated_at = new.updated_at",
                MapSqlParameterSource().addValue("userId", userId).addValue("amount", amount).addValue("at", at),
            )
            val balance = lockedBalance(userId)
            appendLedger(userId, LedgerType.DEPOSIT, amount, balance, null, at)
            balance
        }!!
    }

    /**
     * 차감 (출금 접수). 잔고가 [amount] 이상일 때만 빼고 원장에 WITHDRAW 를 남긴다. 지갑이 없거나 모자라면 0건 → false (아무것도 쓰지 않는다).
     * 호출자 트랜잭션 안에서만 부른다 — false 면 호출자가 예외를 던져 출금 INSERT 까지 롤백한다.
     */
    fun withdraw(userId: String, transferId: UUID, amount: Long, at: Instant): Boolean {
        requireCallerTransaction()
        val ts = Timestamp.from(at.toDbPrecision())
        val updated = jdbc.update(
            "UPDATE wallet SET balance = balance - :amount, version = version + 1, updated_at = :at " +
                "WHERE user_id = :userId AND balance >= :amount",
            MapSqlParameterSource().addValue("userId", userId).addValue("amount", amount).addValue("at", ts),
        )
        if (updated == 0) return false
        appendLedger(userId, LedgerType.WITHDRAW, amount, lockedBalance(userId), transferId, ts)
        return true
    }

    /**
     * 환불 (실패 확정). 이 출금의 WITHDRAW 원장이 있으면 그 금액을 돌려주고 REFUND 를 남긴다. 없으면(차감한 적 없는 P3 시절 출금) false.
     * 금액은 출금 행이 아니라 원장에서 읽는다 — 실제로 뺀 돈만 돌려준다.
     * 같은 출금에 REFUND 가 이미 있으면 uk_ledger_transfer_type 위반 → 예외 → 호출자 트랜잭션(상태 전이 포함)이 롤백된다 (이중 환불 2차 방어).
     */
    fun refund(transferId: UUID, at: Instant): Boolean {
        requireCallerTransaction()
        val (userId, amount) = jdbc.query(
            "SELECT user_id, amount FROM wallet_ledger WHERE transfer_id = :transferId AND type = 'WITHDRAW'",
            MapSqlParameterSource().addValue("transferId", transferId.toString()),
            RowMapper { rs, _ -> rs.getString("user_id") to rs.getLong("amount") },
        ).singleOrNull() ?: return false
        val ts = Timestamp.from(at.toDbPrecision())
        val updated = jdbc.update(
            "UPDATE wallet SET balance = balance + :amount, version = version + 1, updated_at = :at WHERE user_id = :userId",
            MapSqlParameterSource().addValue("userId", userId).addValue("amount", amount).addValue("at", ts),
        )
        check(updated == 1) { "wallet of $userId is missing although transfer $transferId was debited" }
        appendLedger(userId, LedgerType.REFUND, amount, lockedBalance(userId), transferId, ts)
        return true
    }

    /** 지금 잔고. 지갑이 없으면 0. */
    fun balanceOf(userId: String): Long =
        jdbc.query(
            "SELECT balance FROM wallet WHERE user_id = :userId",
            MapSqlParameterSource().addValue("userId", userId),
            RowMapper { rs, _ -> rs.getLong("balance") },
        ).singleOrNull() ?: 0L

    /** 결과가 정해지지 않아 잔고에서 빠진 채 묶인 금액 = 내 출금 중 RECEIVED · SENDING · UNKNOWN 의 합 (idx_transfer_user 로 사용자 범위를 좁힌다). */
    fun pendingOf(userId: String): Long =
        jdbc.queryForObject(
            "SELECT COALESCE(SUM(amount), 0) FROM transfer WHERE user_id = :userId AND status IN ('RECEIVED', 'SENDING', 'UNKNOWN')",
            MapSqlParameterSource().addValue("userId", userId),
            Long::class.java,
        )!!

    private fun lockedBalance(userId: String): Long =
        jdbc.queryForObject(
            "SELECT balance FROM wallet WHERE user_id = :userId",
            MapSqlParameterSource().addValue("userId", userId),
            Long::class.java,
        )!!

    private fun appendLedger(userId: String, type: LedgerType, amount: Long, balanceAfter: Long, transferId: UUID?, at: Timestamp) {
        jdbc.update(
            "INSERT INTO wallet_ledger (user_id, type, amount, balance_after, transfer_id, created_at) " +
                "VALUES (:userId, :type, :amount, :balanceAfter, :transferId, :at)",
            MapSqlParameterSource()
                .addValue("userId", userId)
                .addValue("type", type.name)
                .addValue("amount", amount)
                .addValue("balanceAfter", balanceAfter)
                .addValue("transferId", transferId?.toString(), Types.CHAR)   // DEPOSIT 은 NULL — 타입을 줘서 드라이버가 추측하지 않게
                .addValue("at", at),
        )
    }

    private fun requireCallerTransaction() =
        check(TransactionSynchronizationManager.isActualTransactionActive()) {
            "must run inside the caller's transaction, together with the transfer row"
        }
}
```

`TransferPersistence.kt`:
1) `TransferEntity` 생성자의 `@Id … var id: String = "",` 다음에 `var userId: String? = null,` 를 넣는다. `toDomain()` 의 `id = UUID.fromString(id),` 뒤에 `userId = userId,`, `from()` 의 `id = t.id.toString(),` 뒤에 `userId = t.userId,` 를 넣는다.
2) `TransferJpaRepository` 의 `fun findByIdempotencyKey(idempotencyKey: String): TransferEntity?` 를 다음으로 바꾼다:
```kotlin
    /** 멱등 조회는 사용자 범위 (P4-1). uk_transfer_user_idempotency (user_id, idempotency_key) 를 탄다. */
    fun findByUserIdAndIdempotencyKey(userId: String, idempotencyKey: String): TransferEntity?
```
3) `JpaTransferStore` 의 KDoc · 생성자 · `saveWithEvent` · `findByIdempotencyKey` · `transition` 을 바꾸고 `fail` 을 `republish` 아래에 더한다 (`findById`, `findSendingBefore`, `findUnknownForInquiry`, `recordInquiryAttempt`, `findRepublishable`, `republish`, `outboxRow` 는 그대로):
```kotlin
/**
 * 이체 저장소 (MySQL). saveWithEvent 는 transfer · 잔고 차감(wallet · wallet_ledger) · outbox_event 를 한 트랜잭션에 쓴다:
 * 모두 커밋되거나 모두 롤백되므로 "돈은 빠졌는데 출금이 없는" 상태나 "이체는 있는데 처리 신호가 없는" 상태가 생기지 않는다 (P4-1).
 * 실패 확정(fail)은 상태 전이와 환불을 한 트랜잭션에 쓴다. UNIQUE 위반은 제약 이름을 보고 도메인 예외([StoreConflict])로 바꿔 던진다.
 *
 * 바깥 트랜잭션 안에서 [saveWithEvent] 를 부르면 안 된다. 이 클래스가 자기 트랜잭션을 열고 닫는 것에 기대기 때문이다:
 * UNIQUE 위반이면 이 트랜잭션이 롤백된 뒤에야 호출자(TransferService)가 이긴 쪽 건을 다시 읽을 수 있다.
 * 바깥 트랜잭션에 합류(REQUIRED)하면 롤백이 호출자 쪽으로 미뤄져 그 재조회가 깨진다.
 * 전파는 지금은 REQUIRED(기본값)로 두고, 호출자가 트랜잭션 밖에서 부른다는 전제를 이 문서로 지킨다.
 */
class JpaTransferStore(
    private val transfers: TransferJpaRepository,
    private val outbox: OutboxEventJpaRepository,
    private val tx: TransactionTemplate,
    private val wallets: WalletLedger,
    private val payloadOf: (TransferEvent) -> String = ::outboxPayload,
) : TransferStore {

    /**
     * 순서: 출금 INSERT(flush) → 잔고 차감 + 원장 WITHDRAW → 아웃박스 INSERT. 하나라도 실패하면 전부 롤백된다.
     * 출금 INSERT 를 먼저 하는 이유: 같은 (사용자, 키)가 동시에 오면 진 쪽이 UNIQUE 에서 멈춰(이긴 쪽 커밋까지 대기) 지갑을 건드리지 않고
     * 기존 건을 돌려받는다. 차감을 먼저 하면 잔고가 한 건 몫뿐일 때 진 쪽이 멱등 응답(200) 대신 잔고 부족(422)을 받는다.
     * 아웃박스를 차감 뒤에 두는 이유: 마지막 INSERT 가 실패해도 차감까지 되돌아간다는 것을 테스트로 보일 수 있다.
     */
    override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer {
        // 새 출금은 반드시 지갑에서 뺀다. 사용자 없는 출금을 받으면 차감 없이 돈이 나가는 길이 생긴다
        val userId = requireNotNull(transfer.userId) { "a new transfer must have userId (it is debited from the user's wallet)" }
        return try {
            tx.execute {
                // flush 로 UNIQUE 위반을 여기서 터뜨려야 아래 catch 에서 StoreConflict 로 바꿀 수 있다 (커밋 시점까지 미루지 않는다)
                val saved = transfers.saveAndFlush(TransferEntity.from(transfer))
                if (!wallets.withdraw(userId, transfer.id, transfer.amount, transfer.createdAt)) {
                    // 0건 = 잔고 부족 또는 지갑 없음. 예외로 트랜잭션 전체(방금 INSERT 한 출금 포함)를 롤백한다
                    throw StoreConflict.InsufficientBalance(userId, transfer.amount)
                }
                outbox.saveAndFlush(outboxRow(event))
                saved.toDomain()
            }!!
        } catch (e: DataIntegrityViolationException) {
            // MySQL 메시지 예: Duplicate entry 'user-1-k1' for key 'transfer.uk_transfer_user_idempotency'
            // V3 에서 이름이 바뀌었다. 옛 이름(uk_transfer_idempotency)은 새 이름의 부분 문자열이 아니라 함께 고쳐야 했다
            val message = e.mostSpecificCause.message.orEmpty()
            when {
                "uk_transfer_user_idempotency" in message -> throw StoreConflict.DuplicateIdempotencyKey(transfer.idempotencyKey)
                "uk_transfer_reference" in message -> throw StoreConflict.DuplicateReference(transfer.reference)
                else -> throw e
            }
        }
    }

    override fun findById(id: UUID): Transfer? = transfers.findById(id.toString()).orElse(null)?.toDomain()

    override fun findByIdempotencyKey(userId: String, key: String): Transfer? =
        transfers.findByUserIdAndIdempotencyKey(userId, key)?.toDomain()

    /**
     * 조건부 UPDATE 로만 상태를 바꾼다. TransferEntity.from() 으로 만든 엔티티를 save 하면 INSERT 가 되므로 쓰지 않는다.
     * 한 문장의 UPDATE 라 두 소비자가 동시에 와도 DB 가 한쪽만 1 건으로 통과시킨다.
     * 허용하는 전이는 도메인의 상태 표([TransferStatus.canMoveTo])를 따른다. 표에 없는 전이는 DB 에 가기 전에 거른다 (호출 쪽 버그).
     * FAILED 는 거부한다 — 환불과 한 트랜잭션인 [fail] 로만 간다 (P4-1).
     */
    override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean {
        require(to != TransferStatus.FAILED) { "use fail() for FAILED so the refund is never skipped" }
        require(from.canMoveTo(to)) { "illegal transition $from -> $to" }
        return tx.execute {
            val dbAt = at.toDbPrecision()
            val updated = if (to == TransferStatus.SENDING) {
                transfers.transitionToSending(id.toString(), from, to, dbAt, bankResponseCode)
            } else {
                transfers.transition(id.toString(), from, to, dbAt, bankResponseCode)
            }
            updated == 1
        }!!
    }
```
`republish` 아래(`outboxRow` 위)에 추가:
```kotlin

    /**
     * 실패 확정과 환불을 한 트랜잭션에 쓴다 (P4-1). 조건부 전이가 1건일 때만 환불한다 → 컨슈머와 정리 작업이 겹쳐 두 번 불러도 환불은 한 번.
     * 환불 금액은 원장의 WITHDRAW 에서 읽는다 (없으면 = 차감한 적 없는 P3 시절 출금 → 상태만 바꾼다).
     * 환불이 실패하면(원장 UNIQUE 위반 등) 전이도 롤백된다 — "FAILED 인데 돈은 안 돌아온" 상태가 생기지 않는다.
     */
    override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String): Boolean {
        require(from.canMoveTo(TransferStatus.FAILED)) { "illegal transition $from -> FAILED" }
        return tx.execute {
            val dbAt = at.toDbPrecision()
            if (transfers.transition(id.toString(), from, TransferStatus.FAILED, dbAt, bankResponseCode) != 1) {
                return@execute false
            }
            wallets.refund(id, dbAt)
            true
        }!!
    }
```

`AccountVerificationPersistence.kt`:
1) `AccountVerificationEntity` 생성자의 `@Id … var id: String = "",` 다음에 `var userId: String? = null,` 를 넣는다. `toDomain()` 의 `id = UUID.fromString(id),` 뒤에 `userId = userId,`, `from()` 의 `id = v.id.toString(),` 뒤에 `userId = v.userId,` 를 넣는다. (`applyChanges` 는 그대로 — 사용자는 만들 때 정해지고 바뀌지 않는다.)
2) `AccountVerificationJpaRepository` 의 `existsByBankCodeAndAccountNoAndStatus` 를 다음으로 바꾼다:
```kotlin
    /** 사용자 범위 (P4-1). idx_verification_user_account (user_id, bank_code, account_no, status) 를 탄다. */
    fun existsByUserIdAndBankCodeAndAccountNoAndStatus(
        userId: String, bankCode: String, accountNo: String, status: VerificationStatus,
    ): Boolean
```
3) `JpaVerificationRepository.existsVerified` 를 바꾼다:
```kotlin
    override fun existsVerified(userId: String, bankCode: String, accountNo: String): Boolean =
        jpa.existsByUserIdAndBankCodeAndAccountNoAndStatus(userId, bankCode, accountNo, VerificationStatus.VERIFIED)
```

`AppConfiguration.kt`:
1) import 추가: `io.github.hyeokjinon.banktransfer.api.persistence.WalletLedger`, `org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate`.
2) `transferStore` 빈을 바꾸고 그 위에 `walletLedger` 빈을 둔다:
```kotlin
    /** 잔고 SQL 한 곳 (P4-1). 접수 · 실패 확정 트랜잭션 안에서 JpaTransferStore 가 부르고, 충전 · 조회는 WalletController 가 부른다. */
    @Bean
    fun walletLedger(jdbc: NamedParameterJdbcTemplate, tx: TransactionTemplate) = WalletLedger(jdbc, tx, Clock.systemUTC())

    @Bean
    fun transferStore(
        transfers: TransferJpaRepository,
        outbox: OutboxEventJpaRepository,
        tx: TransactionTemplate,
        wallets: WalletLedger,
    ): TransferStore = JpaTransferStore(transfers, outbox, tx, wallets)
```
3) `transferService` 의 `verifiedAccounts` 를 바꾼다:
```kotlin
        verifiedAccounts = VerifiedAccounts { userId, bankCode, accountNo -> verifications.existsVerified(userId, bankCode, accountNo) },
```

`TransferRequestedListener.kt` 의 CONFLICT 경고 두 줄을 바꾼다:
```kotlin
            // 재시도로 풀 수 없다: 은행이 확정한 결과가 이미 기록된 최종 상태와 다르다. 오프셋은 넘기고 사람이 확인하게 경고만 남긴다.
            // 잔고도 자동으로 고치지 않는다 (P4-1): 예컨대 FAILED 로 확정 · 환불한 뒤 은행이 완료라고 답했다면 환불된 돈이 실제로 나간 상태다 → 수동 확인 · P5 대사
            log.warn("transfer {} conflict: bank result differs from the recorded final status — wallet balance left as is, needs manual check", transferId)
```

- [ ] **Step 4: 구현 — `X-User-Id` 한 곳 · 컨트롤러 · 오류 코드**

`RequestUser.kt` (신규, `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/`):
```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import org.springframework.context.annotation.Configuration
import org.springframework.core.MethodParameter
import org.springframework.web.bind.support.WebDataBinderFactory
import org.springframework.web.context.request.NativeWebRequest
import org.springframework.web.method.support.HandlerMethodArgumentResolver
import org.springframework.web.method.support.ModelAndViewContainer
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer

const val USER_ID_HEADER = "X-User-Id"

/**
 * 요청한 사용자 (P4-1). 지금은 X-User-Id 헤더 값이고, 인증을 붙이면 토큰의 주체로 바뀐다 — 값의 출처는 [RequestUserResolver] 한 곳이다.
 * 컨트롤러는 이 타입을 **첫 인자**로 받는다: 인자는 선언 순서로 해석되므로 헤더 오류가 본문 검증보다 먼저 난다.
 */
data class RequestUser(val id: String)

/** X-User-Id 가 없거나 형식(영숫자 · 하이픈 1~36자)이 아니다 → 400 USER_ID_REQUIRED. */
class UserIdRequiredException : RuntimeException("$USER_ID_HEADER header is required (1-36 letters, digits or hyphens)")

/**
 * 컨트롤러 인자 [RequestUser] 를 채운다. 헤더를 읽고 검사하는 곳은 여기뿐이다.
 * 값은 믿는다(인증은 범위 밖, ADR-0008). DB 는 ascii_bin 이라 대소문자가 다르면 다른 사용자다.
 */
class RequestUserResolver : HandlerMethodArgumentResolver {

    override fun supportsParameter(parameter: MethodParameter): Boolean = parameter.parameterType == RequestUser::class.java

    override fun resolveArgument(
        parameter: MethodParameter,
        mavContainer: ModelAndViewContainer?,
        webRequest: NativeWebRequest,
        binderFactory: WebDataBinderFactory?,
    ): RequestUser {
        val value = webRequest.getHeader(USER_ID_HEADER)
        if (value == null || !USER_ID.matches(value)) throw UserIdRequiredException()
        return RequestUser(value)
    }

    private companion object {
        val USER_ID = Regex("[A-Za-z0-9-]{1,36}")
    }
}

@Configuration(proxyBeanMethods = false)
class WebConfiguration : WebMvcConfigurer {
    override fun addArgumentResolvers(resolvers: MutableList<HandlerMethodArgumentResolver>) {
        resolvers += RequestUserResolver()
    }
}
```

`TransferController.kt`: KDoc 의 `request` 설명에 한 줄 추가 ` * 출금은 요청자(X-User-Id)의 지갑에서 빠진다. 잔고가 모자라면 422 INSUFFICIENT_BALANCE (P4-1).` 그리고 두 메서드를 바꾼다:
```kotlin
    @PostMapping
    fun request(
        user: RequestUser,
        @RequestHeader("Idempotency-Key", required = false) idempotencyKey: String?,
        @Valid @RequestBody body: TransferRequestBody,
    ): ResponseEntity<TransferResponse> {
        val result = service.request(
            RequestTransfer(user.id, idempotencyKey.orEmpty(), body.bankCode, body.accountNo, body.amount, body.memo),
        )
        val response = TransferResponse.from(result.transfer)
        return if (result.created) {
            ResponseEntity.accepted().location(URI.create("/api/v1/transfers/${response.id}")).body(response)
        } else {
            ResponseEntity.ok(response)
        }
    }

    /** 요청자의 출금이 아니면 404 TRANSFER_NOT_FOUND — 존재 여부도 숨긴다 (P4-1). */
    @GetMapping("/{id}")
    fun get(user: RequestUser, @PathVariable id: UUID): TransferResponse = TransferResponse.from(service.get(user.id, id))
```

`AccountVerificationController.kt`: 두 메서드를 바꾼다:
```kotlin
    /**
     * CompletableFuture 를 돌려주면 Spring MVC 가 서블릿 비동기 모드로 처리한다.
     * 은행 응답을 기다리는 동안 Tomcat 요청 스레드는 풀로 돌아간다. 인증 건에는 요청자(X-User-Id)를 남긴다 (P4-1).
     */
    @PostMapping
    fun request(user: RequestUser, @Valid @RequestBody body: VerificationRequest): CompletableFuture<ResponseEntity<VerificationResponse>> =
        service.request(RequestVerification(user.id, body.bankCode, body.accountNo, body.holderName))
            .thenApply { ResponseEntity.status(HttpStatus.CREATED).body(VerificationResponse.from(it)) }

    /** 남의 인증 건이면 404 VERIFICATION_NOT_FOUND (실패 횟수도 늘지 않는다, P4-1). */
    @PostMapping("/{id}/confirm")
    fun confirm(user: RequestUser, @PathVariable id: UUID, @Valid @RequestBody body: ConfirmRequest): VerificationResponse =
        VerificationResponse.from(service.confirm(user.id, id, body.code))
```

`ApiExceptionHandler.kt`:
1) `@ExceptionHandler(` 목록 끝(`TransferException::class,` 다음)에 `UserIdRequiredException::class,` 추가.
2) `when (cause)` 의 `is TransferException.IdempotencyKeyRequired -> …` 위에 `is UserIdRequiredException -> HttpStatus.BAD_REQUEST to "USER_ID_REQUIRED"`, `is TransferException.AccountNotVerified -> …` 아래에 `is TransferException.InsufficientBalance -> HttpStatus.UNPROCESSABLE_CONTENT to "INSUFFICIENT_BALANCE"` 추가.

- [ ] **Step 5: 통과 확인**

Run:
```bash
./gradlew :modules:transfer:test :modules:account:test
./gradlew :apps:api:test --tests '*DatabaseSetupTest*' --tests '*WalletLedgerTest*' --tests '*JpaTransferStoreTest*' \
  --tests '*JpaVerificationRepositoryTest*' --tests '*UserIdHeaderApiTest*' --tests '*OutboxRelayTest*' --tests '*RetryTopicFlowTest*' \
  --tests '*ApiExceptionHandlerTest*' --tests '*TransferRequestedListenerTest*' --tests '*KafkaSetupTest*' --tests '*AdapterTest*'
```
Expected: 모두 PASS. (`TransferApiTest` · `AccountVerificationApiTest` · `TransferProcessingFlowTest` · `ReconcileFlowTest` · `RepublishFlowTest` 는 헤더가 없어 아직 실패 — Task 3.)

- [ ] **Step 6: 커밋**

```bash
git add apps/api
git commit -m "feat(api): V3 지갑 · 원장, 접수 트랜잭션에서 조건부 차감 · 실패 확정 시 같은 트랜잭션에서 환불, X-User-Id 한 곳

잔고 SQL 을 WalletLedger 한 곳에 모은다 (P4-2 에서 락 전략을 바꿔 끼울 이음새). 차감은 조건부 UPDATE
한 문장이라 별도 락 없이 원자적이고, 출금 INSERT 를 먼저 해 같은 키 경합의 진 쪽이 지갑을 건드리지 않는다.
fail() 은 조건부 전이 1건일 때만 원장의 WITHDRAW 금액을 돌려줘 이중 환불을 막는다.
멱등 UNIQUE 이름이 바뀌어 제약 이름 매핑도 함께 고친다. HTTP 테스트는 다음 커밋에서 헤더를 단다.

Co-Authored-By: <작성 모델>"
```

---

### Task 3: 지갑 API · 기존 API · 흐름 테스트와 손 확인 파일에 `X-User-Id` · 충전

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/WalletController.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/WalletApiTest.kt` (신규 5개)
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/TransferApiTest.kt` (도우미 · 4개 추가)
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/AccountVerificationApiTest.kt` (헤더 · 1개 추가)
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/TransferProcessingFlowTest.kt`, `.../reconcile/ReconcileFlowTest.kt`, `.../reconcile/RepublishFlowTest.kt` (사용자 · 충전 · 인증 계좌)
- Modify: `http/account-verification.http`, `http/transfer.http`
- Modify: `postman/collections/bank-transfer/.resources/definition.yaml`, 요청 파일 19개 (아래 목록)
- Create: `postman/collections/bank-transfer/2-transfer-intake/00-deposit.request.yaml`

**Interfaces:**
- Consumes: Task 2 의 `WalletLedger.deposit(userId, amount): Long` · `balanceOf(userId)` · `pendingOf(userId)`, `RequestUser`, `USER_ID_HEADER`, `ApiIntegrationTest` 도우미(`wallets`, `newUserId()`, `fundedUser(amount)`, `verifiedBy(userId, accountNo)`, `unregisteredAccount()`, `ledgerSum(userId)`)
- Produces:
  - `const val MAX_DEPOSIT_AMOUNT: Long = 100_000_000`
  - `data class DepositRequest(amount: Long)`, `data class WalletResponse(userId: String, balance: Long, pending: Long)`
  - `POST /api/v1/wallet/deposits` → 200 `WalletResponse`, `GET /api/v1/wallet` → 200 `WalletResponse`
  - 흐름 테스트 3개 클래스의 `private val user by lazy { fundedUser(FUNDED) }` · `FUNDED = 1_000_000L` (Task 4 가 잔고 단언에 쓴다)

- [ ] **Step 1: 실패하는 테스트**

`WalletApiTest.kt` (신규, `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/`):
```kotlin
package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

/** 지갑 API (P4-1). 충전은 관리 · 테스트용이다 (실제로는 은행 입금 통지). */
class WalletApiTest : ApiIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private data class Response(val status: Int, val body: Map<String, Any?>) {
        fun long(field: String) = (body[field] as Number).toLong()
    }

    private fun deposit(userId: String?, amount: Any): Response =
        client.post().uri("/api/v1/wallet/deposits")
            .contentType(MediaType.APPLICATION_JSON)
            .apply { if (userId != null) header(USER_ID_HEADER, userId) }
            .body(mapOf("amount" to amount))
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap()) }

    private fun wallet(userId: String): Response =
        client.get().uri("/api/v1/wallet").header(USER_ID_HEADER, userId)
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap()) }

    private fun transfer(userId: String, accountNo: String, amount: Long): Response =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to amount, "memo" to "지갑"))
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap()) }

    @Test
    fun `지갑이 없으면 잔고 0 · 보류 0`() {
        val user = newUserId()

        val w = wallet(user)

        assertEquals(200, w.status)
        assertEquals(user, w.body["userId"])
        assertEquals(0L, w.long("balance"))
        assertEquals(0L, w.long("pending"))
    }

    @Test
    fun `충전하면 응답에 충전 뒤 잔고, 조회 잔고와 원장 합이 같다`() {
        val user = newUserId()

        assertEquals(30_000L, deposit(user, 30_000).long("balance"))
        val second = deposit(user, 20_000)

        assertEquals(200, second.status)
        assertEquals(50_000L, second.long("balance"))
        assertEquals(50_000L, wallet(user).long("balance"))
        assertEquals(50_000L, ledgerSum(user))
    }

    @Test
    fun `출금을 접수하면 잔고에서 빠지고 결과가 날 때까지 보류로 보인다`() {
        val user = newUserId()
        deposit(user, 100_000)
        val account = verifiedBy(user, unregisteredAccount())   // 남는 RECEIVED 를 나중에 누가 처리해도 1001 거절 → 환불뿐

        assertEquals(202, transfer(user, account, 30_000).status)

        val w = wallet(user)
        assertEquals(70_000L, w.long("balance"))
        assertEquals(30_000L, w.long("pending"))
    }

    @Test
    fun `충전 금액이 1 ~ 1억 원 밖이면 400 INVALID_REQUEST 이고 잔고는 그대로`() {
        val user = newUserId()
        for (amount in listOf(0L, -1L, 100_000_001L)) {
            val r = deposit(user, amount)
            assertEquals(400, r.status, "amount=$amount")
            assertEquals("INVALID_REQUEST", r.body["code"], "amount=$amount")
        }

        assertEquals(200, deposit(user, 100_000_000).status)   // 한도 정확히는 받는다
        assertEquals(100_000_000L, wallet(user).long("balance"))
    }

    @Test
    fun `X-User-Id 가 없으면 충전도 400 USER_ID_REQUIRED`() {
        val r = deposit(null, 1_000)

        assertEquals(400, r.status)
        assertEquals("USER_ID_REQUIRED", r.body["code"])
    }
}
```

`TransferApiTest.kt`:
1) import 블록 전체를 다음으로 바꾼다:
```kotlin
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
```
2) 클래스 첫 부분(`class TransferApiTest : ApiIntegrationTest() {` 부터 `private fun verifiedAccount(): String { … }` 끝까지)을 다음으로 바꾼다 (`private fun body(...)` 와 기존 테스트 12개는 그대로):
```kotlin
class TransferApiTest : ApiIntegrationTest() {

    @Autowired private lateinit var jdbc: JdbcTemplate

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private data class Response(val status: Int, val body: Map<String, Any?>, val location: String?)

    /** 이 테스트의 사용자 (테스트마다 새로, 1천만 원 충전 — 출금은 이 지갑에서 빠진다, P4-1). */
    private val user by lazy { fundedUser() }

    private fun post(key: String?, body: Map<String, Any>, userId: String = user): Response =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .apply { if (key != null) header("Idempotency-Key", key) }
            .body(body)
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), r.headers.location?.toString()) }

    /** 직렬화를 거치지 않고 JSON 문자열을 그대로 보낸다 (소수 · 문자열 금액 같은 잘못된 형식을 재현). */
    private fun postRaw(key: String, json: String): Response =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", key)
            .body(json)
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), r.headers.location?.toString()) }

    private fun get(id: Any?, userId: String = user): Response =
        client.get().uri("/api/v1/transfers/$id")
            .header(USER_ID_HEADER, userId)
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), null) }

    /**
     * [userId] 가 1원인증을 마친 계좌. 모의 은행에 없는 번호라, 여기서 남긴 RECEIVED 를 다른 테스트의 발행 · 정리 작업이 처리해도
     * 1001 거절 → FAILED → 환불로 끝난다 (공용 계좌에 입금이 생기지 않는다).
     */
    private fun verifiedAccount(userId: String = user): String = verifiedBy(userId, unregisteredAccount())
```
3) 테스트 4개를 클래스 끝에 추가:
```kotlin
    @Test
    fun `잔고보다 큰 출금은 422 INSUFFICIENT_BALANCE 이고 출금도 차감도 남지 않는다`() {
        val poor = fundedUser(49_999)
        val key = UUID.randomUUID().toString()

        val response = post(key, body(verifiedAccount(poor)), userId = poor)

        assertEquals(422, response.status)
        assertEquals("INSUFFICIENT_BALANCE", response.body["code"])
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM transfer WHERE user_id = ? AND idempotency_key = ?", Int::class.java, poor, key))
        assertEquals(49_999L, wallets.balanceOf(poor))
    }

    @Test
    fun `남이 인증한 계좌로는 출금할 수 없다 - 422 ACCOUNT_NOT_VERIFIED`() {
        val othersAccount = verifiedAccount(newUserId())

        val response = post(UUID.randomUUID().toString(), body(othersAccount))

        assertEquals(422, response.status)
        assertEquals("ACCOUNT_NOT_VERIFIED", response.body["code"])
    }

    @Test
    fun `남의 출금을 조회하면 404 TRANSFER_NOT_FOUND - 존재 여부도 숨긴다`() {
        val created = post(UUID.randomUUID().toString(), body(verifiedAccount()))

        val response = get(created.body["id"], userId = newUserId())

        assertEquals(404, response.status)
        assertEquals("TRANSFER_NOT_FOUND", response.body["code"])
    }

    @Test
    fun `같은 키 · 같은 내용이라도 다른 사용자면 각자 새 출금이다 (202 두 번)`() {
        val key = UUID.randomUUID().toString()
        val other = fundedUser()
        val account = unregisteredAccount()
        verifiedBy(user, account)
        verifiedBy(other, account)

        val mine = post(key, body(account))
        val theirs = post(key, body(account), userId = other)

        assertEquals(202, mine.status)
        assertEquals(202, theirs.status)          // 전역 UNIQUE 였다면 남의 출금을 200 으로 돌려받았다
        assertNotEquals(mine.body["id"], theirs.body["id"])
    }
```

`AccountVerificationApiTest.kt`: 파일 전체를 다음으로 바꾼다 (기존 4개는 헤더만 더해지고 1개 추가):
```kotlin
package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * 실제 HTTP -> Spring MVC -> 도메인 -> Netty 게이트웨이 -> 모의 은행(TCP) 까지 한 번에 검증한다.
 * 인증 건은 요청자(X-User-Id)의 것이다 — 확인도 본인만 (P4-1).
 */
class AccountVerificationApiTest : ApiIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private val user = newUserId()

    private fun post(path: String, body: Map<String, String>, userId: String = user): Pair<Int, Map<String, Any?>> =
        client.post().uri(path)
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .body(body)
            .exchange { _, response -> response.statusCode.value() to (response.bodyTo(mapType) ?: emptyMap()) }

    @Test
    fun `1원 인증 요청 후 통장 적요의 코드를 입력하면 VERIFIED`() {
        val (status, created) = post(
            "/api/v1/account-verifications",
            mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"),
        )
        assertEquals(201, status)
        assertEquals("SENT", created["status"])

        // 사용자가 자기 통장에서 1원 입금 적요("BT1234")를 확인하는 단계
        val code = bank.depositsOf("1002123456789").last().memo.removePrefix("BT")

        val (confirmStatus, confirmed) = post("/api/v1/account-verifications/${created["id"]}/confirm", mapOf("code" to code))
        assertEquals(200, confirmStatus)
        assertEquals("VERIFIED", confirmed["status"])
    }

    @Test
    fun `남의 인증 건을 확인하면 404 VERIFICATION_NOT_FOUND, 본인은 그 뒤에도 확인할 수 있다`() {
        val (_, created) = post(
            "/api/v1/account-verifications",
            mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"),
        )
        val code = bank.depositsOf("1002123456789").last().memo.removePrefix("BT")

        val (otherStatus, other) = post("/api/v1/account-verifications/${created["id"]}/confirm", mapOf("code" to code), userId = newUserId())
        assertEquals(404, otherStatus)
        assertEquals("VERIFICATION_NOT_FOUND", other["code"])

        val (mineStatus, mine) = post("/api/v1/account-verifications/${created["id"]}/confirm", mapOf("code" to code))
        assertEquals(200, mineStatus)                // 남의 시도는 실패 횟수도 쓰지 않았다
        assertEquals("VERIFIED", mine["status"])
    }

    @Test
    fun `예금주명이 다르면 422 HOLDER_NAME_MISMATCH`() {
        val (status, body) = post(
            "/api/v1/account-verifications",
            mapOf("bankCode" to "090", "accountNo" to "3333012345678", "holderName" to "홍길동"),
        )
        assertEquals(422, status)
        assertEquals("HOLDER_NAME_MISMATCH", body["code"])
    }

    @Test
    fun `은행이 응답하지 않으면 504 BANK_TIMEOUT`() {
        val (status, body) = post(
            "/api/v1/account-verifications",
            mapOf("bankCode" to "090", "accountNo" to "9990000000001", "holderName" to "홍길동"),
        )
        assertEquals(504, status)
        assertEquals("BANK_TIMEOUT", body["code"])
    }

    @Test
    fun `형식이 틀리면 400 INVALID_REQUEST`() {
        val (status, body) = post(
            "/api/v1/account-verifications",
            mapOf("bankCode" to "9", "accountNo" to "abc", "holderName" to ""),
        )
        assertEquals(400, status)
        assertEquals("INVALID_REQUEST", body["code"])
    }
}
```

흐름 테스트 3개 (`TransferProcessingFlowTest`, `ReconcileFlowTest`, `RepublishFlowTest`) — 같은 방식으로 고친다:
1) import 에서 `io.github.hyeokjinon.banktransfer.account.AccountVerification`, `…account.VerificationRepository`, `…account.VerificationStatus` 를 지우고 `import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER` 를 더한다. `TransferProcessingFlowTest` 와 `RepublishFlowTest` 는 `import java.time.Instant` 도 지운다 (`verify()` 에서만 썼다). `ReconcileFlowTest` 는 `Instant` 를 그대로 둔다 ("30초 넘게 SENDING" 테스트가 쓴다).
2) `@Autowired private lateinit var verifications: VerificationRepository` 줄과 `private fun verify(accountNo: String) { … }` 함수를 지운다.
3) `private val mapType = …` 아래에 넣는다:
```kotlin

    /** 이 테스트의 사용자 (테스트마다 새로 [FUNDED] 원 충전 — 출금은 이 지갑에서 빠진다, P4-1). */
    private val user by lazy { fundedUser(FUNDED) }
```
4) `request(...)` 안의 `.contentType(MediaType.APPLICATION_JSON)` 다음 줄에 `.header(USER_ID_HEADER, user)` 를 넣는다.
5) `verify(account)` 를 모두 `verifiedBy(user, account)` 로 바꾼다 (`TransferProcessingFlowTest` 4곳, `ReconcileFlowTest` 3곳, `RepublishFlowTest` 2곳).
6) 클래스 마지막 `}` 앞에 넣는다 (`RepublishFlowTest` 처럼 companion 이 없으면 새로):
```kotlin

    private companion object {
        const val FUNDED = 1_000_000L
    }
```
7) `ReconcileFlowTest` 의 `30초 넘게 SENDING 에 멈춘 건 - …` 테스트의 `Transfer(` 에 `userId = user,` 를 `id = UUID.randomUUID(),` 다음에 넣는다 (저장소가 사용자 없는 출금을 거부한다).

`http/account-verification.http`:
1) 2행 아래에 `### 모든 요청에 X-User-Id 를 붙인다 (P4-1 — 원래는 인증 토큰에서 꺼내는 값). 인증한 계좌는 이 사용자의 출금에만 쓸 수 있고, 확인도 같은 사용자만 된다.` 를 넣는다.
2) 1) ~ 5) 의 각 `Content-Type: application/json` 다음 줄에 `X-User-Id: user-1` 을 넣는다 (5곳).

`http/transfer.http`:
1) 3행 아래에 넣는다:
```http
### 모든 요청에 X-User-Id: user-1 (P4-1). 출금은 이 사용자의 원화 잔고에서 빠진다 → 먼저 0) 충전. 인증도 같은 사용자(user-1)로 해 둔다.
```
2) `### 1) 접수 → 202 RECEIVED, 응답 id 저장` 바로 위에 넣는다:
```http
### 0) 충전 100만 원 → 200 {userId, balance, pending} (P4-1, 관리 · 테스트용). 7) · 8) 절의 출금 몫까지
POST http://localhost:8080/api/v1/wallet/deposits
Content-Type: application/json
X-User-Id: user-1

{
  "amount": 1000000
}

```
3) POST 요청 1) · 3) · 4) · 5) · 6) · 7-1) · 7-2) · 7-3) · 8-1) 의 `Content-Type: application/json` 다음 줄에 `X-User-Id: user-1` 을 넣는다.
4) GET 요청 2) · 2-1) · 7-4) · 7-5) · 8-2) · 8-3) 의 `GET http://…` 줄 바로 아래에 `X-User-Id: user-1` 을 넣는다.

Postman:
1) 루트 `postman/collections/bank-transfer/.resources/definition.yaml`: `variables` 맨 위(`baseUrl` 다음)에 `userId: user-1` 을 넣고, `description` 의 첫 줄 아래에 줄 추가: `0) (P4-1) 모든 요청에 X-User-Id = 컬렉션 변수 userId(기본 user-1). 출금 전에 "2-0 충전" 을 한 번 Send (폴더 3 · 4 출금 몫까지 100만 원). 인증도 같은 사용자로 해야 그 계좌로 출금된다.`
2) `headers:` 가 있는 요청 파일 12개 — `1-account-verification/confirm-code`, `1-account-verification/request-verification`, `2-transfer-intake/01-request`, `03-replay`, `04-reused-key`, `05-not-verified`, `06-no-key`, `07-fractional-amount`, `3-inquiry-reconcile/01-verify`, `02-confirm`, `03-request`, `4-retry-republish/01-request` (모두 `.request.yaml`) — 의 `- key: Content-Type` / `value: application/json` 두 줄 다음에 넣는다:
```yaml
  - key: X-User-Id
    value: '{{userId}}'
```
3) `headers:` 가 없는 GET 요청 파일 7개 — `2-transfer-intake/02-get`, `08-not-found`, `09-processed`, `3-inquiry-reconcile/04-unknown`, `05-completed`, `4-retry-republish/02-received`, `03-completed` — 의 `order: …` 줄 다음에 넣는다:
```yaml
headers:
  - key: X-User-Id
    value: '{{userId}}'
```
4) `2-transfer-intake/00-deposit.request.yaml` (신규):
```yaml
$kind: http-request
name: 2-0 충전 100만 원 → 200 (출금 전에 한 번, 폴더 3 · 4 출금 몫까지)
method: POST
url: '{{baseUrl}}/api/v1/wallet/deposits'
order: 500
headers:
  - key: Content-Type
    value: application/json
  - key: X-User-Id
    value: '{{userId}}'
body:
  type: json
  content: |-
    {
      "amount": 1000000
    }
scripts:
  - type: afterResponse
    language: text/javascript
    code: |-
      // 관리 · 테스트용 충전 (실제로는 은행 입금 통지가 채운다). 출금은 잔고에서 빠지므로 먼저 채운다 (P4-1).
      pm.test("200, 잔고 100만 원 이상", () => {
        pm.response.to.have.status(200);
        pm.expect(pm.response.json().userId).to.eql(pm.collectionVariables.get("userId"));
        pm.expect(pm.response.json().balance).to.be.at.least(1000000);
      });
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*WalletApiTest*' --tests '*TransferApiTest*'` → FAIL (`WalletApiTest`: 404 — `/api/v1/wallet` 가 없다. `TransferApiTest` 의 기존 테스트는 이제 통과, `WalletApiTest` 만 빨간색)

- [ ] **Step 3: 구현**

`WalletController.kt` (신규, `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/`):
```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.persistence.WalletLedger
import jakarta.validation.Valid
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/** 1회 충전 한도 (원). 관리 · 테스트용 API 라 넉넉히 1억. */
const val MAX_DEPOSIT_AMOUNT: Long = 100_000_000

data class DepositRequest(
    @field:Min(1, message = "충전 금액은 1원 이상")
    @field:Max(MAX_DEPOSIT_AMOUNT, message = "충전 금액은 1억 원 이하")
    val amount: Long,
)

/** 잔고와, 결과가 정해지지 않아 잔고에서 빠진 채 묶인 금액 (RECEIVED · SENDING · UNKNOWN 출금의 합). */
data class WalletResponse(val userId: String, val balance: Long, val pending: Long)

/**
 * 사용자 원화 지갑 (P4-1, ADR-0008).
 * 충전은 **관리 · 테스트용**이다 — 실제로는 은행 입금 통지가 채우고 멱등 처리가 필요하다. 그래서 인증 · 멱등키가 없다 (범위 밖).
 */
@RestController
@RequestMapping("/api/v1/wallet")
class WalletController(private val wallets: WalletLedger) {

    @PostMapping("/deposits")
    fun deposit(user: RequestUser, @Valid @RequestBody body: DepositRequest): WalletResponse {
        val balance = wallets.deposit(user.id, body.amount)
        return WalletResponse(user.id, balance, wallets.pendingOf(user.id))
    }

    /** 지갑이 없으면 잔고 0. */
    @GetMapping
    fun get(user: RequestUser): WalletResponse =
        WalletResponse(user.id, wallets.balanceOf(user.id), wallets.pendingOf(user.id))
}
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*WalletApiTest*' --tests '*TransferApiTest*' --tests '*AccountVerificationApiTest*' --tests '*FlowTest*'` → PASS. 이어서 `./gradlew build` 전체 PASS.

- [ ] **Step 5: 커밋**

```bash
git add apps/api http postman
git commit -m "feat(api): 지갑 API(충전 · 잔고 · 보류), 기존 API · 흐름 테스트와 http · Postman 에 X-User-Id 와 충전 단계

GET /wallet 의 pending 은 결과를 모르는 출금이 잔고에서 빠진 채 묶여 있음을 보여 준다.
충전은 관리 · 테스트용이라 인증 · 멱등키가 없다 (실제는 은행 입금 통지). 남의 출금 조회 · 남의 인증 확인은
존재 여부도 숨기도록 404 로 테스트한다.

Co-Authored-By: <작성 모델>"
```

---

### Task 4: 동시성 · 흐름 증명 — 100건 동시 출금, 같은 키 20건, 실패 4경로 환불

**Files:**
- Create (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/Concurrently.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/WalletConcurrencyTest.kt` (신규 2개)
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/TransferProcessingFlowTest.kt` (단언 3곳)
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/reconcile/ReconcileFlowTest.kt` (단언 3곳)
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/reconcile/RepublishFlowTest.kt` (단언 2곳)

**Interfaces:**
- Consumes: Task 3 의 흐름 테스트 `user` · `FUNDED`, Task 2 의 `wallets` · `ledgerTypes(transferId)` · `ledgerSum(userId)` · `fundedUser` · `verifiedBy` · `unregisteredAccount`
- Produces: `fun <T> concurrently(count: Int, timeout: Duration = Duration.ofSeconds(60), task: (index: Int) -> T): List<T>` (패키지 `io.github.hyeokjinon.banktransfer.api.support`) — P4-2 의 세 전략도 같은 도우미로 정확성부터 본다

- [ ] **Step 1: 실패하는 테스트**

`WalletConcurrencyTest.kt` (신규, `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/`):
```kotlin
package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.concurrently
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * P4 의 정합성 기준선: 같은 사용자의 출금이 동시에 몰려도 잔고는 음수가 되지 않고, 잔고 = 원장 합이다 (P4-1).
 * 차감은 조건부 UPDATE 한 문장 + CHECK. P4-2 에서 락 전략을 바꿔도 이 테스트가 먼저 통과해야 한다.
 * 계좌는 모의 은행에 없는 번호다: 남긴 RECEIVED 를 나중에 다른 테스트가 처리해도 1001 거절 → 이 사용자에게 환불될 뿐이다.
 */
class WalletConcurrencyTest : ApiIntegrationTest() {

    @Autowired private lateinit var jdbc: JdbcTemplate

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun transfer(userId: String, key: String, accountNo: String, amount: Long): Pair<Int, Map<String, Any?>> =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .header("Idempotency-Key", key)
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to amount, "memo" to "동시"))
            .exchange { _, r -> r.statusCode.value() to (r.bodyTo(mapType) ?: emptyMap()) }

    private fun count(sql: String, userId: String): Int = jdbc.queryForObject(sql, Int::class.java, userId)!!

    @Test
    fun `잔고 10만 원에 1만 원 출금 100건이 동시에 와도 정확히 10건 접수, 90건 INSUFFICIENT_BALANCE, 잔고 0, 원장 합 = 잔고`() {
        val user = fundedUser(100_000)
        val account = verifiedBy(user, unregisteredAccount())

        val results = concurrently(100) { transfer(user, UUID.randomUUID().toString(), account, 10_000) }

        val byStatus = results.groupingBy { it.first }.eachCount()
        assertEquals(10, results.count { it.first == 202 }, byStatus.toString())
        assertEquals(90, results.count { it.first == 422 && it.second["code"] == "INSUFFICIENT_BALANCE" }, byStatus.toString())
        assertEquals(0L, wallets.balanceOf(user))
        assertEquals(0L, ledgerSum(user))                                   // 잔고 = 원장 합 (충전 10만 - 출금 10만)
        assertEquals(10, count("SELECT COUNT(*) FROM transfer WHERE user_id = ?", user))
        assertEquals(10, count("SELECT COUNT(*) FROM wallet_ledger WHERE user_id = ? AND type = 'WITHDRAW'", user))
        assertEquals(100_000L, wallets.pendingOf(user))                     // 접수된 10건은 결과 전이라 보류로 묶여 있다
    }

    @Test
    fun `같은 사용자 · 같은 키로 20건이 동시에 와도 출금 1건 · 차감 1번`() {
        val user = fundedUser(100_000)
        val account = verifiedBy(user, unregisteredAccount())
        val key = UUID.randomUUID().toString()

        val results = concurrently(20) { transfer(user, key, account, 10_000) }

        assertEquals(1, results.count { it.first == 202 }, results.map { it.first }.toString())
        assertEquals(19, results.count { it.first == 200 })                 // 진 쪽은 UNIQUE 에서 멈춰 지갑을 건드리지 않고 기존 건을 받는다
        assertEquals(1, results.map { it.second["id"] }.toSet().size)
        assertEquals(90_000L, wallets.balanceOf(user))
        assertEquals(1, count("SELECT COUNT(*) FROM wallet_ledger WHERE user_id = ? AND type = 'WITHDRAW'", user))
        assertEquals(90_000L, ledgerSum(user))
    }
}
```

`TransferProcessingFlowTest.kt` — 테스트 끝에 단언을 더한다:
- `인증 계좌로 접수하면 은행 처리 후 COMPLETED, 은행 입금은 1건` 끝:
```kotlin
        assertEquals(FUNDED - 30_000, wallets.balanceOf(user))   // COMPLETED = 확정, 잔고는 빠진 채
        assertEquals(0L, wallets.pendingOf(user))
        assertEquals(listOf("WITHDRAW"), ledgerTypes(id))
```
- `은행에 없는 계좌면 FAILED 와 응답코드 1001` 끝:
```kotlin
        assertEquals(FUNDED, wallets.balanceOf(user))                 // 은행 거절 → fail() 이 같은 트랜잭션에서 환불
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(id))
        assertEquals(wallets.balanceOf(user), ledgerSum(user))
```
- `은행이 응답하지 않으면 UNKNOWN 이고 다시 보내지 않는다` 끝:
```kotlin
        assertEquals(FUNDED - 30_000, wallets.balanceOf(user))   // 결과를 모르는 돈은 다른 출금에 쓰지 못하게 묶어 둔다 (보류)
        assertEquals(30_000L, wallets.pendingOf(user))
```

`ReconcileFlowTest.kt` — 테스트 끝에 단언을 더한다:
- `처리하고 응답만 안 한 은행 - UNKNOWN 뒤 결과조회로 COMPLETED, 은행 입금은 1건` 끝:
```kotlin
        assertEquals(FUNDED - 30_000, wallets.balanceOf(user))   // 결과조회로 확정돼도 잔고는 빠진 채, 보류만 풀린다
        assertEquals(0L, wallets.pendingOf(user))
```
- `처리도 응답도 안 한 은행 - UNKNOWN 뒤 결과조회로 FAILED 2001` 끝:
```kotlin
        assertEquals(FUNDED, wallets.balanceOf(user))                 // 정리 작업 ② 의 실패 확정도 환불한다
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(id))
```
- `30초 넘게 SENDING 에 멈춘 건 - UNKNOWN 으로 바꾸고 결과조회로 확정` 끝:
```kotlin
        assertEquals(FUNDED, wallets.balanceOf(user))
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(transfer.id))
```

`RepublishFlowTest.kt` — 테스트 끝에 단언을 더한다:
- `은행이 DLT 뒤에 살아나면 정리 작업이 재발행해 COMPLETED, 은행 입금은 1건` 끝:
```kotlin
        assertEquals(FUNDED - 30_000, wallets.balanceOf(user))   // 재발행으로 끝내 완료 — 차감은 한 번
        assertEquals(listOf("WITHDRAW"), ledgerTypes(id))
```
- `은행이 계속 내려가 있으면 재발행 3회 뒤 FAILED E001, 은행 입금은 0건` 의 `withBankDown { … }` 블록 안 마지막 단언 다음:
```kotlin
            assertEquals(FUNDED, wallets.balanceOf(user))             // 정리 작업 ⓪ 의 포기(E001)도 환불한다 — 돈은 은행에 간 적이 없다
            assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(id))
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:compileTestKotlin` → FAIL (`Unresolved reference 'concurrently'`)

- [ ] **Step 3: 구현**

`Concurrently.kt` (신규, `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/`):
```kotlin
package io.github.hyeokjinon.banktransfer.api.support

import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * [count] 개의 작업을 고정 스레드 풀에서 **동시에 출발**시키고 모두 끝날 때까지 기다린 뒤 결과를 순서대로 돌려준다 (P4-1 동시성 테스트).
 * 스레드가 모두 준비된 뒤 래치 하나로 한꺼번에 풀어 준다 — 먼저 만든 스레드가 먼저 끝나 버려 경합이 사라지지 않게.
 * P4-2 에서 차감 전략(비관적 · 낙관적 · Redis 분산락)을 바꿔도 같은 도우미로 정확성부터 확인한다.
 */
fun <T> concurrently(count: Int, timeout: Duration = Duration.ofSeconds(60), task: (index: Int) -> T): List<T> {
    val pool = Executors.newFixedThreadPool(count)
    val ready = CountDownLatch(count)
    val start = CountDownLatch(1)
    try {
        val futures = (0 until count).map { i ->
            pool.submit(Callable {
                ready.countDown()
                start.await()
                task(i)
            })
        }
        check(ready.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) { "$count threads were not ready within $timeout" }
        start.countDown()
        return futures.map { it.get(timeout.toMillis(), TimeUnit.MILLISECONDS) }
    } finally {
        pool.shutdownNow()
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*WalletConcurrencyTest*' --tests '*TransferProcessingFlowTest*' --tests '*ReconcileFlowTest*' --tests '*RepublishFlowTest*'` → PASS. (잔고 단언은 Task 2 구현의 흐름 증명이다 — 실패하면 저장소 · 도메인 쪽 원인을 고치고 그 수정도 이 커밋에 넣는다.) 이어서 `./gradlew build` 전체 PASS.

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "test(api): 동시 출금 100건 → 정확히 10건, 같은 키 20건 → 차감 1번, 실패 확정 경로마다 환불 흐름 증명

잔고가 음수가 되지 않는다는 P4 의 정합성 기준선을 HTTP 동시 요청으로 고정한다 (P4-2 의 락 전략도 이 테스트부터).
은행 거절 · 결과조회 2001 · 멈춘 SENDING · 재발행 포기(E001) 가 모두 잔고를 되돌리고, COMPLETED 는 빠진 채,
UNKNOWN 은 보류로 묶인다는 것을 실제 Kafka · 모의 은행 흐름으로 확인한다.

Co-Authored-By: <작성 모델>"
```

---

### Task 5: 결정 기록 · 문서 · 손 확인

**Files:**
- Create: `docs/adr/0008-wallet-debit-on-intake-refund-on-failure.md`
- Create: `postman/collections/bank-transfer/5-wallet/.resources/definition.yaml`, `01-deposit.request.yaml`, `02-transfer.request.yaml`, `03-wallet.request.yaml`, `04-over-balance.request.yaml`, `05-unknown-account.request.yaml`, `06-failed.request.yaml`, `07-refunded.request.yaml`
- Modify: `docs/database.md`, `OPERATION.md`, `ARCHITECTURE.md`, `README.md`, `SETUP.md`, `CHANGELOG.md`, `HANDOVER.md`, `http/transfer.http`, `postman/collections/bank-transfer/.resources/definition.yaml`, `docs/superpowers/specs/2026-10-26-p4-1-wallet-balance-design.md` (구현 순서 · 손 확인 메모)

- [ ] **Step 1: ADR-0008** — 기존 ADR 형식(제목 · 상태 · 단계 · 맥락 · 결정 · 결과 · 검토한 다른 선택지):
  - 제목: `# ADR-0008 원화 잔고는 접수 트랜잭션에서 조건부 UPDATE 로 차감하고, 실패 확정 시 같은 트랜잭션에서 환불한다 (원장은 append-only)`
  - 상태: 제안 (2026-10-26). 단계 P4-1. 링크: [P4-1 설계](../superpowers/specs/2026-10-26-p4-1-wallet-balance-design.md) · [ADR-0004](0004-idempotency-key-and-transactional-outbox.md)(멱등키 범위를 바꾼다) · [ADR-0007](0007-retry-topic-and-bounded-republish.md)(E001 포기도 환불) · [database.md](../database.md) §4-4 · §4-5 · §6.
  - 맥락: 거래소의 "내 원화"처럼 사용자별 잔고가 있어야 출금 동시성 · 락 비교(P4-2)를 할 수 있다. 같은 사용자의 출금이 몰려도 잔고가 음수가 되면 안 된다(정합성 기준선). 결과를 모르는 돈(`UNKNOWN`)을 다른 출금에 쓰면 안 된다. P3 까지 멱등키 · 받는 계좌 확인이 전역이라 다른 사용자가 같은 키로 남의 출금 정보를 200 으로 받고, 남이 인증한 계좌로 출금할 수 있었다.
  - 결정: (1) `wallet`(잔고, CHECK `balance >= 0`, `version`) + append-only `wallet_ledger`(DEPOSIT · WITHDRAW · REFUND, `amount > 0`, `balance_after`, `uk_ledger_transfer_type`) — 잔고 = 원장 합을 언제든 맞춰 본다(P5 대사 근거) (2) 접수 트랜잭션 안에서 출금 INSERT → `UPDATE wallet SET balance = balance - ? WHERE user_id = ? AND balance >= ?`(0건이면 롤백, 422 `INSUFFICIENT_BALANCE`) → 원장 WITHDRAW → 아웃박스. 출금 INSERT 를 먼저 두는 이유(같은 키 경합의 진 쪽이 지갑을 건드리지 않고 200 을 받는다) (3) 실패 확정 4경로를 `TransferStore.fail()` 하나로 — 조건부 전이 1건일 때만 원장의 WITHDRAW 금액을 돌려주고 REFUND, `transition(…, FAILED)` 거부 (4) `RECEIVED` · `SENDING` · `UNKNOWN` 은 보류, `COMPLETED` 확정, `CONFLICT` 는 잔고를 손대지 않고 경고 (5) `X-User-Id`(영숫자 · 하이픈 1~36자, 웹 계층 한 곳, 값은 신뢰), 멱등 UNIQUE `(user_id, idempotency_key)`, 1원인증 · 조회 · 확인은 본인만(남이면 404) (6) 충전 API 는 관리 · 테스트용(1 ~ 1억 원, 인증 · 멱등키 없음) (7) 잔고 SQL 은 `WalletLedger` 한 곳 — P4-2 의 이음새.
  - 결과: 증명 테스트 이름 (`WalletConcurrencyTest` 2개: 100건 동시 → 정확히 10건 · 잔고 0 · 원장 합 = 잔고, 같은 키 20건 → 차감 1번 / `JpaTransferStoreTest`: 한 트랜잭션 · 아웃박스 실패 시 차감 롤백 · `fail()` 두 번 → 환불 1번 · 환불 실패 시 전이 롤백 · 같은 키 다른 사용자 · P3 행은 환불 없음 / `WalletLedgerTest`: CHECK 가 음수 거부 · REFUND 이중 기록 차단 / 흐름: 은행 거절 · 결과조회 2001 · 멈춘 SENDING · 재발행 포기 E001 → 잔고 복원, COMPLETED 는 빠진 채, UNKNOWN 은 보류 / API: `X-User-Id` 400, 남의 계좌 422, 남의 출금 · 인증 404). 한계: 잔고 행이 핫스팟(같은 사용자 출금은 행 락 대기, P4-2 에서 측정) · `CONFLICT`(예: `FAILED` 확정 · 환불 뒤 은행 완료)는 환불된 돈이 실제로 나간 상태일 수 있어 경고만(P5) · 충전에 인증 · 멱등키가 없다 · `X-User-Id` 신뢰 · 같은 키 경합에서 이긴 쪽이 롤백되면(잔고 부족 등) 기다리던 쪽끼리 MySQL 데드락(1213) → 500 이 드물게 날 수 있다(HANDOVER 의 기존 항목과 같다).
  - 다른 선택지 표: 컨슈머(은행 보내기 직전)에서 차감 = B안(잔고 부족을 접수 뒤에야 알고, 보류를 표현할 상태가 필요) · 잔고 행 `SELECT … FOR UPDATE` 후 계산(비관적 락 — 한 문장 조건부 UPDATE 와 결과가 같고 왕복이 하나 많다 → P4-2 에서 비교) · `version` 낙관적 락(경합 시 재시도 폭증 → P4-2) · Redis 분산락(외부 의존 · 락 만료 위험 → P4-2) · 원장 없이 잔고만(대사 근거가 없다) · 원장 합으로 잔고 계산(읽을 때마다 합산, 잔고 조건 차감을 원자적으로 못 한다) · 전역 멱등 UNIQUE 유지(남의 출금 정보 노출) · `CONFLICT` 자동 보정(돈과 기록이 어긋난 상태에서 자동 이동이 더 위험).
- [ ] **Step 2: `docs/database.md`**:
  - 머리 인용문 기준을 `P4-1 (Flyway V1 · V2 · V3 적용)` 로, 설계 배경에 `[P4-1 설계](superpowers/specs/2026-10-26-p4-1-wallet-balance-design.md)` 를 더한다.
  - §1 `키 컬럼 정렬` 행에 "`user_id`(wallet · wallet_ledger · transfer · account_verification) 도 `ascii_bin`" 추가. `접근 방식` 행에 "잔고는 `WalletLedger`(JDBC, 조건부 UPDATE)" 추가.
  - §3 ERD: `wallet { VARCHAR36 user_id PK; BIGINT balance "CHECK >= 0"; BIGINT version }`, `wallet_ledger { BIGINT id PK; VARCHAR36 user_id; VARCHAR type "DEPOSIT/WITHDRAW/REFUND"; BIGINT amount "> 0"; BIGINT balance_after; CHAR36 transfer_id "NULL = 충전" }` 엔티티, `transfer` · `account_verification` 에 `VARCHAR36 user_id` (transfer 의 `idempotency_key UK` 표기를 `UK(user_id, idempotency_key)` 로), 관계 `wallet ||..o{ wallet_ledger : "user_id"`, `transfer ||..o{ wallet_ledger : "transfer_id (WITHDRAW 1 · REFUND 0..1)"`, `account_verification` ↔ `transfer` 라벨을 `"(user_id, bank_code, account_no) 가 VERIFIED 여야 접수"` 로. FK 를 걸지 않는 이유 목록에 "원장은 append-only 기록이라 이체 행 삭제 · 보존 정책(P5)과 묶지 않는다" 추가.
  - §4-1 표: `id` 다음에 `| user_id | VARCHAR(36) ascii_bin | Y | 인증을 요청한 사용자 (X-User-Id). 확인 · 출금 계좌 확인은 이 사용자만. P3 시절 행은 NULL (누구에게도 인정되지 않는다). V3 (P4-1) |`. 인덱스에 `idx_verification_user_account (user_id, bank_code, account_no, status)` — 출금 계좌 확인.
  - §4-2 표: `id` 다음에 `| user_id | VARCHAR(36) ascii_bin | Y | 출금한 사용자. 이 사용자의 지갑에서 차감 · 환불. P3 시절 행은 NULL (조회 · 멱등 조회 모두 user_id = ? 라 보이지 않고, 차감한 적이 없어 환불도 없다). V3 (P4-1) |`. `idempotency_key` 설명의 `**UNIQUE**` 를 `(user_id, idempotency_key) **UNIQUE**` 로. 인덱스 목록의 `uk_transfer_idempotency` 줄을 `uk_transfer_user_idempotency (user_id, idempotency_key) — 사용자 범위 중복 접수 차단. NULL 은 서로 다른 값이라 P3 행끼리 충돌하지 않는다 (V3 에서 전역 uk_transfer_idempotency 를 대체)` 로, 새 줄 `idx_transfer_user (user_id, created_at) — 지갑의 보류 금액(내 RECEIVED · SENDING · UNKNOWN 합), 사용자별 출금 목록`.
  - 새 절 `### 4-4. wallet — 사용자 원화 잔고 (P4-1)` 표: `user_id` VARCHAR(36) ascii_bin N PK / `balance` BIGINT N 잔고(원). `CHECK (balance >= 0)` = `ck_wallet_balance_non_negative` — 조건부 차감의 2차 방어선 / `version` BIGINT N 바뀔 때마다 +1 (P4-2 낙관적 락 비교용) / `created_at` · `updated_at` DATETIME(6) N. 메모: 지갑은 첫 충전 때 만들어진다(`INSERT … ON DUPLICATE KEY UPDATE`), 지갑이 없으면 잔고 0.
  - 새 절 `### 4-5. wallet_ledger — 원화 원장, append-only (P4-1)` 표: `id` BIGINT AUTO_INCREMENT PK 기록 순서 / `user_id` VARCHAR(36) ascii_bin N / `type` VARCHAR(10) N `DEPOSIT`(충전) · `WITHDRAW`(접수 차감) · `REFUND`(실패 확정 환불) / `amount` BIGINT N 항상 양수, 방향은 type (`ck_ledger_amount_positive`) / `balance_after` BIGINT N 이 기록 직후 잔고 (같은 트랜잭션이 지갑 행 락을 쥔 채 읽은 값) / `transfer_id` CHAR(36) Y 출금 id, DEPOSIT 은 NULL / `created_at` DATETIME(6) N. 인덱스: `uk_ledger_transfer_type (transfer_id, type)` — 같은 이체에 출금 · 환불이 두 번 기록될 수 없다(이중 환불 2차 방어, NULL 은 여러 개 허용), `idx_ledger_user (user_id, id)` — 사용자별 원장 · 합계. 수정 · 삭제하지 않는다.
  - §5 상태 표에 행 `| wallet_ledger.type | DEPOSIT · WITHDRAW · REFUND (상태가 아니라 기록 종류) | – |` 추가, 그 아래 "출금 상태별 잔고" 표(스펙 §4-3) 추가.
  - §6 동시성 표에 행 3개: `| 잔고 차감 (같은 사용자 출금 동시) | 조건부 UPDATE 한 문장 + CHECK, 접수 트랜잭션 안 | INSERT transfer … → UPDATE wallet SET balance = balance - ?, version = version + 1 … WHERE user_id = ? AND balance >= ? → 0건이면 롤백(422) → INSERT wallet_ledger(WITHDRAW) → INSERT outbox_event. 같은 (user_id, 키)는 transfer UNIQUE 에서 먼저 멈춰 지갑을 건드리지 않는다 |`, `| 환불 (컨슈머 · 정리 작업 겹침) | 조건부 전이 1건일 때만 + 원장 UNIQUE | UPDATE transfer SET status = 'FAILED' … WHERE id = ? AND status = ? → 1건이면 SELECT amount FROM wallet_ledger WHERE transfer_id = ? AND type = 'WITHDRAW' → UPDATE wallet SET balance = balance + ? → INSERT wallet_ledger(REFUND). 한 트랜잭션 |`, `| 충전 | INSERT … ON DUPLICATE KEY UPDATE | 지갑이 없으면 만들고 있으면 더한다 + INSERT wallet_ledger(DEPOSIT) |`. 표 아래 문장 "잔액 차감의 락 비교 … P4 에서 이 표에 추가한다" 를 "같은 차감을 비관적 락 · 낙관적 락 · Redis 분산락으로 바꿔 끼운 비교(k6)는 P4-2 에서 이 표에 추가한다" 로.
  - §8 파일명 예시에 `V3__wallet.sql` 추가.
  - §9 운영 쿼리 추가:
```sql
-- 사용자 잔고와 보류 금액 (보류 = 결과를 기다리는 출금 합)
SELECT w.user_id, w.balance,
       (SELECT COALESCE(SUM(t.amount), 0) FROM transfer t
         WHERE t.user_id = w.user_id AND t.status IN ('RECEIVED', 'SENDING', 'UNKNOWN')) AS pending
  FROM wallet w WHERE w.user_id = 'user-1';

-- 잔고 = 원장 합 검증 (결과가 나오면 불일치 — 정상이면 0행)
SELECT w.user_id, w.balance, COALESCE(l.total, 0) AS ledger_total
  FROM wallet w
  LEFT JOIN (SELECT user_id, SUM(CASE type WHEN 'WITHDRAW' THEN -amount ELSE amount END) AS total
               FROM wallet_ledger GROUP BY user_id) l ON l.user_id = w.user_id
 WHERE w.balance <> COALESCE(l.total, 0);

-- 환불 누락 확인: FAILED 인데 차감(WITHDRAW)만 있고 환불(REFUND)이 없는 출금 (정상이면 0행)
SELECT t.id, t.user_id, t.amount, t.updated_at FROM transfer t
  JOIN wallet_ledger w ON w.transfer_id = t.id AND w.type = 'WITHDRAW'
  LEFT JOIN wallet_ledger r ON r.transfer_id = t.id AND r.type = 'REFUND'
 WHERE t.status = 'FAILED' AND r.id IS NULL;

-- 사용자 원장 최근 20줄
SELECT id, type, amount, balance_after, transfer_id, created_at FROM wallet_ledger
 WHERE user_id = 'user-1' ORDER BY id DESC LIMIT 20;
```
  - §10 의 `잔액 테이블: P4 에서 추가.` 를 `잔고 락 전략: P4-2 에서 비관적 · 낙관적 · Redis 분산락을 k6 로 비교한다. 원장 보존(파티셔닝 · 보관)은 P5.` 로.
  - 변경 이력: `| 2026-10-26 | V3: wallet · wallet_ledger, transfer · account_verification 의 user_id, 멱등 UNIQUE 를 (user_id, idempotency_key) 로, 잔고 동시성 · 운영 쿼리(잔고 = 원장 합) | [PR #11](https://github.com/hyeokjinON/bank-transfer/pull/11) (P4-1) |`
- [ ] **Step 3: `OPERATION.md`**:
  - §1 또는 API 절 근처에 새 절 `### 지갑 (P4-1)`: 모든 출금 · 1원인증 · 지갑 API 는 `X-User-Id` 헤더가 필요하다(영숫자 · 하이픈 1~36자, 대소문자 구분, 인증은 범위 밖이라 값을 믿는다). `POST /api/v1/wallet/deposits {"amount": n}` — **관리 · 테스트용** 충전(1 ~ 1억 원, 인증 · 멱등키 없음 → 같은 요청을 두 번 보내면 두 번 충전된다), `GET /api/v1/wallet` → `{userId, balance, pending}`. 출금은 접수할 때 잔고에서 빠지고(`RECEIVED` · `SENDING` · `UNKNOWN` 동안 `pending` 에 보임), `COMPLETED` 면 빠진 채, `FAILED` 면 돌아온다. 예시 curl (`-H 'X-User-Id: user-1'`).
  - §4 오류 코드 표에 행: `| X-User-Id 헤더 없음 · 형식 오류 | 400 | USER_ID_REQUIRED |`, `| 잔고가 출금액보다 적음 (지갑이 없어도) | 422 | INSUFFICIENT_BALANCE |`, `| 충전 금액이 1 ~ 100,000,000 원 밖 | 400 | INVALID_REQUEST |`. `1원인증을 마치지 않은 계좌` 행을 `이 사용자가 1원인증을 마치지 않은 계좌 (남이 인증한 계좌 포함)` 로, `없는 이체 id` 행을 `없는 이체 id · 남의 출금` 으로 고치고 `| 남의 1원인증 건 확인 | 404 | VERIFICATION_NOT_FOUND |` 추가.
  - §6 장애 대응에 행: `| 422 INSUFFICIENT_BALANCE 가 잦음 | GET /api/v1/wallet 의 balance · pending | 잔고 부족. pending 이 크면 결과를 기다리는 출금(UNKNOWN 등)이 돈을 묶고 있다 → 정리 작업 · 결과조회 로그를 본다 |`, `| 잔고와 원장 합이 다름 | database.md §9 "잔고 = 원장 합 검증" | 정상이면 0행. 원장은 수정하지 않는다 — 원인(직접 수정한 SQL 등)을 찾고 보정 기록을 남긴다 (P5 대사) |`, `| FAILED 인데 잔고가 안 돌아옴 | database.md §9 "환불 누락 확인", 해당 출금의 user_id | user_id 가 NULL 이면 P3 시절 출금이라 차감도 환불도 없다(정상). 그 밖이면 버그 — fail() 을 거치지 않은 상태 변경이 있었는지 본다 |`, `| CONFLICT 경고 (wallet balance left as is) | 출금 상태 · 원장 · 은행 거래내역 | 잔고를 자동으로 고치지 않는다. 예: FAILED 확정 · 환불 뒤 은행이 완료 → 환불된 돈이 실제로 나갔다 → 수동 확인 · P5 대사 |`.
- [ ] **Step 4: `ARCHITECTURE.md`**:
  - §1 그림의 `app` 안에 `WAL[WalletController] -- "충전 · 잔고 · 보류" --> WL[WalletLedger<br/>api/persistence]` 와 `WL --> REPO`, `TRF -- "TransferStore (차감 · 환불)" --> REPO` 라벨을 더한다. 그림 아래 bullet 에 "사용자 식별: `X-User-Id` 를 `RequestUserResolver` 한 곳에서 꺼낸다 (인증은 범위 밖, P4-1)" 추가.
  - §2 모듈 표의 `modules/transfer` 설명에 "사용자 범위 접수 · 실패 확정 포트 `fail()`", `apps/api` 설명에 "잔고 SQL(`WalletLedger`)" 추가.
  - 소스 지도: `web/RequestUser.kt  # X-User-Id → RequestUser (헤더를 읽는 유일한 곳), WebConfiguration`, `web/WalletController.kt  # POST /api/v1/wallet/deposits (관리 · 테스트용), GET /api/v1/wallet`, `persistence/WalletLedger.kt  # 충전 · 차감 · 환불 · 잔고 · 보류 SQL 한 곳 (P4-2 이음새)`, 마이그레이션 줄을 `V1__init.sql · V2__transfer_republish.sql · V3__wallet.sql   # V3: wallet · wallet_ledger · user_id (P4-1)` 로, `web/TransferController.kt` 주석에 `X-User-Id`, `TransferService.kt` 주석에 `사용자 범위 · 잔고 부족 422`.
  - §6 제목을 `## 6. 진행 상황 (P3 · P4)` 로, 첫 줄 끝에 ` P4-1 원화 잔고 ✅ (ADR-0008).` 를 붙이고, 절 끝에 새 소절 `### P4-1 원화 잔고 흐름` (mermaid sequenceDiagram): 클라이언트 `POST /transfers (X-User-Id, Idempotency-Key)` → `TransferService` (멱등 조회 · 내 인증 계좌) → `JpaTransferStore.saveWithEvent` 한 트랜잭션 [`INSERT transfer` → `UPDATE wallet … AND balance >= ?` (0건 → 롤백 → 422) → `INSERT wallet_ledger WITHDRAW` → `INSERT outbox_event`] → 202. 실패 확정: 컨슈머(거절) · 정리 작업(2001 · E001) → `fail(id, from)` 한 트랜잭션 [`UPDATE transfer … WHERE status = from` (0건이면 끝) → `UPDATE wallet balance + 원장 WITHDRAW 금액` → `INSERT wallet_ledger REFUND`]. 아래 bullet: 상태별 잔고 표 요약, `CONFLICT` 는 잔고 그대로, `WalletLedger` 가 P4-2 이음새.
  - §8 색인에 `| [0008](docs/adr/0008-wallet-debit-on-intake-refund-on-failure.md) | 원화 잔고는 접수 트랜잭션에서 조건부 UPDATE 로 차감, 실패 확정 시 같은 트랜잭션에서 환불, 원장은 append-only |`.
- [ ] **Step 5: 손 확인 자료**
  - `http/transfer.http` 끝에 절 추가:
```http


### ============================================================
### 손 확인: 원화 잔고 — 충전 → 출금 → 잔고 · 보류 → 잔고보다 큰 출금 422 → 은행이 거절하는 출금 → FAILED 뒤 잔고 복원 (P4-1)
### 준비: 실행 구성 "0. Mock Bank + API". 1002123456789 는 user-1 로 인증해 둔다 (account-verification.http 1) · 2)).
###       "은행이 거절하는 출금"은 모의 은행에 없는 계좌라 1원인증을 API 로 마칠 수 없다 → MySQL 에 인증 행을 한 번 넣어 둔다:
###       docker compose -f infra/docker-compose.yml --env-file infra/.env exec mysql mysql -ubank -p bank_transfer
###       INSERT INTO account_verification (id, user_id, bank_code, account_no, holder_name, code_hash, status, failed_attempts, expires_at, verified_at, created_at)
###       VALUES (UUID(), 'user-1', '090', '5500000000001', '손확인', REPEAT('0', 64), 'VERIFIED', 0, NOW(6), NOW(6), NOW(6));
### ============================================================

### 9-1) 충전 10만 원 → 200, balance 기록
POST http://localhost:8080/api/v1/wallet/deposits
Content-Type: application/json
X-User-Id: user-1

{
  "amount": 100000
}

> {%
    client.global.set("walletBalance", response.body.balance);
%}

### 9-2) 출금 3만 원 → 202 (접수 트랜잭션에서 잔고 차감)
POST http://localhost:8080/api/v1/transfers
Content-Type: application/json
X-User-Id: user-1
Idempotency-Key: {{$random.uuid}}

{
  "bankCode": "090",
  "accountNo": "1002123456789",
  "amount": 30000,
  "memo": "잔고"
}

### 9-3) 지갑 조회 → balance = 9-1 - 30000. pending 은 처리 전이면 30000, 처리(COMPLETED) 뒤면 0 (확정 — 잔고는 빠진 채)
GET http://localhost:8080/api/v1/wallet
X-User-Id: user-1

> {%
    client.global.set("walletBalance", response.body.balance);
    client.global.set("overAmount", response.body.balance + 1);
%}

### 9-4) 잔고보다 1원 큰 출금 → 422 INSUFFICIENT_BALANCE (잔고가 1천만 원 이상이면 1회 한도 INVALID_AMOUNT 가 먼저 난다)
POST http://localhost:8080/api/v1/transfers
Content-Type: application/json
X-User-Id: user-1
Idempotency-Key: {{$random.uuid}}

{
  "bankCode": "090",
  "accountNo": "1002123456789",
  "amount": {{overAmount}},
  "memo": "잔고초과"
}

### 9-5) 은행에 없는 계좌(위 SQL 로 인증)로 2만 원 → 202. 몇 초 안에 은행이 1001 로 거절한다
POST http://localhost:8080/api/v1/transfers
Content-Type: application/json
X-User-Id: user-1
Idempotency-Key: {{$random.uuid}}

{
  "bankCode": "090",
  "accountNo": "5500000000001",
  "amount": 20000,
  "memo": "환불확인"
}

> {%
    client.global.set("refundTransferId", response.body.id);
%}

### 9-6) 몇 초 뒤 조회 → FAILED 1001
GET http://localhost:8080/api/v1/transfers/{{refundTransferId}}
X-User-Id: user-1

### 9-7) 지갑 조회 → balance 가 9-3 과 같다 (실패 확정과 같은 트랜잭션에서 환불). DB 원장: WITHDRAW 20000 → REFUND 20000
GET http://localhost:8080/api/v1/wallet
X-User-Id: user-1
```
  - Postman 루트 `definition.yaml`: `variables` 에 `walletBalance: ""`, `overAmount: ""`, `refundTransferId: ""` 를 넣고, `description` 의 5) 뒤에 줄 추가: `6) (P4-1) "5. 원화 잔고" 폴더: 먼저 1. 폴더로 user-1 이 {{accountNo}} 를 인증하고, MySQL 에 5500000000001 인증 행을 넣는다(SQL 은 http/transfer.http 9절 머리말). 5-1 → 5-7 을 차례로 Send (5-6 · 5-7 은 5-5 뒤 몇 초 기다린다). 대응: http/transfer.http 9-1 ~ 9-7.`
  - `5-wallet/.resources/definition.yaml`:
```yaml
$kind: collection
name: 5. 원화 잔고 (P4-1)
order: 5000
```
  - `5-wallet/01-deposit.request.yaml`:
```yaml
$kind: http-request
name: 5-1 충전 10만 원 → 200, 잔고 기록
method: POST
url: '{{baseUrl}}/api/v1/wallet/deposits'
order: 1000
headers:
  - key: Content-Type
    value: application/json
  - key: X-User-Id
    value: '{{userId}}'
body:
  type: json
  content: |-
    {
      "amount": 100000
    }
scripts:
  - type: afterResponse
    language: text/javascript
    code: |-
      // 관리 · 테스트용 충전. 응답은 충전 뒤 잔고와 보류(결과를 기다리는 출금 합).
      pm.test("200, 사용자 · 잔고", () => {
        pm.response.to.have.status(200);
        pm.expect(pm.response.json().userId).to.eql(pm.collectionVariables.get("userId"));
        pm.expect(pm.response.json().balance).to.be.at.least(100000);
      });
      pm.collectionVariables.set("walletBalance", String(pm.response.json().balance));
```
  - `5-wallet/02-transfer.request.yaml`:
```yaml
$kind: http-request
name: 5-2 출금 3만 원 → 202 (접수 트랜잭션에서 잔고 차감)
method: POST
url: '{{baseUrl}}/api/v1/transfers'
order: 2000
headers:
  - key: Content-Type
    value: application/json
  - key: X-User-Id
    value: '{{userId}}'
  - key: Idempotency-Key
    value: '{{$guid}}'
body:
  type: json
  content: |-
    {
      "bankCode": "090",
      "accountNo": "{{accountNo}}",
      "amount": 30000,
      "memo": "잔고"
    }
scripts:
  - type: afterResponse
    language: text/javascript
    code: |-
      // 1. 폴더에서 같은 사용자로 {{accountNo}} 를 인증해 두어야 한다 (남이 인증한 계좌는 422 ACCOUNT_NOT_VERIFIED).
      pm.test("202 RECEIVED", () => {
        pm.response.to.have.status(202);
        pm.expect(pm.response.json().status).to.eql("RECEIVED");
      });
```
  - `5-wallet/03-wallet.request.yaml`:
```yaml
$kind: http-request
name: 5-3 지갑 조회 → 잔고 3만 원 감소 (COMPLETED 여도 빠진 채)
method: GET
url: '{{baseUrl}}/api/v1/wallet'
order: 3000
headers:
  - key: X-User-Id
    value: '{{userId}}'
scripts:
  - type: afterResponse
    language: text/javascript
    code: |-
      // pending = 이 사용자의 RECEIVED · SENDING · UNKNOWN 출금 합. 5-2 가 처리 전이면 30000 이 보이고, 다른 폴더의 UNKNOWN 이 남아 있으면 그만큼 더 보인다.
      const body = pm.response.json();
      pm.test("잔고 = 5-1 잔고 - 30000", () => {
        pm.response.to.have.status(200);
        pm.expect(body.balance).to.eql(Number(pm.collectionVariables.get("walletBalance")) - 30000);
      });
      console.log("pending = " + body.pending);
      pm.collectionVariables.set("walletBalance", String(body.balance));
      pm.collectionVariables.set("overAmount", String(body.balance + 1));
```
  - `5-wallet/04-over-balance.request.yaml`:
```yaml
$kind: http-request
name: 5-4 잔고보다 1원 큰 출금 → 422 INSUFFICIENT_BALANCE
method: POST
url: '{{baseUrl}}/api/v1/transfers'
order: 4000
headers:
  - key: Content-Type
    value: application/json
  - key: X-User-Id
    value: '{{userId}}'
  - key: Idempotency-Key
    value: '{{$guid}}'
body:
  type: json
  content: |-
    {
      "bankCode": "090",
      "accountNo": "{{accountNo}}",
      "amount": {{overAmount}},
      "memo": "잔고초과"
    }
scripts:
  - type: beforeRequest
    language: text/javascript
    code: |-
      const over = Number(pm.collectionVariables.get("overAmount"));
      if (!(over > 0) || over > 10000000) {
        throw new Error("overAmount 가 비었거나 1천만 원(1회 한도)을 넘습니다 — 5-3 을 먼저 Send 하세요. 잔고가 1천만 원 이상이면 INVALID_AMOUNT 가 먼저 납니다");
      }
  - type: afterResponse
    language: text/javascript
    code: |-
      pm.test("422 INSUFFICIENT_BALANCE (출금도 차감도 남지 않는다)", () => {
        pm.response.to.have.status(422);
        pm.expect(pm.response.json().code).to.eql("INSUFFICIENT_BALANCE");
      });
```
  - `5-wallet/05-unknown-account.request.yaml`:
```yaml
$kind: http-request
name: 5-5 은행에 없는 계좌로 2만 원 → 202 (SQL 로 인증 행을 넣어 둔다)
method: POST
url: '{{baseUrl}}/api/v1/transfers'
order: 5000
headers:
  - key: Content-Type
    value: application/json
  - key: X-User-Id
    value: '{{userId}}'
  - key: Idempotency-Key
    value: '{{$guid}}'
body:
  type: json
  content: |-
    {
      "bankCode": "090",
      "accountNo": "5500000000001",
      "amount": 20000,
      "memo": "환불확인"
    }
scripts:
  - type: afterResponse
    language: text/javascript
    code: |-
      // 422 ACCOUNT_NOT_VERIFIED 면 루트 설명 6) 의 SQL 로 user-1 의 5500000000001 인증 행을 먼저 넣는다 (모의 은행에 없는 계좌라 1원인증 API 로는 못 만든다).
      pm.test("202 RECEIVED", () => {
        pm.response.to.have.status(202);
      });
      pm.collectionVariables.set("refundTransferId", pm.response.json().id);
```
  - `5-wallet/06-failed.request.yaml`:
```yaml
$kind: http-request
name: 5-6 몇 초 뒤 조회 → FAILED 1001 (은행 거절)
method: GET
url: '{{baseUrl}}/api/v1/transfers/{{refundTransferId}}'
order: 6000
headers:
  - key: X-User-Id
    value: '{{userId}}'
scripts:
  - type: afterResponse
    language: text/javascript
    code: |-
      pm.test("FAILED, 은행 응답코드 1001", () => {
        pm.response.to.have.status(200);
        pm.expect(pm.response.json().status).to.eql("FAILED");
        pm.expect(pm.response.json().bankResponseCode).to.eql("1001");
      });
```
  - `5-wallet/07-refunded.request.yaml`:
```yaml
$kind: http-request
name: 5-7 지갑 조회 → 잔고 복원 (실패 확정과 같은 트랜잭션에서 환불)
method: GET
url: '{{baseUrl}}/api/v1/wallet'
order: 7000
headers:
  - key: X-User-Id
    value: '{{userId}}'
scripts:
  - type: afterResponse
    language: text/javascript
    code: |-
      // DB 원장(database.md §9 "사용자 원장 최근 20줄")에는 이 출금의 WITHDRAW 20000 · REFUND 20000 이 보인다.
      pm.test("잔고 = 5-3 의 잔고 (2만 원이 돌아왔다)", () => {
        pm.response.to.have.status(200);
        pm.expect(pm.response.json().balance).to.eql(Number(pm.collectionVariables.get("walletBalance")));
      });
```
- [ ] **Step 6: 나머지 문서**
  - `README.md`: "지금 되는 것"에 `- **원화 잔고 (P4-1)**: 사용자(\`X-User-Id\`)별 원화 잔고와 append-only 원장. 출금 접수 트랜잭션에서 조건부 UPDATE 로 차감(잔고 부족 422), 실패로 확정되면 같은 트랜잭션에서 환불, 결과를 모르는 출금은 보류로 묶음. 동시 출금 100건에도 잔고는 음수가 되지 않음. 멱등키 · 1원인증 · 조회는 사용자 범위 ([ADR-0008](docs/adr/0008-wallet-debit-on-intake-refund-on-failure.md))` 추가. 로드맵 `- [ ] **P4** …` 아래에 `  - [x] P4-1 원화 잔고: 접수 차감 · 실패 환불 · 사용자 범위 (PR #11) → [ADR-0008](docs/adr/0008-wallet-debit-on-intake-refund-on-failure.md)` · `  - [ ] P4-2 같은 차감을 비관적 락 · 낙관적 락 · Redis 분산락으로 바꿔 k6 로 비교` 두 줄. 터미널 curl 예시 두 줄에 `-H 'X-User-Id: user-1'` 를 더하고, 그 아래에 충전 · 지갑 조회 curl 두 줄(`curl -X POST localhost:8080/api/v1/wallet/deposits -H 'Content-Type: application/json' -H 'X-User-Id: user-1' -d '{"amount":100000}'`, `curl localhost:8080/api/v1/wallet -H 'X-User-Id: user-1'`) 추가. 오류 표에 `| X-User-Id 없음 | 400 | USER_ID_REQUIRED |` 추가. 테스트 수(지금 되는 것 · IntelliJ · 터미널 세 곳)를 Step 7 결과로, 테스트 목록 괄호에 "동시 출금 100건 → 정확히 10건 · 잔고 0, 실패 확정 경로마다 환불" 추가.
  - `SETUP.md` 2-2 의 테스트 수를 Step 7 결과로.
  - `CHANGELOG.md`: `## [P3-3b] …` 위에 `## [P4-1] 원화 잔고 — 2026-10-26 ([#11](https://github.com/hyeokjinON/bank-transfer/pull/11))` — Added: `wallet` · `wallet_ledger`(V3), `WalletLedger`, 지갑 API(`POST /api/v1/wallet/deposits` 관리 · 테스트용, `GET /api/v1/wallet` 잔고 · 보류), `TransferStore.fail()`(실패 확정 + 환불 한 트랜잭션), 오류 코드 `USER_ID_REQUIRED` · `INSUFFICIENT_BALANCE`, ADR-0008, 동시성 테스트(100건 · 같은 키 20건), 손 확인(`http/transfer.http` 9절 · Postman 5 폴더). Changed (**호환 깨짐**): 모든 출금 · 1원인증 API 에 `X-User-Id` 필수, 출금은 잔고에서 차감(충전 필요), 멱등키 UNIQUE 를 `(user_id, idempotency_key)` 로, 받는 계좌는 내가 인증한 계좌만, 남의 출금 조회 · 남의 인증 확인은 404, `transition(…, FAILED)` 금지(4경로 모두 `fail()`), CONFLICT 경고에 "잔고 그대로".
  - `HANDOVER.md`: 마지막 갱신일 2026-10-26, 표의 `P4 ~ P9` 행 위에 `| P4-1 원화 잔고 | 🟡 PR #11 리뷰 중 (손 확인 대기) | ADR-0008 제안, 테스트 N개(Step 7). 접수 차감 · 실패 환불 · 사용자 범위 |` 행, `P4 ~ P9` 를 `P4-2 ~ P9` 로. §2 아래에 소절 `### P4-1 설계 (합의된 것)` — [설계 문서](docs/superpowers/specs/2026-10-26-p4-1-wallet-balance-design.md) 링크와 스펙 §2 결정 요약 표 7줄(잔액 모델 · 사용자 식별 · 차감 시점 · 동시성 · 환불 · 멱등키 · 계좌 소유). §3 다음 할 일: 1) 손 확인(Postman 5 폴더) 2) velog `#7` (P4-1: 조건부 UPDATE 한 문장으로 잔고 음수 막기 · 실패 확정 경로 단일화) 3) P4-2 설계 → 계획 (락 3종 + k6, 이 계획의 `WalletConcurrencyTest` · `concurrently` 를 그대로 쓴다). 새 소절 `### P4-1 범위 밖으로 남긴 것`: 인증(`X-User-Id` 신뢰) · 은행 입금 통지로 충전 + 충전 멱등 · 락 비교 · 부하(P4-2) · `CONFLICT` 자동 보정 없음(P5 대사) · P3 시절 출금(user_id NULL)은 아무도 못 본다 · 원장 보존 정책(P5). §4 알려진 한계에 `(P4-1)` 줄: 잔고 행 핫스팟(같은 사용자 출금은 행 락 대기), `CONFLICT` 는 환불된 돈이 실제로 나간 상태일 수 있다, 충전은 인증 · 멱등키가 없다(두 번 보내면 두 번 충전), 같은 키 경합에서 이긴 쪽이 롤백되면 기다리던 쪽끼리 데드락(1213) → 500 이 드물게 난다 — "P3-2 에서 남은 것"의 1213 항목과 합친다. §5 최근 배운 것에 "제약 이름으로 예외를 가르는 코드는 인덱스 이름을 바꿀 때 같이 고친다 (`uk_transfer_idempotency` 는 `uk_transfer_user_idempotency` 의 부분 문자열이 아니다)" 추가.
  - 설계 문서 `docs/superpowers/specs/2026-10-26-p4-1-wallet-balance-design.md`: §4-1 4번 목록 아래에 `> 구현 계획에서 순서를 조정했다 (2026-10-26): 출금 INSERT → 차감 + 원장 → 아웃박스. 같은 (사용자, 키) 경합의 진 쪽이 지갑을 건드리지 않고 멱등 응답(200)을 받게 하려는 것 — 차감을 먼저 하면 잔고가 한 건 몫뿐일 때 진 쪽이 422 를 받는다.` 인용문을 넣고, §6 "손 확인" 문장 끝에 ` 모르는 계좌는 1원인증 API 로 VERIFIED 를 만들 수 없어 MySQL 에 인증 행을 넣어 둔다 (http/transfer.http 9절).` 를 붙인다.
- [ ] **Step 7: 전체 빌드** — `./gradlew build --rerun-tasks`, 모듈별 `tests=` 합계를 README · SETUP · HANDOVER 에 반영. 손 확인(Step 5 절차)은 머지 전에 사람이 한다 — HANDOVER 비고에 "손 확인 대기".
- [ ] **Step 8: 커밋**

```bash
git add docs http postman README.md SETUP.md CHANGELOG.md HANDOVER.md OPERATION.md ARCHITECTURE.md
git commit -m "docs: P4-1 — ADR-0008, 지갑 스키마 · 운영 쿼리(잔고 = 원장 합), 오류 코드, 손 확인 절차 (http · Postman 5 폴더)

차감 · 환불을 왜 접수 · 실패 확정 트랜잭션에 묶었는지와 남은 위험(잔고 핫스팟, CONFLICT 뒤 환불된 돈,
충전 멱등 없음)을 ADR 로 남긴다. 운영자는 잔고 = 원장 합 쿼리로 언제든 불일치를 찾을 수 있다.

Co-Authored-By: <작성 모델>"
```
(push · PR 없음.)

---

## 스펙 대응표 (계획 자체 점검)

| 스펙 | 내용 | Task |
|---|---|---|
| §2 잔액 모델 | `wallet` + append-only `wallet_ledger` | 2 (V3 · `WalletLedger`), 5 (database.md) |
| §2 사용자 식별 | `X-User-Id` 1~36자, 400, 웹 한 곳 | 2 (`RequestUserResolver` · `UserIdHeaderApiTest`), 3 |
| §2 차감 시점 · 동시성 | 접수 트랜잭션 조건부 UPDATE + CHECK, 422 | 1 (`InsufficientBalance`), 2 (`saveWithEvent`), 4 (100건 동시) |
| §2 환불 · §4-2 | 4경로 `fail()` 하나, 전이 · 환불 · 원장 한 트랜잭션, `transition(FAILED)` 거부, 이중 환불 두 겹, P3 행 환불 없음 | 1 (도메인 4경로), 2 (`fail()` · 롤백 · 두 번 · P3 행), 4 (흐름 4경로) |
| §2 보류 · §4-3 | RECEIVED · SENDING · UNKNOWN 빠진 채, COMPLETED 확정, FAILED 복원 | 2 (`pendingOf`), 3 (`GET /wallet`), 4 (흐름 단언) |
| §2 CONFLICT | 잔고 손대지 않고 경고 | 1 (CONFLICT 테스트의 fail 1회), 2 (경고 문구), 5 (OPERATION) |
| §2 멱등키 | UNIQUE `(user_id, idempotency_key)` | 2 (V3 · 매핑 · 같은 키 다른 사용자), 3 (API 202 두 번), 4 (같은 키 20건) |
| §2 계좌 소유 · 조회 · §4-5 | 내가 인증한 계좌만, 확인 본인만, 남의 출금 404 | 1, 2 (`existsVerified`), 3 (API 422 · 404 · 404) |
| §3 데이터 모델 | `V3__wallet.sql` 그대로, NULL 옛 행, KST | 2 (`DatabaseSetupTest` · `WalletLedgerTest` KST) |
| §4-1 접수 | 형식 → 멱등 조회(차감 없음) → 내 인증 계좌 → 한 트랜잭션, 같은 키 경합 롤백 | 1, 2 (순서 조정 — 아래 "조정"), 4 |
| §4-4 지갑 API | 충전 1 ~ 1억(관리 · 테스트용), 조회 balance · pending, 지갑 없으면 0 | 3 (`WalletController` · `WalletApiTest`) |
| §5 컴포넌트 · 포트 | 표의 모든 변경 | 1 (도메인), 2 (`WalletLedger` · `JpaTransferStore` · 웹), 3 (`WalletController`) |
| §6 테스트 | 도메인 · 저장소 · 동시성 · API · 흐름, 기존 테스트 · Postman · http 고침, 손 확인 | 1 · 2 · 3 · 4, 5 (손 확인) |
| §7 위험과 한계 | 핫스팟 · CONFLICT · 충전 · `X-User-Id` 신뢰 | 5 (ADR-0008 · HANDOVER · OPERATION) |
| §8 결정 기록 · 문서 | ADR-0008, database.md, OPERATION, ARCHITECTURE, README, HANDOVER, CHANGELOG, Postman · http | 3 (헤더 · 충전), 5 |

**스펙에서 조정한 것** (Task 5 Step 6 에서 설계 문서에도 메모한다):
1. §4-1 4번의 순서 — 스펙은 "차감 → 원장 → 출금 INSERT → 아웃박스" 로 적었지만 **출금 INSERT → 차감 + 원장 → 아웃박스** 로 한다 (같은 키 경합의 진 쪽이 422 대신 200 을 받게, 아웃박스 실패 시 차감 롤백을 테스트로 보이게).
2. §6 손 확인의 "모르는 계좌 출금" — 모의 은행에 없는 계좌는 1원인증 API 로 VERIFIED 를 만들 수 없어 MySQL 에 인증 행을 넣는다.
3. `saveWithEvent` 는 `userId` 가 없는 출금을 거부한다(스펙에 없음) — 차감 없이 들어가는 길을 막는다. 그래서 출금을 직접 저장하는 테스트도 사용자 · 충전이 필요하다.
4. `transition(…, FAILED)` 거부는 포트 계약(KDoc)과 어댑터(`JpaTransferStore` 의 `require`) · 가짜 저장소로 지킨다 — 도메인 인터페이스만으로는 강제할 수 없다.
5. 충전 응답은 200 + `{userId, balance, pending}` (스펙에 상태 코드가 없음). 지갑 `version` 은 바뀔 때마다 +1 (P4-2 낙관적 락 비교용).
6. 작업 분할: 컴파일 때문에 웹의 `X-User-Id` 처리를 Task 2(저장소)와 함께 넣고, HTTP 를 거치는 기존 테스트 5개 클래스는 Task 3 에서 고친다 (Task 2 끝에서 잠시 빨간색).
