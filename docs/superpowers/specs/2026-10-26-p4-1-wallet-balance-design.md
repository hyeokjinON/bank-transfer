# P4-1 원화 잔고 — 설계

> 승인: 2026-10-26 사용자와 섹션별로 합의 (사용자 원화 잔고, P4 를 둘로 분할, `X-User-Id` + 계좌 소유, A안 = 접수 트랜잭션에서 차감 · 실패 확정 시 환불).
> 다음 단계: P4-2 (같은 차감을 비관적 락 · 낙관적 락 · Redis 분산락으로 바꿔 끼우고 k6 로 비교).

## 1. 목적과 범위

가상자산 거래소의 "내 원화"처럼 **사용자별 원화 잔고**를 두고, 출금 접수 때 잔고에서 빼고(보류), 실패로 확정되면 돌려준다.
동시에 같은 사용자의 출금이 여러 건 와도 **잔고는 절대 음수가 되지 않는다** — 이것이 P4 의 정합성 기준선이다.

**범위 밖**: 인증(`X-User-Id` 는 "원래는 인증 토큰에서 꺼내는 값"으로 둔다), 은행 입금 통지로 잔고 채우기(충전은 관리 · 테스트용 API), 락 전략 비교 · 부하 테스트(P4-2), 일마감 대사(P5).

## 2. 결정 요약

| 결정 | 내용 | 이유 |
|---|---|---|
| 잔액 모델 | 사용자 원화 잔고 `wallet` + append-only 원장 `wallet_ledger` | 실제 거래소 모양. 원장으로 "잔고 = 원장 합"을 언제든 맞춰 볼 수 있다 (P5 대사의 근거) |
| 사용자 식별 | `X-User-Id` 헤더 (영숫자 · 하이픈 1~36자). 없거나 형식이 틀리면 400 | 인증은 범위 밖. 값의 출처만 바뀌면 되게 웹 계층 한 곳에서 꺼낸다 |
| 차감 시점 | **접수 트랜잭션**에서 조건부 UPDATE (A안) | 잔고 부족이면 출금 자체를 받지 않는다(422). 차감 · 출금 기록 · 아웃박스가 함께 되거나 함께 안 된다 |
| 동시성 | `UPDATE wallet SET balance = balance - :amt WHERE user_id = :u AND balance >= :amt` 한 문장 + `CHECK (balance >= 0)` | 별도 락 없이 행 락 한 번으로 원자적. DB 제약이 2차 방어선 |
| 환불 | 실패 확정 4경로를 `TransferStore.fail()` 하나로. 전이 · 환불 · 원장을 한 트랜잭션 | 환불 누락 · 이중 환불 방지. 기존 `transition(…, FAILED)` 는 거부 |
| 보류 | `RECEIVED` · `SENDING` · `UNKNOWN` 은 잔고에서 빠진 채 | 결과를 모르는 돈을 다른 출금에 쓰면 안 된다 |
| `CONFLICT` | 잔고를 건드리지 않고 경고 | 돈과 기록이 어긋난 상태라 자동 보정이 더 위험하다 → 수동 확인 · P5 대사 |
| 멱등키 | UNIQUE 를 **`(user_id, idempotency_key)`** 로 | 전역 UNIQUE 면 다른 사용자가 같은 키 · 같은 내용으로 남의 출금 정보를 200 으로 돌려받는다 |
| 계좌 소유 | 1원인증에 `user_id`. 출금은 **내가 인증한 계좌**만, 확인(confirm)도 본인만 | 남이 인증한 계좌로 출금 · 남의 인증 확인 방지 |
| 조회 | `GET /transfers/{id}` 는 요청자 소유가 아니면 404 | 존재 여부도 숨긴다 |

## 3. 데이터 모델 — `V3__wallet.sql` (V1 · V2 는 고치지 않는다)

```sql
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

- 기존(P3) 행은 `user_id` 가 NULL 이다. 새 컬럼은 NULL 을 허용하고 앱은 새 행에 항상 채운다. NULL 행은 어떤 사용자에게도 보이지 않는다 (조회 · 멱등 조회 · 계좌 확인 모두 `user_id = :u`).
- `uk_transfer_user_idempotency` 에서 NULL 은 서로 다르게 취급된다 — 옛 행끼리는 충돌하지 않는다(이미 전역 UNIQUE 로 들어온 값이라 문제없음).
- 시각은 KST `DATETIME(6)`, 쓰기 전 `toDbPrecision()` (database.md §2).

## 4. 흐름

### 4-1. 접수 (차감)
1. 형식 검증 (기존 + `userId`).
2. `(userId, idempotencyKey)` 로 기존 출금 찾기 → 있으면 지금처럼 같은 내용 200 / 다른 내용 422 (**차감 없음**).
3. `VerifiedAccounts.isVerified(userId, bankCode, accountNo)` → 아니면 422 `ACCOUNT_NOT_VERIFIED`.
4. `TransferStore.saveWithEvent(transfer, event)` 한 트랜잭션 (순서가 중요하다 — 2026-10-26 계획 작성 때 조정):
   - `transfer` INSERT (RECEIVED, user_id) — 먼저 넣어 같은 `(userId, key)` 경합을 UNIQUE 로 먼저 가른다. 차감을 먼저 하면, 잔고가 한 건 몫뿐일 때 진 쪽이 멱등 200 대신 422 를 받는다.
   - 지갑 조건부 UPDATE → 0건이면 롤백(출금 INSERT 도 취소), `StoreConflict.InsufficientBalance` → 422 `INSUFFICIENT_BALANCE` (지갑이 없어도 0건 = 잔고 0).
   - 원장 `WITHDRAW` (amount, balance_after, transfer_id).
   - `outbox_event` INSERT — 마지막이라, 여기서 실패하면 차감까지 되돌아가는 것을 테스트로 증명할 수 있다.
   - `userId` 가 없는 출금은 저장하지 않는다 (차감 없이 돈이 나가는 길을 막는다).
5. 같은 `(userId, key)` 가 동시에 들어와 UNIQUE 충돌 → 트랜잭션 전체 롤백(차감도 취소) → 2번처럼 기존 건 응답 (기존 `DuplicateIdempotencyKey` 경로 그대로).

### 4-2. 실패 확정 (환불)
| 경로 | 전이 | 지금 코드 |
|---|---|---|
| 컨슈머: 은행 거절 | `SENDING → FAILED` | `TransferProcessor.record()` |
| 컨슈머: 정리 작업이 `UNKNOWN` 으로 바꾼 뒤 늦게 온 거절 | `UNKNOWN → FAILED` | `TransferProcessor.record()` |
| 정리 작업 ②: 결과조회 `2001` | `UNKNOWN → FAILED` | `TransferReconciler` |
| 정리 작업 ⓪: 재발행 3회 소진 | `RECEIVED → FAILED (E001)` | `TransferReconciler` |

- 새 포트 `TransferStore.fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String): Boolean` — 한 트랜잭션에서 조건부 전이가 1건이면 `wallet.balance += amount`(`updated_at`) 와 원장 `REFUND` 1줄. 0건이면 아무것도 안 한다.
- 기존 `transition(…, to = FAILED)` 는 `require` 로 거부한다 → 새 실패 경로가 생겨도 환불을 빠뜨릴 수 없다.
- 이중 환불은 조건부 전이 + `uk_ledger_transfer_type` 두 겹으로 막는다.
- user_id 가 NULL 인 옛 출금(P3 시절, 차감한 적 없음)이 실패하면 환불하지 않는다 (원장에 `WITHDRAW` 가 없으면 환불 없음).

### 4-3. 상태별 잔고
| 상태 | 잔고 |
|---|---|
| `RECEIVED` · `SENDING` · `UNKNOWN` | 빠진 채 (보류) |
| `COMPLETED` | 빠진 채 (확정) |
| `FAILED` | 복원 |
| `CONFLICT` 발생 | 손대지 않음 + 경고 (수동 확인, P5) |

### 4-4. 지갑 API
- `POST /api/v1/wallet/deposits {amount}` (헤더 `X-User-Id`): 지갑이 없으면 만들고 `balance += amount` (`INSERT … ON DUPLICATE KEY UPDATE`) + 원장 `DEPOSIT`. 1회 1 ~ 1억 원. **관리 · 테스트용**이라 문서에 표시 (실제는 은행 입금 통지).
- `GET /api/v1/wallet` → `{ userId, balance, pending }`. `pending` = 내 출금 중 `RECEIVED` · `SENDING` · `UNKNOWN` 금액 합. 지갑이 없으면 `balance = 0`.

### 4-5. 1원인증 · 조회의 사용자 범위
- `POST /account-verifications` 는 `user_id` 를 저장, `POST /account-verifications/{id}/confirm` 은 저장된 `user_id` 와 헤더가 다르면 404.
- `GET /transfers/{id}` 는 `transfer.user_id` 가 헤더와 다르면 404.

## 5. 컴포넌트 · 포트 변경

| 위치 | 변경 |
|---|---|
| `modules/transfer` `Transfer` | `userId: String?` (옛 행 NULL) |
| `RequestTransfer` | `userId: String` |
| `TransferStore` | `findByIdempotencyKey(userId, key)`, `findById(id)` 는 그대로 (소유 확인은 서비스), `fail(id, from, at, code): Boolean`, `transition` 은 `FAILED` 거부 |
| `VerifiedAccounts` | `isVerified(userId, bankCode, accountNo)` |
| `StoreConflict` | `InsufficientBalance` |
| `TransferException` | `InsufficientBalance` (422) |
| `TransferService` | `request` 에 userId, `get(userId, id)` 소유 확인 |
| `TransferProcessor` · `TransferReconciler` | `FAILED` 로 가는 곳은 모두 `store.fail(...)` |
| `modules/account` `AccountVerification` | `userId: String?`, 확인 시 소유 검사 |
| `apps/api` `WalletLedger` (신규) | 차감 · 환불 · 충전 SQL 한 곳 — P4-2 에서 락 전략을 바꿔 끼우는 이음새 |
| `apps/api` `JpaTransferStore` | `saveWithEvent` 에 차감, `fail()` |
| `apps/api` 웹 | `X-User-Id` 추출 · 검증 한 곳, `WalletController`, 오류 코드 `INSUFFICIENT_BALANCE` · `USER_ID_REQUIRED` |

## 6. 테스트

| 층 | 증명 테스트 |
|---|---|
| 도메인 | 잔고 부족 → `InsufficientBalance`, 출금 기록 없음 / 실패 4경로가 모두 `fail()` 호출 / `transition(…, FAILED)` 거부 / 남의 출금 `get` → NotFound |
| 저장소 (MySQL) | 차감 + 원장 + 출금 + 아웃박스 한 트랜잭션 (아웃박스 INSERT 실패 → 잔고 그대로) / `fail()` 은 전이 1건일 때만 환불, 두 번 불러도 환불 1번 / `CHECK` 가 음수 UPDATE 거부 / 같은 키 · 다른 사용자는 별개 출금 |
| **동시성 (MySQL)** | 잔고 10만 원, 1만 원 출금 **100건 동시** → 정확히 10건 접수 · 90건 `INSUFFICIENT_BALANCE` · 잔고 0 · 원장 합 = 잔고 / 같은 `(user, key)` 동시 20건 → 차감 1번 |
| API | `X-User-Id` 없음 400 / 남이 인증한 계좌로 출금 422 / 남의 출금 조회 404 / 남의 인증 확인 404 / 충전 → 잔고 · `pending` |
| 흐름 (Kafka + 모의 은행) | 모르는 계좌(은행 거절) → `FAILED` → 잔고 복원, 원장 `WITHDRAW` + `REFUND` / 은행 장애 → 재발행 3회 → `E001` → 잔고 복원 / 정상 → `COMPLETED`, 잔고 빠진 채, `pending` 0 |

동시성 테스트는 고정 스레드 풀 + `CountDownLatch` 로 동시에 출발시킨다. P4-2 의 세 전략에도 같은 테스트로 정확성부터 확인한다.

기존 테스트 · Postman · `http/*.http` 는 `X-User-Id` 헤더와 충전 단계를 더해 고친다.

**손 확인 (머지 전)**: Postman 에서 `X-User-Id: user-1` 로 충전 → 1원인증 → 출금 → `GET /wallet` 잔고 · `pending` → 잔고보다 큰 출금 422 → 모르는 계좌 출금 → `FAILED` 뒤 잔고 복원. (모의 은행이 모르는 계좌는 1원인증을 통과할 수 없으므로, 마지막 단계는 VERIFIED 행을 SQL 로 넣어 준비한다 — http 파일 · Postman 에 쿼리를 둔다.)

## 7. 위험과 한계

- 잔고는 단일 행 핫스팟이다. 같은 사용자의 출금이 몰리면 그 행의 락 대기가 길어진다 → P4-2 에서 측정 · 비교.
- `CONFLICT`(예: 정리 작업이 `FAILED` 로 확정 · 환불한 뒤 은행이 완료라고 답함)는 **환불된 돈이 실제로 나간** 상태일 수 있다. 자동 보정하지 않고 경고만 → P5 대사.
- 충전 API 는 인증 · 멱등키가 없다 (관리 · 테스트용). 운영이라면 은행 입금 통지 + 멱등 처리.
- `X-User-Id` 는 신뢰한다 (인증 범위 밖). 문서에 명시.

## 8. 결정 기록 · 문서

- **ADR-0008** 원화 잔고는 접수 트랜잭션에서 조건부 UPDATE 로 차감하고, 실패 확정 시 같은 트랜잭션에서 환불한다. 원장은 append-only.
- 갱신: `docs/database.md` (V3, ERD, 동시성 표, 운영 쿼리: 잔고 = 원장 합 검증), OPERATION (지갑 API, 422 대응, 잔고 불일치 확인), ARCHITECTURE, README (P4-1), HANDOVER, CHANGELOG, Postman · `http/*.http` (헤더 · 충전 · 지갑 폴더).
