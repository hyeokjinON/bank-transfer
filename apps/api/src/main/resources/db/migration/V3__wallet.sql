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
