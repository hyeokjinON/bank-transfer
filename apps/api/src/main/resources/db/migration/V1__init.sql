-- P3-1 초기 스키마. 정의와 이유는 docs/database.md. 이미 적용된 이 파일은 고치지 않는다 (변경은 V2__ 로).

CREATE TABLE account_verification (
  id              CHAR(36)     NOT NULL PRIMARY KEY,
  bank_code       VARCHAR(3)   NOT NULL,
  account_no      VARCHAR(16)  NOT NULL,
  holder_name     VARCHAR(20)  NOT NULL,
  code_hash       CHAR(64)     NOT NULL,
  status          VARCHAR(10)  NOT NULL,
  failed_attempts INT          NOT NULL,
  expires_at      DATETIME(6)  NOT NULL,
  verified_at     DATETIME(6)  NULL,
  created_at      DATETIME(6)  NOT NULL,
  INDEX idx_verification_account (bank_code, account_no, status)
);

CREATE TABLE transfer (
  id                 CHAR(36)     NOT NULL PRIMARY KEY,
  idempotency_key    VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  reference          CHAR(20)     CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  request_hash       CHAR(64)     NOT NULL,
  bank_code          VARCHAR(3)   NOT NULL,
  account_no         VARCHAR(16)  NOT NULL,
  amount             BIGINT       NOT NULL,
  memo               VARCHAR(20)  NOT NULL,
  status             VARCHAR(10)  NOT NULL,
  bank_response_code VARCHAR(4)   NULL,
  sending_at         DATETIME(6)  NULL,
  inquiry_attempts   INT          NOT NULL DEFAULT 0,
  created_at         DATETIME(6)  NOT NULL,
  updated_at         DATETIME(6)  NOT NULL,
  version            BIGINT       NOT NULL,
  UNIQUE KEY uk_transfer_idempotency (idempotency_key),
  UNIQUE KEY uk_transfer_reference (reference),
  INDEX idx_transfer_status (status, sending_at)
);

CREATE TABLE outbox_event (
  id            BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
  aggregate_id  CHAR(36)     NOT NULL,
  event_type    VARCHAR(50)  NOT NULL,
  payload       JSON         NOT NULL,
  created_at    DATETIME(6)  NOT NULL,
  published_at  DATETIME(6)  NULL,
  INDEX idx_outbox_unpublished (published_at, id)
);
