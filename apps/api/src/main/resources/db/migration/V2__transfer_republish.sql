-- P3-3b 제한 재발행. 정의와 이유는 docs/database.md §4-2. V1 은 고치지 않는다.
-- 인덱스는 추가하지 않는다: 재발행 대상 조회는 idx_transfer_status(status, sending_at) 의 status = 'RECEIVED' 로 범위를 좁힌다.
ALTER TABLE transfer
  ADD COLUMN republish_count INT         NOT NULL DEFAULT 0,
  ADD COLUMN republished_at  DATETIME(6) NULL;
