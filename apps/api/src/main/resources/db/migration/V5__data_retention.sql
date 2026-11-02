-- P5-2 보존 배치. 정의와 이유는 docs/database.md §4-1 · §4-9 와 ADR-0011. V1 ~ V4 는 고치지 않는다.
-- 없으면 삭제 묶음마다 표 전체를 훑는다. 아웃박스는 기존 idx_outbox_unpublished (published_at, id) 를 탄다.
ALTER TABLE account_verification
  ADD INDEX idx_verification_purge (status, expires_at);
