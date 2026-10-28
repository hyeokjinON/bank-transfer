-- P4-2 부하 측정 시드: 부하용 사용자 501명 (load-hot 1명 + load-u001 ~ load-u500) 각각
--   지갑 10억 원 (1원 출금이라 측정 중 422 가 나지 않는다) · 충전 원장 1줄 (잔고 = 원장 합) · VERIFIED 1원인증 1건 (받는 계좌).
-- 받는 계좌 5599000000001 은 모의 은행에 없는 번호다 (혹시 처리돼도 1001 거절 → 환불). 시각은 세션 시간대(KST, 서버 기본) NOW(6).
-- 뒤의 두 INSERT 는 방금 넣은 정확한 501명만 고른다 (cleanup.sql 과 같은 조건) — 접두사(load-%)만 쓰면 진짜 사용자 'load-test' 에도 원장 · 인증이 붙는다.
-- cleanup.sql 뒤에 부른다 (같은 사용자가 남아 있으면 PK 충돌로 멈춘다). load/run.sh 가 부른다.
INSERT INTO wallet (user_id, balance, version, created_at, updated_at)
WITH RECURSIVE n (i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 500)
SELECT IF(i = 0, 'load-hot', CONCAT('load-u', LPAD(i, 3, '0'))), 1000000000, 0, NOW(6), NOW(6) FROM n;

INSERT INTO wallet_ledger (user_id, type, amount, balance_after, transfer_id, created_at)
SELECT user_id, 'DEPOSIT', balance, balance, NULL, NOW(6) FROM wallet
 WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$';

INSERT INTO account_verification
  (id, user_id, bank_code, account_no, holder_name, code_hash, status, failed_attempts, expires_at, verified_at, created_at)
SELECT UUID(), user_id, '090', '5599000000001', 'LOAD', REPEAT('0', 64), 'VERIFIED', 0, NOW(6), NOW(6), NOW(6)
  FROM wallet
 WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$';

SELECT 'seeded_users', COUNT(*) FROM wallet
 WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$';
