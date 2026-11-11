-- P8-2 실험 E3 · E4 시드: 측정 사용자 20명 (load-p82-u001 ~ load-p82-u020) 각각 지갑 10억 원 · 충전 원장 1줄 · VERIFIED 1원인증 1건.
-- 받는 계좌 1002123456789 는 모의 은행에 있는 계좌라 처리되면 COMPLETED 0000 이다 (load/consumer 와 같다). 출금 1원이라 측정 중 422 가 나지 않는다.
-- 시각은 세션 시간대(KST) NOW(6). cleanup.sql 뒤에 부른다 (남아 있으면 PK 충돌로 멈춘다). load/compose/run.sh 가 부른다.
INSERT INTO wallet (user_id, balance, version, created_at, updated_at)
WITH RECURSIVE n (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 20)
SELECT CONCAT('load-p82-u', LPAD(i, 3, '0')), 1000000000, 0, NOW(6), NOW(6) FROM n;

INSERT INTO wallet_ledger (user_id, type, amount, balance_after, transfer_id, created_at)
SELECT user_id, 'DEPOSIT', balance, balance, NULL, NOW(6) FROM wallet
 WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$';

INSERT INTO account_verification
  (id, user_id, bank_code, account_no, holder_name, code_hash, status, failed_attempts, expires_at, verified_at, created_at)
SELECT UUID(), user_id, '090', '1002123456789', 'LOAD', REPEAT('0', 64), 'VERIFIED', 0, NOW(6), NOW(6), NOW(6)
  FROM wallet
 WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$';
