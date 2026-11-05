-- P6-1 컨슈머 측정 시드 (설계 §6 ①). load/consumer/run.sh 가 앞에 `SET @n := N;` 을 붙여 cleanup.sql 뒤에 부른다.
-- 출금 N건을 측정 사용자 100명(load-p61-u001 ~ u100)에 고르게 나눈다 (i 번째 → u((i - 1) % 100 + 1)). 출금마다 RECEIVED + 미발행 아웃박스 1행 —
-- 접수 API 를 거친 것과 같은 상태다: 지갑 = 충전 10억 − 출금 합, 원장 = DEPOSIT 1줄 + 출금마다 WITHDRAW 1줄 (잔고 = 원장 합).
-- 받는 계좌 1002123456789 는 모의 은행에 있는 계좌라 처리되면 COMPLETED 0000 이다. 금액 1원. 거래번호 P61 + 17자리 (앱의 T · V 접두와 겹치지 않는다).
-- 시각은 세션 시간대(KST, 서버 기본) NOW(6).
SET SESSION cte_max_recursion_depth = 200000;   -- 기본 1000 — N 이 그보다 크면 재귀 CTE 가 멈춘다

INSERT INTO transfer
  (id, user_id, idempotency_key, reference, request_hash, bank_code, account_no, amount, memo, status,
   bank_response_code, sending_at, inquiry_attempts, created_at, updated_at, version, republish_count, republished_at)
WITH RECURSIVE n (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < @n)
SELECT UUID(), CONCAT('load-p61-u', LPAD((i - 1) % 100 + 1, 3, '0')), CONCAT('p61-', i), CONCAT('P61', LPAD(i, 17, '0')),
       REPEAT('0', 64), '090', '1002123456789', 1, 'p61', 'RECEIVED',
       NULL, NULL, 0, NOW(6), NOW(6), 0, 0, NULL
  FROM n;

-- 앱의 아웃박스와 같은 모양 {"transferId", "occurredAt"(UTC)} — 발행기가 eventId 를 붙인다. 거래번호 순으로 넣어 발행 순서 = i 순서
INSERT INTO outbox_event (aggregate_id, event_type, payload, created_at, published_at)
SELECT id, 'TransferRequested',
       JSON_OBJECT('transferId', id, 'occurredAt', DATE_FORMAT(CONVERT_TZ(created_at, '+09:00', '+00:00'), '%Y-%m-%dT%H:%i:%s.%fZ')),
       NOW(6), NULL
  FROM transfer
 WHERE user_id LIKE 'load-p61-u%' AND user_id REGEXP '^load-p61-u(00[1-9]|0[1-9][0-9]|100)$'
 ORDER BY reference;

INSERT INTO wallet (user_id, balance, version, created_at, updated_at)
SELECT user_id, 1000000000 - SUM(amount), 0, NOW(6), NOW(6)
  FROM transfer
 WHERE user_id LIKE 'load-p61-u%' AND user_id REGEXP '^load-p61-u(00[1-9]|0[1-9][0-9]|100)$'
 GROUP BY user_id;

INSERT INTO wallet_ledger (user_id, type, amount, balance_after, transfer_id, created_at)
SELECT user_id, 'DEPOSIT', 1000000000, 1000000000, NULL, NOW(6)
  FROM wallet
 WHERE user_id LIKE 'load-p61-u%' AND user_id REGEXP '^load-p61-u(00[1-9]|0[1-9][0-9]|100)$';

INSERT INTO wallet_ledger (user_id, type, amount, balance_after, transfer_id, created_at)
SELECT user_id, 'WITHDRAW', amount, 1000000000 - SUM(amount) OVER (PARTITION BY user_id ORDER BY reference), id, NOW(6)
  FROM transfer
 WHERE user_id LIKE 'load-p61-u%' AND user_id REGEXP '^load-p61-u(00[1-9]|0[1-9][0-9]|100)$';

INSERT INTO account_verification
  (id, user_id, bank_code, account_no, holder_name, code_hash, status, failed_attempts, expires_at, verified_at, created_at)
SELECT UUID(), user_id, '090', '1002123456789', 'LOAD', REPEAT('0', 64), 'VERIFIED', 0, NOW(6), NOW(6), NOW(6)
  FROM wallet
 WHERE user_id LIKE 'load-p61-u%' AND user_id REGEXP '^load-p61-u(00[1-9]|0[1-9][0-9]|100)$';

SELECT 'seeded_transfers', COUNT(*) FROM transfer
 WHERE user_id LIKE 'load-p61-u%' AND user_id REGEXP '^load-p61-u(00[1-9]|0[1-9][0-9]|100)$';
