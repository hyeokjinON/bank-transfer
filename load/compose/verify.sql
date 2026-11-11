-- P8-2 E3 · E4 뒤 정확성 확인 (측정 사용자 20명만). 'transfers' 는 run.sh 가 k6 의 접수된 멱등키 수와 맞춰 본다.
-- 은행에 두 번 간 출금이 없는지는 run.sh 가 모의 은행 거래 기록(거래번호 열)으로 따로 본다.
SELECT 'not_completed', COUNT(*) FROM transfer
 WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$' AND status <> 'COMPLETED';
SELECT 'refunds', COUNT(*) FROM wallet_ledger
 WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$' AND type = 'REFUND';
SELECT 'negative_balance', COUNT(*) FROM wallet
 WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$' AND balance < 0;
SELECT 'balance_ne_ledger', COUNT(*)
  FROM wallet w
  LEFT JOIN (SELECT user_id, SUM(CASE type WHEN 'WITHDRAW' THEN -amount ELSE amount END) AS total
               FROM wallet_ledger
              WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$'
              GROUP BY user_id) l ON l.user_id = w.user_id
 WHERE w.user_id LIKE 'load-p82-%' AND w.user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$'
   AND w.balance <> COALESCE(l.total, 0);
SELECT 'transfer_ne_withdraw',
       ABS((SELECT COUNT(*) FROM transfer
             WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$')
         - (SELECT COUNT(*) FROM wallet_ledger
             WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$' AND type = 'WITHDRAW'));
SELECT 'unpublished_outbox', COUNT(*)
  FROM outbox_event o JOIN transfer t ON t.id = o.aggregate_id
 WHERE t.user_id LIKE 'load-p82-%' AND t.user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$' AND o.published_at IS NULL;
SELECT 'transfers', COUNT(*) FROM transfer
 WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$';
