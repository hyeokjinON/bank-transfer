-- P4-2 부하 측정 뒤 정확성 확인 (부하용 사용자 501명만 — cleanup.sql 과 같은 조건). 앞 세 줄은 0 이어야 한다 — 하나라도 아니면 그 회차 숫자는 버리고 원인을 찾는다.
-- 마지막 줄(transfers)은 접수된 출금 수: load/run.sh 가 k6 의 202 수(워밍업 포함)와 같은지 본다 (409 · 503 이면 아무것도 남지 않아야 한다).
SELECT 'negative_balance', COUNT(*) FROM wallet
 WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$' AND balance < 0;
SELECT 'balance_ne_ledger', COUNT(*)
  FROM wallet w
  LEFT JOIN (SELECT user_id, SUM(CASE type WHEN 'WITHDRAW' THEN -amount ELSE amount END) AS total
               FROM wallet_ledger
              WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$'
              GROUP BY user_id) l ON l.user_id = w.user_id
 WHERE w.user_id LIKE 'load-%' AND w.user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$'
   AND w.balance <> COALESCE(l.total, 0);
SELECT 'transfer_ne_withdraw',
       ABS((SELECT COUNT(*) FROM transfer
             WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$')
         - (SELECT COUNT(*) FROM wallet_ledger
             WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$'
               AND type = 'WITHDRAW'));
SELECT 'transfers', COUNT(*) FROM transfer
 WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$';
