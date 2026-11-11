-- P8-2 실험 데이터 지우기 (측정 사용자 20명만: load-p82-u001 ~ load-p82-u020). load/compose/run.sh 가 매 회차 앞에 부르고, 끝나면 한 번 더 부른다.
-- 접두사(LIKE)는 인덱스 범위로 후보를 줄이고, REGEXP 가 그 안에서 정확한 20명만 남긴다 (스모크 사용자 load-p82-smoke-* 는 건드리지 않는다).
DELETE o FROM outbox_event o JOIN transfer t ON t.id = o.aggregate_id
 WHERE t.user_id LIKE 'load-p82-%' AND t.user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$';
DELETE FROM wallet_ledger
 WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$';
DELETE FROM transfer
 WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$';
DELETE FROM account_verification
 WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$';
DELETE FROM wallet
 WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$';
