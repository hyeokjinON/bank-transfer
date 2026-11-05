-- P6-1 컨슈머 측정 데이터 지우기 (측정 사용자 100명만: load-p61-u001 ~ load-p61-u100). load/consumer/run.sh 가 매 회차 앞에 부르고, 측정이 끝나면 한 번 더 부른다.
-- 사용자 조건: 접두사(LIKE)는 인덱스 범위로 후보를 줄이고, REGEXP 가 그 안에서 정확한 100명만 남긴다 (P4-2 cleanup.sql 과 같은 방식 — 다른 행은 지우지 않는다).
-- outbox_event.aggregate_id 에는 인덱스가 없어 transfer 와 조인해 지운다 (transfer 는 idx_transfer_user 로 범위를 찾는다).
DELETE o FROM outbox_event o JOIN transfer t ON t.id = o.aggregate_id
 WHERE t.user_id LIKE 'load-p61-u%' AND t.user_id REGEXP '^load-p61-u(00[1-9]|0[1-9][0-9]|100)$';
DELETE FROM wallet_ledger
 WHERE user_id LIKE 'load-p61-u%' AND user_id REGEXP '^load-p61-u(00[1-9]|0[1-9][0-9]|100)$';
DELETE FROM transfer
 WHERE user_id LIKE 'load-p61-u%' AND user_id REGEXP '^load-p61-u(00[1-9]|0[1-9][0-9]|100)$';
DELETE FROM account_verification
 WHERE user_id LIKE 'load-p61-u%' AND user_id REGEXP '^load-p61-u(00[1-9]|0[1-9][0-9]|100)$';
DELETE FROM wallet
 WHERE user_id LIKE 'load-p61-u%' AND user_id REGEXP '^load-p61-u(00[1-9]|0[1-9][0-9]|100)$';
