-- P4-2 부하 측정 데이터 지우기 (부하용 사용자 501명만: load-hot, load-u001 ~ load-u500). load/run.sh 가 매 회차 앞에 부르고, 측정이 끝나면 한 번 더 부른다.
-- 사용자 조건: 접두사(load-%)만으로는 진짜 사용자 'load-test' 같은 것까지 지우므로 정확한 집합을 정규식으로 한 번 더 건다.
--   LIKE 'load-%' 는 인덱스 범위 검색으로 후보를 줄이고, REGEXP 가 그 후보 안에서 정확한 501명만 남긴다.
-- 경고: 측정 데이터는 RECEIVED 출금 + 미발행 아웃박스 수만 건이다 (발행기 · 컨슈머를 끄고 측정한다).
--       지우지 않고 평소 설정으로 앱을 띄우면 쌓인 아웃박스가 모두 Kafka → 은행으로 나간다.
-- outbox_event.aggregate_id 에는 인덱스가 없어 transfer 와 조인해 지운다 (transfer 는 idx_transfer_user 로 load- 범위를 찾는다).
DELETE o FROM outbox_event o JOIN transfer t ON t.id = o.aggregate_id
 WHERE t.user_id LIKE 'load-%' AND t.user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$';
DELETE FROM wallet_ledger
 WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$';
DELETE FROM transfer
 WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$';
DELETE FROM account_verification
 WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$';
DELETE FROM wallet
 WHERE user_id LIKE 'load-%' AND user_id REGEXP '^load-(hot|u(00[1-9]|0[1-9][0-9]|[1-4][0-9]{2}|500))$';
