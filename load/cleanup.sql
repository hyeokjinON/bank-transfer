-- P4-2 부하 측정 데이터 지우기 (사용자 id 가 load- 로 시작하는 것만). load/run.sh 가 매 회차 앞에 부르고, 측정이 끝나면 한 번 더 부른다.
-- 경고: 측정 데이터는 RECEIVED 출금 + 미발행 아웃박스 수만 건이다 (발행기 · 컨슈머를 끄고 측정한다).
--       지우지 않고 평소 설정으로 앱을 띄우면 쌓인 아웃박스가 모두 Kafka → 은행으로 나간다.
-- outbox_event.aggregate_id 에는 인덱스가 없어 transfer 와 조인해 지운다 (transfer 는 idx_transfer_user 로 load- 범위를 찾는다).
DELETE o FROM outbox_event o JOIN transfer t ON t.id = o.aggregate_id WHERE t.user_id LIKE 'load-%';
DELETE FROM wallet_ledger WHERE user_id LIKE 'load-%';
DELETE FROM transfer WHERE user_id LIKE 'load-%';
DELETE FROM account_verification WHERE user_id LIKE 'load-%';
DELETE FROM wallet WHERE user_id LIKE 'load-%';
