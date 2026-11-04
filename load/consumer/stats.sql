-- P6-1 측정 계산 (DB 시각, 설계 §6 ④). 측정 사용자 load-p61-* 의 출금만 읽는다. load/consumer/run.sh 가 부른다.
-- start_ms · end_ms = 처리 구간 (첫 sending_at ~ 마지막 updated_at, epoch 밀리초). 세션 시간대가 KST(서버 기본)라 UNIX_TIMESTAMP 가 KST 로 읽는다.
-- p50 · p95 = 건별 지연(updated_at − sending_at)의 가장 가까운 순위값 (ceil(n × p) 번째). Kafka 에서 기다린 시간은 들어가지 않는다.
SELECT 'start_ms', ROUND(UNIX_TIMESTAMP(MIN(sending_at)) * 1000) FROM transfer WHERE user_id LIKE 'load-p61-u%';
SELECT 'end_ms', ROUND(UNIX_TIMESTAMP(MAX(updated_at)) * 1000) FROM transfer WHERE user_id LIKE 'load-p61-u%';
WITH l AS (
  SELECT TIMESTAMPDIFF(MICROSECOND, sending_at, updated_at) / 1000 AS ms,
         ROW_NUMBER() OVER (ORDER BY TIMESTAMPDIFF(MICROSECOND, sending_at, updated_at)) AS rn,
         COUNT(*) OVER () AS n
    FROM transfer
   WHERE user_id LIKE 'load-p61-u%' AND status = 'COMPLETED'
)
SELECT 'p50_ms', ROUND(MAX(CASE WHEN rn = CEIL(n * 0.50) THEN ms END), 1) FROM l
UNION ALL
SELECT 'p95_ms', ROUND(MAX(CASE WHEN rn = CEIL(n * 0.95) THEN ms END), 1) FROM l;
