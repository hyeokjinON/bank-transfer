-- P9b 분산 추적 (스펙 §4-1, 결정 E6): 행을 쓴 구간(접수 HTTP · 정리 작업 재발행)의 W3C traceparent.
-- 발행기가 행마다 이것을 부모로 outbox.publish 구간을 연다 → Kafka 헤더로 컨슈머 · 게이트웨이까지 같은 trace.
-- 형식 00-<32 hex>-<16 hex>-<2 hex> = 55자. NULL = 구간 없이 쓴 행(V7 이전 행 포함) → 발행 구간은 새 trace. 맨 끝 열 추가라 MySQL 8.4 기본 INSTANT
ALTER TABLE outbox_event ADD COLUMN trace_parent VARCHAR(55) NULL;
