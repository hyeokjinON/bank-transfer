-- P8-2 하루 배치 락 (ShedLock). 정의와 이유는 docs/database.md §4-10 과 ADR-0016. V1 ~ V5 는 고치지 않는다.
-- 일마감 대사 · 보존 배치가 api 여러 대 중 한 대에서만 돌게 한다 (락 이름 = 행 하나). 발행 · 정리 작업은 쓰지 않는다 (이미 행 단위로 안전, 스펙 §2).
-- 모양은 ShedLock 의 MySQL 정의에서 시각 타입만 DATETIME(3) 으로 (이 저장소는 TIMESTAMP 를 쓰지 않는다).
-- 시각은 ShedLock 이 DB 시각 UTC_TIMESTAMP(3) 으로 쓰고 비교한다 (usingDbTime — 서버 시계 차이와 무관). locked_at 에 DEFAULT 는 없다 (ShedLock 이 늘 명시해 쓴다).
-- KST DATETIME 정책의 예외는 "값의 시간대" 하나다 (database.md §2): 타입은 규칙대로 DATETIME 이고, 값은 UTC 다. DATETIME 은 시간대 변환이 없어 ShedLock 이 쓴 UTC 값을 그대로 비교한다.
CREATE TABLE shedlock (
  name       VARCHAR(64)  NOT NULL,
  lock_until DATETIME(3)  NOT NULL,
  locked_at  DATETIME(3)  NOT NULL,
  locked_by  VARCHAR(255) NOT NULL,
  PRIMARY KEY (name)
);
