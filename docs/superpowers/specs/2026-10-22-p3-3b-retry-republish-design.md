# P3-3b 재시도 토픽 · DLT · 제한 재발행 — 설계

> 상위 설계: [P3 설계 문서](2026-10-19-p3-transfer-design.md) §6. 이 문서는 P3-3b 부분을 자세히 적은 것이다.
> 승인: 2026-10-21 ~ 22 사용자와 섹션별로 합의 (A안, 간격 5s · 30s · 60s, 재발행 5분 · 3회, 포기 `FAILED E001`).

## 1. 목적과 범위

은행에 **보내지도 못한** 출금(`BankNotSentException`, 전문을 쓰기 전에 연결 없음)을 끝까지 책임진다.

- 짧은 끊김은 **재시도 토픽**으로, 원래 토픽의 뒤 메시지를 막지 않고 다시 시도한다.
- 수 분짜리 장애는 정리 작업이 **아웃박스로 다시 발행**한다. 횟수는 제한한다.
- 그래도 안 되면 `FAILED`(`E001`, 미전송)로 확정한다. 은행에 간 적이 없으므로 돈이 움직이지 않았다는 것이 확실하다.
- P3-3a 에서 넘긴 것 중 **결과조회 중 은행 연결 없음은 시도로 세지 않는다**를 함께 넣는다.

**범위 밖** (HANDOVER "P3-3b 로 넘기는 것"에 그대로 남긴다): 늦은 응답 로그의 거래번호, 스케줄러 실패 로그 · `@EnableScheduling` 위치, 게이트웨이 쓰기 실패 테스트, 다중 인스턴스 발행 순서. 은행 정기 점검 시간대 접수 거절은 P5 후보.

## 2. 결정 요약

| 결정 | 내용 | 이유 |
|---|---|---|
| 재시도 방식 | `@RetryableTopic` 논블로킹 재시도. 기존 블로킹 `DefaultErrorHandler` 빈은 지운다 | 블로킹 재시도는 같은 파티션의 뒤 메시지를 막는다 |
| 재시도 간격 | **5s → 30s → 60s** (시도 4회 = 원래 1 + 재시도 3), 합계 약 95초 | 첫 간격 5초 안에 게이트웨이 재접속이 두 번 시도된다(1초 → 2초 간격, 지수 백오프 최대 30초). 점점 늘려 복구 직후 몰림을 줄인다 |
| 재시도하지 않는 것 | key 가 UUID 가 아닌 메시지 → 바로 DLT | 몇 번 해도 같은 결과 (독 메시지) |
| DLT | 이체 id · 예외 · 원래 토픽 · 오프셋을 경고 로그로. 이체는 `RECEIVED` 그대로 | 정리 작업의 재발행이 이어받는다. 알림 채널은 P5 |
| 재발행 위치 | **정리 작업 단계 ⓪** 이 아웃박스에 이벤트를 넣는다 (A안) | 발행 경로를 아웃박스 하나로 유지 → 이중 쓰기 없음 |
| 재발행 기준 | **마지막 재발행 이후 은행에 보내려다 못 보낸 흔적**이 있을 때만 | Kafka · 컨슈머 장애처럼 은행에 시도도 못 한 경우는 횟수를 쓰지 않는다 |
| 재발행 대기 · 한도 | 마지막 변경 후 **5분**, 최대 **3회** | 재시도 체인(약 95초)과 겹치지 않게. 포기까지 약 20분 |
| 포기 | `RECEIVED → FAILED`, `bank_response_code = 'E001'` | 은행에 간 적 없음 → 사용자가 실패를 보고 다시 요청하면 된다 |
| 결과조회 연결 없음 | `InquiryOutcome.Unavailable` → 그 주기의 나머지 조회를 멈추고 횟수를 세지 않는다 | 100초 넘는 점검에 `UNKNOWN` 이 모두 한도에 닿던 문제 (P3-3a 리뷰) |
| 리스너 설정 | 그룹 · 동시성은 **yml 한 곳** (`spring.kafka.consumer.group-id`, `spring.kafka.listener.concurrency: 3`) | 운영에서 바꿀 때 한 곳만 본다 (P3-2 리뷰) |

## 3. 흐름

```
접수 → RECEIVED + outbox_event ─▶ 발행기 ─▶ transfer.requested
                                               │
                                컨슈머: RECEIVED → SENDING → 은행
                                  ├ 보냄 → COMPLETED / FAILED / UNKNOWN (지금과 같음)
                                  └ 연결 없음 → SENDING → RECEIVED (sending_at 은 남는다), RetryLaterException
                                               │
                      transfer.requested-retry-0 (5s) → -retry-1 (30s) → -retry-2 (60s)
                                               │ 그래도 실패
                                      transfer.requested-dlt  (경고 로그, 이체는 RECEIVED)
                                               │ 5분 뒤
                      정리 작업 ⓪: 재발행 (횟수 < 3) → 새 outbox_event → 발행기 → 처음부터
                                   포기 (횟수 = 3)  → FAILED E001
```

은행이 중간에 살아나면 다음 시도에서 보내고 평소처럼 끝난다. 같은 이체 메시지가 여러 번 와도 컨슈머의 `RECEIVED → SENDING` 조건부 UPDATE 가 은행 호출을 한 번으로 막는다 (ADR-0005).

## 4. 재시도 토픽 · DLT

- `TransferRequestedListener` 에 `@RetryableTopic`:
  - 시도 수 · 간격 · 배수 · 최대 간격은 **설정 placeholder** (`transfer.retry.*`). 운영 기본값 5s · ×6 · 최대 60s → 5s, 30s, 60s. 테스트는 0.1초 단위로 줄인다.
  - 토픽 이름: `transfer.requested-retry-0`, `-retry-1`, `-retry-2`, `transfer.requested-dlt` (인덱스 접미사). 파티션 3 · 복제 1 로 코드에서 만든다 (브로커 자동 생성은 꺼져 있다).
  - `exclude`: key 가 UUID 가 아닐 때 나는 예외 → 바로 DLT.
- `@DltHandler`: 경고 로그 (이체 id, 예외 클래스 · 메시지, 원래 토픽 · 파티션 · 오프셋). 계좌번호는 남기지 않는다.
- `kafkaErrorHandler` 빈(블로킹 재시도)을 지운다. 수동 ack(`manual_immediate`) 는 유지한다.
- **위험 (계획에서 테스트로 증명)**: 수동 ack 와 재시도 토픽을 같이 쓸 때, 다음 토픽으로 넘긴 레코드의 오프셋이 커밋되는지. P3-2 의 `commitRecovered` 와 같은 함정이 있는지 Spring Kafka 4.1 동작을 확인한다.

## 5. 정리 작업 단계 ⓪ — 재발행과 포기

`TransferReconciler.reconcile()` 의 순서: **⓪ 재발행 · 포기 → ① 멈춘 SENDING → ② 결과조회**.

**대상** (`TransferStore.findRepublishable(updatedBefore, limit)`):
```sql
status = 'RECEIVED'
AND updated_at < :now - republishAfter          -- 재시도 토픽 · DLT 까지 끝난 뒤
AND sending_at IS NOT NULL                      -- 은행에 보내려 한 적이 있고
AND (republished_at IS NULL OR sending_at > republished_at)   -- 그게 마지막 재발행 이후
```

| 상황 | `sending_at` | 결과 |
|---|---|---|
| 은행 장애 (컨슈머가 시도했다가 못 보냄) | 마지막 재발행 이후 | 재발행 또는 포기 |
| Kafka 장애 (발행이 안 됨) | 없음 · 재발행 이전 | 건너뜀 (복구되면 밀린 이벤트가 나간다) |
| 컨슈머 장애 (발행됐는데 안 읽힘) | 없음 · 재발행 이전 | 건너뜀 (복구되면 밀린 메시지를 읽는다) |

**동작** (대상마다):
- `republishCount < maxRepublish` → `TransferStore.republish(id, expectedCount, at)`:
  한 트랜잭션에서 `UPDATE transfer SET republish_count = republish_count + 1, republished_at = :at, updated_at = :at WHERE id = :id AND status = 'RECEIVED' AND republish_count = :expected` 이 1건이면 `outbox_event` 에 `TransferRequested` 1줄을 넣는다. 0건이면 아무것도 넣지 않는다 (다른 쪽이 먼저 바꿨다).
- `republishCount >= maxRepublish` → `transition(id, RECEIVED, FAILED, at, "E001")` 후 경고 로그 (가려진 `Transfer`).

**상태 전이 추가**: `RECEIVED → FAILED`. 은행에 간 적 없는 건만 이 전이를 탄다 (정리 작업 ⓪ 만 쓴다).

**경합**:
- 재발행과 컨슈머가 겹쳐도 컨슈머는 `RECEIVED → SENDING` 조건부라 은행에는 한 번만 간다.
- 포기 직후 재시도 토픽에 남은 메시지가 오면 `RECEIVED → SENDING` 이 실패 → `ALREADY_HANDLED`.
- 재시도 토픽 컨슈머가 5분 넘게 밀려 재발행과 재시도가 동시에 돌 수 있다. 메시지가 둘이어도 은행 호출은 조건부로 한 번이고, 재발행 횟수는 위 기준으로만 늘어난다.

## 6. 결과조회 중 연결 없음

- `InquiryOutcome.Unavailable` 추가. `BankInquiryAdapter` 는 게이트웨이가 연결 없음(`BankNotSentException`)으로 실패하면 `Unavailable` 을 돌려준다. 그 밖의 분류는 P3-3a 와 같다.
- `TransferReconciler` ②: `Unavailable` 을 받으면 **그 주기의 남은 조회를 멈춘다**. `inquiry_attempts` 를 올리지 않는다. 다음 주기에 다시 시도한다.
- 효과: 은행 점검이 길어도 `UNKNOWN` 이 한도(10회)에 닿지 않는다. 연결이 없을 때 한 주기가 100건 × 타임아웃으로 늘어지지 않는다.

## 7. 스키마 · 설정

`V2__transfer_republish.sql` (V1 은 고치지 않는다):
```sql
ALTER TABLE transfer
  ADD COLUMN republish_count INT         NOT NULL DEFAULT 0,
  ADD COLUMN republished_at  DATETIME(6) NULL;
```
인덱스는 추가하지 않는다. 기존 `idx_transfer_status(status, sending_at)` 가 `status = 'RECEIVED'` 로 범위를 좁힌다.

```yaml
transfer:
  retry:                       # 재시도 토픽 (P3-3b)
    attempts: 4                # 원래 1 + 재시도 3
    delay-ms: 5000
    multiplier: 6.0            # 5s → 30s → 60s(최대)
    max-delay-ms: 60000
  reconcile:
    republish-after: 5m        # 마지막 변경 후 이만큼 지나야 재발행
    max-republish: 3           # 넘으면 FAILED E001
```

## 8. 포트 · 컴포넌트 변경

| 위치 | 변경 |
|---|---|
| `modules/transfer` `TransferStatus` | `RECEIVED → FAILED` 허용 |
| `Transfer` | `republishCount: Int`, `republishedAt: Instant?` |
| `TransferStore` | `findRepublishable(updatedBefore: Instant, limit: Int)`, `republish(id, expectedCount, at): Boolean` |
| `InquiryOutcome` | `Unavailable` |
| `TransferReconciler` | 단계 ⓪, `Unavailable` 처리, `republishAfter` · `maxRepublish`, `ReconcileResult(+republished, +abandoned)` |
| `apps/api` `JpaTransferStore` | 위 두 메서드 (조건부 UPDATE + 아웃박스 INSERT 한 트랜잭션) |
| `BankInquiryAdapter` | 연결 없음 → `Unavailable` |
| `TransferRequestedListener` | `@RetryableTopic` · `@DltHandler`. 그룹은 `groupId = "${spring.kafka.consumer.group-id}"` 로 yml 값을 참조 (비우면 재시도 · DLT 컨테이너가 모두 같은 그룹이 된다), 동시성은 애너테이션에서 제거 |
| `MessagingConfiguration` | `kafkaErrorHandler` 제거, 재시도 설정 프로퍼티 |
| `application.yml` | `transfer.retry.*`, `reconcile.republish-after` · `max-republish`, `listener.concurrency: 3` |

## 9. 테스트

시간 줄이기: 재시도 간격은 설정으로 0.5s · 1s · 2s ("뒤 메시지가 막히지 않음"을 안정적으로 보려면 간격이 조금 있어야 한다). 재발행 대기 5분은 `republishAfter = 1ms` 인 정리 작업을 테스트에서 직접 만들어 대신한다 — 시계를 앞당기면 `republished_at` 이 미래 시각으로 적혀 2 · 3회차의 `sending_at > republished_at` 비교가 깨지기 때문이다. "은행 연결 없음"은 모의 은행에 `pause()` / `resume()`(같은 포트, 기존 연결도 끊음)을 추가해 만든다. 게이트웨이는 1초부터 두 배씩 최대 30초 간격으로 재접속하므로 복구 대기는 넉넉히 잡는다. `finally` 에서 반드시 `resume()` 한다.

| 층 | 증명 테스트 |
|---|---|
| 도메인 (가짜 저장소 · 시계) | 마지막 재발행 이후 시도 흔적 → 재발행 · 횟수+1 / 흔적 없음 · 재발행 이전 → 건너뜀 / 횟수 3 → `FAILED E001` / `Unavailable` → 남은 조회 멈춤 · 횟수 그대로 |
| 저장소 (MySQL) | 재발행 조건부 UPDATE 와 아웃박스 INSERT 가 한 트랜잭션 (INSERT 실패 시 둘 다 롤백) / 기대 횟수가 다르면 0건 / 대상 쿼리 조건 (5분, `sending_at` vs `republished_at`) |
| 어댑터 | 연결 없는 게이트웨이로 결과조회 → `Unavailable` |
| 흐름 (MySQL + Kafka + 모의 은행) | 은행 내림 → 접수 → 재시도 토픽 → 그동안 같은 파티션의 뒤 메시지는 처리됨 → DLT / 은행 다시 올림 → 정리 작업 재발행 → `COMPLETED`, 입금 1건 / 은행 계속 내림 → 재발행 3회 → `FAILED E001`, 입금 0건 / UUID 아닌 key → 바로 DLT / 넘긴 레코드의 오프셋 커밋 |

**손 확인 (머지 전)**: IntelliJ 에서 Mock Bank 만 Stop → Postman 2-1 접수 → kafka-ui 에서 `transfer.requested-retry-*` → `-dlt` 이동 확인 → Mock Bank Run → (손 확인용 실행 구성은 `republish-after` 30초) 약 1분 뒤 `COMPLETED`, 입금 1건.

## 10. 위험과 한계

- 수동 ack + 재시도 토픽의 오프셋 커밋 (§4) — 계획 첫 태스크에서 테스트로 확인.
- Spring Kafka 4.1 에서 `@RetryableTopic` 의 백오프 지정 방식(애너테이션 이름, 설정 placeholder 지원)이 3.x 와 다를 수 있다 (spring-retry 의존 변경). 계획 작성 때 라이브러리 소스로 확인하고, placeholder 가 안 되면 `RetryTopicConfiguration` 빈으로 코드에서 조립한다.
- 재시도 토픽 메시지는 순서가 바뀐다. 이체당 메시지 1개라 무해.
- 포기까지 약 20분. 은행 정기 점검이 그보다 길면 그 사이 접수된 출금은 `E001` 이 된다 → 점검 시간대 접수 거절은 P5 후보.
- 복구 직후 밀린 건이 한꺼번에 은행에 간다. 컨슈머 3개가 동시 처리 상한이다. 필요하면 P4 부하 테스트 뒤 속도 제한.
- 다중 인스턴스: 재발행 · 포기는 조건부라 한 번만 반영된다 (조회 중복은 P3-3a 와 같음).

## 11. 결정 기록 · 문서

- **ADR-0007** 논블로킹 재시도 토픽 + 은행 시도 흔적 기준의 제한 재발행, 포기는 `FAILED E001`.
- 갱신: P3 설계 문서 §6 (간격 5s · 30s · 60s, 이 문서 링크), `docs/database.md` (V2 컬럼, 재발행 대상 · 포기 건 운영 쿼리, `E001`), OPERATION (재시도 토픽 · DLT 보는 법, 설정 표, 장애 대응), ARCHITECTURE, HANDOVER, CHANGELOG, README, Postman · `http/transfer.http` 손 확인 절.
