# ADR-0007 은행에 못 보낸 출금은 논블로킹 재시도 토픽으로 다시 시도하고, 은행 시도 흔적이 있을 때만 횟수를 제한해 재발행한 뒤 FAILED E001 로 확정한다

- 상태: 채택 (2026-10-26, PR #9 머지)
- 단계: P3-3b · 설계: [P3-3b 설계](../superpowers/specs/2026-10-22-p3-3b-retry-republish-design.md) · [ADR-0005](0005-kafka-delivery-and-consumer-idempotency.md) 결정 8 을 대체 · [ADR-0006](0006-inquire-instead-of-resend.md) · [database.md](../database.md) §4-2

## 맥락
- P3-2 의 블로킹 재시도(1초부터 두 배씩, 최대 10초, 5회)는 같은 파티션의 뒤 메시지를 막는다. 다 쓰면 이체는 `RECEIVED` 로 영영 남는다.
- 미전송(`BankNotSentException`)은 은행에 간 적이 없어 다시 보내도 안전하다. 그래서 자동으로 다시 시도할 수 있다.
- P3-3a 리뷰에서 나온 문제: 연결이 없을 때의 결과조회도 시도 1회로 세어서, 100초가 넘는 은행 점검이면 `UNKNOWN` 이 모두 한도(10회)에 닿아 일마감 대사로 넘어간다.

## 결정
1. **재시도 토픽** (`@RetryableTopic`): 5초 → 30초 → 60초 간격, 시도 4회(원래 1 + 재시도 3, 합계 약 95초). 토픽은 인덱스 접미사 `transfer.requested-retry-0/1/2` 와 `transfer.requested-dlt` 다 — 간격 설정을 바꿔도 이름이 같다. 파티션 수는 원래 토픽과 같은 `TransferTopics.PARTITIONS`(3), 복제 1 이고, 브로커 자동 생성이 꺼져 있어 애너테이션이 앱 기동 때 만든다. 블로킹 `kafkaErrorHandler` 는 지운다. 수동 ack 는 유지하고, 재시도 토픽으로 넘긴 레코드의 오프셋은 프레임워크가 커밋한다(`commitRecovered`, 아래 테스트로 단계마다 증명).
2. **재시도하지 않는 것**: key 가 UUID 가 아니면 리스너가 `InvalidTransferKeyException` 을 던지고 `exclude` 로 바로 DLT 로 간다. `IllegalArgumentException` 을 통째로 빼지 않는다 — `JpaTransferStore.transition` 의 `require` 도 같은 예외를 던진다.
3. **DLT 핸들러**: DLT 로 온 이유(`kafka_exception-cause-fqcn`)로 로그 수준을 가르고 항상 ack 한다. 은행 연결 없음 → WARN(재발행이 이어받음), 잘못된 key → WARN(키는 정리 · 64자로 잘라 한 번만), 그 밖(DB 장애 등) → **ERROR**(자동 재처리 없음, 수동 확인). 한 줄 로그, 계좌번호 · 메시지 값은 남기지 않는다. 이체는 `RECEIVED` 그대로다.
4. **정리 작업 ⓪ 재발행 · 포기** (①, ② 보다 먼저): `status = RECEIVED` 이고 `updated_at` 이 5분 전보다 오래되고 `sending_at` 이 있고 `republished_at` 이 없거나 `sending_at > republished_at` 인 건. `republish_count < 3` 이면 조건부 UPDATE(`republish_count + 1`, `republished_at`) 와 아웃박스 `TransferRequested` INSERT 를 **한 트랜잭션**에 쓴다(UPDATE 가 1건일 때만 INSERT). 3번을 다 썼으면 `RECEIVED → FAILED`(`bank_response_code = E001`, 은행에 간 적 없음)로 확정하고 경고를 남긴다.
5. **결과조회 연결 없음** = `InquiryOutcome.Unavailable`: 정리 작업 ②는 그 주기의 남은 조회를 멈추고 `inquiry_attempts` 를 올리지 않는다.
6. **리스너 그룹 · 동시성은 yml 한 곳**(`spring.kafka.consumer.group-id: transfer-processor`, `spring.kafka.listener.concurrency: 3`). 애너테이션의 `groupId` 는 그 값을 가리키는 placeholder 다. 이렇게 해야 재시도 · DLT 컨테이너 그룹이 `transfer-processor-retry-0 … -dlt` 로 나뉜다(원래 그룹 id 가 비어 있으면 접미사도 붙지 않아 15개 컨슈머가 한 그룹을 같이 쓴다).
7. **재시도 토픽 전용 1스레드 스케줄러**(`RetryTopicSchedulerWrapper`): "아직 때가 안 된" 파티션을 멈췄다 여는 일을 발행기 · 정리 작업과 같은 스레드 2개에 맡기면, 은행이 느려 정리 작업이 오래 걸릴 때 재시도 간격이 설정보다 길어진다.

## 결과
- 증명 테스트:
  - 은행 연결 없음 → 재시도 토픽 3개 → DLT, 원래 · retry-0 · 1 · 2 · DLT 단계마다 그 단계의 그룹 오프셋이 커밋됨 (`RetryTopicFlowTest`, AdminClient 로 확인)
  - 재시도 중인 메시지가 같은 파티션의 뒤 메시지를 막지 않음 — 뒤 메시지의 오프셋 커밋이 앞 메시지의 DLT 레코드 시각보다 먼저임을 순서로 비교 (`RetryTopicFlowTest`; 벽시계 시간 단언은 환경에 따라 흔들려 쓰지 않는다)
  - key 가 UUID 가 아니면 재시도 없이 바로 DLT (`RetryTopicFlowTest`)
  - 은행이 DLT 뒤에 살아나면 정리 작업이 재발행해 `COMPLETED`, 은행 입금은 1건 (`RepublishFlowTest`)
  - 은행이 계속 내려가 있으면 재발행 3회 뒤 `FAILED E001`, 은행 입금 0건 (`RepublishFlowTest`)
  - 정리 작업 ⓪ 의 재발행 · 건너뛰기 · 포기와 결과조회 연결 없음 처리 (`TransferReconcilerTest` 4개)
  - 재발행의 조건부 UPDATE · 아웃박스 한 줄 · 롤백 · 대상 조회 (`JpaTransferStoreTest` 4개)
  - 게이트웨이 연결 없음 → `Unavailable` (`BankInquiryAdapterTest`)
  - 모의 은행을 잠시 내렸다 올리면 같은 포트로 다시 붙음 (`BankGatewayClientTest` pause/resume)
  - 테스트 간격은 0.5초 · 1초 · 2초(합 3.5초)로 줄이고, 흐름 테스트의 재발행 대기는 시계를 앞당기지 않고 `republishAfter = 1ms` 인 정리 작업으로 줄인다. `RetryTopicFlowTest` 가 남기는 `RECEIVED` 이체는 모의 은행에 없는 계좌(1002999999999)라서, 뒤이어 도는 재발행 정리 작업이 `FAILED` 로 끝내고 공용 계좌에 입금이 생기지 않는다.
- 한계:
  - 재시도 메시지는 순서가 바뀐다. 이체당 메시지가 1개라 무해하다.
  - 포기까지 약 26분이 걸린다: 처음 재시도 체인 + 재발행 3번의 체인이 각각 지나고 매번 5분을 기다리므로 (재시도 약 95초 + 대기 5분) × 4 ≈ 26분(정리 작업 주기 최대 10초가 매 회 더해진다). 그보다 긴 은행 점검이면 그 사이 접수된 출금은 `E001` 로 끝난다 (점검 시간대 접수 거절은 P5 후보).
  - 복구 직후 밀린 건이 한꺼번에 은행에 간다. 컨슈머 3개가 상한이다 (P4 뒤 속도 제한을 검토).
  - 재시도 토픽 컨슈머가 5분 넘게 밀리면 재발행과 재시도가 겹칠 수 있다. 은행 호출은 조건부 전이로 한 번뿐이고, 재발행 횟수는 기준대로만 늘어난다.
  - 정리 작업이 여러 인스턴스에서 돌아도 재발행 · 포기는 조건부라 한 번만 반영된다.
  - 앱을 재시작하면 정리 작업 ⓪ 이 바로 돌아, 앱이 5분 넘게 내려가 있던 동안의 시간도 재발행 대기로 친다. 재시도 중이던 건이 재발행 1회를 일찍 쓰거나(이미 3회를 썼으면 바로 `E001`) 한다. 돈은 안전하다 — 은행에 간 적이 없다. 개선안: 기동 후 `republish-after` 동안 ⓪ 을 건너뛴다.
  - 재발행 기준이 두 시계를 비교한다: `republished_at` 은 정리 작업의 시계, `sending_at` 은 컨슈머의 시계. 정리 작업 쪽 시계가 빠르면 `sending_at > republished_at` 이 영영 맞지 않아 경고 없이 `RECEIVED` 로 남을 수 있다. 지금은 단일 인스턴스라 같은 시계다 (다중 인스턴스가 되면 DB 시각으로 통일).
  - DLT 에 온 이유가 은행 연결 없음이 아니면(DB 장애 등) `sending_at` 이 NULL 이라 재발행되지 않는다. DLT 핸들러가 ERROR 로그로 알리고 수동 확인 · 재처리한다 ([OPERATION.md](../../OPERATION.md)). 자동 재처리는 P5 알림 · 대사에서 다룬다.
  - `sending_at` 이 없는 `RECEIVED`(발행 · 소비가 안 된 건)는 재발행 대상이 아니다. Kafka · 컨슈머가 복구되면 밀린 메시지가 처리되므로 횟수를 쓰지 않는다 ([database.md §9](../database.md) 에 확인 쿼리).

## 검토한 다른 선택지
| 선택지 | 버린 이유 |
|---|---|
| 블로킹 재시도 유지 | 재시도 중인 메시지가 같은 파티션의 뒤 메시지를 막는다 |
| DLT 에서 바로 원래 토픽으로 재발행 | Kafka 와 DB 에 이중 쓰기가 되고, 아웃박스를 우회한다 |
| 오래된 `RECEIVED` 를 모두 재발행 | Kafka · 컨슈머 장애에도 횟수를 써서 은행에 간 적 없는 건을 실패로 잘못 확정한다 |
| 무한 재발행 | 사용자가 실패를 확인하지 못하고 계속 기다린다 |
| 간격 1초 · 10초 · 60초 | 첫 간격이 게이트웨이 재접속 백오프(1초 → 2초 …)의 첫 두 번보다 짧아, 연결이 돌아오기 전에 같은 실패를 반복한다 |
| 지연 값 접미사 토픽 이름(`-retry-5000`) | 간격 설정을 바꾸면 토픽 이름이 바뀐다 |
