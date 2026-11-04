# ADR-0012 출금 컨슈머의 은행 대기를 suspend 리스너로도 만든다 — 설정으로 고르고, 판정은 한 곳, DB 는 IO 디스패처에서

- 상태: 제안 (2026-11-04)
- 단계: P6-1 · 설계: [P6-1 설계](../superpowers/specs/2026-11-04-p6-1-consumer-coroutines-design.md) · [ADR-0005](0005-kafka-delivery-and-consumer-idempotency.md) (컨슈머 · 조건부 전이) · [ADR-0007](0007-retry-topic-and-bounded-republish.md) (재시도 토픽) · [측정 결과](../perf/p6-1-consumer-coroutines.md)

## 맥락
- 출금 컨슈머는 은행 응답을 `CompletableFuture.get()` 으로 최대 3초 기다린다. 파티션 3 · 컨슈머 스레드 3이라 **동시에 기다릴 수 있는 출금은 3건**이고, 처리량은 대략 `3 ÷ 은행 응답 시간` 에서 막힌다 (ADR-0005 결과 절 "P6 · P7 에서 비교").
- 컨슈머를 늘려도 파티션 수를 넘으면 논다. 파티션을 늘리는 것은 브로커 · 리밸런스 · 운영 비용이 따른다.
- 이 단계는 그 경로 하나만 코루틴 버전으로도 만들어 같은 PC · 같은 도구로 비교한다. 웹(P6-2) · 가상 스레드(P7)는 범위 밖이다.

## 결정
1. **바꾸는 경로는 컨슈머의 은행 이체 호출 하나.** 정리 작업의 결과조회 · 1원인증 · 웹은 블로킹 그대로다. 파티션 3 · 컨슈머 3 · `max-poll-records` 50 · 은행 게이트웨이(단일 연결 · 이벤트 루프 1개)는 바꾸지 않는다.
2. **Spring Kafka 4.1.1 의 `suspend` `@KafkaListener`** (비동기 반환). 은행 호출은 `CompletionStage.await()`. 레코드 단위 ack 와 `@RetryableTopic` 을 그대로 쓴다 — 문서에 명시돼 있지 않아 진짜 브로커로 실험했다 (`SuspendListenerExperimentTest`, 저장소에 남김, 커밋 5ce2494 · 결정적으로 고친 528605d): ① await 뒤에 던진 예외는 재시도 토픽을 거쳐 DLT 로 갔다 — 토픽별 호출은 원래 토픽 · `-retry-0` · `-retry-1` 이 각 1회, DLT 원인 헤더는 원래 예외, exclude 한 예외는 재시도 없이 DLT 로 곧장, 정상 레코드는 1회 호출 뒤 오프셋 커밋 ② 비동기 ack 는 이전 poll 의 오프셋이 모두 커밋될 때까지 컨슈머를 멈춰, 처리 중 레코드가 정확히 150 (컨슈머 3 × 50) 에서 멈췄다 ③ await 뒤 코드는 future 를 완료한 스레드(모의 은행 이벤트 루프)에서 이어졌다 (await 전은 컨슈머 스레드) — Spring 7 의 `CoroutinesUtils` 가 `Dispatchers.Unconfined` 로 부르기 때문이다.
3. **설정으로 고른다**: `transfer.consumer.mode = blocking`(기본) | `coroutine`. 두 리스너는 `@ConditionalOnProperty` 로 하나만 빈이 된다. 값은 소문자 그대로만 받고 그 밖은 기동 실패 — enum 의 느슨한 바인딩(`co-routine` → `COROUTINE`)과 조건 비교가 어긋나면 리스너 없이 조용히 뜨기 때문이다. 지금 값은 `/actuator/info`.
4. **판정은 한 곳**: `TransferProcessor.process` 를 `begin`(조회 · `RECEIVED → SENDING` 조건부 전이) · `finish`(결과 기록 · CONFLICT 판정)로 나눴다. 블로킹은 셋을 이어 부르고, 코루틴은 `begin` · `finish` 사이의 은행 호출만 suspend 로 한다. 기존 `TransferProcessor` 테스트는 고치지 않고 통과했다. key 읽기 · 결과 마무리(CONFLICT 경고 · `RETRY_LATER` → `RetryLaterException`) · DLT 로그 · 알림도 `TransferRequestedHandling` 하나를 두 리스너가 쓴다. 예외를 "보냈나 / 모르나"로 나누는 규칙도 `classifyBankFailure` 한 함수다.
5. **DB 호출은 `withContext(Dispatchers.IO)`**: 결정 2-③ 대로 은행 응답 뒤 코드는 **Netty 이벤트 루프 스레드**(게이트웨이에 하나뿐)에서 이어진다. 거기서 JDBC 를 기다리면 그동안 모든 은행 응답이 멈춘다. `begin` · `finish` · 결과 마무리(텔레그램 알림도 막는 호출)를 IO 디스패처로 옮긴다. 디스패처는 생성자로 받아 테스트가 이름 붙인 스레드로 바꿔 끼운다.
6. **은행 동시 호출 상한 `Semaphore`** — `transfer.consumer.coroutine.max-in-flight-bank-calls` 기본 64, 1 ~ 1024 밖이면 기동 실패. 처리 중 메시지 수는 결정 2-② 의 poll 묶음 정지로 이미 150 에 묶이지만, 상한은 그와 별개로 **은행에 동시에 보내는 수**를 지킨다 (실제 은행도 기관별 동시 요청을 제한한다). 원래 토픽 · 재시도 토픽 컨테이너가 같은 리스너 빈 = 같은 세마포어를 쓴다.
   - **퍼밋은 `begin` 보다 먼저 받는다** (설계 초안과 다르게 고친 점, 커밋 02e1b51): `bankCalls.withPermit { withContext(db) { begin }; bank.transfer }`, `finish` · 결과 마무리는 퍼밋 밖의 `withContext(db)`. 초안은 `begin` 을 퍼밋 앞에 뒀는데, 그러면 `begin` 이 `SENDING` 으로 바꾼 행이 퍼밋을 기다리는 동안 남는다. 30초를 넘기면 정리 작업이 `UNKNOWN` → 결과조회 → `FAILED` + 환불로 확정하고, 그 뒤 퍼밋을 받은 코루틴이 송금해 환불과 입금이 겹친다 (CONFLICT). 퍼밋 안에서 `begin` 하면 `SENDING` 인 시간이 DB 호출 + 은행 호출뿐이라 정리 작업의 30초 기준과 부딪히지 않는다. 대가: 퍼밋 하나가 짧은 DB 호출(`begin`) 한 번도 덮는다.
7. **취소는 분류하지 않고 다시 던진다.** 은행 응답을 기다리는 중 코루틴이 취소되면(앱 종료 등) 보냈는지 모른다. 아무것도 기록하지 않아 출금은 `SENDING` 으로 남고, 정리 작업이 30초 뒤 `UNKNOWN` → 결과조회로 확정한다. `NotSent` 로 분류하면 `RECEIVED` 로 되돌아가 다시 보내져 이중 지급이다. `CancellationException` 이 한 겹 더 싸여 올 때도 `ResultUnknown`(안전한 쪽)으로 분류한다. `await()` 가 취소한 게이트웨이 future 의 대기 표 항목은 응답 시간 초과(3초) 뒤 지워진다 — 대기 표의 원래 future 에 `orTimeout` 이 걸려 있어서다 (게이트웨이는 바꾸지 않고 `BankGatewayClientTest` 로 고정).
8. **알려 두는 차이 — 같은 파티션 안 처리 순서를 지키지 않는다.** 출금은 서로 독립이고 상태 전이가 조건부라 영향이 없다 (같은 출금의 중복 메시지는 `begin` 의 조건부 전이가 한 번만 통과시킨다). 처리 중 앱이 죽으면 ack 하지 않은 레코드는 다시 오고, 이미 `SENDING` 인 건은 `AlreadyHandled` 로 걸러져 은행에 다시 가지 않는다 (블로킹 장애와 같은 길).
9. **측정 방법**: "밀린 N건 비우기" (은행 장애 복구 뒤 쌓인 출금을 처리하는 상황) — 발행을 먼저 끝내고 처리만 따로 띄워, 처리량을 DB 시각(N ÷ (마지막 `updated_at` − 첫 `sending_at`))으로 잰다. API 기동 시간이 섞이지 않는다. 측정 전용 컨슈머 그룹 · 별도 은행 거래 기록 · 정확성 확인(N건 모두 `COMPLETED`, 거래번호마다 은행 한 번, 잔고 = 원장 합)은 [OPERATION §11](../../OPERATION.md).

## 결과
- 좋아진 것: 은행이 느릴 때 동시에 기다리는 출금 수가 컨슈머 스레드 수에 묶이지 않는다. 판정 · 경고 · 분류 규칙은 한 벌이라 두 모드가 갈라지지 않는다. 블로킹 코드는 지우지 않고 기본값이다.
- 대가: 같은 파티션 순서를 지키지 않는다 (결정 8). 스레드 경계를 손으로 지켜야 한다 — `withContext(db)` 를 빠뜨리면 이벤트 루프가 막힌다 (테스트가 지킨다). 퍼밋 안에서 `begin` 하므로 퍼밋 하나가 DB 호출 한 번도 덮는다 (결정 6). 상한을 키우면 다음 한계는 DB(IO 디스패처 64 · Hikari)로 넘어간다. kotlinx-coroutines · reactor-core 가 클래스패스에 들어온다 (WebFlux 는 아니다).
- **변이 검사** (커밋하지 않고 되돌림, 커밋 fb76d95): `withContext(db)` 제거(`finish`) → 스레드 테스트 실패 / 취소를 `NotSent` 로 분류 → 어댑터 취소 테스트 실패 / `Semaphore`(`withPermit`) 제거 → 상한 테스트(기대 2 · 실제 10)와 퍼밋 대기 중 `RECEIVED` 테스트(기대 `RECEIVED` · 실제 `SENDING`) 실패. 셋 모두 잡혔다. 그때 전체 빌드 테스트는 465개.
- 측정 결과와 운영 기본 판단은 측정 뒤 이 절에 적는다 ([결과 문서](../perf/p6-1-consumer-coroutines.md)). 이 문장은 측정(Task 10)을 마친 뒤 실제 수치와 판단으로 바뀐다.

## 검토한 대안
- **파티션 · 컨슈머를 늘린 블로킹**: 가장 단순하지만 브로커 · 리밸런스 비용과 "파티션 수 = 동시성 상한" 이 그대로다. 이 단계의 비교 대상이 아니다 (범위 밖).
- **WebFlux · Reactor Kafka 컨슈머**: 컨슈머 전체를 리액티브로 다시 써야 하고 JPA · 블로킹 판정을 그대로 쓰기 어렵다. 웹 쪽 비교는 P6-2 (별도 앱 모듈).
- **가상 스레드**: 블로킹 코드를 그대로 두고 스레드 비용만 줄인다 — P7 에서 같은 측정 도구로 비교한다.
- **블로킹 리스너에서 스레드 풀로 넘기기 / `runBlocking`**: 오프셋 커밋 순서 · 재시도 토픽을 직접 관리해야 한다. 프레임워크의 비동기 반환 지원을 쓰는 쪽이 덜 위험하다.
- **`CompletableFuture` 반환 리스너**: 코루틴 없이도 같은 비동기 반환 경로를 쓸 수 있지만, DB 호출 위치 · 상한 · 취소를 `thenApplyAsync` 사슬로 써야 해서 판정 코드가 읽기 어려워진다.
- **배치 리스너 + `coroutineScope`**: 동시성은 쉽지만 레코드 단위 재시도 토픽 · DLT 를 직접 만들어야 한다.
