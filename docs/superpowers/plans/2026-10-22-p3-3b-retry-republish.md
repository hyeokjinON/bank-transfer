# P3-3b 재시도 토픽 · DLT · 제한 재발행 — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 은행에 **보내지도 못한** 출금(`BankNotSentException`)을 끝까지 책임진다 — 짧은 끊김은 논블로킹 재시도 토픽으로, 수 분짜리 장애는 정리 작업의 횟수 제한 재발행(아웃박스)으로, 그래도 안 되면 `FAILED E001` 로 확정한다. 결과조회 중 연결 없음은 시도로 세지 않는다.

**Architecture:** `TransferRequestedListener` 에 `@RetryableTopic`(5s → 30s → 60s, 인덱스 접미사 토픽 3개 + DLT)을 달고 블로킹 `kafkaErrorHandler` 를 지운다. 도메인 `TransferReconciler` 에 단계 ⓪ 을 더해 "마지막 재발행 이후 은행에 보내려다 못 보낸 흔적(`sending_at > republished_at`)이 있는 오래된 `RECEIVED`" 만 조건부 UPDATE + 아웃박스 INSERT(한 트랜잭션)로 다시 발행하고, 3회를 다 쓰면 `RECEIVED → FAILED(E001)`. 결과조회 어댑터는 연결 없음을 `InquiryOutcome.Unavailable` 로 돌려주고 정리 작업은 그 주기의 남은 조회를 멈춘다.

**Tech Stack:** Kotlin 2.3.21 · Spring Boot 4.1.1 · Spring Kafka 4.1.1 (`@RetryableTopic` · `@DltHandler` · `org.springframework.kafka.annotation.BackOff`) · Spring Data JPA (`@Query` + `Limit`) · Flyway · MySQL 8.4 · Kafka 4.1 · Netty 4.2 · Testcontainers 2.0 · Awaitility

**Spec:** [docs/superpowers/specs/2026-10-22-p3-3b-retry-republish-design.md](../specs/2026-10-22-p3-3b-retry-republish-design.md) (상위: [P3 설계 문서](../specs/2026-10-19-p3-transfer-design.md) §6)
2026-10-21 ~ 22 사용자 결정: A안(정리 작업이 아웃박스로 재발행), 간격 5s · 30s · 60s, 재발행 5분 · 3회, 포기 `FAILED E001`.

## Global Constraints

- 도메인 모듈(`modules/account`, `modules/transfer`)은 Spring · JPA · Netty · Kafka 의존성 없음. 로그는 콜백(`onGiveUp` · `onAbandon`)으로 `apps/api` 에 넘긴다.
- **재시도는 논블로킹** `@RetryableTopic`. 기존 블로킹 `DefaultErrorHandler` 빈 `kafkaErrorHandler` 는 지운다. 수동 ack(`ack-mode: manual_immediate`)는 유지한다.
- 재시도 간격 **5s → 30s → 60s** (시도 4회 = 원래 1 + 재시도 3, 합계 약 95초). 값은 설정 placeholder `transfer.retry.attempts / delay-ms / multiplier / max-delay-ms` (운영 4 · 5000 · 6.0 · 60000). **테스트는 500ms · ×2.0 · 최대 2000ms (0.5s → 1s → 2s, 합 3.5초)** — 뒤 메시지가 막히지 않는지 볼 시간 여유를 남긴다.
- 토픽 이름: `transfer.requested-retry-0`, `-retry-1`, `-retry-2`, `transfer.requested-dlt` (**인덱스 접미사** `SUFFIX_WITH_INDEX_VALUE` + `MULTIPLE_TOPICS` — 간격 설정을 바꿔도 이름이 같다). 파티션 3 · 복제 1 을 애너테이션(`numPartitions = "3"`, `replicationFactor = "1"`)으로 코드에서 만든다 (브로커 자동 생성은 꺼져 있다).
- 재시도하지 않는 것: key 가 UUID 가 아닌 메시지 → 리스너가 `InvalidTransferKeyException` 을 던지고 `exclude` 로 바로 DLT. (`IllegalArgumentException` 을 통째로 빼지 않는다 — `JpaTransferStore.transition` 의 `require` 도 같은 예외를 던진다.)
- DLT 핸들러: 경고 로그 (key = 이체 id, 예외 클래스 · 메시지, 원래 토픽 · 파티션 · 오프셋). 계좌번호 · 메시지 값은 남기지 않는다. **ack 한다** (수동 ack 라 안 하면 DLT 오프셋이 커밋되지 않는다). 이체는 `RECEIVED` 그대로.
- 리스너 그룹 · 동시성은 **yml 한 곳**: `spring.kafka.consumer.group-id: transfer-processor`, `spring.kafka.listener.concurrency: 3`. 애너테이션의 `concurrency = "3"` 은 지우고, `groupId` 는 `"\${spring.kafka.consumer.group-id}"` 로 yml 값을 가리킨다 (값은 yml 에만 있다; 이렇게 해야 재시도 · DLT 컨테이너 그룹이 `transfer-processor-retry-0 … -dlt` 로 나뉜다 — 아래 라이브러리 확인 4).
- 정리 작업 순서: **⓪ 재발행 · 포기 → ① 멈춘 `SENDING` → ② 결과조회**. ⓪ 대상:
  ```sql
  status = 'RECEIVED' AND updated_at < :now - republishAfter
  AND sending_at IS NOT NULL AND (republished_at IS NULL OR sending_at > republished_at)
  ```
  `republishCount < maxRepublish`(3) → `republish(id, expectedCount, now)`, 아니면 `transition(id, RECEIVED, FAILED, now, "E001")` + `onAbandon`(가려진 `Transfer`). 재발행 대기 `republishAfter` 5분.
- `republish` = 한 트랜잭션에서 `UPDATE transfer SET republish_count = republish_count + 1, republished_at = :at, updated_at = :at, version = version + 1 WHERE id = :id AND status = 'RECEIVED' AND republish_count = :expected` 가 1건일 때만 `outbox_event` 에 `TransferRequested` 1줄 (접수와 같은 `outboxPayload` 모양 `{"transferId","occurredAt"}`). 0건이면 아무것도 넣지 않는다.
- 상태 전이 추가: `RECEIVED → FAILED` (정리 작업 ⓪ 만 쓴다, `bank_response_code = 'E001'` = 은행에 간 적 없음).
- `InquiryOutcome.Unavailable`: `BankInquiryAdapter` 가 게이트웨이의 `BankNotSentException`(연결 없음, `ExecutionException` 의 cause) 을 받으면 돌려준다. 정리 작업 ②는 그 주기의 남은 조회를 멈추고 `inquiry_attempts` 를 올리지 않는다.
- 스키마: `V2__transfer_republish.sql` 로 `republish_count INT NOT NULL DEFAULT 0`, `republished_at DATETIME(6) NULL`. V1 은 고치지 않는다. 인덱스 추가 없음 (`idx_transfer_status(status, sending_at)` 가 `status = 'RECEIVED'` 로 좁힌다).
- DB 시각 KST, 쓰기 전 `toDbPrecision()` (마이크로초 절사).
- 주석 · 커밋 한국어 "왜". 커밋 `type(scope): 요약` + 구현 모델 이름의 `Co-Authored-By:` 줄. push 하지 않는다.
- 테스트: Docker Desktop 필요, Git Bash 에서 `export PATH="/c/Users/user/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"`. 통합 테스트는 `ApiIntegrationTest` 상속(컨텍스트 공유), 정리 작업 · 발행기 스케줄은 꺼 두고 직접 부른다. 재시도 간격은 `ApiIntegrationTest` 의 프로퍼티로 줄인다. 은행 장애는 `withBankDown { }` (모의 은행 `pause()` → `finally` 에서 반드시 `resume()` + 재접속 대기).
- 흐름 테스트의 재발행 대기는 **시계를 앞당기지 않고** `republishAfter = 1ms` 인 정리 작업으로 줄인다. 이유: 앞당긴 시계로 쓴 `republished_at` 은 미래 시각이라, 컨슈머가 실제 시계로 적는 `sending_at` 이 그보다 이르게 되어 2 · 3회차 재발행 기준(`sending_at > republished_at`)이 깨진다.
- **Task 1 을 마치기 전까지 `apps/api` 는 컴파일되지 않는다** (`TransferStore` 메서드 · `ReconcileResult` 필드 추가). Task 1 은 `:modules:transfer:test` 만, Task 2 부터 전체 빌드.
- 범위 밖 (HANDOVER 에 남긴다): 늦은 응답 로그의 거래번호, 스케줄러 실패 로그 · `@EnableScheduling` 위치, 게이트웨이 쓰기 실패 테스트, 다중 인스턴스 발행 순서, 은행 점검 시간대 접수 거절(P5 후보).

**라이브러리 · 코드 확인 결과 (계획 작성 때 jar · 소스로 확인함 — 구현자는 다시 찾지 않아도 된다)**
1. spring-kafka **4.1.1** (`~/.gradle/caches/modules-2/files-2.1/org.springframework.kafka/spring-kafka/4.1.1/…/spring-kafka-4.1.1.jar`, `javap`).
   - `@RetryableTopic` 속성은 모두 String: `attempts`(기본 "3"), `backOff: org.springframework.kafka.annotation.BackOff`, `numPartitions`(기본 "1"), `replicationFactor`(기본 "-1"), `autoCreateTopics`(기본 "true"), `include` / `exclude: Class<out Throwable>[]`, `includeNames` / `excludeNames`, `traversingCauses`, `retryTopicSuffix`("-retry"), `dltTopicSuffix`("-dlt"), `topicSuffixingStrategy`(기본 **`SUFFIX_WITH_DELAY_VALUE`** → 인덱스로 바꿔야 함), `sameIntervalTopicReuseStrategy`(기본 `SINGLE_TOPIC`), `dltStrategy`(기본 `ALWAYS_RETRY_ON_ERROR`), `concurrency`, `kafkaTemplate`(비우면 KafkaTemplate 빈 하나를 찾는다).
   - 백오프는 spring-retry `@Backoff` 가 아니라 **spring-kafka 자체 `@BackOff`** (`delay`/`delayString`, `maxDelay`/`maxDelayString`, `multiplier`/`multiplierString`, `jitter`/`jitterString`). `BackOffFactory` 가 `resolveEmbeddedValue` + SpEL 로 푼다 → **`${…}` placeholder 사용 가능**, 숫자만 쓰면 밀리초. `attempts` · `numPartitions` 도 `resolveExpressionAsInteger` 로 푼다. → `RetryTopicConfiguration` 빈은 필요 없다.
   - `@DltHandler` 는 속성 없는 표시 애너테이션. 같은 클래스의 메서드에 단다 (일반 리스너 메서드처럼 `ConsumerRecord`, `Acknowledgment` 를 받는다).
   - DLT 헤더 이름(`KafkaHeaders`): `ORIGINAL_TOPIC = "kafka_original-topic"`, `ORIGINAL_PARTITION`(int 4바이트), `ORIGINAL_OFFSET`(long 8바이트), `EXCEPTION_FQCN`, `EXCEPTION_CAUSE_FQCN`, `EXCEPTION_MESSAGE`. 재시도 토픽용 `DeadLetterPublishingRecovererFactory` 는 `setAppendOriginalHeaders(false)` → 원래 토픽 헤더는 첫 홉(`transfer.requested`) 값이 남는다.
2. **수동 ack + 재시도 토픽의 오프셋 커밋**: `ListenerContainerFactoryConfigurer.createErrorHandler` 가 재시도 컨테이너마다 `DefaultErrorHandler(DeadLetterPublishingRecoverer)` 를 만들고 **`setCommitRecovered(true)`** + `defaultFalse`(블로킹 재시도 없음 → 바로 다음 토픽으로) 를 건다. `SeekUtils.seekOrRecover` 는 "복구됨 && commitRecovered && AckMode == `MANUAL_IMMEDIATE`" 일 때 `commitSync`(또는 `commitAsync`)로 `offset + 1` 을 커밋한다. `asyncAcks` 가 아니면 `seekAfterError` 는 기본값(true) 그대로다. → P3-2 의 `commitRecovered` 함정은 재시도 토픽 경로에서는 프레임워크가 막는다. Task 3 의 첫 흐름 테스트가 **모든 단계(원래 · retry-0 · 1 · 2 · DLT)의 그룹 오프셋**을 AdminClient 로 확인해 증명한다.
3. 재시도 토픽의 "아직 때가 안 된 파티션" 일시정지 · 재개는 `RetryTopicSchedulerWrapper` 빈이 있으면 그 스케줄러를, 없으면 하나뿐인 `TaskScheduler` 빈(Boot 의 스케줄러, 스레드 2개 — 발행기 · 정리 작업과 공유)을 쓴다 (`RetryTopicConfigurationSupport.kafkaConsumerBackoffManager`). → 전용 1스레드 `RetryTopicSchedulerWrapper` 를 둔다.
4. 재시도 · DLT 엔드포인트의 그룹 id = 원래 엔드포인트 그룹 id + 토픽 접미사 (`SuffixingRetryTopicNamesProvider.getGroupId` → `Suffixer.maybeAddTo`). **원래 그룹 id 가 비어 있으면 접미사도 붙지 않아** 5개 컨테이너(15 컨슈머)가 한 그룹 `transfer-processor` 를 같이 쓴다. → `groupId = "\${spring.kafka.consumer.group-id}"` 로 두어 `transfer-processor`, `transfer-processor-retry-0/1/2`, `transfer-processor-dlt` 로 나눈다.
5. `sending_at` 은 `SENDING → RECEIVED` 에서 남는다: 도메인 `Transfer.moveTo` (`modules/transfer/.../Transfer.kt:51`, `sendingAt = if (next == SENDING) at else sendingAt`), JPA 는 SENDING 으로 갈 때만 `transitionToSending` 이 `sendingAt = :at` 을 쓰고 (`apps/api/.../persistence/TransferPersistence.kt:99-110`) 그 밖의 `transition` JPQL 은 `sendingAt` 을 건드리지 않는다 (같은 파일 `:113-124`). 재처리 때 `RECEIVED → SENDING` 이 `sending_at` 을 새로 써서 "재발행 이후 시도" 가 된다.
6. 게이트웨이 `inquire()` / `transfer()` 는 연결이 없으면 `CompletableFuture.failedFuture(BankNotSentException)` 을 돌려준다 (`BankGatewayClient.kt:137-139`) → `.get()` 은 `ExecutionException(cause = BankNotSentException)`. 재접속은 3초 고정이 아니라 **1초부터 두 배씩, 최대 30초** (`BankGatewayClient.kt:187-193`, 성공하면 0 으로 돌아감). 그래서 은행을 내렸다 올린 테스트는 재접속을 최대 40초 기다린다.
7. `MockBankServer.close()` 는 이벤트 루프까지 내려 다시 띄울 수 없고, 서버 채널만 닫으면 이미 붙은 연결은 살아 있다 → Task 3 에서 `pause()` / `resume()` 을 더한다 (붙은 연결도 끊고, 같은 포트로 다시 연다, `SO_REUSEADDR`).

## 파일 구조

| 파일 | 책임 |
|---|---|
| `modules/transfer/.../Transfer.kt` (수정) | `RECEIVED → FAILED`, `Transfer.republishCount/republishedAt`, `InquiryOutcome.Unavailable`, `TransferStore.findRepublishable/republish` |
| `modules/transfer/.../TransferReconciler.kt` (수정) | 단계 ⓪ 재발행 · 포기, `Unavailable` 이면 조회 멈춤, `ReconcileResult(+republished, +abandoned)` |
| `apps/api/src/main/resources/db/migration/V2__transfer_republish.sql` (신규) | 컬럼 2개 |
| `apps/api/.../persistence/TransferPersistence.kt` (수정) | 엔티티 필드, 대상 쿼리, 조건부 UPDATE + 아웃박스 INSERT |
| `apps/api/.../config/BankInquiryAdapter.kt` (수정) | 연결 없음 → `Unavailable` |
| `apps/mock-bank/.../MockBankServer.kt` (수정) | `pause()` / `resume()` (은행 장애 흉내) |
| `apps/api/.../messaging/{TransferRequestedListener,MessagingConfiguration}.kt` (수정) | `@RetryableTopic` · `@DltHandler`, `kafkaErrorHandler` 삭제, 전용 재시도 스케줄러, 토픽 이름 상수 |
| `apps/api/.../reconcile/ReconcileConfiguration.kt` (수정) | `republishAfter` · `maxRepublish` 설정, 포기 경고 로그 |
| `apps/api/src/main/resources/application.yml` (수정) | `listener.concurrency`, `transfer.retry.*`, `reconcile.republish-after` · `max-republish` |
| `apps/api/src/test/.../support/{ApiIntegrationTest.kt, TopicReader.kt(신규)}` | 재시도 간격 단축, `withBankDown`, 토픽 읽기 · 그룹 오프셋 도우미 |
| `apps/api/src/test/.../messaging/RetryTopicFlowTest.kt` (신규), `reconcile/RepublishFlowTest.kt` (신규) | 흐름 증명 |
| 문서 · `http/transfer.http` · `postman/…/4-retry-republish/` · `.run/4. API (재발행 손 확인).run.xml` | ADR-0007, 운영 · 구조 문서, 손 확인 |

---

### Task 1: 도메인 — 재발행 · 포기 규칙과 결과조회 "연결 없음"

**Files:**
- Modify: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/Transfer.kt`
- Modify: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferReconciler.kt`
- Test: `modules/transfer/src/test/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferReconcilerTest.kt`
- Modify (tests): `TransferStatusTest.kt` (허용 전이 표), `TransferProcessorTest.kt`, `TransferServiceTest.kt` (FakeStore 메서드 2개)

**Interfaces:**
- Produces:
  - `TransferStatus`: `RECEIVED.canMoveTo(FAILED) == true`
  - `Transfer.republishCount: Int = 0`, `Transfer.republishedAt: Instant? = null` (`inquiryAttempts` 다음 필드)
  - `InquiryOutcome.Unavailable` (`data object`)
  - `TransferStore.findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer>`, `TransferStore.republish(id: UUID, expectedCount: Int, at: Instant): Boolean`
  - `data class ReconcileResult(markedUnknown: Int, completed: Int, failed: Int, stillUnknown: Int, gaveUp: Int, republished: Int, abandoned: Int)`
  - `class TransferReconciler(store, bank: BankInquiryPort, clock = Clock.systemUTC(), sendingTimeout = 30s, maxInquiries = 10, batchSize = 100, republishAfter: Duration = Duration.ofMinutes(5), maxRepublish: Int = 3, onGiveUp: (Transfer) -> Unit = {}, onAbandon: (Transfer) -> Unit = {})`
  - 포기 코드 `"E001"` (`TransferReconciler` 의 private 상수 `NOT_SENT`)

- [ ] **Step 1: 실패하는 테스트**

`TransferReconcilerTest.kt`:
1) import 에 `import kotlin.test.assertTrue` 추가.
2) `FakeStore` 에 이벤트 기록과 메서드 2개를 더한다 (클래스 안 `recordInquiryAttempt` 아래):
```kotlin
        val events = mutableListOf<TransferEvent>()

        override fun findRepublishable(updatedBefore: Instant, limit: Int) =
            transfers.values.filter {
                val sendingAt = it.sendingAt
                val republishedAt = it.republishedAt
                it.status == RECEIVED && it.updatedAt.isBefore(updatedBefore) &&
                    sendingAt != null && (republishedAt == null || sendingAt.isAfter(republishedAt))
            }.sortedBy { it.updatedAt }.take(limit)

        override fun republish(id: UUID, expectedCount: Int, at: Instant): Boolean {
            val current = transfers[id] ?: return false
            if (current.status != RECEIVED || current.republishCount != expectedCount) return false
            transfers[id] = current.copy(republishCount = expectedCount + 1, republishedAt = at, updatedAt = at)
            events += TransferEvent.TransferRequested(id, at)
            return true
        }
```
3) 필드 · 도우미를 바꾼다:
```kotlin
    private val gaveUp = mutableListOf<Transfer>()
    private val abandonedTransfers = mutableListOf<Transfer>()
    private fun reconciler(maxInquiries: Int = 10) = TransferReconciler(
        store, inquiry, Clock.fixed(now, ZoneOffset.UTC), Duration.ofSeconds(30), maxInquiries, 100,
        onGiveUp = { gaveUp += it }, onAbandon = { abandonedTransfers += it },
    )

    private var seq = 0
    private fun transfer(
        status: TransferStatus,
        // 실제 UNKNOWN 은 SENDING 을 거쳐 오므로 sendingAt 이 항상 있다. 기본값은 30초 유예가 지난 시각.
        sendingAt: Instant? = if (status == UNKNOWN) now.minusSeconds(60) else null,
        updatedAt: Instant = now,
        republishCount: Int = 0,
        republishedAt: Instant? = null,
    ): Transfer {
        val t = Transfer(
            id = UUID.randomUUID(), idempotencyKey = "k${++seq}", reference = "T26102100000000000%02d".format(seq),
            requestHash = "h", bankCode = "090", accountNo = "1002123456789", amount = 1_000, memo = "m",
            status = status, sendingAt = sendingAt, republishCount = republishCount, republishedAt = republishedAt,
            createdAt = now, updatedAt = updatedAt,
        )
        store.transfers[t.id] = t
        return t
    }
```
4) 테스트 4개를 클래스 끝에 추가:
```kotlin
    @Test
    fun `마지막 재발행 이후 은행에 보내려다 못 보낸 RECEIVED 는 5분 뒤 다시 발행하고 횟수를 센다`() {
        val t = transfer(RECEIVED, sendingAt = now.minusSeconds(400), updatedAt = now.minusSeconds(301))

        val result = reconciler().reconcile()

        assertEquals(1, result.republished)
        val after = store.transfers.getValue(t.id)
        assertEquals(RECEIVED, after.status)
        assertEquals(1, after.republishCount)
        assertEquals(now, after.republishedAt)
        assertEquals(listOf<TransferEvent>(TransferEvent.TransferRequested(t.id, now)), store.events)   // 발행 경로는 아웃박스 하나
    }

    @Test
    fun `은행에 보내려 한 흔적이 없거나 마지막 재발행 이전이거나 5분이 안 됐으면 건너뛴다`() {
        val old = now.minusSeconds(600)
        val neverTried = transfer(RECEIVED, sendingAt = null, updatedAt = old)   // Kafka · 컨슈머 장애: 은행에 시도도 못 했다
        val notTriedSinceRepublish = transfer(
            RECEIVED, sendingAt = now.minusSeconds(900), updatedAt = old, republishCount = 1, republishedAt = old,
        )                                                                            // 재발행했는데 아직 아무도 안 집어 갔다
        val recent = transfer(RECEIVED, sendingAt = now.minusSeconds(60), updatedAt = now.minusSeconds(60))   // 재시도 토픽 · DLT 가 아직 진행 중일 수 있다

        val result = reconciler().reconcile()

        assertEquals(0, result.republished)
        assertEquals(0, result.abandoned)
        assertTrue(store.events.isEmpty())
        listOf(neverTried, notTriedSinceRepublish, recent).forEach { assertEquals(it, store.transfers.getValue(it.id)) }
    }

    @Test
    fun `재발행 한도를 다 쓰고도 못 보냈으면 FAILED E001 로 포기하고 알린다`() {
        val t = transfer(
            RECEIVED, sendingAt = now.minusSeconds(400), updatedAt = now.minusSeconds(301),
            republishCount = 3, republishedAt = now.minusSeconds(700),
        )

        val result = reconciler().reconcile()

        assertEquals(1, result.abandoned)
        assertEquals(0, result.republished)
        assertEquals(FAILED, store.transfers.getValue(t.id).status)
        assertEquals("E001", store.transfers.getValue(t.id).bankResponseCode)   // 은행에 간 적 없음 — 사용자가 다시 요청하면 된다
        assertEquals(listOf(t.id), abandonedTransfers.map { it.id })
        assertTrue(store.events.isEmpty())
    }

    @Test
    fun `결과조회 중 은행 연결이 없으면 이번 주기의 남은 조회를 멈추고 횟수를 세지 않는다`() {
        val first = transfer(UNKNOWN)
        val second = transfer(UNKNOWN)
        inquiry.outcomes[first.reference] = { InquiryOutcome.Unavailable }
        inquiry.outcomes[second.reference] = { InquiryOutcome.Processed }

        val result = reconciler().reconcile()

        assertEquals(1, inquiry.calls)                                       // 연결이 없으면 남은 건도 같다 — 묻지 않는다
        assertEquals(0, result.stillUnknown)
        assertEquals(0, store.transfers.getValue(first.id).inquiryAttempts) // 은행이 답하지 않은 게 아니다 — 긴 점검에도 한도에 닿지 않게
        assertEquals(UNKNOWN, store.transfers.getValue(second.id).status)
    }
```

`TransferStatusTest.kt` 의 `설계 문서 3장의 전이만 허용한다` 의 `allowed` 첫 줄을 바꾼다:
```kotlin
            RECEIVED to SENDING, RECEIVED to FAILED,   // RECEIVED → FAILED: 재발행 한도 뒤 포기 (P3-3b, E001)
```

`TransferProcessorTest.kt` 의 `FakeStore` 에 추가 (`recordInquiryAttempt` 아래):
```kotlin
        override fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer> = emptyList()
        override fun republish(id: UUID, expectedCount: Int, at: Instant) = false
```

`TransferServiceTest.kt` 의 `FakeStore` 에 추가 (`recordInquiryAttempt` 아래):
```kotlin
        override fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer> = throw UnsupportedOperationException()
        override fun republish(id: UUID, expectedCount: Int, at: Instant): Boolean = throw UnsupportedOperationException()
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:transfer:test` → FAIL (컴파일: `Unresolved reference 'republishCount'`, `'Unavailable'`, `'onAbandon'`, `'findRepublishable' overrides nothing` …)

- [ ] **Step 3: 구현**

`Transfer.kt`:
1) `TransferStatus.allowedNext()` 의 `RECEIVED` 줄을 바꾼다:
```kotlin
        // RECEIVED -> FAILED 는 정리 작업이 재발행 한도를 다 쓰고 포기할 때만 쓴다 (은행에 간 적 없음, E001 — P3-3b)
        RECEIVED -> setOf(SENDING, FAILED)
```
2) `data class Transfer` 의 `val inquiryAttempts: Int = 0,` 아래에 추가:
```kotlin
    /** 정리 작업이 아웃박스로 다시 발행한 횟수 (P3-3b). 한도를 다 쓰고도 은행에 못 보내면 FAILED(E001). */
    val republishCount: Int = 0,
    /** 마지막 재발행 시각. 그 뒤에 sendingAt 이 있어야(은행에 보내려다 못 보냈어야) 다음 재발행을 한다. */
    val republishedAt: Instant? = null,
```
3) `InquiryOutcome` 에 추가 (`data object Unknown` 아래):
```kotlin

    /** 은행 연결이 없어 조회 전문을 보내지도 못했다 (P3-3b). 은행이 답하지 않은 게 아니므로 시도로 세지 않는다. */
    data object Unavailable : InquiryOutcome
```
4) `interface TransferStore` 끝(`recordInquiryAttempt` 아래)에 추가:
```kotlin

    /**
     * 재발행 대상 (P3-3b): status = RECEIVED, updatedAt 이 [updatedBefore] 보다 이르고(재시도 토픽 · DLT 가 끝난 뒤),
     * sendingAt 이 있고 마지막 재발행(republishedAt) 이후인 건 — 은행에 보내려다 못 보낸 흔적이 있는 것만 (오래된 순, 최대 [limit]).
     * Kafka · 컨슈머 장애로 시도조차 못 한 건은 고르지 않는다: 복구되면 밀린 메시지가 처리되고, 횟수를 쓰면 실패로 잘못 확정된다.
     */
    fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer>

    /**
     * RECEIVED 이고 republishCount 가 [expectedCount] 일 때만 횟수 +1, republishedAt · updatedAt = [at] 으로 바꾸고
     * 같은 트랜잭션에서 TransferRequested 이벤트를 하나 더 기록한다 (발행 경로는 아웃박스 하나). 바꿨으면 true.
     * 0건(다른 쪽이 먼저 바꿈)이면 이벤트도 남기지 않는다.
     */
    fun republish(id: UUID, expectedCount: Int, at: Instant): Boolean
```

`TransferReconciler.kt` 전체를 다음으로 바꾼다:
```kotlin
package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Clock
import java.time.Duration
import java.time.Instant

/** 한 번 돈 정리 작업의 결과 (로그 · 테스트용). */
data class ReconcileResult(
    val markedUnknown: Int,
    val completed: Int,
    val failed: Int,
    val stillUnknown: Int,
    val gaveUp: Int,
    /** ⓪ 아웃박스로 다시 발행한 건 (P3-3b). */
    val republished: Int,
    /** ⓪ 재발행 한도를 다 쓰고 FAILED(E001) 로 포기한 건 (P3-3b). */
    val abandoned: Int,
)

/**
 * 정리 작업. 은행에 다시 "보내는" 일은 하지 않는다 — 재발행은 아웃박스에 신호를 넣을 뿐이고, 은행 호출은 늘 컨슈머의 조건부 전이 뒤에 한 번이다.
 * ⓪ (P3-3b) 은행에 보내지도 못한 채 [republishAfter] 넘게 RECEIVED 로 남은 건 → 아웃박스로 다시 발행, [maxRepublish] 번을 다 쓰면 FAILED(E001).
 *    대상은 "마지막 재발행 이후 은행에 보내려 한 흔적(sendingAt > republishedAt)" 이 있는 것만이다: Kafka · 컨슈머 장애로 시도조차 못 한 건은
 *    복구되면 밀린 메시지가 처리되므로 횟수를 쓰지 않는다. 재시도 토픽 체인(약 95초)이 끝난 뒤에 보도록 대기는 5분.
 * ① [sendingTimeout] 보다 오래 SENDING 인 건 → UNKNOWN. 컨슈머가 은행 호출 중 또는 결과 기록 전에 죽은 경우다.
 *    대상은 status = SENDING 으로만 고른다: 미전송(NotSent)으로 RECEIVED 에 되돌린 건에도 sendingAt 이 남기 때문이다.
 * ② sending_at 이 [sendingTimeout] 보다 오래된 UNKNOWN 마다 결과조회 (①과 같은 유예). 컨슈머가 3초 타임아웃으로 적은 UNKNOWN 도
 *    마찬가지다 — 은행이 조금 느릴 뿐 아직 처리 중이면 조회가 2001(처리 안 됨)을 돌려줘 FAILED 로 잘못 확정할 수 있기 때문이다.
 *    결과조회: 처리됨 → COMPLETED(0000), 처리 안 됨 → FAILED(은행 코드), 모름 → 시도 횟수 +1,
 *    연결 없음(Unavailable) → 이번 주기의 남은 조회를 멈추고 횟수를 세지 않는다 (긴 은행 점검에도 한도에 닿지 않게).
 *    [maxInquiries] 번 모르면 더 묻지 않고 [onGiveUp] 으로 알린다 → 일마감 대사(P5)에서 맞춘다.
 * 상태 변경은 모두 조건부라, 그 사이 컨슈머가 먼저 바꿨다면 아무것도 바꾸지 않는다.
 */
class TransferReconciler(
    private val store: TransferStore,
    private val bank: BankInquiryPort,
    private val clock: Clock = Clock.systemUTC(),
    private val sendingTimeout: Duration = Duration.ofSeconds(30),
    private val maxInquiries: Int = 10,
    private val batchSize: Int = 100,
    private val republishAfter: Duration = Duration.ofMinutes(5),
    private val maxRepublish: Int = 3,
    private val onGiveUp: (Transfer) -> Unit = {},
    private val onAbandon: (Transfer) -> Unit = {},
) {

    fun reconcile(): ReconcileResult {
        val now = clock.instant()
        val (republished, abandoned) = republishOrAbandon(now)

        val cutoff = now.minus(sendingTimeout)   // ① ② 가 같은 기준 — 방금 UNKNOWN 이 된 건은 이미 cutoff 보다 오래라 같은 주기에 바로 묻는다
        val markedUnknown = store.findSendingBefore(cutoff, batchSize)
            .count { store.transition(it.id, SENDING, UNKNOWN, now) }

        var completed = 0
        var failed = 0
        var stillUnknown = 0
        var gaveUp = 0
        inquiries@ for (transfer in store.findUnknownForInquiry(maxInquiries, cutoff, batchSize)) {
            val outcome = try {
                bank.inquire(transfer.reference)
            } catch (e: Exception) {
                InquiryOutcome.Unknown   // 포트가 분류하지 못한 예외도 "모름" — 다음 주기에 다시 묻는다
            }
            when (outcome) {
                InquiryOutcome.Processed ->
                    if (store.transition(transfer.id, UNKNOWN, COMPLETED, now, BANK_OK)) completed++
                is InquiryOutcome.NotProcessed ->
                    if (store.transition(transfer.id, UNKNOWN, FAILED, now, outcome.responseCode)) failed++
                InquiryOutcome.Unknown ->
                    if (store.recordInquiryAttempt(transfer.id, now)) {
                        val attempts = transfer.inquiryAttempts + 1
                        if (attempts >= maxInquiries) {
                            gaveUp++
                            onGiveUp(transfer.copy(inquiryAttempts = attempts))
                        } else {
                            stillUnknown++
                        }
                    }
                // 연결이 없으면 남은 건도 똑같다: 건마다 기다리지 않고 다음 주기에 다시 묻는다. 시도로 세지 않는다
                InquiryOutcome.Unavailable -> break@inquiries
            }
        }
        return ReconcileResult(markedUnknown, completed, failed, stillUnknown, gaveUp, republished, abandoned)
    }

    /** ⓪ 재발행 · 포기. 둘 다 조건부라 인스턴스가 여럿이어도 한 번만 반영된다. 돌려주는 값 = (재발행 수, 포기 수). */
    private fun republishOrAbandon(now: Instant): Pair<Int, Int> {
        var republished = 0
        var abandoned = 0
        for (transfer in store.findRepublishable(now.minus(republishAfter), batchSize)) {
            if (transfer.republishCount < maxRepublish) {
                if (store.republish(transfer.id, transfer.republishCount, now)) republished++
            } else if (store.transition(transfer.id, RECEIVED, FAILED, now, NOT_SENT)) {
                abandoned++
                onAbandon(transfer.copy(status = FAILED, bankResponseCode = NOT_SENT, updatedAt = now))
            }
        }
        return republished to abandoned
    }

    private companion object {
        const val BANK_OK = "0000"

        /** 은행에 보내지 못한 채 포기 (이 서비스의 자체 코드 — 은행 응답코드가 아니다). 돈이 움직이지 않았다는 뜻이다. */
        const val NOT_SENT = "E001"
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:transfer:test` → PASS (기존 39 + 정리 작업 4 = 43). `apps/api` 는 아직 컴파일되지 않는다 (Task 2 에서 해결).

- [ ] **Step 5: 커밋**

```bash
git add modules/transfer
git commit -m "feat(transfer): 정리 작업 ⓪ — 은행에 못 보낸 RECEIVED 를 제한 재발행, 3회 뒤 FAILED E001, 결과조회 연결 없음은 세지 않음

Co-Authored-By: <작성 모델>"
```

---

### Task 2: 스키마 V2 · JPA 재발행 · 결과조회 어댑터 — 컴파일 복구

**Files:**
- Create: `apps/api/src/main/resources/db/migration/V2__transfer_republish.sql`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/TransferPersistence.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/BankInquiryAdapter.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/reconcile/ReconcileConfiguration.kt` (`NOTHING` 만)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/JpaTransferStoreTest.kt` (4개 추가)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/DatabaseSetupTest.kt` (1개 추가)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/config/BankInquiryAdapterTest.kt` (1개 바꿈)

**Interfaces:**
- Consumes: Task 1 의 `TransferStore.findRepublishable / republish`, `Transfer.republishCount / republishedAt`, `InquiryOutcome.Unavailable`, `ReconcileResult` 7필드
- Produces: `JpaTransferStore.findRepublishable / republish`, `TransferJpaRepository.findRepublishable(status, updatedBefore, limit: Limit)`, `TransferJpaRepository.incrementRepublishCount(id, status, expected, at): Int`, `BankInquiryAdapter` 가 연결 없음에 `InquiryOutcome.Unavailable`

- [ ] **Step 1: 실패하는 테스트**

`DatabaseSetupTest.kt` 끝에 추가:
```kotlin
    @Test
    fun `V2 가 transfer 에 재발행 컬럼을 더했다 (횟수 NOT NULL 기본 0, 시각 NULL)`() {
        val columns = jdbc.queryForList(
            "SELECT column_name, is_nullable, column_default FROM information_schema.columns " +
                "WHERE table_schema = DATABASE() AND table_name = 'transfer' AND column_name IN ('republish_count', 'republished_at') " +
                "ORDER BY column_name",
        )
        assertEquals(listOf("republish_count", "republished_at"), columns.map { it["column_name"] })
        assertEquals("NO", columns[0]["is_nullable"])
        assertEquals("0", columns[0]["column_default"].toString())
        assertEquals("YES", columns[1]["is_nullable"])
    }
```
(`queryForList` 의 행은 `LinkedCaseInsensitiveMap` 이라 MySQL 8 이 열 라벨을 대문자로 돌려줘도 소문자 키로 읽힌다.)

`JpaTransferStoreTest.kt`: import 에 `import java.time.Duration` 추가, 테스트 4개를 클래스 끝에 추가:
```kotlin
    @Test
    fun `재발행 - RECEIVED 이고 기대 횟수가 맞으면 횟수 +1 과 republished_at, 접수와 같은 모양의 이벤트를 아웃박스에 한 줄 더`() {
        val t = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val at = Instant.parse("2026-10-22T01:00:00.654321Z")

        assertTrue(store.republish(t.id, 0, at))

        val after = store.findById(t.id)!!
        assertEquals(TransferStatus.RECEIVED, after.status)
        assertEquals(1, after.republishCount)
        assertEquals(at, after.republishedAt)
        assertEquals(at, after.updatedAt)
        val rows = jdbc.queryForList(
            "SELECT event_type, JSON_UNQUOTE(JSON_EXTRACT(payload, '$.transferId')) AS transfer_id, " +
                "JSON_UNQUOTE(JSON_EXTRACT(payload, '$.occurredAt')) AS occurred_at, published_at " +
                "FROM outbox_event WHERE aggregate_id = ? ORDER BY id",
            t.id.toString(),
        )
        assertEquals(2, rows.size)
        assertEquals("TransferRequested", rows[1]["event_type"])
        assertEquals(t.id.toString(), rows[1]["transfer_id"])
        assertEquals(at.toString(), rows[1]["occurred_at"])
        assertNull(rows[1]["published_at"])   // 발행기가 평소처럼 내보낸다
    }

    @Test
    fun `재발행 - 기대 횟수가 다르거나 RECEIVED 가 아니면 0건이고 아웃박스도 그대로`() {
        val raced = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val sending = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        store.transition(sending.id, TransferStatus.RECEIVED, TransferStatus.SENDING, Instant.now())

        assertFalse(store.republish(raced.id, 1, Instant.now()))     // 다른 인스턴스가 먼저 0 → 1 로 바꾼 상황
        assertFalse(store.republish(sending.id, 0, Instant.now()))   // 그 사이 컨슈머가 집어 갔다

        assertEquals(0, store.findById(raced.id)!!.republishCount)
        assertEquals(1, outbox.countByAggregateId(raced.id.toString()))
        assertEquals(1, outbox.countByAggregateId(sending.id.toString()))
    }

    @Test
    fun `재발행 - 아웃박스 INSERT 가 실패하면 횟수 증가도 롤백된다`() {
        val broken = JpaTransferStore(transfers, outbox, tx, payloadOf = { "not-json" })
        val t = newTransfer().also { store.saveWithEvent(it, requested(it)) }

        assertFails { broken.republish(t.id, 0, Instant.now()) }

        val after = store.findById(t.id)!!
        assertEquals(0, after.republishCount)   // 횟수만 늘고 메시지는 없는 상태가 생기지 않는다
        assertNull(after.republishedAt)
        assertEquals(1, outbox.countByAggregateId(t.id.toString()))
    }

    @Test
    fun `재발행 대상 - 오래된 RECEIVED 중 마지막 재발행 이후 은행에 보내려 한 흔적이 있는 것만`() {
        val now = Instant.now()
        fun saved() = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        fun triedAndReverted(t: Transfer, at: Instant) {
            store.transition(t.id, TransferStatus.RECEIVED, TransferStatus.SENDING, at)
            store.transition(t.id, TransferStatus.SENDING, TransferStatus.RECEIVED, at)   // 미전송 → RECEIVED (sending_at 은 남는다)
        }

        val tried = saved().also { triedAndReverted(it, now.minusSeconds(600)) }
        val fresh = saved().also { triedAndReverted(it, now.minusSeconds(60)) }
        val neverTried = saved()                                                        // updated_at 은 2026-10-19 (오래됨), sending_at 없음
        val republishedNotTried = saved().also {
            triedAndReverted(it, now.minusSeconds(1_200))
            store.republish(it.id, 0, now.minusSeconds(600))
        }
        val republishedThenTried = saved().also {
            triedAndReverted(it, now.minusSeconds(1_800))
            store.republish(it.id, 0, now.minusSeconds(1_200))
            triedAndReverted(it, now.minusSeconds(600))
        }
        val sending = saved().also { store.transition(it.id, TransferStatus.RECEIVED, TransferStatus.SENDING, now.minusSeconds(600)) }

        val found = store.findRepublishable(now.minus(Duration.ofMinutes(5)), 1_000).map { it.id }

        assertTrue(tried.id in found)
        assertTrue(republishedThenTried.id in found)
        assertFalse(fresh.id in found)                 // 5분이 안 됐다 (재시도 토픽 · DLT 진행 중일 수 있다)
        assertFalse(neverTried.id in found)            // 은행에 시도도 못 했다 (Kafka · 컨슈머 장애)
        assertFalse(republishedNotTried.id in found)   // 재발행한 뒤 아직 아무도 안 집어 갔다
        assertFalse(sending.id in found)
    }
```

`BankInquiryAdapterTest.kt` 의 마지막 테스트를 바꾼다:
```kotlin
    @Test
    fun `연결이 없으면 Unavailable - 묻지도 못했으니 시도로 세지 않는다 (예외를 던지지 않는다)`() {
        val closedPort = MockBankServer().start().use { it.boundPort }
        BankGatewayClient(BankGatewayConfig(host = "localhost", port = closedPort)).start().use { client ->
            assertEquals(InquiryOutcome.Unavailable, BankInquiryAdapter(client).inquire("T2610210000000000012"))
        }
    }
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:compileKotlin` → FAIL (`JpaTransferStore` 가 `findRepublishable` · `republish` 를 구현하지 않음, `ReconcileConfiguration` 의 `ReconcileResult(0, 0, 0, 0, 0)` 인자 부족)

- [ ] **Step 3: 구현**

`V2__transfer_republish.sql`:
```sql
-- P3-3b 제한 재발행. 정의와 이유는 docs/database.md §4-2. V1 은 고치지 않는다.
-- 인덱스는 추가하지 않는다: 재발행 대상 조회는 idx_transfer_status(status, sending_at) 의 status = 'RECEIVED' 로 범위를 좁힌다.
ALTER TABLE transfer
  ADD COLUMN republish_count INT         NOT NULL DEFAULT 0,
  ADD COLUMN republished_at  DATETIME(6) NULL;
```

`TransferPersistence.kt`:
1) `TransferEntity` 생성자의 `var inquiryAttempts: Int = 0,` 아래에 추가:
```kotlin
    var republishCount: Int = 0,
    var republishedAt: Instant? = null,
```
2) `toDomain()` 의 `inquiryAttempts = inquiryAttempts,` 뒤에 `republishCount = republishCount, republishedAt = republishedAt,` 를 넣는다. `from()` 의 `inquiryAttempts = t.inquiryAttempts,` 뒤에 `republishCount = t.republishCount, republishedAt = t.republishedAt?.toDbPrecision(),` 를 넣는다.
3) `TransferJpaRepository` 에 추가 (`incrementInquiryAttempts` 아래):
```kotlin
    /**
     * 재발행 대상 (P3-3b). 두 컬럼 비교(sending_at > republished_at)라 메서드 이름 쿼리로는 쓸 수 없다.
     * status 조건이 idx_transfer_status(status, sending_at) 로 범위를 좁힌다.
     */
    @Query(
        "select t from TransferEntity t where t.status = :status and t.updatedAt < :updatedBefore " +
            "and t.sendingAt is not null and (t.republishedAt is null or t.sendingAt > t.republishedAt) order by t.updatedAt",
    )
    fun findRepublishable(
        @Param("status") status: TransferStatus,
        @Param("updatedBefore") updatedBefore: Instant,
        limit: Limit,
    ): List<TransferEntity>

    /** 재발행 횟수 +1 (P3-3b). 상태나 횟수가 기대와 다르면(다른 쪽이 먼저 바꿈) 0 건. */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        "update TransferEntity t set t.republishCount = t.republishCount + 1, t.republishedAt = :at, t.updatedAt = :at, " +
            "t.version = t.version + 1 where t.id = :id and t.status = :status and t.republishCount = :expected",
    )
    fun incrementRepublishCount(
        @Param("id") id: String,
        @Param("status") status: TransferStatus,
        @Param("expected") expected: Int,
        @Param("at") at: Instant,
    ): Int
```
4) `JpaTransferStore`:
- `saveWithEvent` 안의 `outbox.saveAndFlush(OutboxEventEntity(...))` 를 `outbox.saveAndFlush(outboxRow(event))` 로 바꾸고, 클래스 끝에 도우미를 둔다 (접수와 재발행이 같은 모양의 행을 쓰게):
```kotlin
    /** 아웃박스 한 줄. 접수(saveWithEvent)와 재발행(republish)이 같은 모양을 쓰도록 한곳에서 만든다. */
    private fun outboxRow(event: TransferEvent) = OutboxEventEntity(
        aggregateId = event.transferId.toString(),
        eventType = event::class.simpleName!!,
        payload = payloadOf(event),
        createdAt = event.occurredAt.toDbPrecision(),
    )
```
- 메서드 2개 추가 (`recordInquiryAttempt` 아래):
```kotlin
    override fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer> =
        transfers.findRepublishable(TransferStatus.RECEIVED, updatedBefore.toDbPrecision(), Limit.of(limit)).map { it.toDomain() }

    /**
     * 조건부 UPDATE 와 아웃박스 INSERT 를 한 트랜잭션에 쓴다: INSERT 가 실패하면 횟수 증가도 롤백된다
     * (횟수만 늘고 메시지는 없는 상태가 생기면 한도를 헛되이 쓴다). UPDATE 가 0건이면 INSERT 하지 않는다.
     */
    override fun republish(id: UUID, expectedCount: Int, at: Instant): Boolean =
        tx.execute {
            val dbAt = at.toDbPrecision()
            if (transfers.incrementRepublishCount(id.toString(), TransferStatus.RECEIVED, expectedCount, dbAt) != 1) {
                return@execute false
            }
            outbox.saveAndFlush(outboxRow(TransferEvent.TransferRequested(id, dbAt)))
            true
        }!!
```

`BankInquiryAdapter.kt`:
- import 추가: `io.github.hyeokjinon.banktransfer.gateway.BankNotSentException`, `java.util.concurrent.ExecutionException`
- 클래스 KDoc 을 바꾼다:
```kotlin
/**
 * 결과조회 포트를 게이트웨이에 연결한다. 확실한 답(처리됨 0000 · 원거래 없음 2001)만 확정으로 돌려준다.
 * 연결이 없어 보내지도 못했으면 Unavailable — 정리 작업이 그 주기의 남은 조회를 멈추고 횟수를 세지 않는다 (P3-3b).
 * 그 밖(조회 전문 거절, 모르는 처리결과, 시간 초과, 보낸 뒤 끊김 …)은 Unknown — 다음 주기에 다시 묻는다.
 * 예외를 던지지 않는다.
 */
```
- `inquire` 를 바꾸고 도우미를 더한다:
```kotlin
    override fun inquire(reference: String): InquiryOutcome =
        try {
            classify(reference, client.inquire(reference).get())
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            unknown(reference, e.toString())
        } catch (e: ExecutionException) {
            val cause = e.cause ?: e
            if (cause is BankNotSentException) unavailable(reference) else unknown(reference, cause.toString())
        } catch (e: Exception) {
            unknown(reference, (e.cause ?: e).toString())
        }

    private fun unavailable(reference: String): InquiryOutcome {
        log.warn("bank not connected, inquiries paused until next round: reference={}", reference)
        return InquiryOutcome.Unavailable
    }
```

`ReconcileConfiguration.kt`: `val NOTHING = ReconcileResult(0, 0, 0, 0, 0)` → `val NOTHING = ReconcileResult(0, 0, 0, 0, 0, 0, 0)`.

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*JpaTransferStoreTest*' --tests '*DatabaseSetupTest*' --tests '*BankInquiryAdapterTest*'` → PASS. 이어서 `./gradlew build` 전체 PASS (재시도는 아직 P3-2 블로킹 그대로, 정리 작업 빈은 기본값 5분 · 3회로 동작).

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): V2 재발행 컬럼, 조건부 UPDATE 와 아웃박스 INSERT 를 한 트랜잭션에, 결과조회 연결 없음은 Unavailable

Co-Authored-By: <작성 모델>"
```

---

### Task 3: 재시도 토픽 · DLT — 논블로킹 재시도, 오프셋 커밋 증명

**Files:**
- Modify: `apps/mock-bank/src/main/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankServer.kt` (`pause` / `resume`)
- Test: `modules/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayClientTest.kt` (1개 추가)
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/TransferRequestedListener.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/MessagingConfiguration.kt`
- Modify: `apps/api/src/main/resources/application.yml`
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt`
- Create (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/TopicReader.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/RetryTopicFlowTest.kt` (신규)
- Modify (test): `TransferRequestedListenerTest.kt` (2개 추가), `KafkaSetupTest.kt` (1개 추가)

**Interfaces:**
- Consumes: `OutboxRelay.publishPending()`, `TransferStore`, `ProcessResult.RETRY_LATER`
- Produces:
  - `MockBankServer.pause()`, `MockBankServer.resume(): MockBankServer` (같은 포트, 입금 · 처리 기록 유지)
  - `TransferTopics.RETRY_0 / RETRY_1 / RETRY_2 / DLT` (`"transfer.requested-retry-0"` … `"transfer.requested-dlt"`)
  - `class InvalidTransferKeyException(key: String?) : RuntimeException`
  - `TransferRequestedListener.onDlt(record: ConsumerRecord<String, String>, ack: Acknowledgment)`
  - 테스트 도우미: `ApiIntegrationTest.withBankDown(block: () -> T): T`, `ConsumerFactory<String, String>.readAll(topic): List<ConsumerRecord<String, String>>`, `ConsumerFactory<String, String>.awaitRecords(topic, key, count = 1, timeout = 30s): List<ConsumerRecord<String, String>>`, `KafkaAdmin.committedOffset(group, partition: TopicPartition): Long?`, `ConsumerRecord<String, String>.exceptionClassNames(): List<String>`
  - 컨슈머 그룹: `transfer-processor`, `transfer-processor-retry-0/1/2`, `transfer-processor-dlt`

- [ ] **Step 1: 모의 은행 pause/resume — 실패하는 테스트**

`BankGatewayClientTest.kt` 의 마지막 테스트 뒤(`freePort()` 위)에 추가:
```kotlin
    @Test
    fun `모의 은행을 잠시 내렸다 올리면 같은 포트로 다시 붙는다 (은행 장애 시나리오용)`() {
        val bank = MockBankServer().start().closeLater()
        val client = client(bank.boundPort).start()
        waitUntil { client.isConnected }
        val port = bank.boundPort

        bank.pause()
        waitUntil { !client.isConnected }   // 붙어 있던 연결도 끊긴다
        assertIs<BankNotSentException>(client.inquire("T2610220000000000001").awaitFailure())   // 은행에 가지 않았다

        bank.resume()
        waitUntil { client.isConnected }
        assertEquals(port, bank.boundPort)
        assertEquals(ResponseCodes.ORIGINAL_NOT_FOUND, client.inquire("T2610220000000000001").get(5, TimeUnit.SECONDS).result)
    }
```
Run: `./gradlew :modules:bank-gateway:test` → FAIL (컴파일: `Unresolved reference 'pause'`)

- [ ] **Step 2: 모의 은행 pause/resume — 구현**

`MockBankServer.kt`:
- import 추가: `io.netty.channel.ChannelOption`, `io.netty.channel.group.DefaultChannelGroup`, `io.netty.util.concurrent.GlobalEventExecutor`
- 클래스 KDoc 끝에 ` * - pause/resume: 은행 장애 흉내 (새 연결을 받지 않고 붙은 연결도 끊었다가, 같은 포트로 다시 연다). 입금 · 처리 기록은 그대로 (P3-3b)` 추가
- `private lateinit var serverChannel: Channel` 아래 · `boundPort` 를 다음으로 바꾼다:
```kotlin
    private lateinit var serverChannel: Channel
    private val clients = DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

    /** port = 0 이면 OS 가 고른 포트를 기억해 둔다 — resume 이 같은 포트로 다시 열어야 게이트웨이가 그대로 다시 붙는다. */
    @Volatile private var listenPort = port

    private val bootstrap: ServerBootstrap = ServerBootstrap()
        .group(boss, workers)
        .channel(NioServerSocketChannel::class.java)
        // pause 가 끊은 연결이 TIME_WAIT 로 남아도 같은 포트에 다시 bind 할 수 있게 (리눅스 CI)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childHandler(object : ChannelInitializer<SocketChannel>() {
            override fun initChannel(ch: SocketChannel) {
                clients.add(ch)
                ch.pipeline().addBankCodec().addLast(BankHandler())
            }
        })

    val boundPort: Int get() = listenPort
    val pollCount: Int get() = pollCounter.get()

    fun start(): MockBankServer {
        serverChannel = bootstrap.bind(listenPort).sync().channel()
        listenPort = (serverChannel.localAddress() as InetSocketAddress).port
        log.info("mock bank listening on {}", listenPort)
        return this
    }

    /** 은행 장애 흉내: 새 연결을 받지 않고, 붙어 있던 연결도 끊는다. 입금 내역 · 처리한 거래번호는 남긴다. */
    fun pause() {
        if (serverChannel.isOpen) serverChannel.close().syncUninterruptibly()
        clients.close().awaitUninterruptibly(1, TimeUnit.SECONDS)
        log.info("mock bank paused on {}", listenPort)
    }

    /** pause 전과 같은 포트로 다시 연다. 이미 열려 있으면 아무것도 하지 않는다. */
    fun resume(): MockBankServer {
        if (!serverChannel.isOpen) {
            serverChannel = bootstrap.bind(listenPort).sync().channel()
            log.info("mock bank resumed on {}", listenPort)
        }
        return this
    }
```
(기존 `start()` 안의 `ServerBootstrap()…bind(port)` 체인과 기존 `boundPort` · `pollCount` 정의는 위 코드로 대체한다. `close()` 는 그대로.)

Run: `./gradlew :modules:bank-gateway:test` → PASS (기존 10 + 1 = 11).

- [ ] **Step 3: 오프셋 커밋 위험 테스트부터 (실패하는 테스트)**

`ApiIntegrationTest.kt`:
- import 추가: `java.time.Duration`
- `@SpringBootTest` 를 바꾼다:
```kotlin
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "transfer.outbox.relay.enabled=false",
        "transfer.reconcile.enabled=false",
        // 재시도 간격 0.5초 → 1초 → 2초 (운영 5 · 30 · 60초). 뒤 메시지가 막히지 않는지 볼 여유(3.5초)는 남기고 테스트는 빨리 끝나게
        "transfer.retry.delay-ms=500",
        "transfer.retry.multiplier=2.0",
        "transfer.retry.max-delay-ms=2000",
    ],
)
```
- KDoc 끝에 ` * 재시도 토픽 간격은 위 프로퍼티로 줄인다. 은행 장애는 [withBankDown] 으로 만든다.` 추가
- `waitForBankConnection` 을 바꾸고 도우미를 더한다:
```kotlin
    @BeforeEach
    fun waitForBankConnection() = awaitGateway(connected = true, timeout = Duration.ofSeconds(5))

    /**
     * 은행 장애를 만든다: 모의 은행을 내려(붙어 있던 연결도 끊음) [block] 을 돌리고, 끝나면 **반드시** 같은 포트로 다시 올린다.
     * 게이트웨이 재접속은 1초부터 두 배씩(최대 30초) 늘어나므로 다시 붙을 때까지 넉넉히 기다린다 — 다음 테스트가 끊긴 채 시작하지 않게.
     */
    protected fun <T> withBankDown(block: () -> T): T {
        bank.pause()
        awaitGateway(connected = false, timeout = Duration.ofSeconds(5))
        try {
            return block()
        } finally {
            bank.resume()
            awaitGateway(connected = true, timeout = Duration.ofSeconds(40))
        }
    }

    private fun awaitGateway(connected: Boolean, timeout: Duration) {
        val deadline = System.currentTimeMillis() + timeout.toMillis()
        while (gateway.isConnected != connected) {
            check(System.currentTimeMillis() < deadline) { "gateway connected=${gateway.isConnected}, expected $connected within $timeout" }
            Thread.sleep(20)
        }
    }
```

`TopicReader.kt` (신규):
```kotlin
package io.github.hyeokjinon.banktransfer.api.support

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.support.KafkaHeaders
import java.time.Duration
import java.util.UUID
import kotlin.test.assertTrue

/** 토픽에 지금까지 쌓인 레코드를 처음부터 끝까지 읽는다. assign 으로 읽어 그룹에 참여하지 않고 오프셋도 커밋하지 않는다. */
fun ConsumerFactory<String, String>.readAll(topic: String): List<ConsumerRecord<String, String>> =
    createConsumer("topic-reader-${UUID.randomUUID()}", null).use { consumer ->
        val partitions = consumer.partitionsFor(topic, Duration.ofSeconds(5)).map { TopicPartition(topic, it.partition()) }
        consumer.assign(partitions)
        consumer.seekToBeginning(partitions)
        val end = consumer.endOffsets(partitions)
        val records = mutableListOf<ConsumerRecord<String, String>>()
        val deadline = System.currentTimeMillis() + 10_000
        while (partitions.any { consumer.position(it) < end.getValue(it) }) {
            check(System.currentTimeMillis() < deadline) { "could not read $topic to the end" }
            records += consumer.poll(Duration.ofMillis(200))
        }
        records
    }

/** [topic] 에 key 가 [key] 인 레코드가 [count] 개 이상 쌓일 때까지 기다려 돌려준다. */
fun ConsumerFactory<String, String>.awaitRecords(
    topic: String,
    key: String,
    count: Int = 1,
    timeout: Duration = Duration.ofSeconds(30),
): List<ConsumerRecord<String, String>> {
    var found = emptyList<ConsumerRecord<String, String>>()
    await.atMost(timeout).pollInterval(Duration.ofMillis(200)) untilAsserted {
        found = readAll(topic).filter { it.key() == key }
        assertTrue(found.size >= count, "$topic 에 key=$key 가 ${found.size}개 (기다리는 수 $count)")
    }
    return found
}

/** 그룹 [group] 이 [partition] 에 커밋한 오프셋 (= 다음에 읽을 위치). 커밋이 없으면 null. */
fun KafkaAdmin.committedOffset(group: String, partition: TopicPartition): Long? =
    AdminClient.create(configurationProperties).use { admin ->
        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()[partition]?.offset()
    }

/** DLT · 재시도 레코드에 붙은 예외 클래스 이름들 (리스너 예외와 그 원인). */
fun ConsumerRecord<String, String>.exceptionClassNames(): List<String> =
    listOf(KafkaHeaders.EXCEPTION_FQCN, KafkaHeaders.EXCEPTION_CAUSE_FQCN)
        .mapNotNull { headers().lastHeader(it)?.value()?.toString(Charsets.UTF_8) }
```

`RetryTopicFlowTest.kt` (신규) — 이 단계에서는 첫 테스트만 쓴다:
```kotlin
package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.awaitRecords
import io.github.hyeokjinon.banktransfer.api.support.committedOffset
import io.github.hyeokjinon.banktransfer.api.support.exceptionClassNames
import io.github.hyeokjinon.banktransfer.api.support.readAll
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * 은행에 보내지도 못한 메시지의 재시도 토픽 · DLT 흐름 (P3-3b). 재시도 간격은 ApiIntegrationTest 가 0.5 · 1 · 2초로 줄인다.
 * 은행 장애는 모의 은행 pause/resume 으로 만든다 (withBankDown).
 */
class RetryTopicFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var kafka: KafkaTemplate<String, String>
    @Autowired private lateinit var kafkaAdmin: KafkaAdmin
    @Autowired private lateinit var consumerFactory: ConsumerFactory<String, String>

    private val references = RandomReferenceGenerator()

    /** 접수 API 를 거치지 않고 RECEIVED 이체 + 아웃박스 행을 만든다 (Kafka 경로만 본다). */
    private fun receivedTransfer(): Transfer {
        val now = Instant.now()
        val t = Transfer(
            id = UUID.randomUUID(), idempotencyKey = UUID.randomUUID().toString(), reference = references.next(),
            requestHash = "h".repeat(64), bankCode = "090", accountNo = "1002123456789", amount = 1_000, memo = "retry",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        return store.saveWithEvent(t, TransferEvent.TransferRequested(t.id, now))
    }

    @Test
    fun `은행 연결이 없으면 재시도 토픽 3개를 거쳐 DLT 로 가고, 넘긴 레코드마다 그 단계의 오프셋이 커밋된다`() {
        withBankDown {
            val t = receivedTransfer()
            val key = t.id.toString()
            relay.publishPending()

            val dlt = consumerFactory.awaitRecords(TransferTopics.DLT, key).single()
            assertTrue(RetryLaterException::class.java.name in dlt.exceptionClassNames(), dlt.exceptionClassNames().toString())

            // 위험 확인 (설계 §4): manual_immediate 에서 다음 토픽으로 넘긴 레코드의 오프셋을 커밋하지 않으면,
            // 재시작 뒤 같은 메시지가 그 단계에서 다시 시작된다. 단계마다 그룹이 다르다 (원래 그룹 id + 토픽 접미사).
            val stages = listOf(
                TransferTopics.REQUESTED to GROUP,
                TransferTopics.RETRY_0 to "$GROUP-retry-0",
                TransferTopics.RETRY_1 to "$GROUP-retry-1",
                TransferTopics.RETRY_2 to "$GROUP-retry-2",
                TransferTopics.DLT to "$GROUP-dlt",
            )
            for ((topic, group) in stages) {
                val record = consumerFactory.readAll(topic).single { it.key() == key }
                val partition = TopicPartition(topic, record.partition())
                await.atMost(Duration.ofSeconds(15)) untilAsserted {
                    val committed = kafkaAdmin.committedOffset(group, partition)
                    assertTrue(committed != null && committed > record.offset(), "$group 가 $topic@${record.offset()} 를 커밋하지 않았다: $committed")
                }
            }

            val after = store.findById(t.id)!!
            assertEquals(TransferStatus.RECEIVED, after.status)   // DLT 로 가도 이체는 RECEIVED — 정리 작업의 재발행이 이어받는다
            assertNotNull(after.sendingAt)                         // 은행에 보내려 한 흔적 (재발행 기준)
            assertEquals(0, bank.depositsOf(t.accountNo).count { it.reference == t.reference })
        }
    }

    private companion object {
        const val GROUP = "transfer-processor"   // application.yml spring.kafka.consumer.group-id
        const val PARTITIONS = 3
    }
}
```

- [ ] **Step 4: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*RetryTopicFlowTest*'` → FAIL (컴파일: `Unresolved reference 'DLT'`, `'RETRY_0'` — 토픽 상수와 재시도 토픽이 아직 없다)

- [ ] **Step 5: 나머지 실패하는 테스트**

`RetryTopicFlowTest.kt` 에 테스트 2개 추가 (첫 테스트 아래):
```kotlin
    @Test
    fun `재시도 중인 메시지가 같은 파티션의 뒤 메시지를 막지 않는다`() {
        withBankDown {
            val stuck = receivedTransfer()
            val key = stuck.id.toString()
            relay.publishPending()
            // 발행기는 key 로 파티션을 고른다 (기본 파티셔너 = murmur2). 같은 파티션에 뒤 메시지를 바로 붙이려고 미리 계산한다
            val partition = Utils.toPositive(Utils.murmur2(key.toByteArray(Charsets.UTF_8))) % PARTITIONS

            // 뒤 메시지: 없는 이체 id 라 은행 없이 NOT_FOUND 로 끝나고 ack 된다
            val next = kafka.send(ProducerRecord(TransferTopics.REQUESTED, partition, UUID.randomUUID().toString(), "{}"))
                .get(5, TimeUnit.SECONDS).recordMetadata

            await.atMost(Duration.ofSeconds(3)) untilAsserted {
                val committed = kafkaAdmin.committedOffset(GROUP, TopicPartition(TransferTopics.REQUESTED, partition))
                assertTrue(committed != null && committed > next.offset(), "뒤 메시지(offset ${next.offset()})가 아직 처리되지 않았다: $committed")
            }
            // 그때 앞 메시지는 아직 재시도 중이다 (간격 합 0.5 + 1 + 2 = 3.5초). 블로킹 재시도라면 뒤 메시지가 앞 메시지의 포기를 기다린다
            assertTrue(consumerFactory.readAll(TransferTopics.DLT).none { it.key() == key }, "앞 메시지가 벌써 DLT 에 있다")
            assertEquals(partition, consumerFactory.readAll(TransferTopics.REQUESTED).single { it.key() == key }.partition())

            consumerFactory.awaitRecords(TransferTopics.DLT, key)
        }
    }

    @Test
    fun `key 가 UUID 가 아닌 메시지는 재시도 없이 바로 DLT`() {
        val key = "not-a-uuid-" + UUID.randomUUID()
        val sent = kafka.send(TransferTopics.REQUESTED, key, "{}").get(5, TimeUnit.SECONDS).recordMetadata

        val dlt = consumerFactory.awaitRecords(TransferTopics.DLT, key).single()

        assertTrue(InvalidTransferKeyException::class.java.name in dlt.exceptionClassNames(), dlt.exceptionClassNames().toString())
        assertTrue(consumerFactory.readAll(TransferTopics.RETRY_0).none { it.key() == key })   // 재시도 토픽을 거치지 않았다
        await.atMost(Duration.ofSeconds(15)) untilAsserted {
            val committed = kafkaAdmin.committedOffset(GROUP, TopicPartition(TransferTopics.REQUESTED, sent.partition()))
            assertTrue(committed != null && committed > sent.offset())   // 원래 토픽도 막히지 않고 넘어갔다
        }
    }
```

`TransferRequestedListenerTest.kt`: import 추가 `org.springframework.kafka.support.KafkaHeaders`, `java.nio.ByteBuffer`, `kotlin.test.assertFalse`. 테스트 2개 추가:
```kotlin
    @Test
    fun `key 가 UUID 가 아니면 InvalidTransferKeyException - 재시도 없이 DLT 로 보낸다`() {
        val ack = CountingAck()
        var called = false
        val bad = ConsumerRecord(TransferTopics.REQUESTED, 0, 0L, "not-a-uuid", "{}")

        assertFailsWith<InvalidTransferKeyException> {
            TransferRequestedListener { called = true; ProcessResult.COMPLETED }.on(bad, ack)
        }
        assertFalse(called)
        assertEquals(0, ack.count)
    }

    @Test
    fun `DLT 메시지는 경고만 남기고 ack 한다 - 다시 처리하지 않고 이체는 정리 작업이 이어받는다`() {
        val ack = CountingAck()
        var called = false
        val dlt = ConsumerRecord(TransferTopics.DLT, 1, 7L, UUID.randomUUID().toString(), "{}").apply {
            headers().add(KafkaHeaders.ORIGINAL_TOPIC, TransferTopics.REQUESTED.toByteArray())
            headers().add(KafkaHeaders.ORIGINAL_PARTITION, ByteBuffer.allocate(Int.SIZE_BYTES).putInt(1).array())
            headers().add(KafkaHeaders.ORIGINAL_OFFSET, ByteBuffer.allocate(Long.SIZE_BYTES).putLong(42).array())
            headers().add(KafkaHeaders.EXCEPTION_CAUSE_FQCN, RetryLaterException::class.java.name.toByteArray())
        }

        TransferRequestedListener { called = true; ProcessResult.COMPLETED }.onDlt(dlt, ack)

        assertEquals(1, ack.count)
        assertFalse(called)
    }
```

`KafkaSetupTest.kt` 에 추가:
```kotlin
    @Test
    fun `재시도 토픽 3개와 DLT 가 파티션 3개로 만들어진다 (인덱스 접미사)`() {
        val names = listOf(TransferTopics.RETRY_0, TransferTopics.RETRY_1, TransferTopics.RETRY_2, TransferTopics.DLT)

        val descriptions = admin.describeTopics(*names.toTypedArray())

        names.forEach { assertEquals(3, descriptions.getValue(it).partitions().size, it) }   // DLT 로 보낼 때 원래 파티션 번호를 그대로 쓴다
    }
```

- [ ] **Step 6: 구현**

`TransferRequestedListener.kt` 전체를 다음으로 바꾼다:
```kotlin
package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.transfer.ProcessResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.BackOff
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import java.nio.ByteBuffer
import java.util.UUID

/** 은행에 보내지 못해 나중에 다시 처리해야 한다. 재시도 토픽으로 넘어간다 (P3-3b). */
class RetryLaterException(transferId: UUID) : RuntimeException("bank not reachable, transfer $transferId will be retried")

/** key 가 이체 id(UUID)가 아니다. 몇 번 다시 해도 같은 결과라 재시도 없이 DLT 로 보낸다 (@RetryableTopic exclude). */
class InvalidTransferKeyException(key: String?) : RuntimeException("message key is not a transfer id (UUID): $key")

/**
 * transfer.requested 컨슈머. key = 이체 id. 처리가 끝난 뒤에만 오프셋을 커밋한다 (수동 ack):
 * 처리 중에 죽으면 같은 메시지를 다시 받고, 조건부 상태 변경이 중복 처리를 막는다.
 *
 * 은행에 보내지 못한 경우(RETRY_LATER)는 예외를 던져 **재시도 토픽**으로 넘긴다 (P3-3b, 논블로킹):
 * 원래 토픽의 같은 파티션 뒤 메시지는 기다리지 않는다. 간격 5s → 30s → 60s 뒤에도 안 되면 DLT 로 가고,
 * 이체는 RECEIVED 로 남아 정리 작업의 재발행이 이어받는다. 넘긴 레코드의 오프셋은 프레임워크가 커밋한다(commitRecovered).
 * 그룹 · 동시성은 application.yml 한 곳에서 정한다 (groupId 는 그 값을 가리킬 뿐이다 — 재시도 · DLT 그룹이 접미사로 나뉘게).
 */
class TransferRequestedListener(private val process: (UUID) -> ProcessResult) {
    private val log = LoggerFactory.getLogger(javaClass)

    @RetryableTopic(
        attempts = "\${transfer.retry.attempts:4}",
        backOff = BackOff(
            delayString = "\${transfer.retry.delay-ms:5000}",
            multiplierString = "\${transfer.retry.multiplier:6.0}",
            maxDelayString = "\${transfer.retry.max-delay-ms:60000}",
        ),
        // 브로커 자동 생성은 꺼져 있다. DLT 로 보낼 때 원래 파티션 번호를 그대로 쓰므로 원래 토픽과 같은 3개
        numPartitions = "3",
        replicationFactor = "1",
        // -retry-0/1/2: 간격 설정을 바꿔도 토픽 이름이 같다 (기본값은 간격 값을 이름에 붙인다)
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
        exclude = [InvalidTransferKeyException::class],
    )
    @KafkaListener(topics = [TransferTopics.REQUESTED], groupId = "\${spring.kafka.consumer.group-id}")
    fun on(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val transferId = transferIdOf(record.key())
        val result = process(transferId)
        if (result == ProcessResult.RETRY_LATER) throw RetryLaterException(transferId)
        if (result == ProcessResult.CONFLICT) {
            // 재시도로 풀 수 없다: 은행이 확정한 결과가 이미 기록된 최종 상태와 다르다. 오프셋은 넘기고 사람이 확인하게 경고만 남긴다
            log.warn("transfer {} conflict: bank result differs from the recorded final status — needs manual check", transferId)
        }
        log.info("transfer {} processed: {} ({} partition {}, offset {})", transferId, result, record.topic(), record.partition(), record.offset())
        ack.acknowledge()
    }

    /**
     * 재시도를 다 쓴 메시지 · 재시도하지 않는 메시지. 경고만 남기고 ack 한다 (안 하면 DLT 오프셋이 커밋되지 않아 재시작마다 다시 온다).
     * 이체는 RECEIVED 그대로다 — 은행에 보내려 한 흔적이 있으면 5분 뒤 정리 작업이 아웃박스로 다시 발행한다. 알림 채널은 P5.
     * 메시지 값(계좌번호는 없지만)은 남기지 않는다.
     */
    @DltHandler
    fun onDlt(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        log.warn(
            "transfer message moved to DLT, stays RECEIVED until reconcile republishes it: key={} exception={} message={} original={}-{}@{}",
            record.key(),
            record.headerText(KafkaHeaders.EXCEPTION_CAUSE_FQCN) ?: record.headerText(KafkaHeaders.EXCEPTION_FQCN),
            record.headerText(KafkaHeaders.EXCEPTION_MESSAGE),
            record.headerText(KafkaHeaders.ORIGINAL_TOPIC),
            record.headerInt(KafkaHeaders.ORIGINAL_PARTITION),
            record.headerLong(KafkaHeaders.ORIGINAL_OFFSET),
        )
        ack.acknowledge()
    }

    private fun transferIdOf(key: String?): UUID {
        if (key == null) throw InvalidTransferKeyException(null)
        return try {
            UUID.fromString(key)
        } catch (e: IllegalArgumentException) {
            throw InvalidTransferKeyException(key)
        }
    }
}

private fun ConsumerRecord<*, *>.headerText(name: String): String? =
    headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8)

private fun ConsumerRecord<*, *>.headerInt(name: String): Int? =
    headers().lastHeader(name)?.value()?.takeIf { it.size == Int.SIZE_BYTES }?.let { ByteBuffer.wrap(it).int }

private fun ConsumerRecord<*, *>.headerLong(name: String): Long? =
    headers().lastHeader(name)?.value()?.takeIf { it.size == Long.SIZE_BYTES }?.let { ByteBuffer.wrap(it).long }
```

`MessagingConfiguration.kt`:
- import 삭제: `org.springframework.kafka.listener.DefaultErrorHandler`, `org.springframework.kafka.support.ExponentialBackOffWithMaxRetries`
- import 추가: `org.springframework.kafka.retrytopic.RetryTopicSchedulerWrapper`, `org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler`
- `TransferTopics` 를 바꾼다:
```kotlin
object TransferTopics {
    /** 출금 접수 신호. key = 이체 id → 같은 이체의 메시지는 같은 파티션(순서 보장). */
    const val REQUESTED = "transfer.requested"

    // @RetryableTopic 이 만드는 토픽 (인덱스 접미사 — 간격 설정을 바꿔도 이름이 같다). 운영 문서 · 테스트가 이름을 여기서 본다
    const val RETRY_0 = "$REQUESTED-retry-0"
    const val RETRY_1 = "$REQUESTED-retry-1"
    const val RETRY_2 = "$REQUESTED-retry-2"
    const val DLT = "$REQUESTED-dlt"
}
```
- 클래스 KDoc 첫 줄을 ` * Kafka 쪽 조립. 토픽은 코드로 선언한다 (브로커의 자동 생성은 꺼 두었다 — 오타 토픽 방지). 재시도 · DLT 토픽은 리스너의 @RetryableTopic 이 만든다.` 로 바꾼다.
- `kafkaErrorHandler` 빈(KDoc 포함)을 지우고 그 자리에 추가:
```kotlin
    /**
     * 재시도 토픽에서 "아직 때가 안 된" 파티션을 멈췄다가 다시 여는 스케줄러. Spring Kafka 는 이 빈을 TaskScheduler 보다 먼저 찾는다.
     * 따로 두는 이유: 없으면 공용 스케줄러(스레드 2개)를 쓰는데, 발행기와 정리 작업(은행이 느리면 한 주기가 길다)이 둘 다 잡고 있으면
     * 파티션 재개가 늦어져 재시도 간격이 설정보다 길어진다.
     */
    @Bean
    fun retryTopicSchedulerWrapper() = RetryTopicSchedulerWrapper(
        ThreadPoolTaskScheduler().apply {
            poolSize = 1
            setThreadNamePrefix("retry-topic-")
        },
    )
```

`application.yml`:
- `spring.kafka.listener` 를 바꾼다:
```yaml
    listener:
      ack-mode: manual_immediate
      concurrency: 3               # 컨테이너마다 컨슈머 3개 (파티션 3). 재시도 · DLT 컨테이너도 같다. 그룹 · 동시성은 여기서만 정한다
```
- `transfer:` 아래 `outbox:` 앞에 추가:
```yaml
  retry:                       # 재시도 토픽 (P3-3b). 은행에 보내지도 못한 메시지만 온다
    attempts: 4                # 원래 1 + 재시도 3
    delay-ms: 5000             # 첫 간격. 게이트웨이 재접속(1초 → 2초 …)보다 길게
    multiplier: 6.0            # 5s → 30s → 60s(최대)
    max-delay-ms: 60000
```

- [ ] **Step 7: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*RetryTopicFlowTest*' --tests '*TransferRequestedListenerTest*' --tests '*KafkaSetupTest*'` → PASS.
첫 테스트(오프셋 커밋)의 커밋 단언만 실패하면 라이브러리 확인 2 와 다르게 동작하는 것이다 — 우회 코드를 넣지 말고 멈춰서 실패한 단계(`group`/`topic`)와 로그를 보고한다.
이어서 `./gradlew build` 전체 PASS (P3-2 흐름 테스트의 `listConsumerGroupOffsets("transfer-processor")` 도 그대로 통과해야 한다).

- [ ] **Step 8: 커밋**

```bash
git add apps/mock-bank modules/bank-gateway apps/api
git commit -m "feat(api): 재시도 토픽 5s · 30s · 60s 와 DLT — 블로킹 재시도를 걷어내 뒤 메시지를 막지 않음, 넘긴 오프셋 커밋 증명

Co-Authored-By: <작성 모델>"
```

---

### Task 4: 재발행 연결 — 설정 · 흐름 테스트

**Files:**
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/reconcile/ReconcileConfiguration.kt`
- Modify: `apps/api/src/main/resources/application.yml`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/reconcile/RepublishFlowTest.kt` (신규)

**Interfaces:**
- Consumes: `TransferReconciler(… republishAfter, maxRepublish, onGiveUp, onAbandon)` (Task 1), `BankInquiryAdapter` (Task 2), `withBankDown` · `awaitRecords` · `TransferTopics.DLT` (Task 3), `OutboxRelay`
- Produces: `ReconcileProperties(enabled, intervalMs, sendingTimeout, maxInquiries, batchSize, republishAfter: Duration = 5m, maxRepublish: Int = 3)`

- [ ] **Step 1: 실패하는 테스트**

`RepublishFlowTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.reconcile

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.config.BankInquiryAdapter
import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.messaging.TransferTopics
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.awaitRecords
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferReconciler
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.kafka.core.ConsumerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 은행에 보내지 못한 출금의 끝: DLT 뒤 정리 작업이 아웃박스로 다시 발행하고, 3번을 다 쓰면 FAILED E001 (P3-3b).
 * 모의 은행 1002123456789 = 정상 계좌. 은행 장애는 withBankDown (pause/resume).
 */
class RepublishFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var verifications: VerificationRepository
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var consumerFactory: ConsumerFactory<String, String>
    @Autowired private lateinit var props: ReconcileProperties

    private val abandoned = CopyOnWriteArrayList<Transfer>()

    /**
     * 재발행 대기만 1ms 로 줄인 정리 작업 (5분을 기다리는 대신). 나머지는 운영 구성과 같다.
     * 시계를 앞당기지 않는 이유: republished_at 이 미래 시각으로 적히면 컨슈머가 실제 시계로 적는 sending_at 이 그보다 이르게 되어
     * 2 · 3회차의 재발행 기준(sending_at > republished_at)이 깨진다. 그래서 각 회차는 DLT 도착을 기다린 뒤에 돌린다.
     */
    private val reconciler by lazy {
        TransferReconciler(
            store, BankInquiryAdapter(gateway), Clock.systemUTC(),
            republishAfter = Duration.ofMillis(1), maxRepublish = props.maxRepublish, onAbandon = { abandoned += it },
        )
    }

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun verify(accountNo: String) {
        verifications.save(
            AccountVerification(
                id = UUID.randomUUID(), bankCode = "090", accountNo = accountNo, holderName = "홍길동",
                codeHash = "0".repeat(64), status = VerificationStatus.VERIFIED, failedAttempts = 0,
                expiresAt = Instant.now(), verifiedAt = Instant.now(),
            ),
        )
    }

    private fun request(accountNo: String): UUID {
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to 30_000, "memo" to "P" + Random.nextInt(100_000, 999_999)))
            .retrieve().body(mapType)!!
        return UUID.fromString(body["id"] as String)
    }

    private fun awaitStatus(id: UUID, expected: TransferStatus) {
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(expected, store.findById(id)!!.status)
        }
    }

    @Test
    fun `설정 기본값 - 재발행은 마지막 변경 5분 뒤, 3회까지`() {
        assertEquals(Duration.ofMinutes(5), props.republishAfter)   // 재시도 체인(약 95초)과 겹치지 않게
        assertEquals(3, props.maxRepublish)
    }

    @Test
    fun `은행이 DLT 뒤에 살아나면 정리 작업이 재발행해 COMPLETED, 은행 입금은 1건`() {
        val account = "1002123456789"
        verify(account)
        val id = withBankDown {
            val id = request(account)
            relay.publishPending()
            consumerFactory.awaitRecords(TransferTopics.DLT, id.toString())
            assertEquals(TransferStatus.RECEIVED, store.findById(id)!!.status)
            id
        }
        // withBankDown 이 끝나면 모의 은행이 같은 포트로 다시 떠 있고 게이트웨이도 붙어 있다

        val result = reconciler.reconcile()

        assertTrue(result.republished >= 1, result.toString())
        assertEquals(1, store.findById(id)!!.republishCount)
        awaitStatus(id, TransferStatus.COMPLETED)
        val after = store.findById(id)!!
        assertEquals("0000", after.bankResponseCode)
        assertEquals(1, bank.depositsOf(account).count { it.reference == after.reference })   // 은행 호출은 한 번
    }

    @Test
    fun `은행이 계속 내려가 있으면 재발행 3회 뒤 FAILED E001, 은행 입금은 0건`() {
        val account = "1002123456789"
        verify(account)
        withBankDown {
            val id = request(account)
            relay.publishPending()
            consumerFactory.awaitRecords(TransferTopics.DLT, id.toString(), count = 1)

            for (round in 1..props.maxRepublish) {
                reconciler.reconcile()
                assertEquals(round, store.findById(id)!!.republishCount)
                relay.publishPending()
                // 재발행한 메시지도 재시도 토픽을 다 거쳐 DLT 로 간다 → 그 뒤에야 "재발행 이후 시도 흔적" 이 생긴다
                consumerFactory.awaitRecords(TransferTopics.DLT, id.toString(), count = round + 1)
            }

            val result = reconciler.reconcile()

            val after = store.findById(id)!!
            assertTrue(result.abandoned >= 1, result.toString())
            assertEquals(TransferStatus.FAILED, after.status)
            assertEquals("E001", after.bankResponseCode)
            assertEquals(props.maxRepublish, after.republishCount)
            assertTrue(abandoned.any { it.id == id })
            assertEquals(0, bank.depositsOf(account).count { it.reference == after.reference })   // 돈이 움직이지 않았다
        }
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*RepublishFlowTest*'` → FAIL (컴파일: `Unresolved reference 'maxRepublish'`, `'republishAfter'` — `ReconcileProperties` 에 아직 없다)

- [ ] **Step 3: 구현**

`ReconcileConfiguration.kt`:
- `ReconcileProperties` 를 바꾼다:
```kotlin
@ConfigurationProperties("transfer.reconcile")
data class ReconcileProperties(
    val enabled: Boolean = true,
    val intervalMs: Long = 10_000,
    val sendingTimeout: Duration = Duration.ofSeconds(30),
    val maxInquiries: Int = 10,
    val batchSize: Int = 100,
    /** 마지막 변경 후 이만큼 지나야 재발행한다 (P3-3b). 재시도 토픽 체인(약 95초)이 끝난 뒤에 보도록. */
    val republishAfter: Duration = Duration.ofMinutes(5),
    /** 재발행 한도. 다 쓰고도 은행에 못 보내면 FAILED(E001). */
    val maxRepublish: Int = 3,
)
```
- `transferReconciler` 빈을 바꾼다:
```kotlin
    @Bean
    fun transferReconciler(store: TransferStore, client: BankGatewayClient, props: ReconcileProperties) =
        TransferReconciler(
            store = store,
            bank = BankInquiryAdapter(client),
            clock = Clock.systemUTC(),
            sendingTimeout = props.sendingTimeout,
            maxInquiries = props.maxInquiries,
            batchSize = props.batchSize,
            republishAfter = props.republishAfter,
            maxRepublish = props.maxRepublish,
            // Transfer.toString 은 계좌번호를 가린다
            onGiveUp = { log.warn("still UNKNOWN after {} inquiries, leave it to daily reconciliation: {}", it.inquiryAttempts, it) },
            onAbandon = {
                log.warn("transfer abandoned after {} republishes, the bank was never reached (FAILED E001): {}", it.republishCount, it)
            },
        )
```

`application.yml` 의 `transfer.reconcile` 끝(`batch-size: 100` 아래)에 추가:
```yaml
    republish-after: 5m    # 마지막 변경 후 이만큼 지나야 재발행 (재시도 토픽 · DLT 약 95초와 겹치지 않게, P3-3b)
    max-republish: 3       # 다 쓰고도 은행에 못 보내면 FAILED E001 (은행에 간 적 없음 → 사용자가 다시 요청)
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*RepublishFlowTest*'` → PASS (3). 이어서 `./gradlew build` 전체 PASS, 모든 모듈 `tests=` 합계를 보고서에 적는다.

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 정리 작업 재발행 연결 — 5분 · 3회 설정, 은행 복구 시 COMPLETED · 계속 장애면 FAILED E001 흐름 증명

Co-Authored-By: <작성 모델>"
```

---

### Task 5: 결정 기록 · 문서 · 손 확인

**Files:**
- Create: `docs/adr/0007-retry-topic-and-bounded-republish.md`
- Create: `.run/4. API (재발행 손 확인).run.xml`
- Create: `postman/collections/bank-transfer/4-retry-republish/.resources/definition.yaml`, `01-request.request.yaml`, `02-received.request.yaml`, `03-completed.request.yaml`
- Modify: `docs/adr/0005-kafka-delivery-and-consumer-idempotency.md` (결정 8 에 대체 표시), `docs/superpowers/specs/2026-10-19-p3-transfer-design.md`, `docs/database.md`, `OPERATION.md`, `ARCHITECTURE.md`, `README.md`, `SETUP.md`, `CHANGELOG.md`, `HANDOVER.md`, `http/transfer.http`, `postman/collections/bank-transfer/.resources/definition.yaml`

- [ ] **Step 1: ADR-0007** — 기존 ADR 형식(제목 · 상태 · 단계 · 맥락 · 결정 · 결과 · 검토한 다른 선택지):
  - 제목: `# ADR-0007 은행에 못 보낸 출금은 논블로킹 재시도 토픽으로 다시 시도하고, 은행 시도 흔적이 있을 때만 횟수를 제한해 재발행한 뒤 FAILED E001 로 확정한다`
  - 상태: 제안 (2026-10-22). 단계 P3-3b. 링크: [P3-3b 설계](../superpowers/specs/2026-10-22-p3-3b-retry-republish-design.md) · [ADR-0005](0005-kafka-delivery-and-consumer-idempotency.md) 결정 8 을 대체 · [ADR-0006](0006-inquire-instead-of-resend.md).
  - 맥락: P3-2 의 블로킹 재시도(1s · 2s … 최대 10s, 5회)는 같은 파티션 뒤 메시지를 막고, 다 쓰면 이체가 `RECEIVED` 로 영영 남는다. 미전송(`BankNotSentException`)은 은행에 간 적이 없어 다시 보내도 안전하다. P3-3a 리뷰: 연결 없는 결과조회도 시도로 세어 100초 넘는 점검이면 `UNKNOWN` 이 모두 한도에 닿는다.
  - 결정: (1) `@RetryableTopic` 5s → 30s → 60s(시도 4), 인덱스 접미사 토픽 3개 + DLT, 파티션 3 · 복제 1 코드 생성, 블로킹 핸들러 삭제, 수동 ack 유지 — 넘긴 레코드 오프셋은 프레임워크가 커밋(`commitRecovered`, 테스트로 증명) (2) key 가 UUID 가 아니면 `InvalidTransferKeyException` → 바로 DLT (3) DLT 핸들러는 경고 로그 + ack, 이체는 `RECEIVED` (4) 정리 작업 ⓪: `RECEIVED` + `updated_at` 5분 전 + `sending_at > republished_at`(또는 재발행 없음) → 조건부 UPDATE + 아웃박스 INSERT 한 트랜잭션, 3회 뒤 `RECEIVED → FAILED(E001)` (5) 결과조회 연결 없음 = `Unavailable`, 그 주기 조회 멈춤 · 횟수 안 셈 (6) 그룹 · 동시성은 yml 한 곳(`groupId` 는 placeholder — 재시도 그룹이 접미사로 나뉘게) (7) 재시도 토픽 전용 1스레드 스케줄러.
  - 결과: 증명 테스트 이름 (`RetryTopicFlowTest` 3개, `RepublishFlowTest` 2개 흐름, `TransferReconcilerTest` 4개, `JpaTransferStoreTest` 재발행 4개, `BankInquiryAdapterTest` Unavailable, `BankGatewayClientTest` pause/resume). 한계: 재시도 메시지는 순서가 바뀐다(이체당 메시지 1개라 무해) · 포기까지 약 20분 → 그보다 긴 은행 점검이면 그 사이 접수된 출금은 `E001`(점검 시간대 접수 거절은 P5 후보) · 복구 직후 밀린 건이 한꺼번에 은행에 간다(컨슈머 3개가 상한, P4 뒤 속도 제한 검토) · 재시도 토픽 컨슈머가 5분 넘게 밀리면 재발행과 재시도가 겹칠 수 있다(은행 호출은 조건부로 한 번, 횟수는 기준대로만 증가) · 다중 인스턴스는 조건부라 한 번만 반영.
  - 다른 선택지 표: 블로킹 재시도 유지(뒤 메시지 막힘) · DLT 에서 바로 원래 토픽으로 재발행(Kafka 와 DB 이중 쓰기, 아웃박스를 우회) · 오래된 `RECEIVED` 를 모두 재발행(Kafka · 컨슈머 장애에도 횟수를 써 실패로 잘못 확정) · 무한 재발행(사용자가 실패를 못 보고 기다림) · 간격 1s · 10s · 60s(첫 간격이 재접속 백오프 첫 두 번보다 짧다) · 지연 값 접미사(간격을 바꾸면 토픽 이름이 바뀐다).
- [ ] **Step 2: ADR-0005** 결정 8 끝에 ` → P3-3b 에서 [ADR-0007](0007-retry-topic-and-bounded-republish.md) 로 대체 (재시도 토픽 · DLT · 제한 재발행).` 를 붙인다. 상태 줄은 그대로.
- [ ] **Step 3: 설계 문서** `docs/superpowers/specs/2026-10-19-p3-transfer-design.md`:
  - §3 상태 그림에 `RECEIVED ──(재발행 3회 뒤에도 미전송, 정리 작업)──▶ FAILED (E001)` 줄, 표에 `| RECEIVED → FAILED | 정리 작업: 재발행 한도(3회)를 다 쓰고도 은행에 못 보냄. bank_response_code = E001 (P3-3b) |` 행.
  - §6 끝에 `**2026-10-22 결정 (P3-3b)**` 목록: 재시도 간격 5s · 30s · 60s(인덱스 접미사), 재발행은 정리 작업 ⓪ 이 아웃박스로(기준 `sending_at > republished_at`, 5분 · 3회), 포기 `FAILED E001`, 결과조회 연결 없음은 `Unavailable`(세지 않음), 그룹 · 동시성은 yml 한 곳. 첫 bullet 의 재시도 문장은 이미 갱신돼 있으니 그대로 둔다.
  - §7 증명 테스트의 `[P3-3b]` 두 줄을 실제 테스트에 맞춰 바꾼다: `은행 꺼짐 → 재시도 토픽 → DLT (뒤 메시지는 처리됨, 단계마다 오프셋 커밋) → 은행 복구 → 재발행 → COMPLETED · 입금 1건`, `은행 계속 꺼짐 → 재발행 3회 → FAILED E001 · 입금 0건`, `UUID 아닌 key → 바로 DLT`, `결과조회 연결 없음 → 남은 조회 멈춤 · 횟수 그대로`.
- [ ] **Step 4: `docs/database.md`**:
  - §4-2 표: `inquiry_attempts` 다음에 `| republish_count | INT | N | 정리 작업이 아웃박스로 다시 발행한 횟수 (3에서 멈추고 FAILED E001), 기본 0. V2 (P3-3b) |`, `| republished_at | DATETIME(6) | Y | 마지막 재발행 시각. sending_at 이 이보다 늦어야(재발행 뒤 은행에 보내려다 못 보냈어야) 다음 재발행 대상 |` 행. `status` 설명은 그대로, `bank_response_code` 설명에 `E001` = 은행에 보내지 못하고 포기(자체 코드) 추가. 인덱스 설명에 "재발행 대상(`status = 'RECEIVED'`) 조회도 이 인덱스의 앞 컬럼으로 좁힌다" 추가.
  - §4-3: `event_type` 설명에 "재발행도 같은 `TransferRequested` 한 줄을 더 쓴다 (P3-3b)".
  - §5 상태 표 `transfer.status` 행에 `RECEIVED → FAILED (E001, 재발행 한도 뒤)` 추가.
  - §6 동시성 표에 행: `| 재발행 (정리 작업 여러 대 · 컨슈머와 겹침) | 조건부 UPDATE + 같은 트랜잭션 아웃박스 INSERT | UPDATE transfer SET republish_count = republish_count + 1, republished_at = ? … WHERE id = ? AND status = 'RECEIVED' AND republish_count = ? → 1건일 때만 INSERT INTO outbox_event. 포기는 transition(RECEIVED → FAILED) 조건부 |`.
  - §8 파일명 예시를 `V2__transfer_republish.sql` 로.
  - §9 운영 쿼리 추가: `-- 재발행 대상 (5분 넘게 RECEIVED, 마지막 재발행 이후 은행 시도 흔적)` `SELECT id, reference, republish_count, sending_at, republished_at FROM transfer WHERE status = 'RECEIVED' AND updated_at < NOW(6) - INTERVAL 5 MINUTE AND sending_at IS NOT NULL AND (republished_at IS NULL OR sending_at > republished_at) ORDER BY updated_at;` · `-- 은행에 못 보내고 포기한 출금 (E001)` `SELECT id, reference, republish_count, updated_at FROM transfer WHERE status = 'FAILED' AND bank_response_code = 'E001' ORDER BY updated_at DESC LIMIT 20;` · `-- 발행 · 소비가 안 돼 오래 RECEIVED (sending_at 없음 — 재발행 대상 아님, Kafka · 컨슈머 확인)` `SELECT id, created_at FROM transfer WHERE status = 'RECEIVED' AND sending_at IS NULL AND created_at < NOW(6) - INTERVAL 5 MINUTE;`
  - 변경 이력: `| 2026-10-22 | V2: republish_count · republished_at (제한 재발행), RECEIVED → FAILED(E001), 재발행 동시성 · 운영 쿼리 | [PR #9](https://github.com/hyeokjinON/bank-transfer/pull/9) (P3-3b) |`
- [ ] **Step 5: `OPERATION.md`**:
  - §2 설정 표에 행: `spring.kafka.consumer.group-id` = `transfer-processor` (재시도 · DLT 그룹은 `-retry-0/1/2`, `-dlt` 접미사), `spring.kafka.listener.concurrency` = `3`, `transfer.retry.attempts` = `4`, `transfer.retry.delay-ms` = `5000`, `transfer.retry.multiplier` = `6.0`, `transfer.retry.max-delay-ms` = `60000` (5s → 30s → 60s), `transfer.reconcile.republish-after` = `5m`, `transfer.reconcile.max-republish` = `3`.
  - Kafka 표: 토픽 행에 `transfer.requested-retry-0/1/2`, `transfer.requested-dlt` (파티션 3, 앱이 기동할 때 `@RetryableTopic` 이 만든다), 컨슈머 그룹 행을 5개 그룹으로, `재시도` 행을 "은행에 보내지 못하면 재시도 토픽으로 넘긴다: 5초 → 30초 → 60초 뒤 다시 처리, 그래도 안 되면 DLT. 원래 토픽의 뒤 메시지는 기다리지 않는다. 이체는 `RECEIVED` 로 남고 정리 작업이 재발행한다 ([ADR-0007](docs/adr/0007-retry-topic-and-bounded-republish.md))" 로.
  - 새 절 `### 재시도 토픽 · DLT 보는 법 (P3-3b)`: kafka-ui 에서 메시지가 `transfer.requested` → `-retry-0` → `-retry-1` → `-retry-2` → `-dlt` 로 옮겨 가는 것, 그룹별 랙, DLT 레코드 헤더(`kafka_original-topic/partition/offset`, `kafka_exception-cause-fqcn`, `kafka_exception-message`), 로그 `transfer message moved to DLT, stays RECEIVED until reconcile republishes it: key=…`. DLT 에 온 이체는 손대지 않아도 된다 — 은행 시도 흔적이 있으면 5분 뒤 재발행된다. key 가 UUID 가 아닌 메시지(`InvalidTransferKeyException`)는 보낸 쪽을 확인한다.
  - 정리 작업 표 맨 앞에 순서 `0` 행: `status = RECEIVED`, `updated_at` 5분 전, `sending_at` 이 있고 마지막 재발행 이후 → 아웃박스로 재발행(`republish_count + 1`), 3회를 다 썼으면 `FAILED` `E001`. 로그 목록에 `transfer abandoned after 3 republishes, the bank was never reached (FAILED E001): …` (WARN), `bank not connected, inquiries paused until next round` (WARN) 추가. "알려진 한계: 게이트웨이 연결이 없을 때의 조회도 시도 1회로 센다…" 줄을 지우고 "은행 연결이 없으면 결과조회는 그 주기에서 멈추고 횟수를 세지 않는다 (P3-3b)" 로.
  - §6 장애 대응: "이체가 `RECEIVED` 에서 안 넘어감" 행의 원인 · 조치를 "은행 연결 없음 → 재시도 토픽(약 95초) → DLT → 5분 뒤 재발행(최대 3회) → 그래도 안 되면 `FAILED E001`. `sending_at` 이 없으면 은행에 시도도 못 한 것(발행 · 소비 문제) → 릴레이 로그 · `published_at` · 컨슈머 랙을 본다(재발행하지 않는다, [database.md §9](docs/database.md))" 로. 새 행: `| DLT 에 메시지가 쌓임 | 로그 moved to DLT, 헤더의 예외 | RetryLaterException 이면 은행 장애 — 재발행을 기다린다. InvalidTransferKeyException 이면 보낸 쪽 버그 |`, `| FAILED 인데 bank_response_code = E001 | republish_count = 3 | 은행에 보내지 못하고 포기 — 돈은 움직이지 않았다. 사용자가 다시 요청하면 된다. 은행 장애가 20분 넘게 이어졌는지 본다 |`.
  - §4 API 오류 코드 근처(또는 §3 아래)에 한 줄: 조회 응답의 `bankResponseCode = "E001"` 은 은행 코드가 아니라 "은행에 보내지 못하고 포기" 를 뜻한다.
- [ ] **Step 6: `ARCHITECTURE.md`**:
  - §6 첫 줄을 `P3-3b 재시도 토픽 · DLT · 제한 재발행 ✅ (ADR-0007)` 까지 포함하게.
  - "P3-2 처리 흐름" 아래 bullet 의 블로킹 재시도 문장을 "미전송이면 `RECEIVED` 로 되돌리고 예외 → 재시도 토픽(5s · 30s · 60s) → DLT (P3-3b, 아래)" 로, 다이어그램의 `L->>K: 처리 끝난 뒤 ack (미전송은 예외 → 재시도)` 를 `(미전송은 예외 → 재시도 토픽)` 으로.
  - 새 절 `### P3-3b 재시도 · 재발행 흐름` (mermaid sequenceDiagram): `L` 원래 토픽 → 예외 → `retry-0 (5s)` → `retry-1 (30s)` → `retry-2 (60s)` → `DLT` (경고 로그, ack) → 5분 뒤 `TransferReconciler ⓪` → `findRepublishable` → `republish`(조건부 UPDATE + 아웃박스) → `OutboxRelay` → `transfer.requested` 처음부터 / 3회 뒤 `transition(RECEIVED → FAILED, E001)`. 아래 bullet: 뒤 메시지를 막지 않음, 단계마다 오프셋 커밋, 재발행 기준(은행 시도 흔적), 결과조회 연결 없음은 멈춤.
  - P3-3a 정리 작업 다이어그램의 `else 모름 (무응답 · 연결 없음 · 예외)` 를 `else 모름 (무응답 · 예외)` 로 하고 `else 연결 없음 (P3-3b)` → `R: 이번 주기 조회 멈춤 (횟수 안 셈)` 분기를 더한다.
  - 소스 지도에 `db/migration/V2__transfer_republish.sql`, 설계 결정 색인에 `| [0007](docs/adr/0007-retry-topic-and-bounded-republish.md) | 은행에 못 보낸 출금은 논블로킹 재시도 토픽 + 은행 시도 흔적 기준의 제한 재발행, 포기는 FAILED E001 |`.
- [ ] **Step 7: 손 확인 자료**
  - `.run/4. API (재발행 손 확인).run.xml` — 재시도 간격 2s · 4s · 8s, 재발행 대기 30초로 줄인 API 실행 구성 (운영 간격 그대로면 DLT 까지 95초를 기다려야 한다):
```xml
<component name="ProjectRunConfigurationManager">
  <configuration default="false" name="4. API (재발행 손 확인)" type="GradleRunConfiguration" factoryName="Gradle">
    <ExternalSystemSettings>
      <option name="executionName" />
      <option name="externalProjectPath" value="$PROJECT_DIR$" />
      <option name="externalSystemIdString" value="GRADLE" />
      <option name="scriptParameters" value="--args=&quot;--transfer.reconcile.republish-after=30s --transfer.retry.delay-ms=2000 --transfer.retry.multiplier=2.0 --transfer.retry.max-delay-ms=8000&quot;" />
      <option name="taskDescriptions">
        <list />
      </option>
      <option name="taskNames">
        <list>
          <option value=":apps:api:bootRun" />
        </list>
      </option>
      <option name="vmOptions" />
    </ExternalSystemSettings>
    <ExternalSystemDebugServerProcess>true</ExternalSystemDebugServerProcess>
    <ExternalSystemReattachDebugProcess>true</ExternalSystemReattachDebugProcess>
    <DebugAllEnabled>false</DebugAllEnabled>
    <RunAsTest>false</RunAsTest>
    <method v="2" />
  </configuration>
</component>
```
  - `http/transfer.http` 끝에 절 추가:
```http


### ============================================================
### 손 확인: 은행 장애 → 재시도 토픽 → DLT → 은행 복구 → 정리 작업 재발행으로 COMPLETED (P3-3b)
### 준비: 실행 구성 "1. Mock Bank (9090)" 과 "4. API (재발행 손 확인)" 을 띄운다 (2. 대신 4. — 재시도 2초·4초·8초, 재발행 대기 30초).
###       1002123456789 는 account-verification.http 로 인증해 둔다. kafka-ui 를 띄우면 토픽 이동이 보인다 (OPERATION.md Kafka 절).
### 순서: ① IntelliJ 에서 "1. Mock Bank (9090)" 만 Stop → ② 8-1) 접수 → ③ kafka-ui 에서 transfer.requested → -retry-0 → -retry-1 → -retry-2 → -dlt (약 14초),
###       API 콘솔 "moved to DLT" → ④ 8-2) 조회 → RECEIVED → ⑤ "1. Mock Bank (9090)" Run (API 콘솔 "connected to bank" — 재접속 백오프라 최대 30초)
###       → ⑥ 마지막 시도 30초 + 최대 10초 주기 뒤 API 콘솔 "reconcile: ReconcileResult(… republished=1 …)" → 8-3) 조회 → COMPLETED 0000.
###       모의 은행 콘솔의 50000원 입금은 1건이다. 접수부터 약 1분.
### ============================================================

### 8-1) 은행이 내려간 상태에서 접수 → 202 RECEIVED
POST http://localhost:8080/api/v1/transfers
Content-Type: application/json
Idempotency-Key: {{$random.uuid}}

{
  "bankCode": "090",
  "accountNo": "1002123456789",
  "amount": 50000,
  "memo": "재발행"
}

> {%
    client.global.set("retryTransferId", response.body.id);
%}

### 8-2) DLT 로 간 뒤(접수 15초쯤 뒤) 조회 → RECEIVED (은행에 못 보냈다. 정리 작업이 이어받는다)
GET http://localhost:8080/api/v1/transfers/{{retryTransferId}}

### 8-3) Mock Bank 를 다시 띄우고 40초쯤 뒤 조회 → COMPLETED 0000 (정리 작업이 아웃박스로 재발행 → 처음부터 처리)
GET http://localhost:8080/api/v1/transfers/{{retryTransferId}}
```
  - Postman: 루트 `postman/collections/bank-transfer/.resources/definition.yaml` 의 `variables` 에 `retryTransferId: ""` 를 넣고, `description` 의 4) 뒤에 줄 추가: `5) (P3-3b) "4. 재시도 · 재발행" 폴더: 실행 구성 "4. API (재발행 손 확인)" 으로 API 를 띄우고 Mock Bank 를 Stop → 4-1 → 15초쯤 뒤 4-2(RECEIVED) → Mock Bank Run → 40초쯤 뒤 4-3(COMPLETED). 대응: http/transfer.http 8-1 ~ 8-3.`
  - `4-retry-republish/.resources/definition.yaml`:
```yaml
$kind: collection
name: 4. 재시도 · 재발행 (P3-3b)
order: 4000
```
  - `4-retry-republish/01-request.request.yaml`:
```yaml
$kind: http-request
name: 4-1 Mock Bank 를 Stop 한 뒤 접수 → 202 RECEIVED
method: POST
url: '{{baseUrl}}/api/v1/transfers'
order: 1000
headers:
  - key: Content-Type
    value: application/json
  - key: Idempotency-Key
    value: '{{$guid}}'
body:
  type: json
  content: |-
    {
      "bankCode": "090",
      "accountNo": "{{accountNo}}",
      "amount": 50000,
      "memo": "재발행"
    }
scripts:
  - type: afterResponse
    language: text/javascript
    code: |-
      // 은행이 내려가 있어도 접수는 된다 (아웃박스에 저장). 처리는 재시도 토픽 → DLT 로 간다.
      pm.test("202 RECEIVED", () => {
        pm.response.to.have.status(202);
        pm.expect(pm.response.json().status).to.eql("RECEIVED");
      });
      pm.collectionVariables.set("retryTransferId", pm.response.json().id);
```
  - `4-retry-republish/02-received.request.yaml`:
```yaml
$kind: http-request
name: 4-2 DLT 뒤(15초쯤) 조회 → RECEIVED
method: GET
url: '{{baseUrl}}/api/v1/transfers/{{retryTransferId}}'
order: 2000
scripts:
  - type: afterResponse
    language: text/javascript
    code: |-
      // kafka-ui 에서 transfer.requested-dlt 에 이 id 가 key 인 메시지가 보여야 한다. 이체는 정리 작업이 이어받는다.
      pm.test("RECEIVED (은행에 못 보냄, 재발행 대기)", () => {
        pm.response.to.have.status(200);
        pm.expect(pm.response.json().status).to.eql("RECEIVED");
      });
```
  - `4-retry-republish/03-completed.request.yaml`:
```yaml
$kind: http-request
name: 4-3 Mock Bank Run 뒤 40초쯤 조회 → COMPLETED 0000 (재발행)
method: GET
url: '{{baseUrl}}/api/v1/transfers/{{retryTransferId}}'
order: 3000
scripts:
  - type: afterResponse
    language: text/javascript
    code: |-
      // 모의 은행 콘솔의 50000원 입금은 1건뿐이어야 한다. 아직 RECEIVED 면 재접속 · 정리 작업 주기를 몇 초 더 기다린다.
      pm.test("COMPLETED, 은행 응답코드 0000", () => {
        pm.response.to.have.status(200);
        pm.expect(pm.response.json().status).to.eql("COMPLETED");
        pm.expect(pm.response.json().bankResponseCode).to.eql("0000");
      });
```
- [ ] **Step 8: 나머지 문서**
  - `README.md`: "지금 되는 것"에 `- **재시도 토픽 · 제한 재발행 (P3-3b)**: 은행에 보내지도 못한 출금은 재시도 토픽(5s · 30s · 60s, 뒤 메시지를 막지 않음) → DLT → 정리 작업이 아웃박스로 최대 3번 다시 발행, 그래도 안 되면 FAILED E001 (돈은 움직이지 않음). 결과조회 중 연결 없음은 시도로 세지 않음 ([ADR-0007](docs/adr/0007-retry-topic-and-bounded-republish.md))` 추가. 로드맵 `[ ] P3-3b …` → `[x] P3-3b 재시도: 재시도 토픽 · DLT · 제한 재발행 (PR #9) → [ADR-0007](docs/adr/0007-retry-topic-and-bounded-republish.md)`, `[ ] **P3**` → `[x] **P3**`. 테스트 수(지금 되는 것 · IntelliJ · 터미널 세 곳)를 Step 9 결과로, 테스트 목록 괄호에 "은행 장애 → 재시도 토픽 → DLT → 재발행 → 입금 1건" 추가.
  - `SETUP.md` 2-2 의 테스트 수를 Step 9 결과로.
  - `CHANGELOG.md`: `## [P3-3a] …` 위에 `## [P3-3b] 재시도 토픽 · 제한 재발행 — 2026-10-22 ([#9](https://github.com/hyeokjinON/bank-transfer/pull/9))` — Added: 재시도 토픽 `transfer.requested-retry-0/1/2` · `-dlt`(5s · 30s · 60s, `transfer.retry.*`), DLT 핸들러, key 가 UUID 가 아니면 바로 DLT, 정리 작업 ⓪ 재발행 · 포기(`republish-after` 5분 · `max-republish` 3, `FAILED E001`), `TransferStore.findRepublishable` · `republish`, `InquiryOutcome.Unavailable`, V2 컬럼, 모의 은행 `pause` / `resume`, ADR-0007, 손 확인(`http/transfer.http` 8절 · Postman 4 · 실행 구성 4). Changed: 블로킹 `kafkaErrorHandler` 삭제(뒤 메시지를 막지 않음), 상태 전이 `RECEIVED → FAILED` 추가, 리스너 그룹 · 동시성을 yml 한 곳으로(`listener.concurrency: 3`), 결과조회 중 연결 없음은 그 주기 조회를 멈추고 횟수를 세지 않음, 재시도 토픽 전용 스케줄러.
  - `HANDOVER.md`: 마지막 갱신일 2026-10-22, P3-3b 행 `🟡 PR #9 리뷰 중 (손 확인 대기)` + 비고(ADR-0007 제안, 테스트 수), §2 표의 `실패` 행 간격을 `5s·30s·60s` 로 · `정리 작업` 행에 ⓪ 재발행 한 줄. §3 다음 할 일: velog `#6`(P3-3b: 재시도 토픽 · 은행 시도 흔적 기준 재발행 · 오프셋 커밋 함정 확인), P4 계획. "P3-3b 로 넘기는 것" 에서 처리한 항목(스위퍼, `kafkaErrorHandler` 교체 · UUID 아닌 key → DLT, 그룹 · 동시성 한 곳, 연결 없는 결과조회 횟수)을 지우고 제목을 `### P3-3b 범위 밖으로 남긴 것` 으로 바꾼다 (남는 것: 쓰기 실패 테스트, 릴레이 다중 인스턴스 순서, 정리 작업 한 주기가 느린 은행이면 길어짐 — 연결 없음은 이제 바로 멈춤, 늦은 응답 로그의 거래번호, 스케줄러 실패 로그 · `@EnableScheduling` 위치, 은행 점검 시간대 접수 거절(P5 후보)). §4 의 "(P3-2) 재시도를 다 쓰면 … 블로킹 재시도" 줄을 "(P3-3b) 은행 장애가 약 20분(재시도 95초 + 재발행 5분 × 3)보다 길면 그 사이 접수된 출금은 `FAILED E001` 이 된다. 복구 직후 밀린 건이 한꺼번에 은행에 간다(컨슈머 3개가 상한)" 로.
- [ ] **Step 9: 전체 빌드** — `./gradlew build --rerun-tasks`, 테스트 합계를 README · SETUP · HANDOVER 에 반영. 손 확인(Step 7 절차)은 머지 전에 사람이 한다 — HANDOVER 비고에 "손 확인 대기".
- [ ] **Step 10: 커밋**

```bash
git add docs .run http postman README.md SETUP.md CHANGELOG.md HANDOVER.md OPERATION.md ARCHITECTURE.md
git commit -m "docs: P3-3b — ADR-0007, 재시도 토픽 · DLT 운영, 재발행 쿼리, 손 확인 절차 (실행 구성 · Postman · http)

Co-Authored-By: <작성 모델>"
```
(push · PR 없음.)
