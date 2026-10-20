# P3-2 출금이체 처리 (아웃박스 → Kafka → 컨슈머 → 은행) — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** P3-1 이 아웃박스에 남긴 `TransferRequested` 를 폴링 퍼블리셔로 Kafka 에 발행하고, 컨슈머가 은행 이체 전문을 보내 결과를 `COMPLETED / FAILED / UNKNOWN` 으로 기록한다. 같은 메시지가 두 번 와도 은행에는 한 번만 보낸다.

**Architecture:** 도메인(`modules/transfer`)에 `TransferProcessor` 와 포트 `BankTransferPort`(결과를 Completed · Rejected · ResultUnknown · NotSent 로 분류)를 두고, 상태 변경은 `TransferStore.transition`(조건부 UPDATE)으로만 한다. `apps/api` 에 `OutboxRelay`(SKIP LOCKED 폴링), `TransferRequestedListener`(수동 ack), 게이트웨이 어댑터를 둔다. 게이트웨이는 "보내기 전 실패"를 `BankNotSentException` 으로 구분한다.

**Tech Stack:** Kotlin 2.3.21 · Spring Boot 4.1.1 · Spring Kafka 4.1 (kafka-clients 4.2) · MySQL 8.4 · Kafka 4.1 (KRaft) · Testcontainers 2.0 (`testcontainers-kafka`) · Awaitility

**Spec:** [docs/superpowers/specs/2026-10-19-p3-transfer-design.md](../specs/2026-10-19-p3-transfer-design.md) §3 상태 · §5 처리 흐름 · §7 테스트 · §8 완료 기준 · [docs/database.md](../../database.md) · HANDOVER.md "P3-2 로 넘기는 것"

## Global Constraints

- 도메인 모듈(`modules/account`, `modules/transfer`)은 Spring · JPA · Netty · Kafka 의존성 없음 (`dependencies { }` 비어 있음).
- **결과를 모르면 다시 보내지 않는다.** 은행 무응답 · 보낸 뒤 연결 끊김 · 쓰기 실패는 `UNKNOWN` (P3-3 결과조회로 확정). 다시 보내는 건 "보내기 전에 실패(연결 없음)"뿐 → `SENDING → RECEIVED` 후 재시도.
- 컨슈머 멱등 = **상태 조건부 UPDATE** (`RECEIVED` 일 때만 `SENDING`). `processed_event` 테이블은 쓰지 않는다.
- 은행 호출은 **DB 트랜잭션 밖**에서 한다.
- 토픽 `transfer.requested`, 파티션 3, 복제 1(로컬). 코드로 선언(`NewTopic`) — 브로커 자동 생성은 꺼져 있다.
- 메시지 key = 이체 id(문자열 UUID), value = `{"transferId","occurredAt","eventId"}` (eventId = `outbox_event.id`, 발행 때 `JSON_SET` 으로 붙임).
- 프로듀서 `acks=all`, `enable.idempotence=true`. 컨슈머 group `transfer-processor`, `enable-auto-commit=false`, `ack-mode: manual_immediate`, concurrency 3, `auto-offset-reset: earliest`.
- 폴링 퍼블리셔: 500ms 고정 지연, 한 번에 100건, `SELECT … FOR UPDATE SKIP LOCKED`, 브로커 확인(`send().get(5s)`) 뒤 `published_at = NOW(6)`.
- 상태 변경은 `TransferEntity.from()` + save 로 하지 않는다 (INSERT 전용). JPQL 조건부 UPDATE 만 쓴다.
- DB 시각 KST `DATETIME(6)`, 도메인 `Instant`. Instant 를 DB 에 넘길 때 `toDbPrecision()`.
- 계좌번호 · 적요가 로그에 그대로 남지 않게 `Transfer.toString` 을 마스킹한다.
- 주석 · 커밋 한국어, "왜"를 쓴다. 커밋 `type(scope): 요약` + 끝에 작성 모델 이름의 `Co-Authored-By:` 줄. push 하지 않는다.
- 테스트: Docker Desktop 필요. Git Bash 에서 `export PATH="/c/Users/user/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"`. 통합 테스트는 `ApiIntegrationTest` 를 상속(컨텍스트 1개 공유), 키 · 계좌 · 적요는 테스트마다 새로.
- 계획 작성 전 스파이크로 확인한 것: `apache/kafka:4.1.0` + `org.testcontainers.kafka.KafkaContainer` + `@ServiceConnection`, `spring-boot-starter-kafka`, `@KafkaListener(concurrency="3")` + `Acknowledgment`, 프로듀서 설정이 `acks=all`/`enable.idempotence=true` 로 적용됨, MySQL `FOR UPDATE SKIP LOCKED` 가 두 트랜잭션에 서로 다른 행을 줌.

## 파일 구조

| 파일 | 책임 |
|---|---|
| `modules/bank-gateway/.../BankGatewayClient.kt` (수정) | `BankUnavailableException` open, `BankNotSentException`(보내기 전 실패) 추가 |
| `modules/transfer/.../Transfer.kt` (수정) | `BankTransferOutcome`, `BankTransferPort`, `TransferStore.transition`, `Transfer.toString` 마스킹 |
| `modules/transfer/.../TransferProcessor.kt` (신규) | 처리 규칙: 조건부 SENDING → 은행 → 결과 기록 |
| `apps/api/.../persistence/TransferPersistence.kt` (수정) | JPQL 조건부 UPDATE 2개, `JpaTransferStore.transition` |
| `apps/api/.../messaging/MessagingConfiguration.kt` (신규) | 토픽, 릴레이, 스케줄러, 프로세서, 리스너, 에러 핸들러 빈 |
| `apps/api/.../messaging/OutboxRelay.kt` (신규) | 폴링 퍼블리셔 + 스케줄러 + 설정 |
| `apps/api/.../messaging/TransferRequestedListener.kt` (신규) | 컨슈머 |
| `apps/api/.../config/BankTransferAdapter.kt` (신규) | 게이트웨이 예외 → `BankTransferOutcome` 분류 |
| `apps/api/.../web/ApiExceptionHandler.kt` (수정) | 요청 형식 오류에 `code: INVALID_REQUEST`, `IllegalTransition` 409 |
| `apps/api/src/test/.../support/KafkaTestContainer.kt` (신규), `ApiIntegrationTest.kt` (수정) | Kafka 컨테이너, 릴레이 스케줄 끄기 |

---

### Task 1: 게이트웨이 — "보내기 전 실패"를 구분하는 예외

**Files:**
- Modify: `modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayClient.kt`
- Test: `modules/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayClientTest.kt`

**Interfaces:**
- Produces: `open class BankUnavailableException(message: String, cause: Throwable? = null)`, `class BankNotSentException(message: String) : BankUnavailableException(message)`. `send()` 는 연결이 없으면 `BankNotSentException` 을 담은 실패 future 를 돌려준다. 보낸 뒤의 실패(연결 끊김 `"connection to bank lost"`, 쓰기 실패 `"write failed"`)는 계속 일반 `BankUnavailableException`.

**왜:** `BankUnavailableException` 은 지금 "연결이 없어 안 보냄"과 "보낸 뒤 연결이 끊김"(은행이 받았을 수 있음)에 모두 쓰인다. 출금은 앞의 경우만 다시 보내도 안전하므로 타입으로 구분한다.

- [ ] **Step 1: 실패하는 테스트**

`BankGatewayClientTest.kt` 의 기존 테스트 `연결이 없으면 기다리지 않고 바로 BankUnavailableException` 의 단언을 다음으로 바꾸고(이름은 그대로 둔다 — 하위 타입이므로 기존 의미도 유지), 아래 테스트를 클래스 끝에 추가한다:

```kotlin
    @Test
    fun `연결이 없으면 기다리지 않고 바로 BankUnavailableException`() {
        val client = client(freePort())

        val failure = client.inquireName("090", "1").awaitFailure()
        assertIs<BankUnavailableException>(failure)
        assertIs<BankNotSentException>(failure)   // 은행에 가지 않았다 → 다시 보내도 안전
    }

    @Test
    fun `보낸 뒤 연결이 끊기면 BankNotSentException 이 아니다 (은행이 받았을 수 있다)`() {
        val bank = MockBankServer(silentAccounts = setOf("9990000000001")).start()
        val client = client(bank.boundPort, timeout = Duration.ofSeconds(5))
        waitUntil { client.isConnected }

        val pending = client.transfer("090", "9990000000001", 1, "x")
        bank.close()

        val failure = pending.awaitFailure()
        assertIs<BankUnavailableException>(failure)
        assertFalse(failure is BankNotSentException)
    }
```
import 에 `kotlin.test.assertFalse` 가 없으면 추가한다. `MockBankServer`, `waitUntil`, `freePort`, `awaitFailure`, `client(...)` 는 이 파일에 이미 있다 (파일을 먼저 읽어 확인).

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:bank-gateway:test`
Expected: FAIL — `Unresolved reference 'BankNotSentException'`

- [ ] **Step 3: 구현**

`BankGatewayClient.kt` 의 예외 선언을 다음으로 바꾼다:
```kotlin
/** 은행과 통신할 수 없다. 보낸 뒤에 끊긴 경우도 포함한다 — 그때는 은행이 요청을 받았을 수 있다. */
open class BankUnavailableException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/** 전문을 소켓에 쓰기 전에 실패했다 (연결 없음). 은행에 "확실히" 가지 않았으므로 다시 보내도 안전하다. */
class BankNotSentException(message: String) : BankUnavailableException(message)
```
`send()` 의 연결 확인 줄을 바꾼다:
```kotlin
            return CompletableFuture.failedFuture(BankNotSentException("bank channel is not connected"))
```
`send()` KDoc 의 `실패는 future 안에 담긴다: …` 줄을 다음으로 바꾼다:
```kotlin
     * 실패는 future 안에 담긴다: 연결 없음 -> BankNotSentException (안 보냄),
     * 쓰기 실패 · 보낸 뒤 연결 끊김 -> BankUnavailableException (보냈을 수 있음), 시간 초과 -> BankTimeoutException.
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:bank-gateway:test` → PASS (7 tests). 이어서 `./gradlew build` 로 다른 모듈(1원인증의 503 매핑 등)이 그대로인지 확인.

- [ ] **Step 5: 커밋**

```bash
git add modules/bank-gateway
git commit -m "feat(gateway): 보내기 전 실패를 BankNotSentException 으로 구분

Co-Authored-By: <작성 모델>"
```

---

### Task 2: 도메인 — 처리 규칙 `TransferProcessor`

**Files:**
- Modify: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/Transfer.kt`
- Create: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferProcessor.kt`
- Modify: `modules/transfer/src/test/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferServiceTest.kt` (FakeStore 에 새 메서드)
- Test: `modules/transfer/src/test/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferProcessorTest.kt`

**Interfaces:**
- Consumes: `Transfer`, `TransferStatus`, `TransferStore`(P3-1)
- Produces:
  - `sealed interface BankTransferOutcome { data object Completed; data class Rejected(val responseCode: String); data object ResultUnknown; data object NotSent }`
  - `fun interface BankTransferPort { fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): BankTransferOutcome }`
  - `TransferStore.transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String? = null): Boolean`
  - `enum class ProcessResult { COMPLETED, FAILED, UNKNOWN, ALREADY_HANDLED, NOT_FOUND, RETRY_LATER }`
  - `class TransferProcessor(store: TransferStore, bank: BankTransferPort, clock: Clock = Clock.systemUTC())` · `fun process(transferId: UUID): ProcessResult`

- [ ] **Step 1: 실패하는 테스트**

`TransferProcessorTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.transfer

import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TransferProcessorTest {

    /** 조건부 UPDATE 를 흉내 내는 가짜 저장소: 현재 상태가 from 일 때만 바꾼다. */
    private class FakeStore : TransferStore {
        val transfers = linkedMapOf<UUID, Transfer>()

        override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer = transfer.also { transfers[it.id] = it }
        override fun findById(id: UUID) = transfers[id]
        override fun findByIdempotencyKey(key: String) = transfers.values.firstOrNull { it.idempotencyKey == key }

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean {
            val current = transfers[id] ?: return false
            if (current.status != from) return false
            transfers[id] = current.copy(
                status = to,
                updatedAt = at,
                bankResponseCode = bankResponseCode,
                sendingAt = if (to == TransferStatus.SENDING) at else current.sendingAt,
            )
            return true
        }
    }

    /** 정해 둔 결과를 돌려주고 호출 횟수를 센다 (은행에 몇 번 보냈나). */
    private class FakeBank(var outcome: BankTransferOutcome) : BankTransferPort {
        var calls = 0
        override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): BankTransferOutcome {
            calls++
            return outcome
        }
    }

    private val now = Instant.parse("2026-10-20T01:00:00Z")
    private val store = FakeStore()
    private val bank = FakeBank(BankTransferOutcome.Completed)
    private val processor = TransferProcessor(store, bank, Clock.fixed(now, ZoneOffset.UTC))

    private fun received(): Transfer {
        val t = Transfer(
            id = UUID.randomUUID(), idempotencyKey = "k", reference = "T2610200000000000001", requestHash = "h",
            bankCode = "090", accountNo = "1002123456789", amount = 50_000, memo = "출금",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        store.transfers[t.id] = t
        return t
    }

    @Test
    fun `은행이 처리하면 COMPLETED 와 0000, sendingAt 기록`() {
        val t = received()

        assertEquals(ProcessResult.COMPLETED, processor.process(t.id))

        val after = store.transfers.getValue(t.id)
        assertEquals(TransferStatus.COMPLETED, after.status)
        assertEquals("0000", after.bankResponseCode)
        assertEquals(now, after.sendingAt)
        assertEquals(1, bank.calls)
    }

    @Test
    fun `은행이 거절하면 FAILED 와 응답코드`() {
        val t = received()
        bank.outcome = BankTransferOutcome.Rejected("1001")

        assertEquals(ProcessResult.FAILED, processor.process(t.id))
        assertEquals(TransferStatus.FAILED, store.transfers.getValue(t.id).status)
        assertEquals("1001", store.transfers.getValue(t.id).bankResponseCode)
    }

    @Test
    fun `결과를 모르면 UNKNOWN 이고 다시 보내지 않는다`() {
        val t = received()
        bank.outcome = BankTransferOutcome.ResultUnknown

        assertEquals(ProcessResult.UNKNOWN, processor.process(t.id))
        assertEquals(ProcessResult.ALREADY_HANDLED, processor.process(t.id))   // 같은 메시지가 다시 와도

        assertEquals(TransferStatus.UNKNOWN, store.transfers.getValue(t.id).status)
        assertNull(store.transfers.getValue(t.id).bankResponseCode)
        assertEquals(1, bank.calls)
    }

    @Test
    fun `보내지도 못했으면 RECEIVED 로 되돌리고 다음에 다시 보낸다`() {
        val t = received()
        bank.outcome = BankTransferOutcome.NotSent

        assertEquals(ProcessResult.RETRY_LATER, processor.process(t.id))
        assertEquals(TransferStatus.RECEIVED, store.transfers.getValue(t.id).status)

        bank.outcome = BankTransferOutcome.Completed
        assertEquals(ProcessResult.COMPLETED, processor.process(t.id))
        assertEquals(2, bank.calls)
    }

    @Test
    fun `같은 메시지를 두 번 받아도 은행에는 한 번만`() {
        val t = received()

        processor.process(t.id)
        assertEquals(ProcessResult.ALREADY_HANDLED, processor.process(t.id))
        assertEquals(1, bank.calls)
    }

    @Test
    fun `없는 이체면 NOT_FOUND 이고 은행을 부르지 않는다`() {
        assertEquals(ProcessResult.NOT_FOUND, processor.process(UUID.randomUUID()))
        assertEquals(0, bank.calls)
    }

    @Test
    fun `toString 은 계좌번호 뒤 4자리만, 적요는 찍지 않는다`() {
        val text = received().toString()

        assertTrue("*********6789" in text, text)
        assertFalse("1002123456789" in text, text)
        assertFalse("출금" in text, text)
    }
}
```

`TransferServiceTest.kt` 의 `FakeStore` 에 메서드를 추가한다 (접수 테스트는 상태 변경을 쓰지 않는다):
```kotlin
        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?) =
            throw UnsupportedOperationException("접수 테스트에서는 쓰지 않는다")
```
(필요하면 `java.time.Instant` import 추가)

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:transfer:test`
Expected: FAIL — `Unresolved reference 'TransferProcessor'`, `'BankTransferOutcome'`, `transition` 등

- [ ] **Step 3: 구현**

`Transfer.kt`:

1) `data class Transfer(...) {` 본문의 `moveTo` 아래에 추가:
```kotlin

    /** 로그에 계좌번호 · 적요가 그대로 남지 않게 한다 (data class 기본 toString 은 모든 필드를 찍는다). */
    override fun toString(): String =
        "Transfer(id=$id, reference=$reference, status=$status, bankCode=$bankCode, accountNo=${maskAccountNo(accountNo)}, " +
            "amount=$amount, bankResponseCode=$bankResponseCode)"
```
2) 파일 끝(또는 `RequestTransfer` 위)에 추가:
```kotlin
/** 뒤 4자리만 남긴다: 1002123456789 -> *********6789 */
internal fun maskAccountNo(accountNo: String): String =
    if (accountNo.length <= 4) "*".repeat(accountNo.length) else "*".repeat(accountNo.length - 4) + accountNo.takeLast(4)

/**
 * 은행 이체 결과. "모름"과 "안 보냄"을 구분하는 게 핵심이다:
 * 모르면 은행이 처리했을 수 있으므로 다시 보내지 않고, 안 보냈으면 다시 보내도 된다.
 */
sealed interface BankTransferOutcome {
    /** 은행이 처리했다 (0000). */
    data object Completed : BankTransferOutcome

    /** 은행이 거절했다 (계좌 없음 등). 확정 실패. */
    data class Rejected(val responseCode: String) : BankTransferOutcome

    /** 보냈는데 결과를 모른다 (무응답 · 보낸 뒤 연결 끊김 · 쓰기 실패). */
    data object ResultUnknown : BankTransferOutcome

    /** 보내기 전에 실패했다 (연결 없음). 은행에 가지 않았다. */
    data object NotSent : BankTransferOutcome
}

/** 은행 출금이체 포트. 결과가 나올 때까지 호출 스레드에서 기다린다 (Kafka 컨슈머 스레드가 부른다). */
fun interface BankTransferPort {
    fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): BankTransferOutcome
}
```
3) `interface TransferStore` 에 메서드 추가:
```kotlin

    /**
     * 현재 상태가 [from] 일 때만 [to] 로 바꾼다 (조건부 UPDATE). 바뀌었으면 true.
     * 같은 메시지를 두 번 받아도 두 번째는 false 라서 은행에 다시 보내지 않는다 — 도메인 상태가 곧 멱등키다.
     * [to] 가 SENDING 이면 sendingAt 도 [at] 으로 기록한다. bankResponseCode 는 주어진 값으로 덮어쓴다.
     */
    fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String? = null): Boolean
```

`TransferProcessor.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Clock
import java.util.UUID

/** 처리 결과. 컨슈머는 [RETRY_LATER] 일 때만 메시지를 다시 처리하게 한다. */
enum class ProcessResult { COMPLETED, FAILED, UNKNOWN, ALREADY_HANDLED, NOT_FOUND, RETRY_LATER }

/**
 * TransferRequested 처리 (P3-2).
 * 1. RECEIVED -> SENDING 을 조건부로 바꾼다. false 면 다른 소비자가 이미 집어 갔거나 끝난 건 → 은행에 보내지 않는다.
 * 2. 은행 호출은 DB 트랜잭션 밖에서 한다 (응답을 기다리는 동안 커넥션을 붙잡지 않는다).
 * 3. 결과를 기록한다. 결과를 모르면 UNKNOWN — 다시 보내지 않는다 (P3-3 결과조회로 확정).
 *    보내지도 못했으면 RECEIVED 로 되돌려 재시도하게 한다.
 */
class TransferProcessor(
    private val store: TransferStore,
    private val bank: BankTransferPort,
    private val clock: Clock = Clock.systemUTC(),
) {

    fun process(transferId: UUID): ProcessResult {
        val transfer = store.findById(transferId) ?: return ProcessResult.NOT_FOUND
        if (!store.transition(transferId, RECEIVED, SENDING, clock.instant())) return ProcessResult.ALREADY_HANDLED

        return when (val outcome = bank.transfer(transfer.bankCode, transfer.accountNo, transfer.amount, transfer.memo)) {
            BankTransferOutcome.Completed -> record(transferId, COMPLETED, BANK_OK, ProcessResult.COMPLETED)
            is BankTransferOutcome.Rejected -> record(transferId, FAILED, outcome.responseCode, ProcessResult.FAILED)
            BankTransferOutcome.ResultUnknown -> record(transferId, UNKNOWN, null, ProcessResult.UNKNOWN)
            BankTransferOutcome.NotSent -> record(transferId, RECEIVED, null, ProcessResult.RETRY_LATER)
        }
    }

    /** SENDING 은 이 처리만 만든다. 여기서 false 면 누군가 규칙을 어기고 상태를 바꾼 것이다. */
    private fun record(id: UUID, to: TransferStatus, bankResponseCode: String?, result: ProcessResult): ProcessResult {
        check(store.transition(id, SENDING, to, clock.instant(), bankResponseCode)) { "transfer $id left SENDING unexpectedly" }
        return result
    }

    private companion object {
        const val BANK_OK = "0000"
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:transfer:test` → PASS (기존 20 + 새 7 = 27)

- [ ] **Step 5: 커밋**

```bash
git add modules/transfer
git commit -m "feat(transfer): 출금 처리 규칙 — 조건부 SENDING, 결과 분류(완료·거절·모름·미전송), 계좌 마스킹

Co-Authored-By: <작성 모델>"
```

---

### Task 3: 저장소 — 조건부 상태 변경

**Files:**
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/TransferPersistence.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/JpaTransferStoreTest.kt`

**Interfaces:**
- Consumes: `TransferStore.transition` (Task 2), `toDbPrecision()` (P3-1, 같은 패키지)
- Produces: `TransferJpaRepository.transitionToSending(id, from, to, at, code): Int`, `TransferJpaRepository.transition(id, from, to, at, code): Int`, `JpaTransferStore.transition(...)`

- [ ] **Step 1: 실패하는 테스트**

`JpaTransferStoreTest.kt` 클래스 끝에 추가 (`newTransfer`, `requested`, `store`, `jdbc` 는 이미 있다):
```kotlin
    @Test
    fun `조건부 전이 - 현재 상태가 맞을 때만 바뀌고 sending_at 은 KST 로 기록된다`() {
        val transfer = newTransfer()
        store.saveWithEvent(transfer, requested(transfer))
        val at = Instant.parse("2026-10-20T01:00:00Z")

        assertTrue(store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.SENDING, at))
        assertFalse(store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.SENDING, at))   // 두 번째는 실패

        val after = store.findById(transfer.id)!!
        assertEquals(TransferStatus.SENDING, after.status)
        assertEquals(at, after.sendingAt)
        val row = jdbc.queryForMap(
            "SELECT DATE_FORMAT(sending_at, '%Y-%m-%d %H:%i:%s') AS sending_kst, version FROM transfer WHERE id = ?",
            transfer.id.toString(),
        )
        assertEquals("2026-10-20 10:00:00", row["sending_kst"])
        assertEquals(1L, (row["version"] as Number).toLong())
    }

    @Test
    fun `결과 기록 - SENDING 에서 COMPLETED 와 응답코드`() {
        val transfer = newTransfer()
        store.saveWithEvent(transfer, requested(transfer))
        val at = Instant.parse("2026-10-20T01:00:01Z")
        store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.SENDING, at)

        assertTrue(store.transition(transfer.id, TransferStatus.SENDING, TransferStatus.COMPLETED, at, "0000"))

        val after = store.findById(transfer.id)!!
        assertEquals(TransferStatus.COMPLETED, after.status)
        assertEquals("0000", after.bankResponseCode)
        assertEquals(at, after.updatedAt)
    }
```
import 추가: `kotlin.test.assertTrue`, `kotlin.test.assertFalse` (없으면).

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*JpaTransferStoreTest*'`
Expected: FAIL — 컴파일 오류 `Class 'JpaTransferStore' is not abstract and does not implement abstract member 'transition'` (Task 2 가 포트에 메서드를 추가했기 때문)

- [ ] **Step 3: 구현**

`TransferPersistence.kt`:

1) import 추가: `org.springframework.data.jpa.repository.Modifying`, `org.springframework.data.jpa.repository.Query`, `org.springframework.data.repository.query.Param`

2) `TransferJpaRepository` 를 다음으로 바꾼다:
```kotlin
interface TransferJpaRepository : JpaRepository<TransferEntity, String> {
    fun findByIdempotencyKey(idempotencyKey: String): TransferEntity?

    /** 조건부 상태 변경 (SENDING 으로 갈 때 sending_at 도 기록). 현재 상태가 from 이 아니면 0 건. */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        "update TransferEntity t set t.status = :to, t.sendingAt = :at, t.updatedAt = :at, " +
            "t.bankResponseCode = :code, t.version = t.version + 1 where t.id = :id and t.status = :from",
    )
    fun transitionToSending(
        @Param("id") id: String,
        @Param("from") from: TransferStatus,
        @Param("to") to: TransferStatus,
        @Param("at") at: Instant,
        @Param("code") code: String?,
    ): Int

    /** 조건부 상태 변경 (결과 기록 등). 현재 상태가 from 이 아니면 0 건. */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        "update TransferEntity t set t.status = :to, t.updatedAt = :at, " +
            "t.bankResponseCode = :code, t.version = t.version + 1 where t.id = :id and t.status = :from",
    )
    fun transition(
        @Param("id") id: String,
        @Param("from") from: TransferStatus,
        @Param("to") to: TransferStatus,
        @Param("at") at: Instant,
        @Param("code") code: String?,
    ): Int
}
```
Hibernate 가 `t.version = t.version + 1` 대입을 거부하면(`version` 속성 대입 불가 오류), 두 쿼리를 `update versioned TransferEntity t set …` 로 바꾸고 version 대입을 뺀다 (Hibernate 가 자동 증가). 어느 쪽을 썼는지 보고서에 적는다.

3) `JpaTransferStore` 에 추가 (`findByIdempotencyKey` 아래):
```kotlin

    /**
     * 조건부 UPDATE 로만 상태를 바꾼다. TransferEntity.from() 으로 만든 엔티티를 save 하면 INSERT 가 되므로 쓰지 않는다.
     * 한 문장의 UPDATE 라 두 소비자가 동시에 와도 DB 가 한쪽만 1 건으로 통과시킨다.
     */
    override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean =
        tx.execute {
            val dbAt = at.toDbPrecision()
            val updated = if (to == TransferStatus.SENDING) {
                transfers.transitionToSending(id.toString(), from, to, dbAt, bankResponseCode)
            } else {
                transfers.transition(id.toString(), from, to, dbAt, bankResponseCode)
            }
            updated == 1
        }!!
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*JpaTransferStoreTest*'` → PASS (6 tests)

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 이체 상태 조건부 UPDATE (현재 상태가 맞을 때만, sending_at KST)

Co-Authored-By: <작성 모델>"
```

---

### Task 4: Kafka 기반 — 의존성 · 설정 · 토픽 · 테스트 컨테이너

**Files:**
- Modify: `apps/api/build.gradle.kts`, `apps/api/src/main/resources/application.yml`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/MessagingConfiguration.kt`
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/KafkaTestContainer.kt`
- Modify: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/KafkaSetupTest.kt`

**Interfaces:**
- Produces: `object TransferTopics { const val REQUESTED = "transfer.requested" }`, 빈 `transferRequestedTopic(): NewTopic`, 클래스 `MessagingConfiguration`(이후 Task 5·6 이 빈을 추가), 테스트 지원 `KafkaTestContainer`, `ApiIntegrationTest` 는 `transfer.outbox.relay.enabled=false`

- [ ] **Step 1: 의존성**

`apps/api/build.gradle.kts`:
- `implementation("org.flywaydb:flyway-mysql")` 아래에 `implementation("org.springframework.boot:spring-boot-starter-kafka")`
- `testImplementation("org.testcontainers:testcontainers-mysql")` 아래에 `testImplementation("org.testcontainers:testcontainers-kafka")` 와 `testImplementation("org.awaitility:awaitility-kotlin")`

- [ ] **Step 2: 설정**

`application.yml` 의 `spring:` 아래(`jpa:` 앞)에 추가:
```yaml
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      acks: all                    # ISR 모두 기록돼야 성공 (운영 3대 · min.insync.replicas=2 와 짝)
      properties:
        enable.idempotence: true   # 프로듀서 재시도가 브로커에 중복 기록되지 않게
    consumer:
      group-id: transfer-processor
      auto-offset-reset: earliest
      enable-auto-commit: false    # 처리를 끝낸 뒤에만 커밋 (리스너가 ack)
    listener:
      ack-mode: manual_immediate
```
파일 끝에 추가:
```yaml

transfer:
  outbox:
    relay:
      enabled: true      # 폴링 퍼블리셔 스케줄 (테스트는 끄고 직접 부른다)
      interval-ms: 500
      batch-size: 100
```

- [ ] **Step 3: 테스트 컨테이너와 공유 테스트 설정**

`KafkaTestContainer.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.support

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.kafka.KafkaContainer

/** 통합 테스트용 Kafka. infra/docker-compose.yml 과 같은 apache/kafka:4.1.0 (KRaft). spring.kafka.bootstrap-servers 를 바꿔 준다. */
@TestConfiguration(proxyBeanMethods = false)
class KafkaTestContainer {

    @Bean
    @ServiceConnection
    fun kafka(): KafkaContainer = KafkaContainer("apache/kafka:4.1.0")
}
```
`ApiIntegrationTest.kt`:
- `@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)` 를 `@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = ["transfer.outbox.relay.enabled=false"])` 로
- `@Import(MySqlTestContainer::class)` 를 `@Import(MySqlTestContainer::class, KafkaTestContainer::class)` 로
- KDoc 끝에 한 줄 추가: ` * 폴링 퍼블리셔 스케줄은 끈다: 테스트가 OutboxRelay.publishPending() 을 직접 불러 발행 시점을 정한다 (다른 테스트의 이체 상태가 갑자기 바뀌지 않게).`

- [ ] **Step 4: 실패하는 테스트**

`apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/KafkaSetupTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import kotlin.test.Test
import kotlin.test.assertEquals

class KafkaSetupTest : ApiIntegrationTest() {

    @Autowired private lateinit var kafka: KafkaTemplate<String, String>
    @Autowired private lateinit var admin: KafkaAdmin

    @Test
    fun `프로듀서는 acks=all, 멱등 프로듀서로 설정된다 (설정이 실제로 적용됐는지)`() {
        val config = kafka.producerFactory.configurationProperties

        assertEquals("all", config["acks"].toString())
        assertEquals("true", config["enable.idempotence"].toString())
    }

    @Test
    fun `transfer_requested 토픽이 파티션 3개로 만들어진다`() {
        val description = admin.describeTopics(TransferTopics.REQUESTED).getValue(TransferTopics.REQUESTED)

        assertEquals(3, description.partitions().size)
    }
}
```

- [ ] **Step 5: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*KafkaSetupTest*'`
Expected: FAIL — `Unresolved reference 'TransferTopics'`

- [ ] **Step 6: 구현**

`MessagingConfiguration.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.messaging

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

object TransferTopics {
    /** 출금 접수 신호. key = 이체 id → 같은 이체의 메시지는 같은 파티션(순서 보장). */
    const val REQUESTED = "transfer.requested"
}

/** Kafka 쪽 조립. 토픽은 코드로 선언한다 (브로커의 자동 생성은 꺼 두었다 — 오타 토픽 방지). */
@Configuration(proxyBeanMethods = false)
class MessagingConfiguration {

    @Bean
    fun transferRequestedTopic(): NewTopic = TopicBuilder.name(TransferTopics.REQUESTED).partitions(3).replicas(1).build()
}
```

- [ ] **Step 7: 통과 확인**

Run: `./gradlew :apps:api:test` → 기존 테스트 전부 + KafkaSetupTest 2 PASS (첫 실행은 `apache/kafka:4.1.0` 이미지 다운로드)

- [ ] **Step 8: 커밋**

```bash
git add apps/api
git commit -m "feat(api): Kafka 설정 (acks=all · 멱등 프로듀서 · 수동 커밋) 과 transfer.requested 토픽, 테스트 Kafka 컨테이너

Co-Authored-By: <작성 모델>"
```

---

### Task 5: 폴링 퍼블리셔 `OutboxRelay`

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/OutboxRelay.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/MessagingConfiguration.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/OutboxRelayTest.kt`

**Interfaces:**
- Consumes: `TransferTopics.REQUESTED` (Task 4), `TransferStore.saveWithEvent` (P3-1)
- Produces: `class OutboxRelay(jdbc: JdbcTemplate, tx: TransactionTemplate, kafka: KafkaTemplate<String, String>, batchSize: Int)` · `fun publishPending(): Int`, `class OutboxRelayScheduler(relay: OutboxRelay)`, `@ConfigurationProperties("transfer.outbox.relay") data class OutboxRelayProperties(enabled, intervalMs, batchSize)`

- [ ] **Step 1: 실패하는 테스트**

`OutboxRelayTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.ConsumerFactory
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class OutboxRelayTest : ApiIntegrationTest() {

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var consumerFactory: ConsumerFactory<String, String>

    private val references = RandomReferenceGenerator()

    /** 계좌는 모의 은행에 없는 번호라 컨슈머가 처리하면 FAILED 가 된다 (이 테스트는 발행만 본다). */
    private fun saveTransfer(): Transfer {
        val now = Instant.now()
        val t = Transfer(
            id = UUID.randomUUID(), idempotencyKey = UUID.randomUUID().toString(), reference = references.next(),
            requestHash = "h".repeat(64), bankCode = "090", accountNo = "5500000000000", amount = 1_000, memo = "relay",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        return store.saveWithEvent(t, TransferEvent.TransferRequested(t.id, now))
    }

    private fun pendingCount(): Int =
        jdbc.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL", Int::class.java)!!

    private fun publishAll(): Int {
        var total = 0
        while (true) {
            val n = relay.publishPending()
            if (n == 0) return total
            total += n
        }
    }

    @Test
    fun `미발행 이벤트를 key = 이체 id 로 발행하고 published_at 을 기록한다`() {
        val transfer = saveTransfer()

        publishAll()

        val publishedAt = jdbc.queryForObject(
            "SELECT published_at FROM outbox_event WHERE aggregate_id = ?", java.sql.Timestamp::class.java, transfer.id.toString(),
        )
        assertNotNull(publishedAt)

        // 별도 그룹의 소비자로 토픽을 처음부터 읽어 메시지 모양을 확인한다
        consumerFactory.createConsumer("relay-test-${UUID.randomUUID()}", null).use { consumer ->
            consumer.subscribe(listOf(TransferTopics.REQUESTED))
            val seen = mutableListOf<ConsumerRecord<String, String>>()
            await.atMost(Duration.ofSeconds(20)) untilAsserted {
                seen += consumer.poll(Duration.ofMillis(500))
                val record = seen.single { it.key() == transfer.id.toString() }
                assertTrue("\"transferId\": \"${transfer.id}\"" in record.value() || "\"transferId\":\"${transfer.id}\"" in record.value(), record.value())
                assertTrue("\"eventId\"" in record.value(), record.value())
            }
        }
    }

    @Test
    fun `발행기 두 개가 동시에 돌아도 이벤트마다 한 번만 발행한다 (SKIP LOCKED)`() {
        repeat(20) { saveTransfer() }
        val pending = pendingCount()

        val pool = Executors.newFixedThreadPool(2)
        val published = List(2) { pool.submit(Callable { publishAll() }) }.sumOf { it.get() }
        pool.shutdown()

        assertEquals(pending, published)   // 같은 행을 두 번 보냈다면 합계가 더 크다
        assertEquals(0, pendingCount())
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*OutboxRelayTest*'`
Expected: FAIL — `Unresolved reference 'OutboxRelay'`

- [ ] **Step 3: 구현**

`OutboxRelay.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.messaging

import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.transaction.support.TransactionTemplate
import java.util.concurrent.TimeUnit

@ConfigurationProperties("transfer.outbox.relay")
data class OutboxRelayProperties(
    val enabled: Boolean = true,
    val intervalMs: Long = 500,
    val batchSize: Int = 100,
)

/**
 * 폴링 퍼블리셔: outbox_event 의 미발행 행을 Kafka 로 보내고 published_at 을 기록한다.
 * - FOR UPDATE SKIP LOCKED: 앱이 여러 대여도 같은 행을 두 번 보내지 않고, 서로 기다리지도 않는다.
 * - 브로커가 받았다고 확인한(get) 뒤에 published_at 을 쓴다. 그 사이에 죽으면 다음 주기에 다시 보낸다 → 적어도 한 번.
 *   중복은 컨슈머의 조건부 상태 변경(RECEIVED 일 때만 SENDING)이 막는다.
 * - 메시지 key = 이체 id, value = payload 에 eventId(outbox_event.id)를 붙인 JSON.
 */
class OutboxRelay(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
    private val kafka: KafkaTemplate<String, String>,
    private val batchSize: Int,
) {

    /** 최대 batchSize 건을 발행하고, 발행한 건수를 돌려준다. */
    fun publishPending(): Int =
        tx.execute {
            val events = jdbc.query(
                """
                SELECT id, aggregate_id, JSON_SET(payload, '$.eventId', id) AS message
                  FROM outbox_event
                 WHERE published_at IS NULL
                 ORDER BY id
                 LIMIT ?
                   FOR UPDATE SKIP LOCKED
                """.trimIndent(),
                { rs, _ -> PendingEvent(rs.getLong("id"), rs.getString("aggregate_id"), rs.getString("message")) },
                batchSize,
            )
            events.forEach { event ->
                kafka.send(TransferTopics.REQUESTED, event.aggregateId, event.message).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                jdbc.update("UPDATE outbox_event SET published_at = NOW(6) WHERE id = ?", event.id)
            }
            events.size
        }!!

    private data class PendingEvent(val id: Long, val aggregateId: String, val message: String)

    private companion object {
        const val SEND_TIMEOUT_SECONDS = 5L
    }
}

/** 스케줄 실행은 따로 둔다: 테스트는 이 빈을 끄고 OutboxRelay.publishPending() 을 직접 부른다. */
class OutboxRelayScheduler(private val relay: OutboxRelay) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${transfer.outbox.relay.interval-ms:500}")
    fun run() {
        try {
            relay.publishPending()
        } catch (e: Exception) {
            // 브로커 장애 등. 트랜잭션은 롤백됐고 published_at 도 그대로라 다음 주기에 다시 보낸다.
            log.warn("outbox relay failed, retrying next round: {}", e.message)
        }
    }
}
```

`MessagingConfiguration.kt` — 클래스에 `@EnableScheduling`, `@EnableConfigurationProperties(OutboxRelayProperties::class)` 를 붙이고 빈 추가:
```kotlin
    @Bean
    fun outboxRelay(
        jdbc: JdbcTemplate,
        tx: TransactionTemplate,
        kafka: KafkaTemplate<String, String>,
        props: OutboxRelayProperties,
    ) = OutboxRelay(jdbc, tx, kafka, props.batchSize)

    @Bean
    @ConditionalOnProperty(prefix = "transfer.outbox.relay", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun outboxRelayScheduler(relay: OutboxRelay) = OutboxRelayScheduler(relay)
```
import: `org.springframework.boot.autoconfigure.condition.ConditionalOnProperty`, `org.springframework.boot.context.properties.EnableConfigurationProperties`, `org.springframework.jdbc.core.JdbcTemplate`, `org.springframework.kafka.core.KafkaTemplate`, `org.springframework.scheduling.annotation.EnableScheduling`, `org.springframework.transaction.support.TransactionTemplate`.
(`BankTransferApplication` 에 `@ConfigurationPropertiesScan` 이 있으므로 `@EnableConfigurationProperties` 가 중복이면 빼도 된다 — 빈 생성 오류가 나는 쪽을 버린다.)

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*OutboxRelayTest*'` → PASS (2 tests)

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 폴링 퍼블리셔 — SKIP LOCKED 로 미발행 아웃박스를 Kafka 에 발행 (적어도 한 번)

Co-Authored-By: <작성 모델>"
```

---

### Task 6: 컨슈머 · 은행 어댑터 · 처리 흐름

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/BankTransferAdapter.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/TransferRequestedListener.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/MessagingConfiguration.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/TransferRequestedListenerTest.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/TransferProcessingFlowTest.kt`

**Interfaces:**
- Consumes: `TransferProcessor`, `ProcessResult`, `BankTransferPort`, `BankTransferOutcome` (Task 2), `BankNotSentException` (Task 1), `OutboxRelay` (Task 5), `TransferTopics` (Task 4)
- Produces: `class BankTransferAdapter(client: BankGatewayClient) : BankTransferPort`, `class TransferRequestedListener(process: (UUID) -> ProcessResult)` · `fun on(record: ConsumerRecord<String, String>, ack: Acknowledgment)`, `class RetryLaterException(transferId: UUID)`

- [ ] **Step 1: 실패하는 테스트**

`TransferRequestedListenerTest.kt` (Spring 없이):
```kotlin
package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.transfer.ProcessResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.support.Acknowledgment
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class TransferRequestedListenerTest {

    private class CountingAck : Acknowledgment {
        var count = 0
        override fun acknowledge() { count++ }
    }

    private fun record(id: UUID) = ConsumerRecord(TransferTopics.REQUESTED, 0, 0L, id.toString(), """{"transferId":"$id"}""")

    @Test
    fun `처리가 끝나면 ack 한다`() {
        val id = UUID.randomUUID()
        val ack = CountingAck()
        var processed: UUID? = null

        TransferRequestedListener { processed = it; ProcessResult.COMPLETED }.on(record(id), ack)

        assertEquals(id, processed)
        assertEquals(1, ack.count)
    }

    @Test
    fun `이미 처리된 메시지도 ack 한다 (다시 받지 않게)`() {
        val ack = CountingAck()

        TransferRequestedListener { ProcessResult.ALREADY_HANDLED }.on(record(UUID.randomUUID()), ack)

        assertEquals(1, ack.count)
    }

    @Test
    fun `은행에 못 보냈으면 ack 하지 않고 예외를 던져 재시도하게 한다`() {
        val ack = CountingAck()

        assertFailsWith<RetryLaterException> {
            TransferRequestedListener { ProcessResult.RETRY_LATER }.on(record(UUID.randomUUID()), ack)
        }
        assertEquals(0, ack.count)
    }
}
```

`TransferProcessingFlowTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * 접수(HTTP) → 아웃박스 → 발행(relay) → 컨슈머 → 은행(모의, TCP) → 상태 기록 까지 한 번에.
 * 모의 은행: 1002123456789 는 처리(0000), 등록 안 된 계좌는 1001, 9990000000001 은 무응답.
 */
class TransferProcessingFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var verifications: VerificationRepository
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var kafka: KafkaTemplate<String, String>

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

    /** 접수하고 이체 id 를 돌려준다. 적요는 테스트마다 달라서 모의 은행 입금 내역에서 이 건만 셀 수 있다. */
    private fun request(accountNo: String, memo: String): UUID {
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to 30_000, "memo" to memo))
            .retrieve().body(mapType)!!
        return UUID.fromString(body["id"] as String)
    }

    private fun awaitStatus(id: UUID, expected: TransferStatus) {
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(expected, store.findById(id)!!.status)
        }
    }

    private fun uniqueMemo() = "F" + Random.nextInt(100_000, 999_999)

    @Test
    fun `인증 계좌로 접수하면 은행 처리 후 COMPLETED, 은행 입금은 1건`() {
        val account = "1002123456789"
        verify(account)
        val memo = uniqueMemo()

        val id = request(account, memo)
        awaitStatus(id, TransferStatus.COMPLETED)

        assertEquals("0000", store.findById(id)!!.bankResponseCode)
        assertEquals(1, bank.depositsOf(account).count { it.memo == memo })
    }

    @Test
    fun `은행에 없는 계좌면 FAILED 와 응답코드 1001`() {
        val account = "55" + Random.nextLong(10_000_000_000, 99_999_999_999)
        verify(account)

        val id = request(account, uniqueMemo())
        awaitStatus(id, TransferStatus.FAILED)

        assertEquals("1001", store.findById(id)!!.bankResponseCode)
    }

    @Test
    fun `은행이 응답하지 않으면 UNKNOWN 이고 다시 보내지 않는다`() {
        val account = "9990000000001"
        verify(account)

        val id = request(account, uniqueMemo())
        awaitStatus(id, TransferStatus.UNKNOWN)

        assertNull(store.findById(id)!!.bankResponseCode)
    }

    @Test
    fun `같은 메시지가 두 번 와도 은행에는 한 번만 보낸다`() {
        val account = "1002123456789"
        verify(account)
        val memo = uniqueMemo()
        val id = request(account, memo)
        awaitStatus(id, TransferStatus.COMPLETED)

        // 발행기가 커밋 직전에 죽어 같은 메시지를 다시 보낸 상황을 흉내 낸다
        kafka.send(TransferTopics.REQUESTED, id.toString(), """{"transferId":"$id"}""").get(5, TimeUnit.SECONDS)

        await.during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)) untilAsserted {
            assertEquals(1, bank.depositsOf(account).count { it.memo == memo })
        }
        assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*TransferRequestedListenerTest*' --tests '*TransferProcessingFlowTest*'`
Expected: FAIL — `Unresolved reference 'TransferRequestedListener'`, `'RetryLaterException'`

- [ ] **Step 3: 구현**

`BankTransferAdapter.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.transfer.BankTransferOutcome
import io.github.hyeokjinon.banktransfer.transfer.BankTransferPort
import java.util.concurrent.ExecutionException

/**
 * transfer 모듈의 BankTransferPort 를 Netty 게이트웨이에 연결한다. 예외를 "보냈나 / 모르나"로 분류하는 곳이 여기다.
 * - BankNotSentException (연결 없음): 은행에 가지 않았다 → NotSent (다시 보내도 안전)
 * - 그 밖의 실패 (응답 시간 초과, 보낸 뒤 연결 끊김, 쓰기 실패 …): 은행이 받았을 수 있다 → ResultUnknown (다시 보내지 않는다)
 * 응답 시간 초과는 게이트웨이(bank.gateway.response-timeout)가 건다.
 */
class BankTransferAdapter(private val client: BankGatewayClient) : BankTransferPort {

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): BankTransferOutcome =
        try {
            val response = client.transfer(bankCode, accountNo, amount, memo).get()
            if (response.header.isOk) BankTransferOutcome.Completed else BankTransferOutcome.Rejected(response.header.responseCode)
        } catch (e: ExecutionException) {
            if (e.cause is BankNotSentException) BankTransferOutcome.NotSent else BankTransferOutcome.ResultUnknown
        }
}
```

`TransferRequestedListener.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.transfer.ProcessResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import java.util.UUID

/** 은행에 보내지 못해 나중에 다시 처리해야 한다. 에러 핸들러가 이 예외를 보고 재시도한다. */
class RetryLaterException(transferId: UUID) : RuntimeException("bank not reachable, transfer $transferId will be retried")

/**
 * transfer.requested 컨슈머. key = 이체 id. 처리가 끝난 뒤에만 오프셋을 커밋한다 (수동 ack):
 * 처리 중에 죽으면 같은 메시지를 다시 받고, 조건부 상태 변경이 중복 처리를 막는다.
 * 은행에 보내지 못한 경우(RETRY_LATER)만 ack 하지 않고 예외를 던진다.
 */
class TransferRequestedListener(private val process: (UUID) -> ProcessResult) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = [TransferTopics.REQUESTED], groupId = "transfer-processor", concurrency = "3")
    fun on(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val transferId = UUID.fromString(record.key())
        val result = process(transferId)
        if (result == ProcessResult.RETRY_LATER) throw RetryLaterException(transferId)
        log.info("transfer {} processed: {} (partition {}, offset {})", transferId, result, record.partition(), record.offset())
        ack.acknowledge()
    }
}
```

`MessagingConfiguration.kt` 에 빈 추가 (import: `io.github.hyeokjinon.banktransfer.api.config.BankTransferAdapter`, `io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient`, `io.github.hyeokjinon.banktransfer.transfer.TransferProcessor`, `io.github.hyeokjinon.banktransfer.transfer.TransferStore`, `org.springframework.kafka.listener.DefaultErrorHandler`, `org.springframework.kafka.support.ExponentialBackOffWithMaxRetries`, `java.time.Clock`):
```kotlin
    @Bean
    fun transferProcessor(store: TransferStore, client: BankGatewayClient) =
        TransferProcessor(store, BankTransferAdapter(client), Clock.systemUTC())

    @Bean
    fun transferRequestedListener(processor: TransferProcessor) = TransferRequestedListener(processor::process)

    /**
     * 컨슈머 예외 처리 (P3-2): 1초부터 두 배씩, 최대 10초 간격으로 5번 다시 시도한 뒤 로그를 남기고 넘어간다.
     * 그동안 같은 파티션의 다음 메시지는 기다린다 (블로킹 재시도).
     * 다 실패한 이체는 RECEIVED 로 남는다 → P3-3 에서 재시도 토픽 + DLT 로 바꾼다.
     */
    @Bean
    fun kafkaErrorHandler(): DefaultErrorHandler =
        DefaultErrorHandler(
            ExponentialBackOffWithMaxRetries(5).apply {
                initialInterval = 1_000
                multiplier = 2.0
                maxInterval = 10_000
            },
        )
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*TransferRequestedListenerTest*' --tests '*TransferProcessingFlowTest*'` → PASS (3 + 4)
그다음 `./gradlew build` 전체 PASS. 실패하면 특히 P3-1 테스트 중 "조회 직후 상태가 RECEIVED" 를 가정한 것이 없는지 확인한다 (릴레이 스케줄이 꺼져 있으므로 테스트가 publishPending 을 부르기 전에는 처리되지 않아야 한다).

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 출금 처리 컨슈머 (수동 ack · 블로킹 재시도) 와 은행 어댑터 — 접수부터 COMPLETED/FAILED/UNKNOWN 까지

Co-Authored-By: <작성 모델>"
```

---

### Task 7: API 오류 응답 정리 — `INVALID_REQUEST`, `ILLEGAL_TRANSITION`

**Files:**
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/ApiExceptionHandler.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/TransferApiTest.kt`, `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/AccountVerificationApiTest.kt`

**Interfaces:**
- Produces: Spring MVC 표준 예외(본문 JSON 깨짐 · 검증 실패 · 경로 변수 타입 오류 등)의 ProblemDetail 에 `code` 를 붙인다 — 400 이면 `INVALID_REQUEST`, 그 밖은 `HttpStatus` 이름. `TransferException.IllegalTransition` → 409 `ILLEGAL_TRANSITION`.

**왜:** 지금은 형식 오류 400 에 `code` 가 없어 클라이언트가 400 응답 모양을 두 가지로 처리해야 한다 (P3-1 리뷰 판단에서 미뤘던 항목).

- [ ] **Step 1: 실패하는 테스트**

`TransferApiTest.kt` 에 추가 (`postRaw`, `get`, `mapType` 는 이미 있다):
```kotlin
    @Test
    fun `JSON 이 깨지면 400 INVALID_REQUEST`() {
        val response = postRaw(UUID.randomUUID().toString(), """{"bankCode": "090", """)

        assertEquals(400, response.status)
        assertEquals("INVALID_REQUEST", response.body["code"])
    }

    @Test
    fun `조회 id 가 UUID 가 아니면 400 INVALID_REQUEST`() {
        val response = get("not-a-uuid")

        assertEquals(400, response.status)
        assertEquals("INVALID_REQUEST", response.body["code"])
    }
```
`AccountVerificationApiTest.kt` 의 `형식이 틀리면 400` 을 다음으로 바꾼다:
```kotlin
    @Test
    fun `형식이 틀리면 400 INVALID_REQUEST`() {
        val (status, body) = post(
            "/api/v1/account-verifications",
            mapOf("bankCode" to "9", "accountNo" to "abc", "holderName" to ""),
        )
        assertEquals(400, status)
        assertEquals("INVALID_REQUEST", body["code"])
    }
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*TransferApiTest*' --tests '*AccountVerificationApiTest*'`
Expected: FAIL — `expected: <INVALID_REQUEST> but was: <null>`

- [ ] **Step 3: 구현**

`ApiExceptionHandler` 가 `ResponseEntityExceptionHandler` 를 상속하게 하고 공통 처리 지점에서 `code` 를 붙인다 (Spring Boot 의 기본 ProblemDetail 핸들러는 이 타입의 빈이 있으면 물러난다):
```kotlin
@RestControllerAdvice
class ApiExceptionHandler : ResponseEntityExceptionHandler() {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Spring MVC 가 던지는 표준 예외(본문 JSON 깨짐, 검증 실패, 경로 변수 타입 오류 …)의 ProblemDetail 에도 code 를 붙인다.
     * 클라이언트는 모든 오류를 code 하나로 분기할 수 있다.
     */
    override fun handleExceptionInternal(
        ex: Exception,
        body: Any?,
        headers: HttpHeaders,
        statusCode: HttpStatusCode,
        request: WebRequest,
    ): ResponseEntity<Any>? {
        if (body is ProblemDetail && body.properties?.containsKey("code") != true) {
            body.setProperty("code", if (statusCode.value() == 400) "INVALID_REQUEST" else HttpStatus.resolve(statusCode.value())?.name ?: "ERROR")
        }
        return super.handleExceptionInternal(ex, body, headers, statusCode, request)
    }
```
(Kotlin 시그니처는 Spring 7 의 nullability 에 맞춰 컴파일러가 요구하는 대로 조정한다 — 예: `ResponseEntity<Any>?`.)
기존 `@ExceptionHandler(...) fun handle(e: Exception)` 은 그대로 두고, `when` 에 한 줄 추가:
```kotlin
            is TransferException.IllegalTransition -> HttpStatus.CONFLICT to "ILLEGAL_TRANSITION"
```
import 추가: `org.springframework.http.HttpHeaders`, `org.springframework.http.HttpStatusCode`, `org.springframework.http.ResponseEntity`, `org.springframework.web.context.request.WebRequest`, `org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler`.

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test` → 전부 PASS (기존 형식 오류 · 소수 금액 400 테스트 포함)

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 요청 형식 오류에도 code INVALID_REQUEST, 잘못된 상태 전이는 409 ILLEGAL_TRANSITION

Co-Authored-By: <작성 모델>"
```

---

### Task 8: 결정 기록 · 손 확인 · 문서

**Files:**
- Create: `docs/adr/0005-kafka-delivery-and-consumer-idempotency.md`
- Modify: `http/transfer.http`, `postman/collections/bank-transfer/2-transfer-intake/` (요청 1개 추가), `docs/database.md`, `README.md`, `ARCHITECTURE.md`, `OPERATION.md`, `SETUP.md`, `CHANGELOG.md`, `HANDOVER.md`

- [ ] **Step 1: ADR-0005**

`docs/adr/0005-kafka-delivery-and-consumer-idempotency.md` — 기존 ADR 형식(상태 · 단계 · 맥락 · 결정 · 결과 · 검토한 다른 선택지)으로:
- 상태: 제안 (날짜) — PR 리뷰 후 채택. 단계 P3-2.
- 맥락: 아웃박스 발행은 적어도 한 번, 은행 호출은 되돌릴 수 없음, 예전 시스템은 결과조회 · 대사로 맞춤.
- 결정: (1) 폴링 퍼블리셔 + SKIP LOCKED + 발행 확인 후 published_at (2) 프로듀서 acks=all · 멱등 (3) key = 이체 id, 파티션 3, 컨슈머 3 (4) 수동 ack, 처리 후 커밋 (5) 컨슈머 멱등 = 상태 조건부 UPDATE (processed_event 미사용 이유) (6) 은행 결과 4분류, 결과 모름은 재전송 금지, 미전송만 재시도 (게이트웨이 BankNotSentException) (7) 은행 호출은 트랜잭션 밖 (8) P3-2 재시도는 블로킹 지수 백오프 5회 후 로그 → P3-3 재시도 토픽 · DLT.
- 결과: 증명 테스트 이름(같은 메시지 두 번 → 입금 1건, 발행기 2개 → 이벤트당 1회, 프로듀서 설정 확인, UNKNOWN 재전송 없음). 한계: 재시도를 다 쓰면 RECEIVED 로 남음, UNKNOWN 은 P3-3 전까지 그대로, 컨슈머 스레드가 은행 응답을 블로킹으로 기다림(P4 측정 · P6/P7 비교).
- 다른 선택지: Debezium(P8 비교), Kafka 트랜잭션(DB 와 원자성 안 됨), processed_event 테이블(상태 있는 도메인엔 중복), @RetryableTopic 바로 도입(P3-3 범위).

- [ ] **Step 2: 손 확인 요청 추가**

`http/transfer.http` 의 2) 아래에 추가:
```http
### 2-1) 처리 결과 → 몇 초 안에 COMPLETED (P3-2 부터: 아웃박스 → Kafka → 은행)
GET http://localhost:8080/api/v1/transfers/{{transferId}}
```
`postman/collections/bank-transfer/2-transfer-intake/09-processed.request.yaml`:
```yaml
$kind: http-request
name: 2-9 처리 결과 → COMPLETED (2-1 후 몇 초 뒤)
method: GET
url: '{{baseUrl}}/api/v1/transfers/{{transferId}}'
order: 9000
scripts:
  - type: afterResponse
    language: text/javascript
    code: |-
      pm.test("COMPLETED, 은행 응답코드 0000 (아웃박스 → Kafka → 은행)", () => {
        pm.response.to.have.status(200);
        pm.expect(pm.response.json().status).to.eql("COMPLETED");
        pm.expect(pm.response.json().bankResponseCode).to.eql("0000");
      });
```
그리고 `postman collection lint postman/collections/bank-transfer` 로 오류 0 확인 (CLI 가 없으면 생략하고 보고서에 적는다).

- [ ] **Step 3: 문서**

- `docs/database.md`: §6 동시성 표에서 "(P3-2 예정)" 표기 두 개를 지운다. `outbox_event` 설명에 발행 메시지 모양(`JSON_SET(payload, '$.eventId', id)`, key = aggregate_id)과 `published_at = NOW(6)`(KST) 추가. §9 운영 쿼리에 `-- 처리 상태별 건수` 와 `-- 발행 지연: 미발행 중 가장 오래된 것` (`SELECT MIN(created_at) FROM outbox_event WHERE published_at IS NULL;`) 추가. 변경 이력 한 줄.
- `ARCHITECTURE.md`: §6 을 "P3-1 접수 ✅, P3-2 처리 ✅(ADR-0005), P3-3 예정" 으로, 접수 → 아웃박스 → 릴레이 → `transfer.requested` → 컨슈머 → 은행 → 상태 기록 흐름을 mermaid sequenceDiagram 으로 추가 (실제 클래스 이름: `TransferController`, `JpaTransferStore`, `OutboxRelay`, `TransferRequestedListener`, `TransferProcessor`, `BankTransferAdapter`). 모듈 표 · 소스 지도에 `api/messaging/` 과 `TransferProcessor` 추가. ADR 색인에 0005.
- `OPERATION.md`: §1 구성 요소 표 API 행에 "Kafka 필요"; §2 설정 표에 `spring.kafka.bootstrap-servers`(`KAFKA_BOOTSTRAP_SERVERS`), `transfer.outbox.relay.*` 3개; 새 절 "Kafka" — 토픽 `transfer.requested`(파티션 3), 그룹 `transfer-processor`, kafka-ui(`--profile tools`, http://localhost:8089) 에서 메시지 · 랙 보기; §4 오류 코드 표에 `INVALID_REQUEST`(400, 요청 형식), `ILLEGAL_TRANSITION`(409); §6 장애 대응에 `UNKNOWN 이 남아 있음 → P3-3 결과조회 전까지 그대로 (재전송 금지)`, `RECEIVED 에서 안 넘어감 → 릴레이 로그 · published_at · 컨슈머 랙 확인, 은행 연결 없음이면 재시도 5회 후 남는다 (P3-3 에서 DLT)` 행 추가. "형식 오류 400 (code 없음)" 행을 `INVALID_REQUEST` 로 고친다.
- `SETUP.md`: 인프라 안내에 "API 는 MySQL 과 Kafka 를 쓴다" 로.
- `README.md`: "지금 되는 것"에 P3-2 한 줄, 로드맵 `[x] P3-2`, 테스트 수(Task 7 끝의 실제 합계)로 갱신.
- `CHANGELOG.md`: `## [P3-2] 출금이체 처리 — <날짜> (PR #N)` 절 (Added / Changed: `BankNotSentException`, 형식 오류 code).
- `HANDOVER.md`: 마지막 갱신일, P3-2 행 🟡 PR 리뷰 중, "P3-2 로 넘기는 것" 에서 처리한 항목 지우고 남은 것(UNIQUE 메시지 의존 · 데드락 500, WARN 로그) 유지, 다음 할 일에 P3-3.

- [ ] **Step 4: 전체 빌드**

Run: `./gradlew build --rerun-tasks` → BUILD SUCCESSFUL. 모든 모듈의 `build/test-results/test/*.xml` 의 `tests=` 합계를 보고서에 적고 README · HANDOVER 의 테스트 수를 그 값으로 맞춘다.

- [ ] **Step 5: 커밋**

```bash
git add docs http postman README.md ARCHITECTURE.md OPERATION.md SETUP.md CHANGELOG.md HANDOVER.md
git commit -m "docs: P3-2 처리 — ADR-0005, 손 확인 요청, DB · 운영 · 구조 문서 갱신

Co-Authored-By: <작성 모델>"
```
push 와 PR 은 하지 않는다 (최종 리뷰 후 컨트롤러가 한다).
