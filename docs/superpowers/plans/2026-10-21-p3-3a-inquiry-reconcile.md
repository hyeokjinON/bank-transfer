# P3-3a 결과조회와 정리 작업 (전문 v0.2) — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 결과를 모르는 출금(`UNKNOWN`, 30초 넘게 멈춘 `SENDING`)을 은행에 **결과조회 전문**으로 물어 `COMPLETED / FAILED` 로 확정한다. 은행에 다시 보내지 않는다.

**Architecture:** 전문 규격 v0.2 — 이체 전문에 이용기관 거래번호 20자, 새 업무 `INQ` 결과조회. 도메인(`modules/transfer`)에 `TransferReconciler` 와 포트 `BankInquiryPort` 를 두고, `apps/api` 가 게이트웨이 어댑터 · 조회 쿼리 · 10초 스케줄을 맡는다. 정리 작업과 컨슈머가 같은 이체를 다툴 때는 은행이 직접 준 확정 결과가 이긴다.

**Tech Stack:** Kotlin 2.3.21 · Spring Boot 4.1.1 (Data JPA `Limit`, `@Scheduled`) · Netty 4.2 · MySQL 8.4 · Kafka 4.1 · Testcontainers 2.0

**Spec:** [docs/superpowers/specs/2026-10-19-p3-transfer-design.md](../specs/2026-10-19-p3-transfer-design.md) §3 상태 · §6 실패 처리 (규격 v0.2 · 정리 작업) · §7 테스트 · HANDOVER.md "P3-3 로 넘기는 것"
2026-10-21 사용자 결정: P3-3 을 a(규격 v0.2 · 결과조회 · 정리 작업) / b(재시도 토픽 · DLT · 오래된 RECEIVED 재발행)로 나눈다. 1원인증 이체의 거래번호는 API 어댑터가 `V` 접두로 만든다(저장하지 않음).

## Global Constraints

- 도메인 모듈(`modules/account`, `modules/transfer`)은 Spring · JPA · Netty · Kafka 의존성 없음.
- **결과를 모르면 다시 보내지 않는다.** 정리 작업은 은행에 **묻기만** 한다 (`BankInquiryPort`). `BankTransferPort` 를 쓰지 않는다.
- 전문 v0.2 이체 개별부 = 은행코드 3 + 계좌번호 16 + 금액 13 + 적요 20 + **이용기관 거래번호 20** = 72바이트. 결과조회 `INQ`: 요청 = 거래번호 20, 응답 = 거래번호 20 + 처리결과 4 (`0000` 처리됨 / `2001` 원거래 없음). 공통부 응답코드는 조회 전문 자체의 성공 여부다.
- 출금이체는 저장된 `transfer.reference`(T…)를 보낸다. 1원인증 이체는 API 어댑터가 `RandomReferenceGenerator(prefix = 'V')` 로 만든 번호를 보낸다.
- 정리 작업(10초 고정 지연): ① `status = SENDING` 이고 `sending_at` 이 30초보다 오래된 건 → `UNKNOWN` (`sendingAt != null` 로 고르지 않는다) ② `UNKNOWN` 이고 `inquiry_attempts < 10` 인 건마다 결과조회 → 처리됨 `COMPLETED`(0000) / 처리 안 됨 `FAILED`(은행 코드) / 모름 `inquiry_attempts + 1`, 10번째에 멈추고 경고 (일마감 대사 대상). 한 번에 100건.
- 모든 상태 변경은 조건부 (`TransferStore.transition`). 정리 작업이 먼저 `UNKNOWN` 으로 바꾼 뒤 컨슈머가 은행의 **확정 결과(완료 · 거절)** 를 받으면 `UNKNOWN → COMPLETED/FAILED` 로 기록한다. 그 밖에는 로그만 남기고 넘어간다 (`check()` 로 던지지 않는다).
- 스케줄러 스레드 2개 (`spring.task.scheduling.pool.size: 2`) — 발행기와 정리 작업이 서로 막지 않게.
- 모의 은행: `9990000000001` = 무응답 · 처리 안 함 (기존). `9990000000002` = **처리하고 응답만 안 함** (1원 이체는 정상 응답 — 1원인증으로 수취 계좌 등록이 되게). 처리한 이체의 거래번호를 기억해 결과조회에 답한다.
- DB 시각 KST, `toDbPrecision()`. 스키마 변경 없음 (`inquiry_attempts`, `idx_transfer_status(status, sending_at)` 는 V1 에 있다).
- 주석 · 커밋 한국어 "왜". 커밋 `type(scope): 요약` + 작성 모델 이름의 `Co-Authored-By:` 줄. push 하지 않는다.
- 테스트: Docker Desktop 필요, Git Bash 에서 `export PATH="/c/Users/user/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"`. 통합 테스트는 `ApiIntegrationTest` 상속(컨텍스트 공유), 정리 작업 · 발행기 스케줄은 테스트에서 꺼 두고 직접 부른다.
- **Task 1 · 2 를 마치기 전까지 `apps/api` 는 컴파일되지 않는다** (포트와 게이트웨이 시그니처가 바뀌기 때문). Task 1 은 `:modules:transfer:test`, Task 2 는 `:modules:bank-protocol:test :modules:bank-gateway:test` 만 돌린다. Task 3 부터 전체 빌드.

## 파일 구조

| 파일 | 책임 |
|---|---|
| `modules/transfer/.../Transfer.kt` (수정) | `inquiryAttempts`, `BankTransferPort.transfer(+reference)`, `InquiryOutcome`, `BankInquiryPort`, `TransferStore` 조회 3개 |
| `modules/transfer/.../TransferProcessor.kt` (수정) | 거래번호 전달, 경합 시 확정 결과 우선 기록 |
| `modules/transfer/.../TransferReconciler.kt` (신규) | 정리 작업 규칙 |
| `modules/bank-protocol/.../{BankMessage,Header,BankMessageSerializer}.kt` (수정) | v0.2 이체 · 결과조회 전문 |
| `modules/bank-gateway/.../BankGatewayClient.kt` (수정) | `transfer(+reference)`, `inquire(reference)` |
| `apps/mock-bank/.../MockBankServer.kt` (수정) | 거래번호 기억, 결과조회 응답, "처리 후 무응답" 계좌 |
| `apps/api/.../config/{AppConfiguration,BankTransferAdapter}.kt` (수정), `BankInquiryAdapter.kt` (신규) | 게이트웨이 연결 |
| `apps/api/.../persistence/TransferPersistence.kt` (수정) | 조회 쿼리 · 시도 횟수 증가 |
| `apps/api/.../reconcile/ReconcileConfiguration.kt` (신규) | 설정 · 스케줄 · 조립 |

---

### Task 1: 도메인 — 결과조회 포트와 정리 작업 규칙

**Files:**
- Modify: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/Transfer.kt`
- Modify: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferProcessor.kt`
- Create: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferReconciler.kt`
- Test: `modules/transfer/src/test/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferReconcilerTest.kt`
- Modify (tests): `TransferProcessorTest.kt`, `TransferServiceTest.kt` (FakeStore · FakeBank 시그니처)

**Interfaces:**
- Produces:
  - `Transfer.inquiryAttempts: Int = 0` (sendingAt 다음 필드)
  - `fun interface BankTransferPort { fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome }`
  - `sealed interface InquiryOutcome { data object Processed; data class NotProcessed(val responseCode: String); data object Unknown }`
  - `fun interface BankInquiryPort { fun inquire(reference: String): InquiryOutcome }`
  - `TransferStore.findSendingBefore(cutoff: Instant, limit: Int): List<Transfer>`, `findUnknownForInquiry(maxAttempts: Int, limit: Int): List<Transfer>`, `recordInquiryAttempt(id: UUID, at: Instant): Boolean`
  - `data class ReconcileResult(val markedUnknown: Int, val completed: Int, val failed: Int, val stillUnknown: Int, val gaveUp: Int)`
  - `class TransferReconciler(store, bank: BankInquiryPort, clock = Clock.systemUTC(), sendingTimeout = Duration.ofSeconds(30), maxInquiries = 10, batchSize = 100, onGiveUp: (Transfer) -> Unit = {})` · `fun reconcile(): ReconcileResult`

- [ ] **Step 1: 실패하는 테스트**

`TransferReconcilerTest.kt`:
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
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

class TransferReconcilerTest {

    /** 조건부 UPDATE 와 조회를 흉내 내는 가짜 저장소. */
    private class FakeStore : TransferStore {
        val transfers = linkedMapOf<UUID, Transfer>()

        override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer = transfer.also { transfers[it.id] = it }
        override fun findById(id: UUID) = transfers[id]
        override fun findByIdempotencyKey(key: String) = transfers.values.firstOrNull { it.idempotencyKey == key }

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean {
            val current = transfers[id] ?: return false
            if (current.status != from) return false
            transfers[id] = current.copy(
                status = to, updatedAt = at, bankResponseCode = bankResponseCode,
                sendingAt = if (to == SENDING) at else current.sendingAt,
            )
            return true
        }

        override fun findSendingBefore(cutoff: Instant, limit: Int) =
            transfers.values.filter { it.status == SENDING && it.sendingAt!!.isBefore(cutoff) }.take(limit)

        override fun findUnknownForInquiry(maxAttempts: Int, limit: Int) =
            transfers.values.filter { it.status == UNKNOWN && it.inquiryAttempts < maxAttempts }.take(limit)

        override fun recordInquiryAttempt(id: UUID, at: Instant): Boolean {
            val current = transfers[id] ?: return false
            if (current.status != UNKNOWN) return false
            transfers[id] = current.copy(inquiryAttempts = current.inquiryAttempts + 1, updatedAt = at)
            return true
        }
    }

    /** 거래번호별로 정해 둔 결과를 돌려주고 호출 횟수를 센다. */
    private class FakeInquiry : BankInquiryPort {
        val outcomes = mutableMapOf<String, () -> InquiryOutcome>()
        var calls = 0
        override fun inquire(reference: String): InquiryOutcome {
            calls++
            return outcomes.getValue(reference)()
        }
    }

    private val now = Instant.parse("2026-10-21T01:00:00Z")
    private val store = FakeStore()
    private val inquiry = FakeInquiry()
    private val gaveUp = mutableListOf<Transfer>()
    private fun reconciler(maxInquiries: Int = 10) = TransferReconciler(
        store, inquiry, Clock.fixed(now, ZoneOffset.UTC), Duration.ofSeconds(30), maxInquiries, 100, onGiveUp = { gaveUp += it },
    )

    private var seq = 0
    private fun transfer(status: TransferStatus, sendingAt: Instant? = null): Transfer {
        val t = Transfer(
            id = UUID.randomUUID(), idempotencyKey = "k${++seq}", reference = "T26102100000000000%02d".format(seq),
            requestHash = "h", bankCode = "090", accountNo = "1002123456789", amount = 1_000, memo = "m",
            status = status, sendingAt = sendingAt, createdAt = now, updatedAt = now,
        )
        store.transfers[t.id] = t
        return t
    }

    @Test
    fun `30초 넘은 SENDING 만 UNKNOWN 으로 바꾸고 바로 결과조회한다`() {
        val stuck = transfer(SENDING, sendingAt = now.minusSeconds(60))
        val inFlight = transfer(SENDING, sendingAt = now.minusSeconds(10))
        inquiry.outcomes[stuck.reference] = { InquiryOutcome.Processed }

        val result = reconciler().reconcile()

        assertEquals(1, result.markedUnknown)
        assertEquals(1, result.completed)
        assertEquals(COMPLETED, store.transfers.getValue(stuck.id).status)
        assertEquals("0000", store.transfers.getValue(stuck.id).bankResponseCode)
        assertEquals(SENDING, store.transfers.getValue(inFlight.id).status)   // 아직 은행 응답을 기다리는 중일 수 있다
    }

    @Test
    fun `RECEIVED 는 sendingAt 이 남아 있어도 건드리지 않는다`() {
        // NotSent 로 되돌린 건: 은행에 안 갔으므로 결과조회 대상이 아니다
        val reverted = transfer(RECEIVED, sendingAt = now.minusSeconds(120))

        reconciler().reconcile()

        assertEquals(RECEIVED, store.transfers.getValue(reverted.id).status)
        assertEquals(0, inquiry.calls)
    }

    @Test
    fun `처리 안 됨이면 FAILED 와 은행 코드`() {
        val t = transfer(UNKNOWN)
        inquiry.outcomes[t.reference] = { InquiryOutcome.NotProcessed("2001") }

        val result = reconciler().reconcile()

        assertEquals(1, result.failed)
        assertEquals(FAILED, store.transfers.getValue(t.id).status)
        assertEquals("2001", store.transfers.getValue(t.id).bankResponseCode)
    }

    @Test
    fun `조회도 모르면 시도 횟수만 늘리고, 한도에 닿으면 멈추고 알린다`() {
        val t = transfer(UNKNOWN)
        inquiry.outcomes[t.reference] = { InquiryOutcome.Unknown }
        val reconciler = reconciler(maxInquiries = 3)

        repeat(4) { reconciler.reconcile() }

        assertEquals(UNKNOWN, store.transfers.getValue(t.id).status)
        assertEquals(3, store.transfers.getValue(t.id).inquiryAttempts)
        assertEquals(3, inquiry.calls)                 // 4번째에는 묻지 않는다
        assertEquals(listOf(t.id), gaveUp.map { it.id })
    }

    @Test
    fun `조회 중 예외도 모름으로 본다`() {
        val t = transfer(UNKNOWN)
        inquiry.outcomes[t.reference] = { throw IllegalStateException("boom") }

        val result = reconciler().reconcile()

        assertEquals(1, result.stillUnknown)
        assertEquals(1, store.transfers.getValue(t.id).inquiryAttempts)
    }

    @Test
    fun `그 사이 컨슈머가 먼저 확정했으면 바꾸지 않는다`() {
        val t = transfer(UNKNOWN)
        inquiry.outcomes[t.reference] = {
            store.transition(t.id, UNKNOWN, COMPLETED, now, "0000")   // 컨슈머가 은행의 늦은 응답으로 먼저 확정
            InquiryOutcome.NotProcessed("2001")
        }

        val result = reconciler().reconcile()

        assertEquals(0, result.failed)
        assertEquals(COMPLETED, store.transfers.getValue(t.id).status)
    }
}
```

`TransferProcessorTest.kt`:
- `FakeStore` 에 메서드 3개 추가:
```kotlin
        override fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer> = emptyList()
        override fun findUnknownForInquiry(maxAttempts: Int, limit: Int): List<Transfer> = emptyList()
        override fun recordInquiryAttempt(id: UUID, at: Instant) = false
```
- `FakeBank.transfer` 시그니처에 `reference: String` 을 추가하고 `var lastReference: String? = null` 에 기록.
- 이 파일 안의 다른 `BankTransferPort` 람다(예외를 던지는 테스트)도 매개변수 5개로 맞춘다.
- 테스트 추가 (클래스 끝):
```kotlin
    @Test
    fun `은행에는 이체의 이용기관 거래번호를 보낸다`() {
        val t = received()

        processor.process(t.id)

        assertEquals(t.reference, bank.lastReference)
    }

    @Test
    fun `정리 작업이 먼저 UNKNOWN 으로 바꿨어도 은행의 확정 결과로 기록한다`() {
        val t = received()
        val racing = BankTransferPort { _, _, _, _, _ ->
            store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, now)   // 30초 넘게 늦은 응답 사이에 정리 작업이 끼어듦
            BankTransferOutcome.Completed
        }

        assertEquals(ProcessResult.COMPLETED, TransferProcessor(store, racing, Clock.fixed(now, ZoneOffset.UTC)).process(t.id))
        assertEquals(TransferStatus.COMPLETED, store.transfers.getValue(t.id).status)
        assertEquals("0000", store.transfers.getValue(t.id).bankResponseCode)
    }

    @Test
    fun `정리 작업이 먼저 UNKNOWN 으로 바꿨고 결과도 모르면 그대로 둔다`() {
        val t = received()
        val racing = BankTransferPort { _, _, _, _, _ ->
            store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, now)
            BankTransferOutcome.ResultUnknown
        }

        assertEquals(ProcessResult.ALREADY_HANDLED, TransferProcessor(store, racing, Clock.fixed(now, ZoneOffset.UTC)).process(t.id))
        assertEquals(TransferStatus.UNKNOWN, store.transfers.getValue(t.id).status)
    }
```

`TransferServiceTest.kt` 의 `FakeStore` 에 추가:
```kotlin
        override fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer> = throw UnsupportedOperationException()
        override fun findUnknownForInquiry(maxAttempts: Int, limit: Int): List<Transfer> = throw UnsupportedOperationException()
        override fun recordInquiryAttempt(id: UUID, at: Instant): Boolean = throw UnsupportedOperationException()
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:transfer:test` → FAIL (컴파일: `Unresolved reference 'TransferReconciler'`, `'InquiryOutcome'` …)

- [ ] **Step 3: 구현**

`Transfer.kt`:
1) `data class Transfer` 의 `val sendingAt: Instant? = null,` 아래에 추가:
```kotlin
    /** 결과조회를 시도했는데 은행도 답하지 않은 횟수 (P3-3a). 한도에 닿으면 정리 작업이 멈춘다. */
    val inquiryAttempts: Int = 0,
```
2) `BankTransferPort` 를 바꾼다:
```kotlin
/** 은행 출금이체 포트. 결과가 나올 때까지 호출 스레드에서 기다린다 (Kafka 컨슈머 스레드가 부른다). */
fun interface BankTransferPort {
    /** [reference] = 이용기관 거래번호. 결과를 모를 때 이 번호로 결과조회한다. */
    fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome
}
```
3) 그 아래에 추가:
```kotlin
/** 결과조회 결과. 처리됨 · 처리 안 됨(확정) · 모름(조회에도 답이 없음 등). */
sealed interface InquiryOutcome {
    data object Processed : InquiryOutcome

    /** 은행에 원거래가 없다 = 처리되지 않았다. [responseCode] 는 은행 처리결과 코드 (예: 2001). */
    data class NotProcessed(val responseCode: String) : InquiryOutcome

    data object Unknown : InquiryOutcome
}

/** 은행 결과조회 포트. 이체를 다시 보내지 않고 "처리했나?"만 묻는다. */
fun interface BankInquiryPort {
    fun inquire(reference: String): InquiryOutcome
}
```
4) `interface TransferStore` 에 추가 (`transition` 아래):
```kotlin

    /** status = SENDING 이고 sendingAt 이 [cutoff] 보다 이른 건 (오래된 순, 최대 [limit]). */
    fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer>

    /** status = UNKNOWN 이고 결과조회 시도가 [maxAttempts] 미만인 건 (오래 기다린 순, 최대 [limit]). */
    fun findUnknownForInquiry(maxAttempts: Int, limit: Int): List<Transfer>

    /** UNKNOWN 인 동안에만 결과조회 시도 횟수를 1 늘린다. 늘렸으면 true. */
    fun recordInquiryAttempt(id: UUID, at: Instant): Boolean
```

`TransferProcessor.kt`:
- `bank.transfer(transfer.bankCode, transfer.accountNo, transfer.amount, transfer.memo)` → `bank.transfer(transfer.bankCode, transfer.accountNo, transfer.amount, transfer.memo, transfer.reference)`
- 클래스 KDoc 의 마지막 줄을 다음으로 바꾼다: ` * 은행 응답 뒤 DB 기록이 실패하면 SENDING 으로 남는다 — 정리 작업(TransferReconciler)이 30초 넘게 SENDING 인 건을 UNKNOWN 으로 바꾸고 결과조회로 확정한다.`
- `record` 를 다음으로 바꾼다:
```kotlin
    /**
     * SENDING 에서 결과로 옮긴다. false 면 정리 작업이 먼저 SENDING → UNKNOWN 으로 바꾼 것이다 (은행 응답이 30초 넘게 늦은 경우).
     * 그때 은행이 직접 준 확정 결과(완료 · 거절)는 결과조회보다 정확하므로 UNKNOWN 에서 바로 확정한다.
     * 그 밖(모름 · 미전송)은 정리 작업이 결과조회로 맡으므로 손대지 않는다.
     */
    private fun record(id: UUID, to: TransferStatus, bankResponseCode: String?, result: ProcessResult): ProcessResult {
        val now = clock.instant()
        if (store.transition(id, SENDING, to, now, bankResponseCode)) return result
        if ((to == COMPLETED || to == FAILED) && store.transition(id, UNKNOWN, to, now, bankResponseCode)) return result
        return ProcessResult.ALREADY_HANDLED
    }
```

`TransferReconciler.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Clock
import java.time.Duration

/** 한 번 돈 정리 작업의 결과 (로그 · 테스트용). */
data class ReconcileResult(
    val markedUnknown: Int,
    val completed: Int,
    val failed: Int,
    val stillUnknown: Int,
    val gaveUp: Int,
)

/**
 * 결과를 모르는 이체를 은행에 물어서 확정한다 (P3-3a). 은행에 다시 보내지 않는다 — 묻기만 한다.
 * 1. [sendingTimeout] 보다 오래 SENDING 인 건 → UNKNOWN. 컨슈머가 은행 호출 중 또는 결과 기록 전에 죽은 경우다.
 *    대상은 status = SENDING 으로만 고른다: 미전송(NotSent)으로 RECEIVED 에 되돌린 건에도 sendingAt 이 남기 때문이다.
 * 2. UNKNOWN 마다 결과조회: 처리됨 → COMPLETED(0000), 처리 안 됨 → FAILED(은행 코드), 모름 → 시도 횟수 +1.
 *    [maxInquiries] 번 모르면 더 묻지 않고 [onGiveUp] 으로 알린다 → 일마감 대사(P5)에서 맞춘다.
 * 상태 변경은 모두 조건부라, 그 사이 컨슈머가 은행의 늦은 응답으로 먼저 확정했다면 아무것도 바꾸지 않는다.
 */
class TransferReconciler(
    private val store: TransferStore,
    private val bank: BankInquiryPort,
    private val clock: Clock = Clock.systemUTC(),
    private val sendingTimeout: Duration = Duration.ofSeconds(30),
    private val maxInquiries: Int = 10,
    private val batchSize: Int = 100,
    private val onGiveUp: (Transfer) -> Unit = {},
) {

    fun reconcile(): ReconcileResult {
        val now = clock.instant()
        val markedUnknown = store.findSendingBefore(now.minus(sendingTimeout), batchSize)
            .count { store.transition(it.id, SENDING, UNKNOWN, now) }

        var completed = 0
        var failed = 0
        var stillUnknown = 0
        var gaveUp = 0
        for (transfer in store.findUnknownForInquiry(maxInquiries, batchSize)) {
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
            }
        }
        return ReconcileResult(markedUnknown, completed, failed, stillUnknown, gaveUp)
    }

    private companion object {
        const val BANK_OK = "0000"
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:transfer:test` → PASS (기존 28 + 정리 작업 6 + 처리 3 = 37). `apps/api` 는 아직 컴파일되지 않는다 (Task 3 에서 해결).

- [ ] **Step 5: 커밋**

```bash
git add modules/transfer
git commit -m "feat(transfer): 결과조회 포트와 정리 작업 — 멈춘 SENDING · UNKNOWN 을 은행에 물어 확정, 경합 시 확정 결과 우선

Co-Authored-By: <작성 모델>"
```

---

### Task 2: 전문 v0.2 — 이체 거래번호 · 결과조회, 게이트웨이, 모의 은행

**Files:**
- Modify: `modules/bank-protocol/src/main/kotlin/io/github/hyeokjinon/banktransfer/protocol/{BankMessage.kt, Header.kt, BankMessageSerializer.kt}`
- Test: `modules/bank-protocol/src/test/kotlin/io/github/hyeokjinon/banktransfer/protocol/BankMessageSerializerTest.kt`
- Modify: `modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayClient.kt`
- Test: `modules/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayClientTest.kt`
- Modify: `apps/mock-bank/src/main/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankServer.kt`

**Interfaces:**
- Produces:
  - `TransferRequest/TransferResponse(header, bankCode, accountNo, amount, memo, reference: String)`
  - `data class InquiryRequest(header, reference: String)`, `data class InquiryResponse(header, reference: String, result: String)`
  - `BizType.INQUIRY("INQ")`, `ResponseCodes.ORIGINAL_NOT_FOUND = "2001"`
  - `BankGatewayClient.transfer(bankCode, accountNo, amount, memo, reference): CompletableFuture<TransferResponse>`, `BankGatewayClient.inquire(reference: String): CompletableFuture<InquiryResponse>`
  - `MockBankServer(port = 0, accounts = DEFAULT_ACCOUNTS, silentAccounts = emptySet(), silentAfterProcessingAccounts = DEFAULT_SILENT_AFTER_PROCESSING, responseDelay = ZERO)`, `DEFAULT_ACCOUNTS` 에 `"9990000000002" to "무응답"` 추가, `DEFAULT_SILENT_AFTER_PROCESSING = setOf("9990000000002")`, `fun hasProcessed(reference: String): Boolean`

- [ ] **Step 1: 실패하는 테스트**

`BankMessageSerializerTest.kt`:
- `한글은 EUC-KR 2바이트로 계산한다` 를 바꾼다 (적요 뒤에 거래번호 20바이트가 붙는다):
```kotlin
    @Test
    fun `한글은 EUC-KR 2바이트로 계산한다`() {
        val transfer = TransferRequest(header(BizType.TRANSFER), "090", "1002123456789", 1, "BT1234홍길동", "V2610210000000000001")
        val bytes = BankMessageSerializer.serialize(transfer)

        // 적요 20바이트 = "BT1234"(6) + "홍길동"(6) + 공백 8, 그 뒤에 거래번호 20바이트
        val memoBytes = bytes.copyOfRange(bytes.size - 40, bytes.size - 20)
        assertEquals(6 + 6, String(memoBytes, BankCharsets.EUC_KR).trimEnd().toByteArray(BankCharsets.EUC_KR).size)
        assertEquals(transfer, BankMessageSerializer.deserialize(bytes))
    }
```
- `금액이 13자리를 넘으면 예외` 의 생성자에 거래번호 `""` 를 추가.
- 테스트 추가:
```kotlin
    @Test
    fun `이체 전문 개별부는 72바이트 (v0_2 - 이용기관 거래번호 20 추가)`() {
        val transfer = TransferResponse(header(BizType.TRANSFER, MessageClass.RESPONSE), "090", "1002123456789", 50_000, "출금", "T2610210000000000001")

        val bytes = BankMessageSerializer.serialize(transfer)

        assertEquals(Header.LENGTH + 72, bytes.size)
        assertEquals("T2610210000000000001", String(bytes.copyOfRange(bytes.size - 20, bytes.size), Charsets.US_ASCII))
        assertEquals(transfer, BankMessageSerializer.deserialize(bytes))
    }

    @Test
    fun `결과조회 요청 · 응답은 직렬화 후 다시 읽어도 같다`() {
        val request = InquiryRequest(header(BizType.INQUIRY), "T2610210000000000001")
        val response = InquiryResponse(header(BizType.INQUIRY, MessageClass.RESPONSE), "T2610210000000000001", ResponseCodes.ORIGINAL_NOT_FOUND)

        val requestBytes = BankMessageSerializer.serialize(request)
        val responseBytes = BankMessageSerializer.serialize(response)

        assertEquals(Header.LENGTH + 20, requestBytes.size)
        assertEquals(Header.LENGTH + 24, responseBytes.size)
        assertEquals(request, BankMessageSerializer.deserialize(requestBytes))
        assertEquals(response, BankMessageSerializer.deserialize(responseBytes))
    }
```

`BankGatewayClientTest.kt`:
- 기존 `client.transfer(...)` 호출 두 곳에 거래번호 인자를 붙인다: 동시 50건 테스트는 `"R$i"`, 연결 끊김 테스트는 `"R1"`.
- 테스트 추가:
```kotlin
    @Test
    fun `결과조회 - 처리한 거래번호는 0000, 모르는 번호는 2001`() {
        val bank = MockBankServer().start().closeLater()
        val client = client(bank.boundPort).start()
        waitUntil { client.isConnected }

        client.transfer("090", "1002123456789", 1_000, "m", "T2610210000000000001").get(5, TimeUnit.SECONDS)

        assertEquals(ResponseCodes.OK, client.inquire("T2610210000000000001").get(5, TimeUnit.SECONDS).result)
        assertEquals(ResponseCodes.ORIGINAL_NOT_FOUND, client.inquire("T2610219999999999999").get(5, TimeUnit.SECONDS).result)
    }

    @Test
    fun `처리하고 응답만 안 한 은행 - 이체는 시간 초과, 결과조회는 처리됨`() {
        val bank = MockBankServer().start().closeLater()   // 9990000000002 = 처리 후 무응답 (기본값)
        val client = client(bank.boundPort, timeout = Duration.ofMillis(300)).start()
        waitUntil { client.isConnected }

        assertIs<BankTimeoutException>(client.transfer("090", "9990000000002", 1_000, "m", "T2610210000000000002").awaitFailure())

        assertTrue(bank.hasProcessed("T2610210000000000002"))
        assertEquals(ResponseCodes.OK, client.inquire("T2610210000000000002").get(5, TimeUnit.SECONDS).result)
    }

    @Test
    fun `처리 후 무응답 계좌도 1원 이체에는 응답한다 (1원인증으로 수취 계좌 등록)`() {
        val bank = MockBankServer().start().closeLater()
        val client = client(bank.boundPort).start()
        waitUntil { client.isConnected }

        assertEquals(ResponseCodes.OK, client.transfer("090", "9990000000002", 1, "BT1234", "V2610210000000000001").get(5, TimeUnit.SECONDS).header.responseCode)
    }
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:bank-protocol:test :modules:bank-gateway:test` → FAIL (컴파일: `InquiryRequest`, 생성자 인자 수 등)

- [ ] **Step 3: 구현**

`BankMessage.kt`: `TransferRequest` · `TransferResponse` 끝에 필드 추가, 결과조회 전문 추가:
```kotlin
    /** 이용기관 거래번호 20자 (v0.2). 세션 안에서 돌고 도는 전문번호와 달리 업무 키라서 결과조회에 쓴다. */
    val reference: String,
```
```kotlin
/** 결과조회 요청 (v0.2): 결과를 모르는 이체를 이용기관 거래번호로 묻는다. */
data class InquiryRequest(
    override val header: Header,
    val reference: String,
) : BankMessage

/**
 * 결과조회 응답 (v0.2). [result] 는 원거래 처리결과(0000 처리됨 · 2001 원거래 없음)이고,
 * 공통부 응답코드는 조회 전문 자체가 성공했는지를 뜻한다.
 */
data class InquiryResponse(
    override val header: Header,
    val reference: String,
    val result: String,
) : BankMessage
```

`Header.kt`: `BizType` 에 `INQUIRY("INQ");   // 결과조회 (v0.2)` 추가 (TRANSFER 뒤의 `;` 위치를 옮긴다), `ResponseCodes` 에 `const val ORIGINAL_NOT_FOUND = "2001"   // 결과조회: 원거래 없음`.

`BankMessageSerializer.kt`:
- KDoc 레이아웃: `- 이체 요청/응답: 은행코드 3 + 계좌번호 16 + 금액 13 + 적요 20 + 이용기관 거래번호 20 (v0.2)`, `- 결과조회 요청: 이용기관 거래번호 20 / 응답: 거래번호 20 + 처리결과 4 (v0.2)`
- `serialize` 의 `when` 에:
```kotlin
            is TransferRequest -> w.transferBody(message.bankCode, message.accountNo, message.amount, message.memo, message.reference)
            is TransferResponse -> w.transferBody(message.bankCode, message.accountNo, message.amount, message.memo, message.reference)
            is InquiryRequest -> w.text(message.reference, 20)
            is InquiryResponse -> w.text(message.reference, 20).text(message.result, 4)
```
- `deserialize` 의 TRANSFER 분기에서 `val memo = r.text(20)` 다음에 `val reference = r.text(20)` 을 읽어 두 생성자에 넘기고, 분기 추가:
```kotlin
            BizType.INQUIRY -> {
                val reference = r.text(20)
                if (request) InquiryRequest(header, reference) else InquiryResponse(header, reference, r.text(4))
            }
```
- `transferBody` 에 `reference: String` 매개변수와 `.text(reference, 20)`.

`BankGatewayClient.kt`:
```kotlin
    fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> =
        send(TransferRequest(newHeader(BizType.TRANSFER), bankCode, accountNo, amount, memo, reference)).thenApply { it.expect<TransferResponse>() }

    /** 결과조회 (v0.2). 결과를 모르는 이체를 이용기관 거래번호로 묻는다 — 이체를 다시 보내지 않는다. */
    fun inquire(reference: String): CompletableFuture<InquiryResponse> =
        send(InquiryRequest(newHeader(BizType.INQUIRY), reference)).thenApply { it.expect<InquiryResponse>() }
```
(import `InquiryRequest`, `InquiryResponse`. `ClientHandler` 의 `else` 분기가 응답을 전문번호로 매칭하므로 결과조회 응답도 그대로 처리된다.)

`MockBankServer.kt`:
- 클래스 KDoc 에 추가: ` * - 결과조회(v0.2): 처리한 이체의 이용기관 거래번호면 0000, 아니면 2001` / ` * - silentAfterProcessingAccounts: 이체를 처리(입금 · 거래번호 기록)하고 응답만 보내지 않는다. 단 1원 이체는 정상 응답 — 1원인증으로 수취 계좌를 등록할 수 있게 (결과조회 시나리오용)`
- 생성자에 `private val silentAfterProcessingAccounts: Set<String> = DEFAULT_SILENT_AFTER_PROCESSING,` (silentAccounts 다음)
- 필드 `private val processedReferences: MutableSet<String> = ConcurrentHashMap.newKeySet()` 와 `fun hasProcessed(reference: String): Boolean = reference in processedReferences`
- `respond` 의 TransferRequest 분기:
```kotlin
        is TransferRequest -> when {
            message.accountNo in silentAccounts -> null
            message.accountNo !in accounts -> message.reply(ResponseCodes.ACCOUNT_NOT_FOUND)
            else -> {
                deposit(message)
                // 처리는 했는데 응답이 사라진 상황 (결과조회로만 확정할 수 있다)
                if (message.accountNo in silentAfterProcessingAccounts && message.amount > 1) null else message.reply(ResponseCodes.OK)
            }
        }
        is InquiryRequest -> {
            val result = if (message.reference in processedReferences) ResponseCodes.OK else ResponseCodes.ORIGINAL_NOT_FOUND
            log.info("[결과조회] {} -> {}", message.reference, result)
            InquiryResponse(message.header.toResponse(), message.reference, result)
        }
        is PollResponse, is NameInquiryResponse, is TransferResponse, is InquiryResponse -> null
```
- 입금 기록을 함수로 뺀다:
```kotlin
    private fun deposit(message: TransferRequest) {
        deposits.computeIfAbsent(message.accountNo) { CopyOnWriteArrayList() }.add(message)
        processedReferences += message.reference
        log.info("[통장] {} 입금 {}원 적요='{}' 거래번호={}", message.accountNo, message.amount, message.memo, message.reference)
    }
```
- `reply` 에 `reference` 전달: `TransferResponse(header.toResponse(code), bankCode, accountNo, amount, memo, reference)`
- `companion object`: `DEFAULT_ACCOUNTS` 에 `"9990000000002" to "무응답",` 추가, `val DEFAULT_SILENT_AFTER_PROCESSING = setOf("9990000000002")` 추가.
- import `InquiryRequest`, `InquiryResponse`.

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:bank-protocol:test :modules:bank-gateway:test` → PASS (protocol 11 + 2 = 13, gateway 7 + 3 = 10). `apps/api` 는 아직 컴파일되지 않는다.

- [ ] **Step 5: 커밋**

```bash
git add modules/bank-protocol modules/bank-gateway apps/mock-bank
git commit -m "feat(protocol): 전문 v0.2 — 이체에 이용기관 거래번호, 결과조회(INQ), 모의 은행 처리 후 무응답 시나리오

Co-Authored-By: <작성 모델>"
```

---

### Task 3: API 어댑터와 저장소 — 컴파일 복구

**Files:**
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/AppConfiguration.kt` (`BankGatewayAdapter`)
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/BankTransferAdapter.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/BankInquiryAdapter.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/TransferPersistence.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/config/BankInquiryAdapterTest.kt`
- Modify (test): `BankTransferAdapterTest.kt` (거래번호 인자), `JpaTransferStoreTest.kt` (테스트 2개 추가)

**Interfaces:**
- Consumes: Task 1 의 포트 · `InquiryOutcome`, Task 2 의 `client.transfer(…, reference)`, `client.inquire`, `ResponseCodes.ORIGINAL_NOT_FOUND`
- Produces: `class BankInquiryAdapter(client: BankGatewayClient) : BankInquiryPort`, `BankGatewayAdapter(client, executor, references: ReferenceGenerator = RandomReferenceGenerator(prefix = 'V'))`, `JpaTransferStore.findSendingBefore / findUnknownForInquiry / recordInquiryAttempt`

- [ ] **Step 1: 실패하는 테스트**

`BankInquiryAdapterTest.kt` (Spring 없이, `BankTransferAdapterTest` 의 `newClient` · `awaitConnected` 모양을 따른다 — 파일을 읽어 같은 도우미를 이 클래스에 둔다):
```kotlin
class BankInquiryAdapterTest {
    // newClient(bank, timeout = Duration.ofMillis(500)), awaitConnected(client) 는 BankTransferAdapterTest 와 같게

    @Test
    fun `처리한 거래번호는 Processed`() {
        MockBankServer().start().use { bank ->
            newClient(bank).use { client ->
                awaitConnected(client)
                client.transfer("090", "1002123456789", 1_000, "m", "T2610210000000000011").get(5, TimeUnit.SECONDS)

                assertEquals(InquiryOutcome.Processed, BankInquiryAdapter(client).inquire("T2610210000000000011"))
            }
        }
    }

    @Test
    fun `모르는 거래번호는 NotProcessed 2001`() {
        MockBankServer().start().use { bank ->
            newClient(bank).use { client ->
                awaitConnected(client)
                assertEquals(InquiryOutcome.NotProcessed("2001"), BankInquiryAdapter(client).inquire("T2610219999999999998"))
            }
        }
    }

    @Test
    fun `연결이 없으면 Unknown (예외를 던지지 않는다)`() {
        val closedPort = MockBankServer().start().use { it.boundPort }
        BankGatewayClient(BankGatewayConfig(host = "localhost", port = closedPort)).start().use { client ->
            assertEquals(InquiryOutcome.Unknown, BankInquiryAdapter(client).inquire("T2610210000000000012"))
        }
    }
}
```
`BankTransferAdapterTest.kt`: `transfer(...)` 호출마다 다섯 번째 인자 `"T2610210000000000099"` 를 추가.

`JpaTransferStoreTest.kt` 에 추가:
```kotlin
    @Test
    fun `멈춘 SENDING 조회 - 기준 시각보다 오래된 SENDING 만`() {
        val old = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val recent = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val now = Instant.now()
        store.transition(old.id, TransferStatus.RECEIVED, TransferStatus.SENDING, now.minusSeconds(60))
        store.transition(recent.id, TransferStatus.RECEIVED, TransferStatus.SENDING, now)

        val found = store.findSendingBefore(now.minusSeconds(30), 1_000).map { it.id }

        assertTrue(old.id in found)
        assertFalse(recent.id in found)
    }

    @Test
    fun `결과조회 대상 - 시도 횟수가 한도 미만인 UNKNOWN, 시도 기록은 UNKNOWN 일 때만`() {
        val unknown = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val at = Instant.now()
        store.transition(unknown.id, TransferStatus.RECEIVED, TransferStatus.SENDING, at)
        store.transition(unknown.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, at)

        assertTrue(store.recordInquiryAttempt(unknown.id, at))

        assertEquals(1, store.findById(unknown.id)!!.inquiryAttempts)
        assertFalse(unknown.id in store.findUnknownForInquiry(1, 1_000).map { it.id })
        assertTrue(unknown.id in store.findUnknownForInquiry(2, 1_000).map { it.id })

        val received = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        assertFalse(store.recordInquiryAttempt(received.id, at))   // UNKNOWN 이 아니면 늘리지 않는다
    }
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:compileKotlin` → FAIL (어댑터 시그니처, `JpaTransferStore` 미구현 메서드)

- [ ] **Step 3: 구현**

`AppConfiguration.kt` 의 `BankGatewayAdapter`:
```kotlin
class BankGatewayAdapter(
    private val client: BankGatewayClient,
    private val executor: Executor,
    /** 1원인증 이체의 이용기관 거래번호 (V 접두). 결과조회 대상이 아니라 저장하지 않는다. */
    private val references: ReferenceGenerator = RandomReferenceGenerator(prefix = 'V'),
) : BankPort {
```
`transfer` 안의 호출을 `client.transfer(bankCode, accountNo, amount, memo, references.next())` 로. import `io.github.hyeokjinon.banktransfer.transfer.ReferenceGenerator`.

`BankTransferAdapter.kt`: `override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String)` 로 바꾸고 `client.transfer(bankCode, accountNo, amount, memo, reference)`.

`BankInquiryAdapter.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.transfer.BankInquiryPort
import io.github.hyeokjinon.banktransfer.transfer.InquiryOutcome
import org.slf4j.LoggerFactory

/**
 * 결과조회 포트를 게이트웨이에 연결한다. 확실한 답(처리됨 0000 · 원거래 없음 2001)만 확정으로 돌려주고,
 * 그 밖(조회 전문 거절, 모르는 처리결과, 시간 초과, 연결 없음 …)은 모두 Unknown — 다음 주기에 다시 묻는다.
 * 예외를 던지지 않는다.
 */
class BankInquiryAdapter(private val client: BankGatewayClient) : BankInquiryPort {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun inquire(reference: String): InquiryOutcome =
        try {
            val response = client.inquire(reference).get()
            when {
                !response.header.isOk -> unknown(reference, "inquiry rejected: ${response.header.responseCode}")
                response.result == ResponseCodes.OK -> InquiryOutcome.Processed
                response.result == ResponseCodes.ORIGINAL_NOT_FOUND -> InquiryOutcome.NotProcessed(response.result)
                else -> unknown(reference, "unexpected result: ${response.result}")
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            unknown(reference, e.toString())
        } catch (e: Exception) {
            unknown(reference, (e.cause ?: e).toString())
        }

    private fun unknown(reference: String, reason: String): InquiryOutcome {
        log.warn("bank inquiry inconclusive: reference={} reason={}", reference, reason)
        return InquiryOutcome.Unknown
    }
}
```

`TransferPersistence.kt`:
1) `TransferEntity.toDomain()` 에 `inquiryAttempts = inquiryAttempts,` 추가, `from()` 에 `inquiryAttempts = t.inquiryAttempts,` 추가.
2) `TransferJpaRepository` 에 추가 (import `org.springframework.data.domain.Limit`):
```kotlin
    fun findByStatusAndSendingAtBeforeOrderBySendingAt(status: TransferStatus, cutoff: Instant, limit: Limit): List<TransferEntity>

    fun findByStatusAndInquiryAttemptsLessThanOrderByUpdatedAt(status: TransferStatus, maxAttempts: Int, limit: Limit): List<TransferEntity>

    /** 결과조회 시도 +1. 상태가 바뀌었으면(컨슈머가 먼저 확정) 0 건. */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        "update TransferEntity t set t.inquiryAttempts = t.inquiryAttempts + 1, t.updatedAt = :at, " +
            "t.version = t.version + 1 where t.id = :id and t.status = :status",
    )
    fun incrementInquiryAttempts(@Param("id") id: String, @Param("status") status: TransferStatus, @Param("at") at: Instant): Int
```
3) `JpaTransferStore` 에 추가:
```kotlin
    /** idx_transfer_status(status, sending_at) 를 탄다. */
    override fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer> =
        transfers.findByStatusAndSendingAtBeforeOrderBySendingAt(TransferStatus.SENDING, cutoff.toDbPrecision(), Limit.of(limit))
            .map { it.toDomain() }

    override fun findUnknownForInquiry(maxAttempts: Int, limit: Int): List<Transfer> =
        transfers.findByStatusAndInquiryAttemptsLessThanOrderByUpdatedAt(TransferStatus.UNKNOWN, maxAttempts, Limit.of(limit))
            .map { it.toDomain() }

    override fun recordInquiryAttempt(id: UUID, at: Instant): Boolean =
        tx.execute { transfers.incrementInquiryAttempts(id.toString(), TransferStatus.UNKNOWN, at.toDbPrecision()) == 1 }!!
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*BankInquiryAdapterTest*' --tests '*BankTransferAdapterTest*' --tests '*JpaTransferStoreTest*'` → PASS. 이어서 `./gradlew build` 전체 PASS (기존 P3-2 흐름 테스트 포함 — 정리 작업은 아직 연결 전이다).

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 결과조회 어댑터, 이체 · 1원인증에 이용기관 거래번호, 정리 작업용 조회 쿼리

Co-Authored-By: <작성 모델>"
```

---

### Task 4: 정리 작업 연결 — 설정 · 스케줄 · 흐름 테스트

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/reconcile/ReconcileConfiguration.kt`
- Modify: `apps/api/src/main/resources/application.yml`
- Modify: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/reconcile/ReconcileFlowTest.kt`

**Interfaces:**
- Consumes: `TransferReconciler` (Task 1), `BankInquiryAdapter` (Task 3), `OutboxRelay`, `TransferStore`
- Produces: `@ConfigurationProperties("transfer.reconcile") data class ReconcileProperties(enabled, intervalMs, sendingTimeout, maxInquiries, batchSize)`, 빈 `transferReconciler`, `TransferReconcilerScheduler`

- [ ] **Step 1: 설정**

`application.yml`:
- `spring:` 아래에 추가:
```yaml
  task:
    scheduling:
      pool:
        size: 2   # 발행기(0.5초)와 정리 작업(10초)이 한 스레드를 나눠 쓰면 서로 막는다
```
- 파일 끝 `transfer:` 아래에 (`outbox:` 와 같은 깊이):
```yaml
  reconcile:
    enabled: true          # 정리 작업 스케줄 (테스트는 끄고 직접 부른다)
    interval-ms: 10000
    sending-timeout: 30s   # 이보다 오래 SENDING 이면 UNKNOWN 으로 보고 결과조회
    max-inquiries: 10      # 결과조회도 계속 모르면 멈추고 경고 → 일마감 대사
    batch-size: 100
```
`ApiIntegrationTest.kt`: `properties = ["transfer.outbox.relay.enabled=false"]` → `properties = ["transfer.outbox.relay.enabled=false", "transfer.reconcile.enabled=false"]`, KDoc 의 스케줄 문장에 "정리 작업도 마찬가지로 끈다 (TransferReconciler.reconcile() 을 직접 부른다)." 추가, `bank` 주석을 `/** 9990000000001 = 무응답 · 처리 안 함. 9990000000002 = 처리 후 무응답 (MockBankServer 기본값). … */` 로.

- [ ] **Step 2: 실패하는 테스트**

`ReconcileFlowTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.reconcile

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferReconciler
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * 결과를 모르는 출금을 결과조회로 확정하는 흐름 (P3-3a). 은행에는 다시 보내지 않는다.
 * 모의 은행: 9990000000002 = 처리하고 응답만 안 함, 9990000000001 = 처리도 응답도 안 함.
 */
class ReconcileFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var verifications: VerificationRepository
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var reconciler: TransferReconciler

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
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to 30_000, "memo" to "R" + Random.nextInt(100_000, 999_999)))
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
    fun `처리하고 응답만 안 한 은행 - UNKNOWN 뒤 결과조회로 COMPLETED, 은행 입금은 1건`() {
        val account = "9990000000002"
        verify(account)
        val id = request(account)
        awaitStatus(id, TransferStatus.UNKNOWN)

        reconciler.reconcile()

        val after = store.findById(id)!!
        assertEquals(TransferStatus.COMPLETED, after.status)
        assertEquals("0000", after.bankResponseCode)
        assertEquals(1, bank.depositsOf(account).count { it.reference == after.reference })   // 다시 보내지 않았다
    }

    @Test
    fun `처리도 응답도 안 한 은행 - UNKNOWN 뒤 결과조회로 FAILED 2001`() {
        val account = "9990000000001"
        verify(account)
        val id = request(account)
        awaitStatus(id, TransferStatus.UNKNOWN)

        reconciler.reconcile()

        assertEquals(TransferStatus.FAILED, store.findById(id)!!.status)
        assertEquals("2001", store.findById(id)!!.bankResponseCode)
    }

    @Test
    fun `30초 넘게 SENDING 에 멈춘 건 - UNKNOWN 으로 바꾸고 결과조회로 확정`() {
        // 컨슈머가 SENDING 커밋 뒤 은행에 보내기 전에 죽은 상황: 은행은 이 거래번호를 모른다
        val now = Instant.now()
        val transfer = Transfer(
            id = UUID.randomUUID(), idempotencyKey = UUID.randomUUID().toString(), reference = RandomReferenceGenerator().next(),
            requestHash = "h".repeat(64), bankCode = "090", accountNo = "9990000000001", amount = 1_000, memo = "stuck",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        store.saveWithEvent(transfer, TransferEvent.TransferRequested(transfer.id, now))
        store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.SENDING, now.minusSeconds(60))

        val result = reconciler.reconcile()

        assertEquals(true, result.markedUnknown >= 1)
        assertEquals(TransferStatus.FAILED, store.findById(transfer.id)!!.status)
        assertEquals("2001", store.findById(transfer.id)!!.bankResponseCode)
    }
}
```

- [ ] **Step 3: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*ReconcileFlowTest*'` → FAIL (`No qualifying bean of type 'TransferReconciler'`)

- [ ] **Step 4: 구현**

`ReconcileConfiguration.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.reconcile

import io.github.hyeokjinon.banktransfer.api.config.BankInquiryAdapter
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.transfer.ReconcileResult
import io.github.hyeokjinon.banktransfer.transfer.TransferReconciler
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.Scheduled
import java.time.Clock
import java.time.Duration

@ConfigurationProperties("transfer.reconcile")
data class ReconcileProperties(
    val enabled: Boolean = true,
    val intervalMs: Long = 10_000,
    val sendingTimeout: Duration = Duration.ofSeconds(30),
    val maxInquiries: Int = 10,
    val batchSize: Int = 100,
)

/** 정리 작업을 주기적으로 돌린다. 테스트는 이 빈을 끄고 TransferReconciler.reconcile() 을 직접 부른다. */
class TransferReconcilerScheduler(private val reconciler: TransferReconciler) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${transfer.reconcile.interval-ms:10000}")
    fun run() {
        try {
            val result = reconciler.reconcile()
            if (result != NOTHING) log.info("reconcile: {}", result)
        } catch (e: Exception) {
            // DB 장애 등. 상태 변경은 모두 조건부라 다음 주기에 그대로 다시 하면 된다.
            log.warn("reconcile failed, retrying next round: {}", e.message)
        }
    }

    private companion object {
        val NOTHING = ReconcileResult(0, 0, 0, 0, 0)
    }
}

@Configuration(proxyBeanMethods = false)
class ReconcileConfiguration {
    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    fun transferReconciler(store: TransferStore, client: BankGatewayClient, props: ReconcileProperties) =
        TransferReconciler(
            store = store,
            bank = BankInquiryAdapter(client),
            clock = Clock.systemUTC(),
            sendingTimeout = props.sendingTimeout,
            maxInquiries = props.maxInquiries,
            batchSize = props.batchSize,
            // Transfer.toString 은 계좌번호를 가린다
            onGiveUp = { log.warn("still UNKNOWN after {} inquiries, leave it to daily reconciliation: {}", it.inquiryAttempts, it) },
        )

    @Bean
    @ConditionalOnProperty(prefix = "transfer.reconcile", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun transferReconcilerScheduler(reconciler: TransferReconciler) = TransferReconcilerScheduler(reconciler)
}
```
(`@EnableScheduling` 은 `MessagingConfiguration` 에 이미 있다. `ReconcileProperties` 는 `@ConfigurationPropertiesScan` 이 등록한다.)

- [ ] **Step 5: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*ReconcileFlowTest*'` → PASS (3). 이어서 `./gradlew build` 전체 PASS, 모든 모듈 `tests=` 합계를 보고서에 적는다.

- [ ] **Step 6: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 정리 작업 스케줄 (10초) — 멈춘 SENDING · UNKNOWN 을 결과조회로 확정, 스케줄러 스레드 2개

Co-Authored-By: <작성 모델>"
```

---

### Task 5: 결정 기록 · 규격 · 문서

**Files:**
- Create: `docs/adr/0006-inquire-instead-of-resend.md`
- Modify: `docs/protocol.md`, `docs/superpowers/specs/2026-10-19-p3-transfer-design.md`, `docs/database.md`, `OPERATION.md`, `ARCHITECTURE.md`, `README.md`, `CHANGELOG.md`, `HANDOVER.md`, `http/transfer.http`

- [ ] **Step 1: ADR-0006** — `docs/adr/0006-inquire-instead-of-resend.md`, 기존 ADR 형식(상태 · 단계 · 맥락 · 결정 · 결과 · 검토한 다른 선택지):
  - 상태: 제안 (2026-10-21). 단계 P3-3a.
  - 맥락: 무응답 · 보낸 뒤 끊김은 은행 처리 여부를 모른다(P3-2 에서 UNKNOWN 으로만 남김). 예전 시스템은 결과조회 API 와 일마감 대사로 맞췄다. 전문번호는 세션 안에서 재사용돼 업무 키가 될 수 없다.
  - 결정: (1) 규격 v0.2 이용기관 거래번호 20 · INQ (2) 출금은 저장된 reference, 1원인증은 어댑터가 V 번호(저장 안 함 — 결과조회 대상 아님) (3) 정리 작업 10초: SENDING 30초 → UNKNOWN(status 로만 선택), UNKNOWN → 결과조회, 10회 후 멈추고 경고 (4) 결과조회는 다시 보내지 않는다 (5) 컨슈머 경합: 은행의 확정 결과가 이긴다(UNKNOWN → COMPLETED/FAILED), 그 밖은 정리 작업에 맡김 (6) 스케줄러 스레드 2.
  - 결과: 증명 테스트 이름 (처리 후 무응답 → COMPLETED · 입금 1건, 미처리 → FAILED 2001, 멈춘 SENDING → 확정, 한도 후 멈춤, 경합). 한계: 원거래가 은행 안에서 아직 처리 중일 때 조회하면 2001 이 올 수 있다(30초 유예로 줄임, 최종 안전망 P5 대사), 결과조회도 계속 모르면 UNKNOWN 으로 남는다, 인스턴스가 여러 대면 같은 UNKNOWN 을 두 정리 작업이 동시에 물을 수 있다(조건부 전이라 결과는 같다 — 조회만 중복).
  - 다른 선택지: 타임아웃 시 재전송(이중 출금), 사람이 수동 확인, 은행 콜백 대기(모의 은행에 없음), 결과조회 대상 선택을 sendingAt 으로(되돌린 RECEIVED 까지 잡힘).
- [ ] **Step 2: `docs/protocol.md`** 를 v0.2 로: 제목 버전, §3 업무구분에 `INQ` 결과조회 · 응답코드 `2001` 원거래 없음, §4 개별부 표 (이체 72 · 결과조회 요청 20 · 응답 24), §5 예시는 그대로 두고 결과조회 요청 예시 한 줄 추가, §6 세션 규칙의 마지막 문장을 "늦게 온 응답은 로그만 남기고, 결과는 결과조회 전문으로 확정한다 (P3-3a)" 로. 변경 이력 절이 없으면 맨 아래에 `## 변경 이력` (v0.1 2026-10-01, v0.2 2026-10-21) 추가.
- [ ] **Step 3: 설계 문서** `docs/superpowers/specs/2026-10-19-p3-transfer-design.md` §0 분할 표에 P3-3 을 `P3-3a`(규격 v0.2 · 결과조회 · 정리 작업) / `P3-3b`(재시도 토픽 · DLT · 오래된 RECEIVED 재발행)로 나눈 줄을 추가하고, §6 에 2026-10-21 결정 두 줄(1원인증 V 거래번호는 어댑터가 · 저장 안 함 / 오래된 RECEIVED 는 P3-3b 에서 횟수 제한 재발행)을 적는다. 모의 은행 시나리오 계좌 번호(9990000000001 · 9990000000002)와 "1원 이체는 정상 응답" 규칙도 §6 에.
- [ ] **Step 4: 나머지 문서**
  - `docs/database.md`: §4-2 `inquiry_attempts` 설명을 "결과조회에도 답이 없던 횟수 (정리 작업이 10에서 멈춘다)" 로, §6 동시성 표에 "정리 작업과 컨슈머가 같은 이체를 확정 | 조건부 UPDATE (UNKNOWN 에서만 확정) | …" 행, §9 운영 쿼리에 `-- 결과조회 한도에 닿은 UNKNOWN (일마감 대사 대상)` (`SELECT id, reference, inquiry_attempts FROM transfer WHERE status = 'UNKNOWN' AND inquiry_attempts >= 10;`), 변경 이력 한 줄 (PR #N).
  - `OPERATION.md`: 정리 작업 절 (주기 · 30초 · 10회 · 로그 `reconcile:` / `still UNKNOWN after` / `bank inquiry inconclusive`), 설정 표에 `transfer.reconcile.*` 와 `spring.task.scheduling.pool.size`, 모의 은행 시나리오 표에 9990000000002 행, 장애 대응에서 "UNKNOWN 이 남아 있음" 행을 "정리 작업이 10초마다 결과조회, 10번 후에도 모르면 경고 → 대사" 로, "SENDING 이 남음" 행에 "30초 뒤 정리 작업이 UNKNOWN 으로" 추가.
  - `ARCHITECTURE.md`: §6 P3 진행 상황에 P3-3a, 정리 작업 흐름(sequenceDiagram 또는 목록: Reconciler → findSendingBefore → UNKNOWN → BankInquiryAdapter → INQ → 확정), 소스 지도에 `api/reconcile/`, `TransferReconciler`, ADR 색인 0006.
  - `http/transfer.http`: 결과조회 확인 절 추가 — `### 손 확인: 처리 후 무응답 → 결과조회로 COMPLETED` 주석으로 순서 안내 (1원인증을 `9990000000002`(예금주 `무응답`)로 → 접수 → 3초 뒤 조회하면 `UNKNOWN` → 10초 안에 다시 조회하면 `COMPLETED`), 그 계좌로 접수하는 POST 와 GET 요청.
  - `README.md`: "지금 되는 것"에 P3-3a 한 줄, 로드맵 P3-3 줄을 `[x] P3-3a …` / `[ ] P3-3b …` 로, 테스트 수 갱신.
  - `CHANGELOG.md`: `## [P3-3a] 결과조회와 정리 작업 — 2026-10-21 (PR #N)` (Added / Changed: 규격 v0.2 이체 72바이트, 1원인증 V 거래번호, 컨슈머 경합 처리).
  - `HANDOVER.md`: 마지막 갱신일, P3-3a 행 🟡 PR 리뷰 중 (손 확인 대기), P3-3b 행 ⬜ 다음, "P3-3 로 넘기는 것" 중 처리한 항목(정리 작업 status 기준, 스케줄러 스레드, record() check) 지우고 남은 것은 "P3-3b 로 넘기는 것"으로 이름 변경, 다음 할 일.
- [ ] **Step 5: 전체 빌드** — `./gradlew build --rerun-tasks`, 테스트 합계를 README · SETUP · HANDOVER 에 반영.
- [ ] **Step 6: 커밋** — `docs: P3-3a — ADR-0006, 전문 v0.2, 정리 작업 운영 · 구조 문서` (push · PR 없음).
