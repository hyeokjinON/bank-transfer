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
import kotlin.test.assertTrue

class TransferReconcilerTest {

    /** 조건부 UPDATE 와 조회를 흉내 내는 가짜 저장소. */
    private class FakeStore : TransferStore {
        val transfers = linkedMapOf<UUID, Transfer>()

        override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer = transfer.also { transfers[it.id] = it }
        override fun findById(id: UUID) = transfers[id]
        override fun findByIdempotencyKey(userId: String, key: String) =
            transfers.values.firstOrNull { it.userId == userId && it.idempotencyKey == key }

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean {
            require(to != FAILED) { "FAILED 는 fail() 로만 간다 (환불을 빠뜨리지 않게)" }
            val current = transfers[id] ?: return false
            if (current.status != from) return false
            transfers[id] = current.copy(
                status = to, updatedAt = at, bankResponseCode = bankResponseCode,
                sendingAt = if (to == SENDING) at else current.sendingAt,
            )
            return true
        }

        /** 실제로 FAILED 로 바꾼 fail() 호출 (id, from, 은행 코드). 실제 저장소는 바로 이때 같은 트랜잭션에서 환불한다. */
        val failCalls = mutableListOf<Triple<UUID, TransferStatus, String>>()

        /** 깨진 데이터(지갑 행 없음 · REFUND 중복 등)로 예외를 던지는 행. */
        val failThrowsFor = mutableSetOf<UUID>()
        val republishThrowsFor = mutableSetOf<UUID>()

        override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String): Boolean {
            if (id in failThrowsFor) throw IllegalStateException("corrupted row $id")
            val current = transfers[id] ?: return false
            if (current.status != from) return false
            transfers[id] = current.copy(status = FAILED, updatedAt = at, bankResponseCode = bankResponseCode)
            failCalls += Triple(id, from, bankResponseCode)
            return true
        }

        override fun findSendingBefore(cutoff: Instant, limit: Int) =
            transfers.values.filter { it.status == SENDING && it.sendingAt!!.isBefore(cutoff) }.take(limit)

        override fun findUnknownForInquiry(maxAttempts: Int, sendingBefore: Instant, limit: Int) =
            transfers.values
                .filter { it.status == UNKNOWN && it.inquiryAttempts < maxAttempts && it.sendingAt!!.isBefore(sendingBefore) }
                .sortedBy { it.updatedAt }.take(limit)

        override fun recordInquiryAttempt(id: UUID, at: Instant): Boolean {
            val current = transfers[id] ?: return false
            if (current.status != UNKNOWN) return false
            transfers[id] = current.copy(inquiryAttempts = current.inquiryAttempts + 1, updatedAt = at)
            return true
        }

        val events = mutableListOf<TransferEvent>()

        override fun findRepublishable(updatedBefore: Instant, limit: Int) =
            transfers.values.filter {
                val sendingAt = it.sendingAt
                val republishedAt = it.republishedAt
                it.status == RECEIVED && it.updatedAt.isBefore(updatedBefore) &&
                    sendingAt != null && (republishedAt == null || sendingAt.isAfter(republishedAt))
            }.sortedBy { it.updatedAt }.take(limit)

        override fun republish(id: UUID, expectedCount: Int, at: Instant): Boolean {
            if (id in republishThrowsFor) throw IllegalStateException("corrupted row $id")
            val current = transfers[id] ?: return false
            if (current.status != RECEIVED || current.republishCount != expectedCount) return false
            transfers[id] = current.copy(republishCount = expectedCount + 1, republishedAt = at, updatedAt = at)
            events += TransferEvent.TransferRequested(id, at)
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
    private val abandonedTransfers = mutableListOf<Transfer>()
    private val itemErrors = mutableListOf<Pair<Transfer, Exception>>()
    private fun reconciler(maxInquiries: Int = 10) = TransferReconciler(
        store, inquiry, Clock.fixed(now, ZoneOffset.UTC), Duration.ofSeconds(30), maxInquiries, 100,
        onGiveUp = { gaveUp += it }, onAbandon = { abandonedTransfers += it },
        onItemError = { t, e -> itemErrors += t to e },
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
        assertEquals(listOf(Triple(t.id, UNKNOWN, "2001")), store.failCalls)   // 환불 경로
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
    fun `UNKNOWN 이어도 sendingAt 이 30초 안쪽이면 아직 묻지 않는다 - 느린 은행의 2001 을 믿을 수 없다`() {
        val fresh = transfer(UNKNOWN, sendingAt = now.minusSeconds(1))     // 컨슈머가 3초 타임아웃으로 방금 UNKNOWN 으로 적은 건
        val old = transfer(UNKNOWN, sendingAt = now.minusSeconds(31))
        inquiry.outcomes[old.reference] = { InquiryOutcome.Processed }

        val result = reconciler().reconcile()

        assertEquals(1, inquiry.calls)                                      // 오래된 건 하나만 묻는다
        assertEquals(1, result.completed)
        assertEquals(COMPLETED, store.transfers.getValue(old.id).status)
        assertEquals(UNKNOWN, store.transfers.getValue(fresh.id).status)
        assertEquals(0, store.transfers.getValue(fresh.id).inquiryAttempts) // 시도 횟수도 늘지 않는다
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
        assertEquals(listOf(Triple(t.id, RECEIVED, "E001")), store.failCalls)   // 은행에 간 적 없음 → 차감액을 돌려준다
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

    @Test
    fun `포기하다 예외가 나는 행은 건너뛰고 알리며 나머지 행은 계속 처리한다`() {
        val broken = transfer(
            RECEIVED, sendingAt = now.minusSeconds(650), updatedAt = now.minusSeconds(600),
            republishCount = 3, republishedAt = now.minusSeconds(700),
        )
        val healthy = transfer(
            RECEIVED, sendingAt = now.minusSeconds(400), updatedAt = now.minusSeconds(301),
            republishCount = 3, republishedAt = now.minusSeconds(700),
        )
        store.failThrowsFor += broken.id

        val result = reconciler().reconcile()

        assertEquals(1, result.errors)
        assertEquals(1, result.abandoned)                                       // 깨진 행 뒤의 행도 처리됐다
        assertEquals(listOf(broken.id), itemErrors.map { it.first.id })
        assertEquals("corrupted row ${broken.id}", itemErrors.single().second.message)
        assertEquals(RECEIVED, store.transfers.getValue(broken.id).status)      // 손으로 고칠 때까지 그대로 — 다음 주기에 다시 시도 · 기록된다
        assertEquals(FAILED, store.transfers.getValue(healthy.id).status)
    }

    @Test
    fun `재발행하다 예외가 나는 행도 건너뛰고 나머지를 재발행한다`() {
        val broken = transfer(RECEIVED, sendingAt = now.minusSeconds(900), updatedAt = now.minusSeconds(600))
        val healthy = transfer(RECEIVED, sendingAt = now.minusSeconds(400), updatedAt = now.minusSeconds(301))
        store.republishThrowsFor += broken.id

        val result = reconciler().reconcile()

        assertEquals(1, result.errors)
        assertEquals(1, result.republished)
        assertEquals(listOf(broken.id), itemErrors.map { it.first.id })
        assertEquals(1, store.transfers.getValue(healthy.id).republishCount)
    }

    @Test
    fun `결과조회 뒤 확정하다 예외가 나는 행은 건너뛰고 뒤의 UNKNOWN 을 계속 묻는다`() {
        val broken = transfer(UNKNOWN, sendingAt = now.minusSeconds(120))
        val healthy = transfer(UNKNOWN, sendingAt = now.minusSeconds(60))
        inquiry.outcomes[broken.reference] = { InquiryOutcome.NotProcessed("2001") }
        inquiry.outcomes[healthy.reference] = { InquiryOutcome.Processed }
        store.failThrowsFor += broken.id

        val result = reconciler().reconcile()

        assertEquals(1, result.errors)
        assertEquals(0, result.failed)
        assertEquals(1, result.completed)
        assertEquals(listOf(broken.id), itemErrors.map { it.first.id })
        assertEquals(UNKNOWN, store.transfers.getValue(broken.id).status)
        assertEquals(COMPLETED, store.transfers.getValue(healthy.id).status)
    }

    @Test
    fun `예외가 없으면 errors 는 0 이고 onItemError 는 부르지 않는다`() {
        transfer(UNKNOWN).also { inquiry.outcomes[it.reference] = { InquiryOutcome.Processed } }

        val result = reconciler().reconcile()

        assertEquals(0, result.errors)
        assertTrue(itemErrors.isEmpty())
    }
}
