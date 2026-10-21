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
