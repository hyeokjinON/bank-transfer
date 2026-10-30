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

    /** 조건부 UPDATE 를 흉내 내는 가짜 저장소: 현재 상태가 from 일 때만 바꾼다. FAILED 는 실제 저장소처럼 fail() 로만 간다. */
    private class FakeStore : TransferStore {
        val transfers = linkedMapOf<UUID, Transfer>()

        /** 실제로 FAILED 로 바꾼 fail() 호출 (id, from, 은행 코드). 실제 저장소는 바로 이때 같은 트랜잭션에서 환불한다. */
        val failCalls = mutableListOf<Triple<UUID, TransferStatus, String>>()

        override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer = transfer.also { transfers[it.id] = it }
        override fun findById(id: UUID) = transfers[id]
        override fun findByIdempotencyKey(userId: String, key: String) =
            transfers.values.firstOrNull { it.userId == userId && it.idempotencyKey == key }

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?, sentWithin: OpenEndRange<Instant>?): Boolean {
            require(to != TransferStatus.FAILED) { "FAILED 는 fail() 로만 간다 (환불을 빠뜨리지 않게)" }
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

        override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String, sentWithin: OpenEndRange<Instant>?): Boolean {
            val current = transfers[id] ?: return false
            if (current.status != from) return false
            transfers[id] = current.copy(status = TransferStatus.FAILED, updatedAt = at, bankResponseCode = bankResponseCode)
            failCalls += Triple(id, from, bankResponseCode)
            return true
        }

        override fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer> = emptyList()
        override fun findUnknownForInquiry(maxAttempts: Int, sendingBefore: Instant, limit: Int): List<Transfer> = emptyList()
        override fun recordInquiryAttempt(id: UUID, at: Instant) = false
        override fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer> = emptyList()
        override fun republish(id: UUID, expectedCount: Int, at: Instant) = false
    }

    /** 정해 둔 결과를 돌려주고 호출 횟수를 센다 (은행에 몇 번 보냈나). */
    private class FakeBank(var outcome: BankTransferOutcome) : BankTransferPort {
        var calls = 0
        var lastReference: String? = null
        override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome {
            calls++
            lastReference = reference
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
        assertEquals(listOf(Triple(t.id, TransferStatus.SENDING, "1001")), store.failCalls)   // 환불 경로 (저장소가 같은 트랜잭션에서 돌려준다)
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
    fun `은행 호출이 예외를 던지면 UNKNOWN 이고 다시 보내지 않는다`() {
        val t = received()
        var calls = 0
        val throwingProcessor = TransferProcessor(
            store,
            BankTransferPort { _, _, _, _, _ ->
                calls++
                throw IllegalStateException("boom")
            },
            Clock.fixed(now, ZoneOffset.UTC),
        )

        assertEquals(ProcessResult.UNKNOWN, throwingProcessor.process(t.id))
        assertEquals(TransferStatus.UNKNOWN, store.transfers.getValue(t.id).status)
        assertNull(store.transfers.getValue(t.id).bankResponseCode)

        assertEquals(ProcessResult.ALREADY_HANDLED, throwingProcessor.process(t.id))
        assertEquals(1, calls)
    }

    @Test
    fun `toString 은 계좌번호 뒤 4자리만, 적요는 찍지 않는다`() {
        val text = received().toString()

        assertTrue("*********6789" in text, text)
        assertFalse("1002123456789" in text, text)
        assertFalse("출금" in text, text)
    }

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

    @Test
    fun `이미 다른 결과로 확정된 건에 은행의 확정 결과가 오면 CONFLICT`() {
        val t = received()
        val racing = BankTransferPort { _, _, _, _, _ ->
            store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, now)
            store.fail(t.id, TransferStatus.UNKNOWN, now, "2001")   // 결과조회가 먼저 FAILED 로 확정 (환불까지)
            BankTransferOutcome.Completed                                                          // 은행의 늦은 응답은 완료
        }

        assertEquals(ProcessResult.CONFLICT, TransferProcessor(store, racing, Clock.fixed(now, ZoneOffset.UTC)).process(t.id))
        assertEquals(TransferStatus.FAILED, store.transfers.getValue(t.id).status)
        assertEquals(1, store.failCalls.size)   // CONFLICT 는 잔고를 더 건드리지 않는다 — 환불은 결과조회의 한 번뿐
    }

    @Test
    fun `정리 작업이 먼저 UNKNOWN 으로 바꾼 뒤 늦게 온 거절도 fail() 로 확정한다 (환불 경로)`() {
        val t = received()
        val racing = BankTransferPort { _, _, _, _, _ ->
            store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, now)   // 30초 넘게 늦은 응답 사이에 정리 작업이 끼어듦
            BankTransferOutcome.Rejected("1001")
        }

        assertEquals(ProcessResult.FAILED, TransferProcessor(store, racing, Clock.fixed(now, ZoneOffset.UTC)).process(t.id))
        assertEquals(TransferStatus.FAILED, store.transfers.getValue(t.id).status)
        // SENDING 에서 먼저 시도(0건) → UNKNOWN 에서 확정. 환불은 실제로 바뀐 한 번뿐이다
        assertEquals(listOf(Triple(t.id, TransferStatus.UNKNOWN, "1001")), store.failCalls)
    }
}
