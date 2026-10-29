package io.github.hyeokjinon.banktransfer.transfer

import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class StatementSettlerTest {

    /**
     * 출금 하나만 담는 가짜 저장소. 조건부 전이 · 실패 확정은 현재 상태가 from 이고 (범위를 주면) sendingAt 이 그 안일 때만 바꾼다 (진짜와 같다).
     * [raceTo] · [race] 를 주면 첫 확정 시도 직전에 다른 쪽(정리 작업 · 컨슈머)이 먼저 바꾼 것처럼 만든다.
     * [snapshotStale] 이면 확정은 늘 0건이고 다시 읽어도 옛 상태가 보인다 (REPEATABLE READ 스냅숏 흉내).
     */
    private class OneTransferStore(var current: Transfer) : TransferStore {
        val calls = mutableListOf<String>()
        var raceTo: TransferStatus? = null
        var race: ((Transfer) -> Transfer)? = null
        var snapshotStale = false

        private fun changes(from: TransferStatus, sentWithin: OpenEndRange<Instant>?): Boolean {
            raceTo?.let { current = current.copy(status = it); raceTo = null }
            race?.let { current = it(current); race = null }
            return !snapshotStale && current.status == from && (sentWithin == null || current.sendingAt?.let { it in sentWithin } == true)
        }

        override fun findById(id: UUID): Transfer? = current.takeIf { it.id == id }

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?, sentWithin: OpenEndRange<Instant>?): Boolean {
            calls += "transition $from->$to $bankResponseCode"
            if (!changes(from, sentWithin)) return false
            current = current.copy(status = to, bankResponseCode = bankResponseCode)
            return true
        }

        override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String, sentWithin: OpenEndRange<Instant>?): Boolean {
            calls += "fail $from $bankResponseCode"
            if (!changes(from, sentWithin)) return false
            current = current.copy(status = TransferStatus.FAILED, bankResponseCode = bankResponseCode)
            return true
        }

        override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer = error("not used")
        override fun findByIdempotencyKey(userId: String, key: String): Transfer? = error("not used")
        override fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer> = error("not used")
        override fun findUnknownForInquiry(maxAttempts: Int, sendingBefore: Instant, limit: Int): List<Transfer> = error("not used")
        override fun recordInquiryAttempt(id: UUID, at: Instant): Boolean = error("not used")
        override fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer> = error("not used")
        override fun republish(id: UUID, expectedCount: Int, at: Instant): Boolean = error("not used")
    }

    private val clock = Clock.fixed(Instant.parse("2026-10-28T17:00:00Z"), ZoneOffset.UTC)

    private fun transfer(status: TransferStatus) = Transfer(
        id = UUID.randomUUID(), userId = "u", idempotencyKey = "k", reference = REF, requestHash = "h",
        bankCode = "090", accountNo = "1002123456789", amount = 30_000, memo = "m",
        status = status, createdAt = Instant.EPOCH, updatedAt = Instant.EPOCH,
    )

    /** 대사일 D = 2026-10-28 (KST) 의 범위와 그 안 · 다음 날의 송신 시각. */
    private val dayD = Instant.parse("2026-10-27T15:00:00Z")..<Instant.parse("2026-10-28T15:00:00Z")
    private val sentOnD = Instant.parse("2026-10-28T01:00:00Z")
    private val resentOnNextDay = Instant.parse("2026-10-28T16:30:00Z")

    private val paid = BankStatementLine(LocalDate.of(2026, 10, 28), 2, REF, "090", "1002123456789", 30_000, Instant.EPOCH)

    @Test
    fun `UNKNOWN + 같은 줄 - COMPLETED(0000) 로 확정하고 판정 시점 상태를 돌려준다`() {
        val store = OneTransferStore(transfer(TransferStatus.UNKNOWN))
        var askedFor: String? = null

        val settlement = StatementSettler(store, clock).settle(store.current.id) { askedFor = it; listOf(paid) }

        assertEquals(ReconVerdict.AUTO_COMPLETE, settlement.verdict)
        assertEquals(TransferStatus.UNKNOWN, settlement.transfer!!.status)
        assertEquals(REF, askedFor)
        assertEquals(listOf("transition UNKNOWN->COMPLETED 0000"), store.calls)
        assertEquals(TransferStatus.COMPLETED, store.current.status)
    }

    @Test
    fun `SENDING + 없음 - fail(E002) 로 확정 (환불은 저장소가 같은 트랜잭션에서)`() {
        val store = OneTransferStore(transfer(TransferStatus.SENDING))

        val settlement = StatementSettler(store, clock).settle(store.current.id) { emptyList() }

        assertEquals(ReconVerdict.AUTO_FAIL, settlement.verdict)
        assertEquals(listOf("fail SENDING E002"), store.calls)
        assertEquals("E002", store.current.bankResponseCode)
        assertEquals("E002", StatementSettler.NOT_AT_BANK)
    }

    @Test
    fun `확정된 건은 건드리지 않는다 - 판정만 돌려준다`() {
        val store = OneTransferStore(transfer(TransferStatus.COMPLETED))

        val settlement = StatementSettler(store, clock).settle(store.current.id) { emptyList() }

        assertEquals(ReconVerdict.COMPLETED_NOT_AT_BANK, settlement.verdict)
        assertTrue(store.calls.isEmpty())
    }

    @Test
    fun `경합 - 확정이 0건이면(그 사이 정리 작업이 COMPLETED) 다시 읽어 바뀐 상태로 판정한다`() {
        val store = OneTransferStore(transfer(TransferStatus.UNKNOWN)).apply { raceTo = TransferStatus.COMPLETED }

        val settlement = StatementSettler(store, clock).settle(store.current.id) { listOf(paid) }

        assertEquals(ReconVerdict.MATCHED, settlement.verdict)
        assertEquals(TransferStatus.COMPLETED, settlement.transfer!!.status)
        assertEquals(listOf("transition UNKNOWN->COMPLETED 0000"), store.calls)   // 두 번째 판정은 일치라 확정하지 않는다
    }

    @Test
    fun `경합 - 그 사이 결과조회가 FAILED 로 확정했는데 은행 줄이 있으면 REFUNDED_BUT_PAID`() {
        val store = OneTransferStore(transfer(TransferStatus.UNKNOWN)).apply { raceTo = TransferStatus.FAILED }

        val settlement = StatementSettler(store, clock).settle(store.current.id) { listOf(paid) }

        assertEquals(ReconVerdict.REFUNDED_BUT_PAID, settlement.verdict)
    }

    @Test
    fun `다시 읽어도 계속 바뀌면(옛 스냅숏) 3번 뒤 포기 - 단계가 실패하고 재시작으로 이어간다`() {
        val store = OneTransferStore(transfer(TransferStatus.UNKNOWN)).apply { snapshotStale = true }

        assertFailsWith<IllegalStateException> { StatementSettler(store, clock).settle(store.current.id) { listOf(paid) } }
        assertEquals(3, store.calls.size)
    }

    @Test
    fun `경합 - 확정 0건 뒤 다시 읽으니 SENDING 이 UNKNOWN(컨슈머 타임아웃) - 다시 판정해 UNKNOWN 에서 fail(E002)`() {
        val store = OneTransferStore(transfer(TransferStatus.SENDING).copy(sendingAt = sentOnD)).apply { raceTo = TransferStatus.UNKNOWN }

        val settlement = StatementSettler(store, clock).settle(store.current.id, dayD) { emptyList() }

        assertEquals(ReconVerdict.AUTO_FAIL, settlement.verdict)
        assertEquals(TransferStatus.UNKNOWN, settlement.transfer!!.status)
        assertEquals(listOf("fail SENDING E002", "fail UNKNOWN E002"), store.calls)
        assertEquals(TransferStatus.FAILED, store.current.status)
    }

    @Test
    fun `경합 - 읽은 뒤 확정 전에 다시 보내지면(RECEIVED 거쳐 SENDING, sendingAt D+1) 환불하지 않는다 = RESENT_LATER`() {
        val store = OneTransferStore(transfer(TransferStatus.SENDING).copy(sendingAt = sentOnD)).apply {
            race = { it.copy(status = TransferStatus.SENDING, sendingAt = resentOnNextDay) }
        }

        val settlement = StatementSettler(store, clock).settle(store.current.id, dayD) { emptyList() }

        assertEquals(ReconVerdict.RESENT_LATER, settlement.verdict)
        assertEquals(resentOnNextDay, settlement.transfer!!.sendingAt)
        assertEquals(listOf("fail SENDING E002"), store.calls)          // 상태만 보면 통과했을 확정이 sendingAt 조건에 걸려 0건
        assertEquals(TransferStatus.SENDING, store.current.status)       // 환불 없음 — 은행이 지금 처리 중일 수 있다
    }

    @Test
    fun `경합 - 다시 보내진 UNKNOWN 이 은행 줄과 같아도 자동 완료하지 않는다 (그 날짜의 대사가 맡는다)`() {
        val store = OneTransferStore(transfer(TransferStatus.UNKNOWN).copy(sendingAt = sentOnD)).apply {
            race = { it.copy(status = TransferStatus.UNKNOWN, sendingAt = resentOnNextDay) }
        }

        val settlement = StatementSettler(store, clock).settle(store.current.id, dayD) { listOf(paid) }

        assertEquals(ReconVerdict.RESENT_LATER, settlement.verdict)
        assertEquals(listOf("transition UNKNOWN->COMPLETED 0000"), store.calls)
        assertEquals(TransferStatus.UNKNOWN, store.current.status)
    }

    @Test
    fun `리더가 읽은 뒤 판정 전에 이미 다른 날로 옮겨 갔으면 확정을 시도하지도 않는다`() {
        val store = OneTransferStore(transfer(TransferStatus.SENDING).copy(sendingAt = resentOnNextDay))

        val settlement = StatementSettler(store, clock).settle(store.current.id, dayD) { emptyList() }

        assertEquals(ReconVerdict.RESENT_LATER, settlement.verdict)
        assertTrue(store.calls.isEmpty())
    }

    @Test
    fun `범위 안이면 범위를 준 확정도 그대로 - SENDING + 없음 = fail(E002)`() {
        val store = OneTransferStore(transfer(TransferStatus.SENDING).copy(sendingAt = sentOnD))

        val settlement = StatementSettler(store, clock).settle(store.current.id, dayD) { emptyList() }

        assertEquals(ReconVerdict.AUTO_FAIL, settlement.verdict)
        assertEquals(TransferStatus.FAILED, store.current.status)
    }

    @Test
    fun `우리에게 없는 거래번호의 줄 - UNKNOWN_REFERENCE, 저장소를 보지 않는다`() {
        val store = OneTransferStore(transfer(TransferStatus.UNKNOWN))

        val settlement = StatementSettler(store, clock).unknownReference(listOf(paid))

        assertEquals(ReconVerdict.UNKNOWN_REFERENCE, settlement.verdict)
        assertEquals(null, settlement.transfer)
        assertTrue(store.calls.isEmpty())
    }

    private companion object {
        const val REF = "T261028SETTLER000001"
    }
}
