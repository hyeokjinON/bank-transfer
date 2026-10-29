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
     * 출금 하나만 담는 가짜 저장소. 조건부 전이 · 실패 확정은 현재 상태가 from 일 때만 바꾼다 (진짜와 같다).
     * [raceTo] 를 주면 첫 확정 시도 직전에 다른 쪽(정리 작업 · 컨슈머)이 그 상태로 먼저 바꾼 것처럼 만든다.
     * [snapshotStale] 이면 확정은 늘 0건이고 다시 읽어도 옛 상태가 보인다 (REPEATABLE READ 스냅숏 흉내).
     */
    private class OneTransferStore(var current: Transfer) : TransferStore {
        val calls = mutableListOf<String>()
        var raceTo: TransferStatus? = null
        var snapshotStale = false

        private fun changes(from: TransferStatus): Boolean {
            raceTo?.let { current = current.copy(status = it); raceTo = null }
            return !snapshotStale && current.status == from
        }

        override fun findById(id: UUID): Transfer? = current.takeIf { it.id == id }

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean {
            calls += "transition $from->$to $bankResponseCode"
            if (!changes(from)) return false
            current = current.copy(status = to, bankResponseCode = bankResponseCode)
            return true
        }

        override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String): Boolean {
            calls += "fail $from $bankResponseCode"
            if (!changes(from)) return false
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
