package io.github.hyeokjinon.banktransfer.transfer

import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

/**
 * begin · finish (P6-1). 코루틴 컨슈머는 이 둘 사이에서 은행만 suspend 로 부른다 — 판정은 여기 한 곳이다.
 * process 의 동작이 같다는 근거는 고치지 않은 TransferProcessorTest 이고, 여기서는 나눈 두 단계와 "process 와 같은 결과" 를 본다.
 */
class TransferProcessorBeginFinishTest {

    /** TransferProcessorTest 의 가짜 저장소와 같은 규칙 (그 테스트는 고치지 않고 통과해야 해서 따로 둔다). */
    private class FakeStore : TransferStore {
        val transfers = linkedMapOf<UUID, Transfer>()
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

    private val now = Instant.parse("2026-11-04T01:00:00Z")
    private val clock = Clock.fixed(now, ZoneOffset.UTC)
    private val store = FakeStore()

    /** begin · finish 는 은행을 부르지 않는다 — 부르면 테스트가 깨진다 (코루틴 판은 블로킹 포트 없이 이 둘만 쓴다). */
    private val processor = TransferProcessor(store, BankTransferPort { _, _, _, _, _ -> error("begin · finish 는 은행을 부르지 않는다") }, clock)

    private fun received(): Transfer {
        val t = Transfer(
            id = UUID.randomUUID(), idempotencyKey = "k", reference = "T2611040000000000001", requestHash = "h",
            bankCode = "090", accountNo = "1002123456789", amount = 50_000, memo = "출금",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        store.transfers[t.id] = t
        return t
    }

    @Test
    fun `begin - RECEIVED 면 SENDING 으로 바꾸고 보낼 출금을 돌려준다`() {
        val t = received()

        assertEquals(TransferProcessor.Begin.Started(t), processor.begin(t.id))

        val after = store.transfers.getValue(t.id)
        assertEquals(TransferStatus.SENDING, after.status)
        assertEquals(now, after.sendingAt)
    }

    @Test
    fun `begin - 두 번째는 AlreadyHandled (같은 메시지가 다시 와도 은행에는 한 번)`() {
        val t = received()
        processor.begin(t.id)

        assertEquals(TransferProcessor.Begin.AlreadyHandled, processor.begin(t.id))
    }

    @Test
    fun `begin - 없는 출금은 NotFound`() {
        assertEquals(TransferProcessor.Begin.NotFound, processor.begin(UUID.randomUUID()))
    }

    @Test
    fun `finish - 은행 결과마다 process 와 같은 상태 · 응답코드 · 결과`() {
        val cases = listOf(
            BankTransferOutcome.Completed to ProcessResult.COMPLETED,
            BankTransferOutcome.Rejected("1001") to ProcessResult.FAILED,
            BankTransferOutcome.ResultUnknown to ProcessResult.UNKNOWN,
            BankTransferOutcome.NotSent to ProcessResult.RETRY_LATER,
        )
        for ((outcome, expected) in cases) {
            val split = received()
            assertIs<TransferProcessor.Begin.Started>(processor.begin(split.id))
            val splitResult = processor.finish(split.id, outcome)

            val whole = received()
            val wholeResult = TransferProcessor(store, BankTransferPort { _, _, _, _, _ -> outcome }, clock).process(whole.id)

            assertEquals(expected, splitResult, "$outcome")
            assertEquals(wholeResult, splitResult, "$outcome")
            assertEquals(store.transfers.getValue(whole.id).status, store.transfers.getValue(split.id).status, "$outcome")
            assertEquals(store.transfers.getValue(whole.id).bankResponseCode, store.transfers.getValue(split.id).bankResponseCode, "$outcome")
        }
    }

    @Test
    fun `finish - 거절은 SENDING 에서 fail() 로 확정한다 (환불 경로)`() {
        val t = received()
        processor.begin(t.id)

        assertEquals(ProcessResult.FAILED, processor.finish(t.id, BankTransferOutcome.Rejected("1001")))
        assertEquals(listOf(Triple(t.id, TransferStatus.SENDING, "1001")), store.failCalls)
    }

    @Test
    fun `finish - 정리 작업이 먼저 UNKNOWN 으로 바꿨어도 은행의 확정 결과로 기록한다`() {
        val t = received()
        processor.begin(t.id)
        store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, now)   // 응답이 30초 넘게 늦은 사이에 정리 작업이 끼어듦

        assertEquals(ProcessResult.COMPLETED, processor.finish(t.id, BankTransferOutcome.Completed))
        assertEquals(TransferStatus.COMPLETED, store.transfers.getValue(t.id).status)
        assertEquals("0000", store.transfers.getValue(t.id).bankResponseCode)
    }

    @Test
    fun `finish - 이미 다른 결과로 확정됐으면 CONFLICT, 잔고는 더 건드리지 않는다`() {
        val t = received()
        processor.begin(t.id)
        store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, now)
        store.fail(t.id, TransferStatus.UNKNOWN, now, "2001")   // 결과조회가 먼저 FAILED 로 확정 (환불까지)

        assertEquals(ProcessResult.CONFLICT, processor.finish(t.id, BankTransferOutcome.Completed))
        assertEquals(TransferStatus.FAILED, store.transfers.getValue(t.id).status)
        assertEquals(1, store.failCalls.size)
    }

    @Test
    fun `finish - 미전송이면 RECEIVED 로 되돌려 다음 begin 이 다시 Started`() {
        val t = received()
        processor.begin(t.id)

        assertEquals(ProcessResult.RETRY_LATER, processor.finish(t.id, BankTransferOutcome.NotSent))
        assertEquals(TransferStatus.RECEIVED, store.transfers.getValue(t.id).status)
        assertIs<TransferProcessor.Begin.Started>(processor.begin(t.id))
    }
}
