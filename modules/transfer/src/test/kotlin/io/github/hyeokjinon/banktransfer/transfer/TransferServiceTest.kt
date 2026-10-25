package io.github.hyeokjinon.banktransfer.transfer

import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TransferServiceTest {

    /** DB 대신 쓰는 가짜 저장소. UNIQUE 위반 상황(동시 요청 · 거래번호 충돌)을 흉내 낼 수 있다. */
    private class FakeStore : TransferStore {
        val transfers = linkedMapOf<UUID, Transfer>()
        val events = mutableListOf<TransferEvent>()

        /** 지정하면 다음 저장 직전에 "다른 요청이 먼저 같은 키로 저장한" 상황을 만든다. */
        var raceWinner: Transfer? = null

        /** 남은 횟수만큼 거래번호 충돌을 낸다. */
        var referenceConflicts = 0

        override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer {
            raceWinner?.let { winner ->
                raceWinner = null
                transfers[winner.id] = winner
                throw StoreConflict.DuplicateIdempotencyKey(transfer.idempotencyKey)
            }
            if (referenceConflicts > 0) {
                referenceConflicts--
                throw StoreConflict.DuplicateReference(transfer.reference)
            }
            transfers[transfer.id] = transfer
            events += event
            return transfer
        }

        override fun findById(id: UUID) = transfers[id]

        override fun findByIdempotencyKey(key: String) = transfers.values.firstOrNull { it.idempotencyKey == key }

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?) =
            throw UnsupportedOperationException("접수 테스트에서는 쓰지 않는다")

        override fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer> = throw UnsupportedOperationException()
        override fun findUnknownForInquiry(maxAttempts: Int, sendingBefore: Instant, limit: Int): List<Transfer> = throw UnsupportedOperationException()
        override fun recordInquiryAttempt(id: UUID, at: Instant): Boolean = throw UnsupportedOperationException()
        override fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer> = throw UnsupportedOperationException()
        override fun republish(id: UUID, expectedCount: Int, at: Instant): Boolean = throw UnsupportedOperationException()
    }

    private val store = FakeStore()
    private val verified = mutableSetOf("090" to "1002123456789")
    private var referenceSeq = 0
    private val clock = Clock.fixed(Instant.parse("2026-10-19T01:00:00Z"), ZoneOffset.UTC)
    private val service = TransferService(
        store = store,
        verifiedAccounts = { bankCode, accountNo -> (bankCode to accountNo) in verified },
        referenceGenerator = { "T261019" + "%013d".format(++referenceSeq) },
        clock = clock,
    )

    private fun command(key: String = "key-1", amount: Long = 50_000, memo: String = "출금") =
        RequestTransfer(key, "090", "1002123456789", amount, memo)

    @Test
    fun `처음 요청은 RECEIVED 로 접수하고 이벤트를 함께 남긴다`() {
        val result = service.request(command())

        assertTrue(result.created)
        assertEquals(TransferStatus.RECEIVED, result.transfer.status)
        assertEquals("T2610190000000000001", result.transfer.reference)
        assertEquals(listOf<TransferEvent>(TransferEvent.TransferRequested(result.transfer.id, clock.instant())), store.events)
    }

    @Test
    fun `같은 키 같은 내용이면 기존 건을 돌려주고 새로 만들지 않는다`() {
        val first = service.request(command()).transfer
        val again = service.request(command())

        assertFalse(again.created)
        assertEquals(first.id, again.transfer.id)
        assertEquals(1, store.transfers.size)
        assertEquals(1, store.events.size)
    }

    @Test
    fun `같은 키에 다른 내용이면 IdempotencyKeyReused`() {
        service.request(command())

        assertFailsWith<TransferException.IdempotencyKeyReused> { service.request(command(amount = 60_000)) }
    }

    @Test
    fun `인증되지 않은 계좌면 접수하지 않는다`() {
        verified.clear()

        assertFailsWith<TransferException.AccountNotVerified> { service.request(command()) }
        assertTrue(store.transfers.isEmpty())
    }

    @Test
    fun `금액은 1원 이상 1천만원 이하`() {
        for (amount in listOf(0L, -1L, MAX_TRANSFER_AMOUNT + 1)) {
            assertFailsWith<TransferException.InvalidAmount> { service.request(command(key = "k$amount", amount = amount)) }
        }
        assertTrue(service.request(command(key = "max", amount = MAX_TRANSFER_AMOUNT)).created)
    }

    @Test
    fun `적요는 EUC-KR 20바이트까지, 인코딩할 수 없는 문자는 거절`() {
        assertTrue(service.request(command(key = "m1", memo = "가".repeat(10))).created)   // 20바이트
        assertFailsWith<TransferException.InvalidMemo> { service.request(command(key = "m2", memo = "가".repeat(11))) }
        assertFailsWith<TransferException.InvalidMemo> { service.request(command(key = "m3", memo = "😀")) }
    }

    @Test
    fun `키가 비었거나 64자를 넘으면 IdempotencyKeyRequired`() {
        assertFailsWith<TransferException.IdempotencyKeyRequired> { service.request(command(key = "")) }
        assertFailsWith<TransferException.IdempotencyKeyRequired> { service.request(command(key = "a".repeat(65))) }
    }

    @Test
    fun `키가 정확히 64자면 받는다`() {
        assertTrue(service.request(command(key = "a".repeat(64))).created)
    }

    @Test
    fun `키에 공백이나 ASCII 가 아닌 문자가 있으면 IdempotencyKeyRequired`() {
        for (key in listOf("a b", " key", "key ", "키", "ordér-1", "tab	key")) {
            assertFailsWith<TransferException.IdempotencyKeyRequired>("key=[$key]") { service.request(command(key = key)) }
        }
        assertTrue(store.transfers.isEmpty())
    }

    @Test
    fun `은행코드는 숫자 3자리, 계좌번호는 숫자 10~16자리`() {
        val invalid = listOf(
            RequestTransfer("b1", "09", "1002123456789", 50_000, "출금"),          // 은행코드 2자리
            RequestTransfer("b2", "0a0", "1002123456789", 50_000, "출금"),        // 은행코드에 문자
            RequestTransfer("a1", "090", "123456789", 50_000, "출금"),             // 계좌 9자리
            RequestTransfer("a2", "090", "1|23456789012", 50_000, "출금"),        // 계좌에 구분자
            RequestTransfer("a3", "090", "12345678901234567", 50_000, "출금"),    // 계좌 17자리
        )
        for (command in invalid) {
            assertFailsWith<TransferException.InvalidAccount> { service.request(command) }
        }
        assertTrue(store.transfers.isEmpty())
    }

    @Test
    fun `같은 키가 동시에 들어와 저장에서 지면 이긴 쪽 건을 돌려준다`() {
        val now = clock.instant()
        val winner = Transfer(
            id = UUID.randomUUID(), idempotencyKey = "key-1", reference = "T2610199999999999999",
            requestHash = TransferService.requestHash(command()), bankCode = "090", accountNo = "1002123456789",
            amount = 50_000, memo = "출금", status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        store.raceWinner = winner

        val result = service.request(command())

        assertFalse(result.created)
        assertEquals(winner.id, result.transfer.id)
        assertTrue(store.events.isEmpty())
    }

    @Test
    fun `동시 요청에서 졌는데 이긴 쪽의 내용이 다르면 IdempotencyKeyReused`() {
        val now = clock.instant()
        store.raceWinner = Transfer(
            id = UUID.randomUUID(), idempotencyKey = "key-1", reference = "T2610199999999999999",
            requestHash = TransferService.requestHash(command(amount = 70_000)), bankCode = "090", accountNo = "1002123456789",
            amount = 70_000, memo = "출금", status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )

        assertFailsWith<TransferException.IdempotencyKeyReused> { service.request(command()) }
        assertTrue(store.events.isEmpty())
    }

    @Test
    fun `거래번호가 겹치면 새 번호로 다시 시도한다`() {
        store.referenceConflicts = 1

        val result = service.request(command())

        assertTrue(result.created)
        assertEquals("T2610190000000000002", result.transfer.reference)
    }

    @Test
    fun `없는 id 는 TransferNotFound`() {
        assertFailsWith<TransferException.TransferNotFound> { service.get(UUID.randomUUID()) }
    }
}
