package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.StoreConflict
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class JpaTransferStoreTest : ApiIntegrationTest() {

    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var transfers: TransferJpaRepository
    @Autowired private lateinit var outbox: OutboxEventJpaRepository
    @Autowired private lateinit var tx: TransactionTemplate
    @Autowired private lateinit var jdbc: JdbcTemplate

    private val references = RandomReferenceGenerator()

    private fun newTransfer(key: String = UUID.randomUUID().toString(), reference: String = references.next()): Transfer {
        val now = Instant.parse("2026-10-19T01:00:00.123456Z")
        return Transfer(
            id = UUID.randomUUID(), idempotencyKey = key, reference = reference, requestHash = "h".repeat(64),
            bankCode = "090", accountNo = "1002123456789", amount = 50_000, memo = "출금",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
    }

    private fun requested(t: Transfer) = TransferEvent.TransferRequested(t.id, t.createdAt)

    @Test
    fun `이체와 아웃박스 이벤트를 함께 저장한다`() {
        val transfer = newTransfer()

        val saved = store.saveWithEvent(transfer, requested(transfer))

        assertEquals(transfer, saved)
        assertEquals(transfer, store.findById(transfer.id))
        assertEquals(transfer, store.findByIdempotencyKey(transfer.idempotencyKey))
        assertEquals(1, outbox.countByAggregateId(transfer.id.toString()))
        val payloadTransferId = jdbc.queryForObject(
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(payload, '$.transferId')) FROM outbox_event WHERE aggregate_id = ?",
            String::class.java, transfer.id.toString(),
        )
        assertEquals(transfer.id.toString(), payloadTransferId)
    }

    @Test
    fun `같은 키면 DuplicateIdempotencyKey 이고 두 번째 이벤트는 남지 않는다`() {
        val first = newTransfer()
        store.saveWithEvent(first, requested(first))
        val second = newTransfer(key = first.idempotencyKey)

        assertFailsWith<StoreConflict.DuplicateIdempotencyKey> { store.saveWithEvent(second, requested(second)) }
        assertEquals(0, outbox.countByAggregateId(second.id.toString()))
    }

    @Test
    fun `같은 거래번호면 DuplicateReference`() {
        val first = newTransfer()
        store.saveWithEvent(first, requested(first))
        val second = newTransfer(reference = first.reference)

        assertFailsWith<StoreConflict.DuplicateReference> { store.saveWithEvent(second, requested(second)) }
    }

    @Test
    fun `아웃박스 저장이 실패하면 이체도 저장되지 않는다`() {
        val broken = JpaTransferStore(transfers, outbox, tx, payloadOf = { "not-json" })
        val transfer = newTransfer()

        assertFails { broken.saveWithEvent(transfer, requested(transfer)) }
        assertNull(store.findById(transfer.id))
    }

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
    fun `상태 표에 없는 전이는 DB 에 가기 전에 거부한다`() {
        val transfer = newTransfer()
        store.saveWithEvent(transfer, requested(transfer))

        assertFailsWith<IllegalArgumentException> {
            store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.COMPLETED, Instant.now())
        }
        assertEquals(TransferStatus.RECEIVED, store.findById(transfer.id)!!.status)
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
    fun `결과조회 대상 - sending_at 이 유예 기준보다 이른 UNKNOWN 만`() {
        val now = Instant.now()
        val old = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val fresh = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        listOf(old to now.minusSeconds(31), fresh to now.minusSeconds(1)).forEach { (t, sendingAt) ->
            store.transition(t.id, TransferStatus.RECEIVED, TransferStatus.SENDING, sendingAt)
            store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, now)   // updated_at 은 now, sending_at 은 그대로
        }

        val found = store.findUnknownForInquiry(10, now.minusSeconds(30), 1_000).map { it.id }

        assertTrue(old.id in found)
        assertFalse(fresh.id in found)   // 방금 UNKNOWN 이 된 건은 유예 안쪽 — 아직 묻지 않는다
    }

    @Test
    fun `결과조회 대상 - 시도 횟수가 한도 미만인 UNKNOWN, 시도 기록은 UNKNOWN 일 때만`() {
        val unknown = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val at = Instant.now()
        store.transition(unknown.id, TransferStatus.RECEIVED, TransferStatus.SENDING, at)
        store.transition(unknown.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, at)

        assertTrue(store.recordInquiryAttempt(unknown.id, at))

        assertEquals(1, store.findById(unknown.id)!!.inquiryAttempts)
        val graceOver = at.plusSeconds(1)   // sending_at(at) 은 이 기준보다 이르다
        assertFalse(unknown.id in store.findUnknownForInquiry(1, graceOver, 1_000).map { it.id })
        assertTrue(unknown.id in store.findUnknownForInquiry(2, graceOver, 1_000).map { it.id })

        val received = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        assertFalse(store.recordInquiryAttempt(received.id, at))   // UNKNOWN 이 아니면 늘리지 않는다
    }
}
