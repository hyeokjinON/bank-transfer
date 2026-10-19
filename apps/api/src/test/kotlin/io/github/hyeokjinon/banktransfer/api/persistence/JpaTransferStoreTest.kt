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
import kotlin.test.assertNull

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
}
