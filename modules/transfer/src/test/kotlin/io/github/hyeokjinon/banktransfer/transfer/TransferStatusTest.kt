package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TransferStatusTest {

    @Test
    fun `설계 문서 3장의 전이만 허용한다`() {
        val allowed = setOf(
            RECEIVED to SENDING, RECEIVED to FAILED,   // RECEIVED → FAILED: 재발행 한도 뒤 포기 (P3-3b, E001)
            SENDING to RECEIVED, SENDING to COMPLETED, SENDING to FAILED, SENDING to UNKNOWN,
            UNKNOWN to COMPLETED, UNKNOWN to FAILED,
        )
        for (from in TransferStatus.entries) {
            for (to in TransferStatus.entries) {
                assertEquals((from to to) in allowed, from.canMoveTo(to), "$from -> $to")
            }
        }
    }

    @Test
    fun `COMPLETED 와 FAILED 만 종결 상태다`() {
        assertEquals(setOf(COMPLETED, FAILED), TransferStatus.entries.filter { it.isFinal }.toSet())
    }

    @Test
    fun `SENDING 이 되면 sendingAt 을 기록한다`() {
        val at = Instant.parse("2026-10-19T01:00:00Z")
        val moved = transfer(RECEIVED).moveTo(SENDING, at)

        assertEquals(SENDING, moved.status)
        assertEquals(at, moved.sendingAt)
        assertEquals(at, moved.updatedAt)
    }

    @Test
    fun `금지된 전이는 IllegalTransition`() {
        assertFailsWith<TransferException.IllegalTransition> { transfer(RECEIVED).moveTo(COMPLETED, Instant.now()) }
        assertFailsWith<TransferException.IllegalTransition> { transfer(COMPLETED).moveTo(FAILED, Instant.now()) }
        assertTrue(transfer(UNKNOWN).status.canMoveTo(COMPLETED))
        assertFalse(transfer(FAILED).status.canMoveTo(SENDING))
    }

    private fun transfer(status: TransferStatus) = Transfer(
        id = UUID.randomUUID(),
        idempotencyKey = "key",
        reference = "T2610190000000000001",
        requestHash = "h",
        bankCode = "090",
        accountNo = "1002123456789",
        amount = 1000,
        memo = "출금",
        status = status,
        createdAt = Instant.parse("2026-10-19T00:00:00Z"),
        updatedAt = Instant.parse("2026-10-19T00:00:00Z"),
    )
}
