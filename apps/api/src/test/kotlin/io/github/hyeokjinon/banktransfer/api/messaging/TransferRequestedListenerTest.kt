package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.transfer.ProcessResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.support.Acknowledgment
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class TransferRequestedListenerTest {

    private class CountingAck : Acknowledgment {
        var count = 0
        override fun acknowledge() { count++ }
    }

    private fun record(id: UUID) = ConsumerRecord(TransferTopics.REQUESTED, 0, 0L, id.toString(), """{"transferId":"$id"}""")

    @Test
    fun `처리가 끝나면 ack 한다`() {
        val id = UUID.randomUUID()
        val ack = CountingAck()
        var processed: UUID? = null

        TransferRequestedListener { processed = it; ProcessResult.COMPLETED }.on(record(id), ack)

        assertEquals(id, processed)
        assertEquals(1, ack.count)
    }

    @Test
    fun `이미 처리된 메시지도 ack 한다 (다시 받지 않게)`() {
        val ack = CountingAck()

        TransferRequestedListener { ProcessResult.ALREADY_HANDLED }.on(record(UUID.randomUUID()), ack)

        assertEquals(1, ack.count)
    }

    @Test
    fun `은행에 못 보냈으면 ack 하지 않고 예외를 던져 재시도하게 한다`() {
        val ack = CountingAck()

        assertFailsWith<RetryLaterException> {
            TransferRequestedListener { ProcessResult.RETRY_LATER }.on(record(UUID.randomUUID()), ack)
        }
        assertEquals(0, ack.count)
    }
}
