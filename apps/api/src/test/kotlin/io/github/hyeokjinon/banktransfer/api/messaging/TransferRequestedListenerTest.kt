package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.transfer.ProcessResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import java.nio.ByteBuffer
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

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

    @Test
    fun `은행 결과가 기록과 충돌해도 ack 한다 (재시도로 풀 수 없다, 사람이 확인)`() {
        val ack = CountingAck()

        TransferRequestedListener { ProcessResult.CONFLICT }.on(record(UUID.randomUUID()), ack)

        assertEquals(1, ack.count)
    }

    @Test
    fun `key 가 UUID 가 아니면 InvalidTransferKeyException - 재시도 없이 DLT 로 보낸다`() {
        val ack = CountingAck()
        var called = false
        val bad = ConsumerRecord(TransferTopics.REQUESTED, 0, 0L, "not-a-uuid", "{}")

        assertFailsWith<InvalidTransferKeyException> {
            TransferRequestedListener { called = true; ProcessResult.COMPLETED }.on(bad, ack)
        }
        assertFalse(called)
        assertEquals(0, ack.count)
    }

    @Test
    fun `DLT 메시지는 경고만 남기고 ack 한다 - 다시 처리하지 않고 이체는 정리 작업이 이어받는다`() {
        val ack = CountingAck()
        var called = false
        val dlt = ConsumerRecord(TransferTopics.DLT, 1, 7L, UUID.randomUUID().toString(), "{}").apply {
            headers().add(KafkaHeaders.ORIGINAL_TOPIC, TransferTopics.REQUESTED.toByteArray())
            headers().add(KafkaHeaders.ORIGINAL_PARTITION, ByteBuffer.allocate(Int.SIZE_BYTES).putInt(1).array())
            headers().add(KafkaHeaders.ORIGINAL_OFFSET, ByteBuffer.allocate(Long.SIZE_BYTES).putLong(42).array())
            headers().add(KafkaHeaders.EXCEPTION_CAUSE_FQCN, RetryLaterException::class.java.name.toByteArray())
        }

        TransferRequestedListener { called = true; ProcessResult.COMPLETED }.onDlt(dlt, ack)

        assertEquals(1, ack.count)
        assertFalse(called)
    }
}
