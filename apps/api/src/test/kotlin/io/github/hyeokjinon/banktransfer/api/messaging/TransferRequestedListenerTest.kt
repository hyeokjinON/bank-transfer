package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.support.captureLogs
import io.github.hyeokjinon.banktransfer.transfer.ProcessResult
import ch.qos.logback.classic.Level
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import java.nio.ByteBuffer
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TransferRequestedListenerTest {

    private class CountingAck : Acknowledgment {
        var count = 0
        override fun acknowledge() { count++ }
    }

    /** 받은 알림을 모은다 (P5-1). */
    private class RecordingNotifier : AlertNotifier {
        val alerts = mutableListOf<Alert>()
        override fun notify(alert: Alert) {
            alerts += alert
        }
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

    // --- DLT 핸들러: 이유별로 로그 수준 · 내용이 다르다. 항상 ack ---

    private val listener = TransferRequestedListener { ProcessResult.COMPLETED }

    private fun dltRecord(key: String, causeFqcn: String?, message: String? = null) =
        ConsumerRecord(TransferTopics.DLT, 1, 7L, key, """{"accountNo":"1002123456789"}""").apply {
            headers().add(KafkaHeaders.ORIGINAL_TOPIC, TransferTopics.REQUESTED.toByteArray())
            headers().add(KafkaHeaders.ORIGINAL_PARTITION, ByteBuffer.allocate(Int.SIZE_BYTES).putInt(1).array())
            headers().add(KafkaHeaders.ORIGINAL_OFFSET, ByteBuffer.allocate(Long.SIZE_BYTES).putLong(42).array())
            causeFqcn?.let { headers().add(KafkaHeaders.EXCEPTION_CAUSE_FQCN, it.toByteArray()) }
            message?.let { headers().add(KafkaHeaders.EXCEPTION_MESSAGE, it.toByteArray()) }
        }

    @Test
    fun `DLT - 은행 연결 없음이면 WARN, 이체는 RECEIVED 로 남아 정리 작업이 재발행한다 - ack 한다`() {
        val ack = CountingAck()
        val id = UUID.randomUUID().toString()

        val logs = captureLogs(TransferRequestedListener::class.java) { listener.onDlt(dltRecord(id, RetryLaterException::class.java.name), ack) }

        assertEquals(1, ack.count)
        val log = logs.single()
        assertEquals(Level.WARN, log.level)
        assertTrue(log.formattedMessage.contains(id))
        assertTrue(log.formattedMessage.contains("reconcile will republish"))
        assertTrue(log.formattedMessage.contains("${TransferTopics.REQUESTED}-1@42"))   // 원래 위치
    }

    @Test
    fun `DLT - 잘못된 key 는 WARN 으로 키를 한 번만, 정리해서 짧게 남긴다 - ack 한다`() {
        val ack = CountingAck()
        val badKey = "evil\r\nFAKE LOG LINE " + "x".repeat(200)

        val logs = captureLogs(TransferRequestedListener::class.java) {
            listener.onDlt(dltRecord(badKey, InvalidTransferKeyException::class.java.name, "Listener method threw exception"), ack)
        }

        assertEquals(1, ack.count)
        val log = logs.single()
        assertEquals(Level.WARN, log.level)
        val text = log.formattedMessage
        assertFalse(text.contains('\r') || text.contains('\n'))
        assertTrue(text.contains("evil??FAKE LOG LINE"))
        assertEquals(1, Regex("evil").findAll(text).count())   // 키는 한 번만
        assertTrue(text.length < 300)                          // 200자짜리 키가 그대로 실리지 않는다
    }

    @Test
    fun `DLT - 그 밖의 예외(DB 장애 등)는 ERROR - 자동으로 다시 하지 않으니 확인하라고 알리고 ack 한다`() {
        val ack = CountingAck()
        val id = UUID.randomUUID().toString()
        val dbError = "org.springframework.dao.DataAccessResourceFailureException"
        val message = realDltMessage("could not\nconnect " + "y".repeat(500))

        val logs = captureLogs(TransferRequestedListener::class.java) { listener.onDlt(dltRecord(id, dbError, message), ack) }

        assertEquals(1, ack.count)
        val log = logs.single()
        assertEquals(Level.ERROR, log.level)
        val text = log.formattedMessage
        assertTrue(text.contains("not retried automatically"))
        assertTrue(text.contains("sending_at"))
        assertTrue(text.contains(id))
        assertTrue(text.contains("DataAccessResourceFailureException"))
        assertTrue(text.contains("cause=could not?connect"))      // 래퍼 문구가 아니라 원래 예외 메시지가 보인다
        assertFalse(text.contains("Endpoint handler details"))
        assertTrue(text.contains("${TransferTopics.REQUESTED}-1@42"))
        assertFalse(text.contains('\n'))
        assertTrue(text.length < 700)                           // 500자짜리 메시지가 잘린다
        assertFalse(text.contains("1002123456789"))             // 레코드 값(계좌번호)은 남기지 않는다
    }

    @Test
    fun `DLT - 원인 헤더가 없으면 예외 헤더로 가르고, 둘 다 없어도 ack 한다`() {
        val ack = CountingAck()
        val record = ConsumerRecord(TransferTopics.DLT, 0, 0L, UUID.randomUUID().toString(), "{}").apply {
            headers().add(KafkaHeaders.EXCEPTION_FQCN, RetryLaterException::class.java.name.toByteArray())
        }
        val none = ConsumerRecord(TransferTopics.DLT, 0, 1L, UUID.randomUUID().toString(), "{}")

        val logs = captureLogs(TransferRequestedListener::class.java) { listener.onDlt(record, ack); listener.onDlt(none, ack) }

        assertEquals(2, ack.count)
        assertEquals(listOf(Level.WARN, Level.ERROR), logs.map { it.level })
    }

    @Test
    fun `dltKind - 원인 클래스 이름으로 가른다`() {
        assertEquals(DltKind.BANK_NOT_CONNECTED, dltKind(RetryLaterException::class.java.name))
        assertEquals(DltKind.INVALID_KEY, dltKind(InvalidTransferKeyException::class.java.name))
        assertEquals(DltKind.OTHER, dltKind("org.springframework.dao.DataAccessResourceFailureException"))
        assertEquals(DltKind.OTHER, dltKind(null))
    }

    @Test
    fun `logSafe - 제어문자는 바꾸고 길이는 자른다`() {
        assertEquals("a?b?c", logSafe("a\nb\tc", 64))
        assertEquals("abc...", logSafe("abcdef", 3))
        assertEquals("null", logSafe(null, 10))
    }

    @Test
    fun `rootMessage - spring-kafka 4_1 의 래퍼 메시지 뒤 원래 예외 메시지만 남긴다 (그 안의 세미콜론은 지킨다)`() {
        assertEquals("boom; retry later", rootMessage(realDltMessage("boom; retry later")))
        assertEquals("plain", rootMessage("plain"))
        assertNull(rootMessage(null))
    }

    // --- 알림 (P5-1): 사람이 봐야 하는 경고 3곳 중 두 곳이 여기다 ---

    @Test
    fun `CONFLICT 면 WARN 알림 하나 - 이체 id 만 싣는다`() {
        val alerts = RecordingNotifier()
        val id = UUID.randomUUID()

        TransferRequestedListener(alerts) { ProcessResult.CONFLICT }.on(record(id), CountingAck())

        val alert = alerts.alerts.single()
        assertEquals(AlertLevel.WARN, alert.level)
        assertTrue(alert.lines.any { id.toString() in it })
    }

    @Test
    fun `알림이 예외를 던져도 ack 한다 - 알림 실패가 메시지 처리를 바꾸지 않는다`() {
        val ack = CountingAck()

        TransferRequestedListener({ error("telegram down") }) { ProcessResult.CONFLICT }.on(record(UUID.randomUUID()), ack)

        assertEquals(1, ack.count)
    }

    @Test
    fun `DLT - 은행 장애가 아닌 이유(DB 장애 등)만 WARN 알림, 은행 연결 없음 · 잘못된 key 는 알리지 않는다`() {
        val alerts = RecordingNotifier()
        val listener = TransferRequestedListener(alerts) { ProcessResult.COMPLETED }
        val id = UUID.randomUUID().toString()

        listener.onDlt(dltRecord(id, RetryLaterException::class.java.name), CountingAck())
        listener.onDlt(dltRecord("not-a-uuid", InvalidTransferKeyException::class.java.name), CountingAck())
        listener.onDlt(dltRecord(id, "org.springframework.dao.DataAccessResourceFailureException", realDltMessage("db down")), CountingAck())

        val alert = alerts.alerts.single()
        assertEquals(AlertLevel.WARN, alert.level)
        assertTrue(alert.lines.any { id in it })
        assertTrue(alert.lines.any { "DataAccessResourceFailureException" in it })
        assertFalse(alert.lines.any { "1002123456789" in it })   // 레코드 값(계좌번호)은 싣지 않는다
    }

    /** spring-kafka 4.1.1 DeadLetterRecordManager 가 EXCEPTION_MESSAGE 헤더에 넣는 모양: 래퍼 메시지 + "; " + 원래 예외 메시지. */
    private fun realDltMessage(rootCause: String) =
        "Listener method 'public void io.github.hyeokjinon.banktransfer.api.messaging.TransferRequestedListener.on(" +
            "org.apache.kafka.clients.consumer.ConsumerRecord,org.springframework.kafka.support.Acknowledgment)' threw exception\n" +
            "Endpoint handler details:\n" +
            "Method [public void io.github.hyeokjinon.banktransfer.api.messaging.TransferRequestedListener.on(" +
            "org.apache.kafka.clients.consumer.ConsumerRecord,org.springframework.kafka.support.Acknowledgment)]\n" +
            "Bean [io.github.hyeokjinon.banktransfer.api.messaging.TransferRequestedListener@1a2b3c]; " + rootCause
}
