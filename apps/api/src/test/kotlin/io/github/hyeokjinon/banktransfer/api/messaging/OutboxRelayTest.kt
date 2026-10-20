package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.ConsumerFactory
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class OutboxRelayTest : ApiIntegrationTest() {

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var consumerFactory: ConsumerFactory<String, String>

    private val references = RandomReferenceGenerator()

    /** 계좌는 모의 은행에 없는 번호라 컨슈머가 처리하면 FAILED 가 된다 (이 테스트는 발행만 본다). */
    private fun saveTransfer(): Transfer {
        val now = Instant.now()
        val t = Transfer(
            id = UUID.randomUUID(), idempotencyKey = UUID.randomUUID().toString(), reference = references.next(),
            requestHash = "h".repeat(64), bankCode = "090", accountNo = "5500000000000", amount = 1_000, memo = "relay",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        return store.saveWithEvent(t, TransferEvent.TransferRequested(t.id, now))
    }

    private fun pendingCount(): Int =
        jdbc.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL", Int::class.java)!!

    private fun publishAll(): Int {
        var total = 0
        while (true) {
            val n = relay.publishPending()
            if (n == 0) return total
            total += n
        }
    }

    @Test
    fun `미발행 이벤트를 key = 이체 id 로 발행하고 published_at 을 기록한다`() {
        val transfer = saveTransfer()

        publishAll()

        val publishedAt = jdbc.queryForObject(
            "SELECT published_at FROM outbox_event WHERE aggregate_id = ?", java.sql.Timestamp::class.java, transfer.id.toString(),
        )
        assertNotNull(publishedAt)

        // 별도 그룹의 소비자로 토픽을 처음부터 읽어 메시지 모양을 확인한다
        consumerFactory.createConsumer("relay-test-${UUID.randomUUID()}", null).use { consumer ->
            consumer.subscribe(listOf(TransferTopics.REQUESTED))
            val seen = mutableListOf<ConsumerRecord<String, String>>()
            await.atMost(Duration.ofSeconds(20)) untilAsserted {
                seen += consumer.poll(Duration.ofMillis(500))
                val record = seen.single { it.key() == transfer.id.toString() }
                assertTrue("\"transferId\": \"${transfer.id}\"" in record.value() || "\"transferId\":\"${transfer.id}\"" in record.value(), record.value())
                assertTrue("\"eventId\"" in record.value(), record.value())
            }
        }
    }

    @Test
    fun `발행기 두 개가 동시에 돌아도 이벤트마다 한 번만 발행한다 (SKIP LOCKED)`() {
        repeat(20) { saveTransfer() }
        val pending = pendingCount()

        val pool = Executors.newFixedThreadPool(2)
        val published = List(2) { pool.submit(Callable { publishAll() }) }.sumOf { it.get() }
        pool.shutdown()

        assertEquals(pending, published)   // 같은 행을 두 번 보냈다면 합계가 더 크다
        assertEquals(0, pendingCount())
    }
}
