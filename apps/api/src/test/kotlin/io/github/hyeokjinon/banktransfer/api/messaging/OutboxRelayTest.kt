package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.awaitRecords
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.ConsumerFactory
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** 아웃박스 발행기 (P3-2). 사용자는 테스트마다 새로 충전한다 (접수 = 차감, P4-1). */
class OutboxRelayTest : ApiIntegrationTest() {

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var consumerFactory: ConsumerFactory<String, String>

    private val references = RandomReferenceGenerator()
    private val user by lazy { fundedUser() }

    /** 계좌는 모의 은행에 없는 번호라 컨슈머가 처리하면 FAILED 가 된다 (이 테스트는 발행만 본다). */
    private fun saveTransfer(): Transfer {
        val now = Instant.now()
        val t = Transfer(
            id = UUID.randomUUID(), userId = user, idempotencyKey = UUID.randomUUID().toString(), reference = references.next(),
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

        // 토픽을 처음부터 끝 오프셋까지 읽어(assign, 그룹 없음) 메시지 모양을 확인한다.
        // 그룹 구독 + poll 로 읽으면 안 된다: poll 한 번은 max-poll-records(50) 건까지만 주고(전체 빌드에서는 앞 테스트들의 레코드가 40건 넘게 쌓여 있다),
        // 첫 poll 은 그룹 참여가 늦으면 빈 채로 끝난다. 거기서 single 이 NoSuchElementException 을 던지면 untilAsserted 는 AssertionError 만 다시 시도하므로 곧바로 실패했다
        val records = consumerFactory.awaitRecords(TransferTopics.REQUESTED, transfer.id.toString(), timeout = Duration.ofSeconds(20))
        assertEquals(1, records.size, "key=${transfer.id} 레코드가 ${records.size}건 (한 번만 발행돼야 한다)")
        val value = records.single().value()
        assertTrue("\"transferId\": \"${transfer.id}\"" in value || "\"transferId\":\"${transfer.id}\"" in value, value)
        assertTrue("\"eventId\"" in value, value)
    }

    @Test
    fun `발행기 두 개가 동시에 돌아도 이벤트마다 한 번만 발행한다 (이중 발행 없음)`() {
        repeat(20) { saveTransfer() }
        val pending = pendingCount()

        val pool = Executors.newFixedThreadPool(2)
        val published = List(2) { pool.submit(Callable { publishAll() }) }.sumOf { it.get() }
        pool.shutdown()

        assertEquals(pending, published)   // 같은 행을 두 번 보냈다면 합계가 더 크다
        assertEquals(0, pendingCount())
    }

    /**
     * 발행기가 SELECT … FOR UPDATE SKIP LOCKED 로 행을 잡고 있는 동안(= Kafka 응답을 기다리는 동안)에도 접수의 outbox INSERT 는 막히지 않아야 한다.
     * REPEATABLE READ 에서는 이 SELECT 가 next-key/gap 락을 잡아 INSERT 가 락 대기(ERROR 1205)에 걸린다. READ COMMITTED 는 고른 행만 잡는다.
     */
    @Test
    fun `발행기가 잡고 있는 동안에도 새 접수(아웃박스 INSERT)는 막히지 않는다`() {
        saveTransfer()   // 미발행 행이 하나는 있어야 발행기의 SELECT 가 락을 잡는다
        val locked = CountDownLatch(1)
        val release = CountDownLatch(1)
        val pool = Executors.newSingleThreadExecutor()
        val holder = pool.submit {
            relay.tx.execute {
                jdbc.query(OutboxRelay.SELECT_PENDING_SQL, { rs, _ -> rs.getLong("id") }, 100)
                locked.countDown()
                release.await(30, TimeUnit.SECONDS)   // 발행기가 Kafka 응답을 기다리는 시간
            }
        }
        try {
            assertTrue(locked.await(10, TimeUnit.SECONDS), "발행기 쪽 SELECT 가 락을 잡지 못했다")

            assertTimeoutPreemptively(Duration.ofSeconds(5)) { saveTransfer() }
        } finally {
            release.countDown()
            holder.get(10, TimeUnit.SECONDS)
            pool.shutdown()
        }
    }
}
