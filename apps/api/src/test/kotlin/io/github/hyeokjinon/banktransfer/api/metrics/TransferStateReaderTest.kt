package io.github.hyeokjinon.banktransfer.api.metrics

import io.github.hyeokjinon.banktransfer.api.reconcile.ReconcileProperties
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.UUID
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 상태 Gauge 의 SQL (P9a 스펙 §4-2). 공유 DB 라 다른 테스트의 행이 섞인다 —
 * 건수는 같은 순간의 독립 COUNT 와 비교하고(바뀌는 중이면 다시), 나이는 아주 오래된 행(KST 2000-01-01)을 넣어 그것이 가장 오래되게 한다.
 * 넣은 행은 끝나면 지운다 (다른 테스트의 정리 작업 호출이 집어 가지 않게).
 */
class TransferStateReaderTest : ApiIntegrationTest() {

    @Autowired private lateinit var reader: TransferStateReader
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var reconcile: ReconcileProperties

    private val owner = UUID.randomUUID().toString()
    private val outboxIds = mutableListOf<Long>()
    private val old = "2000-01-01 00:00:00.000000"
    private val oldAgeSeconds: Long get() = Duration.between(LocalDateTime.of(2000, 1, 1, 0, 0).atZone(ZoneId.of("Asia/Seoul")).toInstant(), Instant.now()).seconds

    @AfterEach
    fun removeRows() {
        jdbc.update("DELETE FROM transfer WHERE user_id = ?", owner)
        outboxIds.forEach { jdbc.update("DELETE FROM outbox_event WHERE id = ?", it) }
    }

    private fun insertTransfer(status: TransferStatus, sendingAt: String?, updatedAt: String, inquiryAttempts: Int = 0) {
        val id = UUID.randomUUID().toString()
        jdbc.update(
            """
            INSERT INTO transfer (id, user_id, idempotency_key, reference, request_hash, bank_code, account_no, amount, memo,
                                  status, sending_at, inquiry_attempts, created_at, updated_at, version)
            VALUES (?, ?, ?, ?, ?, '090', '5500000000000', 1, 'm', ?, ?, ?, ?, ?, 0)
            """.trimIndent(),
            id, owner, id, "M" + id.replace("-", "").take(19).uppercase(), "0".repeat(64),
            status.name, sendingAt, inquiryAttempts, updatedAt, updatedAt,
        )
    }

    private fun count(sql: String, vararg args: Any): Long = jdbc.queryForObject(sql, Long::class.java, *args)!!

    @Test
    fun `상태별 건수 · 결과조회 소진은 같은 순간의 독립 COUNT 와 같다`() {
        val now = "2026-11-17 12:00:00.000000"
        insertTransfer(TransferStatus.RECEIVED, null, now)
        insertTransfer(TransferStatus.RECEIVED, null, now)
        insertTransfer(TransferStatus.SENDING, now, now)
        insertTransfer(TransferStatus.UNKNOWN, now, now, inquiryAttempts = reconcile.maxInquiries)
        insertTransfer(TransferStatus.UNKNOWN, now, now, inquiryAttempts = 0)

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            val s = reader.read()
            for (status in TransferStateSnapshot.OPEN_STATUSES) {
                assertEquals(count("SELECT COUNT(*) FROM transfer WHERE status = ?", status.name), s.open.getValue(status), "$status")
            }
            assertEquals(count("SELECT COUNT(*) FROM transfer WHERE status = 'UNKNOWN' AND inquiry_attempts >= ?", reconcile.maxInquiries), s.unknownExhausted)
            assertTrue(s.open.getValue(TransferStatus.RECEIVED) >= 2)
            assertTrue(s.unknownExhausted >= 1)
        }
    }

    @Test
    fun `가장 오래된 나이는 SQL NOW(6) 로 잰다 - RECEIVED 는 updated_at, SENDING · UNKNOWN 은 sending_at`() {
        insertTransfer(TransferStatus.RECEIVED, null, old)
        insertTransfer(TransferStatus.SENDING, old, "2026-11-17 12:00:00.000000")
        insertTransfer(TransferStatus.UNKNOWN, old, "2026-11-17 12:00:00.000000")

        val s = reader.read()
        val expected = oldAgeSeconds.toDouble()

        for (status in TransferStateSnapshot.OPEN_STATUSES) {
            val actual = s.oldestSeconds.getValue(status)
            assertTrue(abs(actual - expected) < 5, "$status 나이 $actual, 기대 약 $expected")
        }
    }

    @Test
    fun `아웃박스 미발행 - 건수와 가장 오래된 나이 (가장 작은 id 로 넣어 맨 앞이 되게)`() {
        val id = -Random.nextLong(1, 1_000_000_000)
        outboxIds += id
        jdbc.update(
            "INSERT INTO outbox_event (id, aggregate_id, event_type, payload, created_at, published_at) VALUES (?, ?, 'TransferRequested', JSON_OBJECT('transferId', ?), ?, NULL)",
            id, UUID.randomUUID().toString(), "metrics-test", old,
        )

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            val s = reader.read()
            assertEquals(count("SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL"), s.outboxUnpublished)
            assertTrue(abs(s.outboxOldestSeconds - oldAgeSeconds) < 5, "아웃박스 나이 ${s.outboxOldestSeconds}")
        }
    }
}
