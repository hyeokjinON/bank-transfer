package io.github.hyeokjinon.banktransfer.api.retention

import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 지우는 조건 (P5-2, 설계 §3 · §6). 되돌릴 수 없는 삭제라 조건을 줄마다 고정한다. 실제 MySQL. */
class RetentionRepositoryTest : RetentionTestSupport() {

    private val anyLimit = 10_000

    @Test
    fun `아웃박스 - 기준 전에 발행된 행만 지운다, 기준 −1µs 는 삭제 · 기준 그대로는 유지`() {
        val cutoff = nextCutoff()
        val justBefore = outboxRow(cutoff.minus(1, ChronoUnit.MICROS))
        val exactly = outboxRow(cutoff)

        assertEquals(1, retention.deleteOutboxEvents(cutoff, anyLimit))

        assertFalse(outboxExists(justBefore))
        assertTrue(outboxExists(exactly))
    }

    @Test
    fun `아웃박스 - 미발행은 아무리 오래돼도 남는다 (NULL 은 비교에 걸리지 않는다)`() {
        val cutoff = nextCutoff()
        val unpublished = outboxRow(publishedAt = null, createdAt = cutoff.minus(Duration.ofDays(365)))

        assertEquals(0, retention.deleteOutboxEvents(cutoff, anyLimit))

        assertTrue(outboxExists(unpublished))
    }

    @Test
    fun `아웃박스 - 한 번에 limit 행까지만 지운다`() {
        val cutoff = nextCutoff()
        outboxRows(3, cutoff.minusSeconds(1))

        assertEquals(2, retention.deleteOutboxEvents(cutoff, 2))
        assertEquals(1, retention.deleteOutboxEvents(cutoff, 2))
        assertEquals(0, retention.deleteOutboxEvents(cutoff, 2))
    }

    @Test
    fun `NOW(6) 로 쓴 published_at 이 KST 로 비교된다 - 1시간 전 기준이면 남고 1시간 뒤 기준이면 지워진다 (테스트 JVM 은 UTC)`() {
        val row = outboxRow(publishedAt = null)
        jdbc.update("UPDATE outbox_event SET published_at = NOW(6) WHERE aggregate_id = ?", row)
        val now = Instant.now()

        // 실제 시각 기준 삭제는 공유 DB 의 다른 테스트 행까지 건다 → 한 트랜잭션 안에서 보고 되돌린다
        TransactionTemplate(transactionManager).apply { isolationLevel = TransactionDefinition.ISOLATION_READ_COMMITTED }.executeWithoutResult { status ->
            status.setRollbackOnly()

            // 바인딩이 KST 로 어긋나 9시간 뒤로 밀리면 여기서 지워진다
            retention.deleteOutboxEvents(now.minus(Duration.ofHours(1)), 1_000_000)
            assertTrue(outboxExists(row))

            // UTC 벽시계로 바인딩되면 기준이 9시간 앞이라 여기서 남는다
            retention.deleteOutboxEvents(now.plus(Duration.ofHours(1)), 1_000_000)
            assertFalse(outboxExists(row))
        }

        assertTrue(outboxExists(row))
    }

    @Test
    fun `1원인증 - SENT · EXPIRED · LOCKED 는 기준 −1µs 면 지우고 기준 그대로면 남긴다`() {
        val cutoff = nextCutoff()
        val justBefore = cutoff.minus(1, ChronoUnit.MICROS)
        for (status in listOf("SENT", "EXPIRED", "LOCKED")) {
            val old = verificationRow(status, justBefore)
            val exactly = verificationRow(status, cutoff)

            assertEquals(1, retention.deleteVerifications(cutoff, anyLimit), status)

            assertFalse(verificationExists(old), status)
            assertTrue(verificationExists(exactly), status)
        }
    }

    @Test
    fun `1원인증 - 코드를 입력하지 않고 시간만 지나 SENT 로 남은 행도 지운다 (EXPIRED 만 지우면 영원히 남는다)`() {
        val cutoff = nextCutoff()
        val neverConfirmed = verificationRow("SENT", cutoff.minus(Duration.ofDays(1)))

        assertEquals(1, retention.deleteVerifications(cutoff, anyLimit))

        assertFalse(verificationExists(neverConfirmed))
    }

    @Test
    fun `1원인증 - VERIFIED 는 expires_at 이 아무리 지나도 지우지 않는다 (출금 인증의 근거)`() {
        val cutoff = nextCutoff()
        val verified = verificationRow("VERIFIED", cutoff.minus(Duration.ofDays(365)))

        assertEquals(0, retention.deleteVerifications(cutoff, anyLimit))

        assertTrue(verificationExists(verified))
    }

    @Test
    fun `1원인증 - 목록에 없는 새 상태는 expires_at 이 기준보다 한참 전이어도 지우지 않는다 (VERIFIED 가 아닌 것 전부가 아니라 목록)`() {
        val cutoff = nextCutoff()
        val unknownStatus = verificationRow("PENDING", cutoff.minus(Duration.ofDays(365)))   // VARCHAR(10) 에 맞는 가상의 새 상태

        try {
            assertEquals(0, retention.deleteVerifications(cutoff, anyLimit))

            assertTrue(verificationExists(unknownStatus))
        } finally {
            jdbc.update("DELETE FROM account_verification WHERE id = ?", unknownStatus)
        }
    }

    @Test
    fun `1원인증 - 한 번에 limit 행까지만 지운다`() {
        val cutoff = nextCutoff()
        verificationRows(3, "SENT", cutoff.minusSeconds(1))

        assertEquals(2, retention.deleteVerifications(cutoff, 2))
        assertEquals(1, retention.deleteVerifications(cutoff, 2))
    }
}
