package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.junit.jupiter.api.AfterEach
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.repository.JobRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

/**
 * 보존 배치 테스트 공용 (P5-2, 공유 DB). 날짜는 [RetentionTestDates] 로만 고른다.
 * 심은 미발행 행은 테스트 끝에 지운다 — 다른 테스트의 발행기가 집어 가거나 OutboxRelayTest 의 미발행 건수에 섞이지 않게.
 * 시각은 앱과 같이 Timestamp(Instant) 로 넘긴다 (KST 변환은 JDBC 설정, database.md §2).
 */
abstract class RetentionTestSupport : ApiIntegrationTest() {

    @Autowired protected lateinit var jdbc: JdbcTemplate
    @Autowired protected lateinit var retention: RetentionRepository
    @Autowired protected lateinit var props: DataRetentionProperties
    @Autowired protected lateinit var jobRepository: JobRepository
    @Autowired protected lateinit var jobOperator: JobOperator
    @Autowired protected lateinit var transactionManager: PlatformTransactionManager

    private val unpublishedSeeds = mutableListOf<String>()

    @AfterEach
    fun removeUnpublishedSeeds() {
        unpublishedSeeds.forEach { jdbc.update("DELETE FROM outbox_event WHERE aggregate_id = ?", it) }
        unpublishedSeeds.clear()
    }

    /** 이 테스트만의 기준 시각 — 저장소를 직접 부르는 테스트용. */
    protected fun nextCutoff(): Instant = RetentionPolicy.cutoff(RetentionTestDates.next(), Duration.ZERO)

    protected fun outboxCutoff(date: LocalDate): Instant = RetentionPolicy.cutoff(date, props.outboxRetention)

    protected fun verificationCutoff(date: LocalDate): Instant = RetentionPolicy.cutoff(date, props.verificationRetention)

    /** 아웃박스 한 행을 심어 aggregate_id 를 돌려준다. [publishedAt] 이 null 이면 미발행. */
    protected fun outboxRow(publishedAt: Instant?, createdAt: Instant = Instant.now()): String {
        val aggregateId = UUID.randomUUID().toString()
        jdbc.update(
            "INSERT INTO outbox_event (aggregate_id, event_type, payload, created_at, published_at) VALUES (?, 'TransferRequested', '{}', ?, ?)",
            aggregateId, Timestamp.from(createdAt), publishedAt?.let { Timestamp.from(it) },
        )
        if (publishedAt == null) unpublishedSeeds += aggregateId
        return aggregateId
    }

    protected fun outboxRows(count: Int, publishedAt: Instant) {
        val at = Timestamp.from(publishedAt)
        jdbc.batchUpdate(
            "INSERT INTO outbox_event (aggregate_id, event_type, payload, created_at, published_at) VALUES (?, 'TransferRequested', '{}', ?, ?)",
            List(count) { arrayOf<Any>(UUID.randomUUID().toString(), at, at) },
        )
    }

    protected fun outboxExists(aggregateId: String): Boolean =
        jdbc.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE aggregate_id = ?", Int::class.java, aggregateId) == 1

    /** 1원인증 한 행을 심어 id 를 돌려준다. 계좌는 모의 은행에 없는 번호. */
    protected fun verificationRow(status: String, expiresAt: Instant): String {
        val id = UUID.randomUUID().toString()
        jdbc.update(VERIFICATION_INSERT, id, newUserId(), status, Timestamp.from(expiresAt))
        return id
    }

    protected fun verificationRows(count: Int, status: String, expiresAt: Instant) {
        val at = Timestamp.from(expiresAt)
        jdbc.batchUpdate(VERIFICATION_INSERT, List(count) { arrayOf<Any>(UUID.randomUUID().toString(), newUserId(), status, at) })
    }

    protected fun verificationExists(id: String): Boolean =
        jdbc.queryForObject("SELECT COUNT(*) FROM account_verification WHERE id = ?", Int::class.java, id) == 1

    private companion object {
        const val VERIFICATION_INSERT =
            "INSERT INTO account_verification (id, user_id, bank_code, account_no, holder_name, code_hash, status, failed_attempts, expires_at, created_at) " +
                "VALUES (?, ?, '090', '5500000000000', '홍길동', REPEAT('0', 64), ?, 0, ?, NOW(6))"
    }
}
