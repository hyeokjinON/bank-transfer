package io.github.hyeokjinon.banktransfer.api.retention

import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.SimpleJob
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.test.util.ReflectionTestUtils
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.DefaultTransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.sql.SQLException
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 왜 READ COMMITTED 인가 (P5-2, 설계 §4 · §6): 삭제 트랜잭션을 열어 둔 채 출금 접수의 아웃박스 INSERT 를 해 본다.
 * 미발행 행은 published_at 이 NULL 이라 인덱스 맨 앞에 모이고, 새 행 (NULL, 가장 큰 id) 이 들어갈 자리는 "마지막 NULL 행과 첫 발행 행 사이" 다.
 * StatementSettlerIsolationTest 처럼 두 격리 수준을 나란히 재현한다. 어느 쪽이든 2초 락 대기 + 20초 Future 로 끝이 있다.
 */
class RetentionLockTest : RetentionTestSupport() {

    @Autowired @Qualifier(DataRetention.JOB_NAME) private lateinit var job: Job
    @Autowired private lateinit var dataSource: DataSource

    private enum class InsertOutcome { INSERTED, LOCK_WAIT_TIMEOUT }

    /** 잡이 실제로 쓰는 트랜잭션 속성 — 잡 설정에서 격리 수준을 지우면 아래 READ COMMITTED 테스트가 깨진다 (변이 검사). */
    private fun stepTransaction(): TransactionDefinition {
        val step = checkNotNull((job as SimpleJob).getStep(DataRetention.PURGE_OUTBOX)) { "step is missing" }
        return ReflectionTestUtils.getField(step, "transactionAttribute") as TransactionDefinition
    }

    private fun repeatableRead(): TransactionDefinition =
        DefaultTransactionDefinition().apply { isolationLevel = TransactionDefinition.ISOLATION_REPEATABLE_READ }

    /** 다른 커넥션에서 미발행 행 하나를 넣는다 (autocommit). 락 대기는 [LOCK_WAIT_SECONDS] 까지 — 넘으면 1205. */
    private fun insertUnpublished(aggregateId: String): InsertOutcome =
        dataSource.connection.use { connection ->
            connection.createStatement().use { it.execute("SET SESSION innodb_lock_wait_timeout = $LOCK_WAIT_SECONDS") }
            try {
                connection.prepareStatement(
                    "INSERT INTO outbox_event (aggregate_id, event_type, payload, created_at) VALUES (?, 'TransferRequested', '{}', NOW(6))",
                ).use {
                    it.setString(1, aggregateId)
                    it.executeUpdate()
                }
                InsertOutcome.INSERTED
            } catch (e: SQLException) {
                if (e.errorCode != ER_LOCK_WAIT_TIMEOUT) throw e
                InsertOutcome.LOCK_WAIT_TIMEOUT
            } finally {
                // 풀로 돌아가는 커넥션이다 — connection-init-sql 값으로 되돌린다
                connection.createStatement().use { it.execute("SET SESSION innodb_lock_wait_timeout = $POOL_LOCK_WAIT_SECONDS") }
            }
        }

    /** [definition] 으로 삭제 트랜잭션을 연 채 INSERT 해 본다. 지운 행은 되돌린다 — 잠금만 본다. */
    private fun insertWhileDeleteIsOpen(definition: TransactionDefinition): InsertOutcome {
        val cutoff = nextCutoff()
        val published = outboxRow(cutoff.minusSeconds(60))
        outboxRow(publishedAt = null)   // 마지막 미발행 행 — 새 행은 이 행과 위 발행 행 사이로 들어간다
        val inserted = UUID.randomUUID().toString()
        var insert: CompletableFuture<InsertOutcome>? = null
        var failure: Throwable? = null
        try {
            val outcome = TransactionTemplate(transactionManager, definition).execute { status ->
                status.setRollbackOnly()
                assertEquals(1, retention.deleteOutboxEvents(cutoff, 1_000))
                val started = CompletableFuture.supplyAsync { insertUnpublished(inserted) }
                insert = started
                started.get(WAIT_BOUND_SECONDS, TimeUnit.SECONDS)
            }!!
            assertTrue(outboxExists(published))
            return outcome
        } catch (e: Throwable) {
            failure = e
            throw e
        } finally {
            // 정리가 실패해도 진짜 실패 원인을 가리지 않는다: 원래 예외를 주(主)로 두고 정리 실패는 suppressed 로 붙인다
            try {
                insert?.let(::awaitSettled)
                jdbc.update("DELETE FROM outbox_event WHERE aggregate_id = ?", inserted)
            } catch (cleanup: Throwable) {
                if (failure == null) throw cleanup
                failure.addSuppressed(cleanup)
            }
        }
    }

    /**
     * 바깥 Future 가 시간 초과해도 비동기 INSERT 는 취소되지 않는다. 먼저 지우면 늦은 INSERT 가 그 뒤에 커밋되어 미발행 행이 공유 DB 에 남고,
     * 표 전체를 세는 OutboxRelayTest 가 엉뚱한 곳에서 깨진다. 삭제 트랜잭션은 이미 되돌렸으므로 INSERT 를 막는 것은 없다.
     * 그래도 안 끝나면 지워도 소용없으니 정리하지 않고 시끄럽게 실패한다. INSERT 의 성공 · 실패는 여기서 보지 않는다 — 끝났는지만 본다.
     */
    private fun awaitSettled(insert: CompletableFuture<InsertOutcome>) {
        runCatching { insert.get(SETTLE_BOUND_SECONDS, TimeUnit.SECONDS) }
        check(insert.isDone) { "outbox INSERT did not settle within $SETTLE_BOUND_SECONDS seconds - an unpublished row may remain in the shared DB" }
    }

    private fun record(label: String, outcome: InsertOutcome) = println("[RetentionLockTest] observed: $label = $outcome")

    @Test
    fun `READ COMMITTED (잡의 실제 설정) - 삭제를 열어 둔 채 미발행 아웃박스 INSERT 가 들어간다`() {
        val outcome = insertWhileDeleteIsOpen(stepTransaction())

        record("READ COMMITTED", outcome)
        assertEquals(InsertOutcome.INSERTED, outcome)
    }

    @Test
    fun `REPEATABLE READ 였다면 next-key 락에 막혀 INSERT 가 락 대기로 끝난다 - 단계를 READ COMMITTED 로 두는 이유`() {
        val outcome = insertWhileDeleteIsOpen(repeatableRead())

        record("REPEATABLE READ", outcome)
        assertEquals(InsertOutcome.LOCK_WAIT_TIMEOUT, outcome)
    }

    private companion object {
        const val LOCK_WAIT_SECONDS = 2
        const val POOL_LOCK_WAIT_SECONDS = 5
        const val WAIT_BOUND_SECONDS = 20L
        const val SETTLE_BOUND_SECONDS = LOCK_WAIT_SECONDS + 10L
        const val ER_LOCK_WAIT_TIMEOUT = 1205
    }
}
