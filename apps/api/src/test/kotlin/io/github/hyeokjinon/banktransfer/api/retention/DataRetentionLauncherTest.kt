package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.batch.failureCauses
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.Job
import org.springframework.batch.infrastructure.item.ExecutionContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.dao.QueryTimeoutException
import java.time.LocalDate
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** 실행 결과 · 건수 합 · 재실행 거부 · 재시작 · 동시 실행 · 실패 알림 (P5-2, 설계 §4 · §6). */
class DataRetentionLauncherTest : RetentionTestSupport() {

    @Autowired private lateinit var launcher: DataRetentionLauncher
    @Autowired @Qualifier(DataRetention.JOB_NAME) private lateinit var job: Job

    private fun launcherOf(target: Job) = DataRetentionLauncher(jobOperator, target, jobRepository, recordedAlerts)

    private fun seed(day: LocalDate, outbox: Int, verifications: Int) {
        outboxRows(outbox, outboxCutoff(day).minusSeconds(1))
        verificationRows(verifications, "SENT", verificationCutoff(day).minusSeconds(1))
    }

    @Test
    fun `완료하면 표별 건수를 돌려주고 텔레그램은 보내지 않는다`() {
        val day = RetentionTestDates.next()
        seed(day, outbox = 3, verifications = 2)

        val run = launcher.run(day)

        assertEquals(RetentionDeleted(outboxEvents = 3, verifications = 2), assertIs<RetentionRun.Completed>(run).deleted)
        assertTrue(recordedAlerts.about(day).isEmpty())
    }

    @Test
    fun `끝난 날짜를 다시 돌리면 AlreadyCompleted - 실행이 늘지 않고 알리지 않는다`() {
        val day = RetentionTestDates.next()
        assertIs<RetentionRun.Completed>(launcher.run(day))

        assertIs<RetentionRun.AlreadyCompleted>(launcher.run(day))

        assertEquals(1, executions(day).size)
        assertTrue(recordedAlerts.about(day).isEmpty())
    }

    @Test
    fun `② 둘째 묶음에서 실패하면 WARN(예외 종류만), 재시작은 ② 부터 - 건수는 모든 실행의 합`() {
        val day = RetentionTestDates.next()
        seed(day, outbox = 5, verifications = 15)
        val calls = AtomicInteger()
        val failingOnce = buildDataRetentionJob(
            jobRepository, transactionManager, props.copy(batchSize = 10), retention::deleteOutboxEvents,
        ) { cutoff, limit ->
            if (calls.incrementAndGet() == 2) throw QueryTimeoutException("내부 메시지")
            retention.deleteVerifications(cutoff, limit)
        }
        val failing = launcherOf(failingOnce)

        val first = assertIs<RetentionRun.Failed>(failing.run(day)).failure

        assertEquals("ERROR" to QueryTimeoutException::class.java.name, first.code to first.message)
        val alert = recordedAlerts.about(day).single()
        assertEquals(AlertLevel.WARN, alert.level)
        assertEquals("보존 배치 $day 실패", alert.title)
        assertFalse(alert.lines.any { "내부 메시지" in it })                     // 내부 메시지는 로그에만

        val second = failing.run(day)

        // 첫 실행 ② 10건 + 재시작 ② 5건. 마지막 단계 실행만 보면 5 다
        assertEquals(RetentionDeleted(outboxEvents = 5, verifications = 15), assertIs<RetentionRun.Completed>(second).deleted)
        val runs = executions(day)
        assertEquals(2, runs.size)
        val restarted = runs.maxBy { it.id }
        assertEquals(listOf(DataRetention.PURGE_VERIFICATIONS), restarted.stepExecutions.map { it.stepName })   // ① 은 다시 돌지 않는다
    }

    @Test
    fun `동시 실행 - 같은 날짜를 두 곳에서 돌리면 하나만 실행되고 다른 하나는 Running, 알리지 않는다`() {
        val day = RetentionTestDates.next()
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val held = launcherOf(
            buildDataRetentionJob(jobRepository, transactionManager, props, { cutoff, limit ->
                entered.countDown()
                check(release.await(20, TimeUnit.SECONDS)) { "not released" }
                retention.deleteOutboxEvents(cutoff, limit)
            }, retention::deleteVerifications),
        )

        val first = CompletableFuture.supplyAsync { held.run(day) }
        try {
            check(entered.await(20, TimeUnit.SECONDS)) { "the job did not reach its first step" }
            assertIs<RetentionRun.Running>(held.run(day))
        } finally {
            release.countDown()
        }

        assertIs<RetentionRun.Completed>(first.get(20, TimeUnit.SECONDS))
        assertEquals(1, executions(day).size)
        assertTrue(recordedAlerts.about(day).isEmpty())
    }

    @Test
    fun `실행이 동시에 둘 만들어지면(실패한 날짜의 동시 재시작) 나중 실행은 가드가 멈춘다 - 실제 잡 빈에 달려 있다`() {
        val day = RetentionTestDates.next()
        val earlier = startedExecution(day)
        val later = jobRepository.createJobExecution(earlier.jobInstance, DataRetention.parameters(day), ExecutionContext())
        try {
            job.execute(later)

            assertEquals(BatchStatus.FAILED, later.status)
            assertTrue(later.failureCauses().any { it is RetentionRunningException })
            assertTrue(later.stepExecutions.isEmpty())                       // 아무것도 지우지 않았다
        } finally {
            abandon(earlier)
        }
    }

    @Test
    fun `도중에 JVM 이 죽어 STARTED 로 남으면 그 날짜는 Running 이지만 다음 날 인스턴스는 막지 않는다`() {
        val day = RetentionTestDates.next()
        val stuck = startedExecution(day)
        try {
            assertIs<RetentionRun.Running>(launcher.run(day))
            assertTrue(recordedAlerts.about(day).isEmpty())

            assertIs<RetentionRun.Completed>(launcher.run(day.plusDays(1)))
        } finally {
            abandon(stuck)
        }
    }

    @Test
    fun `잡 인스턴스만 남아 있으면(처음 시작하다 죽은 곳) 예외 없이 Failed ERROR + WARN`() {
        val day = RetentionTestDates.next()
        jobRepository.createJobInstance(DataRetention.JOB_NAME, DataRetention.parameters(day))

        val failure = assertIs<RetentionRun.Failed>(launcher.run(day)).failure

        assertEquals("ERROR" to IllegalStateException::class.java.name, failure.code to failure.message)
        assertEquals(AlertLevel.WARN, recordedAlerts.about(day).single().level)
    }
}
