package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.infrastructure.item.ExecutionContext
import org.springframework.beans.factory.annotation.Autowired
import java.nio.file.Files
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** 실행 결과 · 재실행 거부 · 재시작 · 동시 실행 · 실패 알림 (P5-1, 설계 §5-2 · §10). */
class DailyReconLauncherTest : DailyReconTestSupport() {

    @Autowired private lateinit var launcher: DailyReconLauncher
    @Autowired private lateinit var summaries: ReconSummaryReader

    private fun executions(day: LocalDate): Int =
        jobRepository.getJobInstance(DailyRecon.JOB_NAME, DailyRecon.parameters(day))?.let { jobRepository.getJobExecutions(it).size } ?: 0

    /** [day] 의 잡 인스턴스만 (실행 없음) — 다른 곳이 처음 시작하며 인스턴스를 막 만든 순간, 또는 그러다 죽은 곳. */
    private fun instanceOnly(day: LocalDate) = jobRepository.createJobInstance(DailyRecon.JOB_NAME, DailyRecon.parameters(day))

    /** [day] 의 STARTED 실행 — 다른 곳이 돌고 있거나, 대사 도중 JVM 이 죽어 남은 실행. */
    private fun startedExecution(day: LocalDate): JobExecution {
        val params = DailyRecon.parameters(day)
        val instance = jobRepository.getJobInstance(DailyRecon.JOB_NAME, params) ?: jobRepository.createJobInstance(DailyRecon.JOB_NAME, params)
        return jobRepository.createJobExecution(instance, params, ExecutionContext()).also {
            it.status = BatchStatus.STARTED
            it.setStartTime(LocalDateTime.now())
            jobRepository.update(it)
        }
    }

    /** 이 날짜는 다시 쓰지 않지만 '돌고 있는 실행' 으로 남기지 않는다. */
    private fun abandon(vararg executions: JobExecution) = executions.forEach {
        it.status = BatchStatus.FAILED
        it.setEndTime(LocalDateTime.now())   // 게터는 @Nullable, 세터는 non-null 이라 Kotlin 속성 대입이 안 된다
        jobRepository.update(it)
    }

    @Test
    fun `끝난 날짜를 다시 돌리면 AlreadyCompleted - 환불 · 이슈가 늘지 않는다`() {
        val day = ReconTestDates.unique()
        val user = fundedUser()
        val notPaid = seed(user, TransferStatus.UNKNOWN, at(day))           // 파일에 없음 → 자동 실패 + 환불
        val missing = seed(user, TransferStatus.COMPLETED, at(day))         // 파일에 없음 → COMPLETED_NOT_AT_BANK
        writeBankFile(day, emptyList())

        assertIs<ReconRun.Completed>(launcher.run(day, ReconTrigger.MANUAL))
        val issues = issueRefs(day)
        val balance = wallets.balanceOf(user)

        assertIs<ReconRun.AlreadyCompleted>(launcher.run(day, ReconTrigger.SCHEDULE))

        assertEquals(issues, issueRefs(day))
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(notPaid.id))
        assertEquals(balance, wallets.balanceOf(user))
        assertEquals(listOf(missing.reference), issues["COMPLETED_NOT_AT_BANK"])
        assertEquals(1, executions(day))
    }

    @Test
    fun `재시작 - ③ 중간 청크에서 실패하면 잡이 실패하고, 고친 뒤 다시 돌리면 멈춘 곳부터 끝까지 - 결과는 한 번에 끝낸 것과 같다 (이중 환불 없음)`() {
        val day = ReconTestDates.unique()
        val user = fundedUser(10_000_000)
        val broken = fundedUser(1_000)
        // ③ 은 id 순으로 100건씩 읽는다. 151건 중 121번째(두 번째 청크)를 지갑이 없는 사용자의 출금으로 — 환불이 실패한다
        // (지갑 행이 없으면 WalletLedger.refund 의 check 가 던진다: 손으로 고칠 때까지 같은 자리에서 실패하는 실제 장애 모양)
        val ids = List(151) { UUID.randomUUID() }.sortedBy { it.toString() }
        val transfers = ids.mapIndexed { i, id -> seed(if (i == 120) broken else user, TransferStatus.UNKNOWN, at(day), amount = 1_000, id = id) }
        jdbc.update("DELETE FROM wallet WHERE user_id = ?", broken)
        writeBankFile(day, emptyList())                                       // 은행은 아무것도 처리하지 않았다 → 151건 모두 자동 실패 + 환불

        val first = launcher.run(day, ReconTrigger.SCHEDULE)

        assertEquals("ERROR", assertIs<ReconRun.Failed>(first).failure.code)
        assertEquals(List(100) { TransferStatus.FAILED } + List(51) { TransferStatus.UNKNOWN }, transfers.map { status(it) })   // 첫 청크만 커밋
        assertEquals(AlertLevel.CRITICAL, recordedAlerts.about(day).single().level)

        jdbc.update("INSERT INTO wallet (user_id, balance, version, created_at, updated_at) VALUES (?, 0, 0, NOW(6), NOW(6))", broken)   // 운영자가 고쳤다
        val second = launcher.run(day, ReconTrigger.SCHEDULE)

        val summary = assertIs<ReconRun.Completed>(second).summary
        assertEquals(151, summary.transfers)                                  // 첫 실행의 100건 + 재시작의 51건 (단계 건수가 이어진다)
        assertEquals(151, summary.autoFailed)
        assertTrue(transfers.all { status(it) == TransferStatus.FAILED })
        assertEquals(
            151,
            jdbc.queryForObject(
                "SELECT COUNT(*) FROM wallet_ledger WHERE type = 'REFUND' AND transfer_id IN (${ids.joinToString(",") { "'$it'" }})",
                Int::class.java,
            ),
        )
        assertEquals(10_000_000L, wallets.balanceOf(user))
        assertEquals(1_000L, wallets.balanceOf(broken))
        assertEquals(2, executions(day))
    }

    @Test
    fun `동시 실행 - 같은 날짜를 두 곳에서 돌리면 하나만 실행되고 다른 하나는 Running`() {
        val day = ReconTestDates.unique()
        writeBankFile(day, emptyList())

        // 첫 실행을 ⑥ 보고(완료 알림)에서 붙잡아 둔다 — 잡이 돌고 있는 동안 두 번째가 온다
        val (first, second) = whileReportHeld(day, { launcher.run(day, ReconTrigger.MANUAL) }) { launcher.run(day, ReconTrigger.MANUAL) }

        assertIs<ReconRun.Running>(second)
        assertIs<ReconRun.Completed>(first)
        assertEquals(1, executions(day))
    }

    @Test
    fun `같은 날짜의 실행이 동시에 둘 만들어지면(실패한 날짜의 동시 재시작) 먼저 만든 쪽만 계속한다 - SingleRunGuard`() {
        val day = ReconTestDates.unique()
        val params = DailyRecon.parameters(day)
        val instance = jobRepository.createJobInstance(DailyRecon.JOB_NAME, params)
        val earlier = jobRepository.createJobExecution(instance, params, ExecutionContext())
        val later = jobRepository.createJobExecution(instance, params, ExecutionContext())
        val guard = SingleRunGuard(jobRepository)
        try {
            guard.beforeJob(earlier)                                                  // 먼저 만든 쪽은 계속한다
            assertFailsWith<ReconRunningException> { guard.beforeJob(later) }       // 나중 쪽은 멈춘다 → 실행기가 Running 으로 답한다
        } finally {
            abandon(earlier, later)
        }
    }

    @Test
    fun `처음 시작 경합 - 인스턴스만 보이면 잠깐 뒤 한 번 더 시도해 Running (그 사이 다른 곳이 실행을 만들었다)`() {
        val day = ReconTestDates.unique()
        instanceOnly(day)
        var other: JobExecution? = null
        // Batch 6 은 인스턴스와 실행을 다른 트랜잭션에서 만든다 — 그 사이에 온 쪽을 재현: 기다리는 동안 먼저 온 쪽이 실행을 만든다
        val racing = DailyReconLauncher(jobOperator, job, jobRepository, summaries, recordedAlerts) { other = startedExecution(day) }
        try {
            assertIs<ReconRun.Running>(racing.run(day, ReconTrigger.SCHEDULE))
            assertEquals(1, executions(day))                                     // 먼저 온 쪽의 실행 하나뿐
            assertTrue(recordedAlerts.about(day).isEmpty())
        } finally {
            other?.let { abandon(it) }
        }
    }

    @Test
    fun `처음 시작 경합 - 다시 시도해도 실행이 없으면(만들던 쪽이 죽음) 예외 없이 Failed ERROR + 긴급 알림`() {
        val day = ReconTestDates.unique()
        instanceOnly(day)

        val failure = assertIs<ReconRun.Failed>(launcher.run(day, ReconTrigger.SCHEDULE)).failure

        assertEquals("ERROR" to IllegalStateException::class.java.name, failure.code to failure.message)
        assertEquals(AlertLevel.CRITICAL, recordedAlerts.about(day).single().level)
    }

    @Test
    fun `마지막 회차에 아직 돌고 있으면(STARTED 로 남은 실행) 대사 미완료 긴급 알림 - 그 전 회차의 Running 은 알리지 않는다`() {
        val day = ReconTestDates.unique()
        val stuck = startedExecution(day)
        try {
            assertIs<ReconRun.Running>(launcher.run(day, ReconTrigger.SCHEDULE))
            assertTrue(recordedAlerts.about(day).isEmpty())

            assertIs<ReconRun.Running>(launcher.run(day, ReconTrigger.SCHEDULE_FINAL))

            val alert = recordedAlerts.about(day).single()
            assertEquals(AlertLevel.CRITICAL, alert.level)
            assertTrue("미완료" in alert.title)
            assertTrue(alert.lines.any { "RUNNING" in it })
        } finally {
            abandon(stuck)
        }
    }

    @Test
    fun `파일이 아직 없으면 FILE_NOT_READY 로 실패 - 마지막 회차 전에는 알리지 않고, 파일을 놓고 다시 돌리면 완료 (환불은 한 번)`() {
        val day = ReconTestDates.unique()
        val user = fundedUser(100_000)
        val notPaid = seed(user, TransferStatus.UNKNOWN, at(day))
        val balance = wallets.balanceOf(user)

        val first = launcher.run(day, ReconTrigger.SCHEDULE)

        assertEquals(DailyRecon.FILE_NOT_READY, assertIs<ReconRun.Failed>(first).failure.code)
        assertTrue(recordedAlerts.about(day).isEmpty())
        assertEquals(TransferStatus.UNKNOWN, status(notPaid))                // ① 에서 멈췄다 — 아무것도 확정하지 않았다
        assertEquals(listOf("WITHDRAW"), ledgerTypes(notPaid.id))

        writeBankFile(day, emptyList())
        assertIs<ReconRun.Completed>(launcher.run(day, ReconTrigger.SCHEDULE))

        assertTrue("완료" in recordedAlerts.about(day).single().title)
        assertEquals(TransferStatus.FAILED, status(notPaid))
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(notPaid.id))
        assertEquals(balance + notPaid.amount, wallets.balanceOf(user))
        assertEquals(ledgerSum(user), wallets.balanceOf(user))
    }

    @Test
    fun `마지막 회차(06시)에도 끝내지 못하면 대사 미완료 긴급 알림, 수동 실행의 FILE_NOT_READY 는 알리지 않는다`() {
        val manual = ReconTestDates.unique()
        assertIs<ReconRun.Failed>(launcher.run(manual, ReconTrigger.MANUAL))
        assertTrue(recordedAlerts.about(manual).isEmpty())

        val final = ReconTestDates.unique()
        assertIs<ReconRun.Failed>(launcher.run(final, ReconTrigger.SCHEDULE_FINAL))
        val alert = recordedAlerts.about(final).single()
        assertEquals(AlertLevel.CRITICAL, alert.level)
        assertTrue("미완료" in alert.title)
        assertTrue(alert.lines.any { DailyRecon.FILE_NOT_READY in it })
    }

    @Test
    fun `형식이 틀린 파일은 FILE_INVALID 로 실패하고 긴급 알림 - 출금은 그대로, 파일을 고쳐 다시 돌리면 환불은 한 번`() {
        val day = ReconTestDates.unique()
        val user = fundedUser(100_000)
        val unknown = seed(user, TransferStatus.UNKNOWN, at(day))
        val balance = wallets.balanceOf(user)
        val file = props.reconFile(day)
        Files.createDirectories(file.parent)
        Files.writeString(file, "H${day.toString().replace("-", "")}090\n")   // 59바이트가 아니고 트레일러도 없다

        val run = launcher.run(day, ReconTrigger.SCHEDULE)

        assertEquals(DailyRecon.FILE_INVALID, assertIs<ReconRun.Failed>(run).failure.code)
        assertEquals(AlertLevel.CRITICAL, recordedAlerts.about(day).single().level)
        assertEquals(TransferStatus.UNKNOWN, status(unknown))
        assertEquals(listOf("WITHDRAW"), ledgerTypes(unknown.id))
        assertEquals(balance, wallets.balanceOf(user))

        writeBankFile(day, emptyList())                                       // 은행이 파일을 다시 보냈다
        assertIs<ReconRun.Completed>(launcher.run(day, ReconTrigger.SCHEDULE))

        assertEquals(TransferStatus.FAILED, status(unknown))
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(unknown.id))
        assertEquals(balance + unknown.amount, wallets.balanceOf(user))
        assertEquals(ledgerSum(user), wallets.balanceOf(user))
    }
}
