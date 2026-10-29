package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertText
import io.github.hyeokjinon.banktransfer.protocol.ReconDetail
import io.github.hyeokjinon.banktransfer.transfer.ReconIssueType
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.SimpleJob
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.batch.autoconfigure.JobLauncherApplicationRunner
import org.springframework.context.ApplicationContext
import org.springframework.orm.jpa.JpaTransactionManager
import org.springframework.test.util.ReflectionTestUtils
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.interceptor.TransactionAttribute
import java.time.Instant
import java.time.LocalTime
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** 일마감 대사 잡 전 칸 (P5-1, 설계 §6 · §10). 실제 Spring Batch · Testcontainers MySQL. */
class DailyReconJobTest : DailyReconTestSupport() {

    @Autowired private lateinit var repo: ReconRepository
    @Autowired private lateinit var summaries: ReconSummaryReader
    @Autowired private lateinit var context: ApplicationContext
    @Autowired private lateinit var transactionManager: PlatformTransactionManager

    private fun strangerRef(prefix: Char = 'T') = prefix + UUID.randomUUID().toString().replace("-", "").take(19).uppercase()

    @Test
    fun `단계 트랜잭션 - 여섯 단계 모두 JPA 트랜잭션 관리자, ③ ④ 는 READ COMMITTED`() {
        // Batch 6 은 트랜잭션 관리자를 빼먹어도 조용히 ResourcelessTransactionManager(트랜잭션 없음)로 돈다 — 환불 · 전이가 청크와 함께
        // 롤백되지 않는 사고를 막으려고 설정 자체를 고정한다 (계획 "버전 · API 확인 결과" 6 · 12). 필드 이름은 6.0.5 소스 기준
        assertIs<JpaTransactionManager>(transactionManager)
        val names = listOf(DailyRecon.VALIDATE_FILE, DailyRecon.LOAD_STATEMENT, DailyRecon.MATCH_TRANSFERS, DailyRecon.MATCH_BANK_LINES, DailyRecon.CHECK_LEDGER, DailyRecon.REPORT)
        val steps = names.associateWith { name -> checkNotNull((job as SimpleJob).getStep(name)) { "step $name is missing" } }

        steps.forEach { (name, step) -> assertSame(transactionManager, ReflectionTestUtils.getField(step, "transactionManager"), name) }
        for (name in listOf(DailyRecon.MATCH_TRANSFERS, DailyRecon.MATCH_BANK_LINES)) {
            val attribute = ReflectionTestUtils.getField(steps.getValue(name), "transactionAttribute") as TransactionAttribute
            assertEquals(TransactionDefinition.ISOLATION_READ_COMMITTED, attribute.isolationLevel, name)
        }
    }

    @Test
    fun `④ - 거래번호가 T 도 V 도 아닌 은행 줄도 UNKNOWN_REFERENCE 로 기록 · 알린다 (은행이 낸 돈을 놓치지 않는다)`() {
        val day = ReconTestDates.unique()
        val odd = strangerRef('X')
        writeBankFile(day, listOf(ReconDetail(odd, "090", "1002123456789", 7_000, LocalTime.of(12, 0))))

        assertEquals(BatchStatus.COMPLETED, runJob(day).status)

        assertEquals(mapOf("UNKNOWN_REFERENCE" to listOf<String?>(odd)), issueRefs(day))
        val alert = recordedAlerts.about(day).single()
        assertEquals(AlertLevel.CRITICAL, alert.level)
        assertEquals("은행 줄 1건 (1원인증 0건은 대조하지 않음)", alert.lines[1])
        assertTrue(alert.items.single().startsWith("UNKNOWN_REFERENCE $odd"))
    }

    @Test
    fun `④ 여러 페이지 - 청크 크기보다 많은 줄도 거래번호마다 정확히 한 번 판정한다`() {
        val day = ReconTestDates.unique()
        val refs = List(props.chunkSize * 2 + 1) { strangerRef() }   // 세 페이지
        // 첫 거래번호는 파일 끝에 한 줄 더 (이중 지급) — 다른 페이지에 있어도 첫 줄 하나만 읽는다
        writeBankFile(day, (refs + refs.first()).map { ReconDetail(it, "090", "1002123456789", 1_000, LocalTime.of(12, 0)) })

        val execution = runJob(day)

        assertEquals(BatchStatus.COMPLETED, execution.status)
        val step = execution.stepExecutions.single { it.stepName == DailyRecon.MATCH_BANK_LINES }
        assertEquals(refs.size.toLong() to refs.size.toLong(), step.readCount to step.writeCount)
        assertEquals(refs.size, VerdictCounts.from(step.executionContext)[ReconVerdict.UNKNOWN_REFERENCE])
        assertEquals(refs.toSet(), issueRefs(day).getValue("UNKNOWN_REFERENCE").toSet())
    }

    @Test
    fun `전 칸 - 설계 §6 의 각 경우를 심은 출금과 은행 파일로 잡을 돌리면 미확정만 확정하고 나머지는 기록 · 알린다`() {
        val day = ReconTestDates.unique()
        val user = fundedUser(1_000_000)
        val sent = at(day)
        val completed = seed(user, COMPLETED, sent)                    // 1 일치
        val failed = seed(user, FAILED, sent)                          // 2 일치
        val unknownPaid = seed(user, UNKNOWN, sent)                    // 3 자동 완료
        val sendingPaid = seed(user, SENDING, sent)                    // 3 자동 완료 (SENDING)
        val unknownNotPaid = seed(user, UNKNOWN, sent)                 // 4 자동 실패 + 환불
        val wrongAmount = seed(user, UNKNOWN, sent)                    // 5 금액이 다름 → 확정하지 않는다
        val completedMissing = seed(user, COMPLETED, sent)             // 6
        val refundedPaid = seed(user, FAILED, sent)                    // 7
        val receivedPaid = seed(user, RECEIVED, sent)                  // 8 (긴급)
        val retrying = seed(user, RECEIVED, sent)                      // — 넘어감
        val paidTwice = seed(user, COMPLETED, sent)                    // 11 (긴급)
        val lateUnknown = seed(user, UNKNOWN, at(day.minusDays(1)))    // ④ 전날 보낸 출금이 오늘 파일에 → 같은 판정(자동 완료)
        val strangerRef = "T" + UUID.randomUUID().toString().replace("-", "").take(19).uppercase()   // 9 우리에게 없는 거래번호
        val brokenWallet = fundedUser(1_000).also { jdbc.update("UPDATE wallet SET balance = balance + 1 WHERE user_id = ?", it) }   // 10
        // ⑤ 는 공유 DB 의 모든 지갑을 훑는다 — 뒤 테스트가 이 지갑 때문에 불일치를 보지 않게 깨뜨린 잔고는 반드시 되돌린다
        try {
            writeBankFile(
                day,
                listOf(
                    lineOf(completed), lineOf(unknownPaid), lineOf(sendingPaid), lineOf(wrongAmount, amount = wrongAmount.amount + 1),
                    lineOf(refundedPaid), lineOf(receivedPaid), lineOf(paidTwice), lineOf(paidTwice), lineOf(lateUnknown),
                    ReconDetail(strangerRef, "090", "1002123456789", 5_000, LocalTime.of(11, 0)),
                    ReconDetail("V" + strangerRef.drop(1), "090", "1002123456789", 1, LocalTime.of(11, 1)),   // 1원인증 — 대조하지 않고 건수만
                ),
            )
            val balanceBefore = wallets.balanceOf(user)

            val execution = runJob(day)

            assertEquals(BatchStatus.COMPLETED, execution.status)
            // 결과를 모르던 것만 확정했다
            assertEquals(COMPLETED, status(unknownPaid))
            assertEquals("0000", store.findById(unknownPaid.id)!!.bankResponseCode)
            assertEquals(COMPLETED, status(sendingPaid))
            assertEquals(COMPLETED, status(lateUnknown))
            val autoFailed = store.findById(unknownNotPaid.id)!!
            assertEquals(FAILED to "E002", autoFailed.status to autoFailed.bankResponseCode)
            assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(unknownNotPaid.id))
            assertEquals(balanceBefore + unknownNotPaid.amount, wallets.balanceOf(user))
            assertEquals(ledgerSum(user), wallets.balanceOf(user))
            // 확정된 건 · 불일치 · 재시도 중은 건드리지 않았다
            assertEquals(UNKNOWN, status(wrongAmount))
            assertEquals(COMPLETED, status(completedMissing))
            assertEquals(FAILED, status(refundedPaid))
            assertEquals(RECEIVED, status(receivedPaid))
            assertEquals(RECEIVED, status(retrying))
            // 이슈 — 종류마다 정확히 그 건
            val issues = issueRefs(day)
            assertEquals(setOf(unknownPaid.reference, sendingPaid.reference, lateUnknown.reference), issues.getValue("AUTO_COMPLETED").toSet())
            assertEquals(listOf(unknownNotPaid.reference), issues["AUTO_FAILED"])
            assertEquals(listOf(wrongAmount.reference), issues["AMOUNT_OR_ACCOUNT_MISMATCH"])
            assertEquals(listOf(completedMissing.reference), issues["COMPLETED_NOT_AT_BANK"])
            assertEquals(listOf(refundedPaid.reference), issues["REFUNDED_BUT_PAID"])
            assertEquals(listOf(receivedPaid.reference), issues["PAID_BUT_RECEIVED"])
            assertEquals(listOf(strangerRef), issues["UNKNOWN_REFERENCE"])
            assertEquals(listOf(paidTwice.reference), issues["PAID_TWICE"])
            assertEquals(
                1,
                jdbc.queryForObject(
                    "SELECT COUNT(*) FROM recon_issue WHERE recon_date = ? AND type = 'BALANCE_LEDGER_MISMATCH' AND user_id = ?",
                    Int::class.java, day, brokenWallet,
                ),
            )
            assertFalse(issues.values.flatten().any { it in setOf(completed.reference, failed.reference, retrying.reference) })
            // 보고 (⑥): 요약 + 불일치 목록, 긴급 먼저, 사용자 id · 계좌 원문 없음
            val alert = recordedAlerts.about(day).single()
            assertEquals(AlertLevel.CRITICAL, alert.level)
            assertEquals("출금 11건 (D일 송신): 일치 2 · 자동 완료 3 · 자동 실패(E002, 환불) 1 · 재시도 중 1", alert.lines[0])
            assertEquals("은행 줄 10건 (1원인증 1건은 대조하지 않음)", alert.lines[1])
            assertTrue(alert.items.first().startsWith("[긴급]"))
            val text = AlertText.render(alert)
            assertFalse(user in text)
            assertFalse(brokenWallet in text)
            assertFalse("1002123456789" in text)
        } finally {
            jdbc.update("UPDATE wallet SET balance = balance - 1 WHERE user_id = ?", brokenWallet)
        }
    }

    @Test
    fun `불일치가 없으면 잡이 INFO 로 알린다 - 기동 때는 잡이 돌지 않았다`() {
        // spring.batch.job.enabled=false: 기동 때 잡을 돌리는 러너가 없고, 이 날짜의 인스턴스는 테스트가 시작하기 전에는 없다
        assertTrue(context.getBeansOfType(JobLauncherApplicationRunner::class.java).isEmpty())
        val day = ReconTestDates.unique()
        assertNull(jobRepository.getJobInstance(DailyRecon.JOB_NAME, DailyRecon.parameters(day)))
        val user = fundedUser(100_000)
        val completed = seed(user, COMPLETED, at(day))
        val failed = seed(user, FAILED, at(day))
        writeBankFile(day, listOf(lineOf(completed)))

        val execution = runJob(day)

        assertEquals(BatchStatus.COMPLETED, execution.status)
        assertEquals(COMPLETED to FAILED, status(completed) to status(failed))
        assertEquals(emptyMap(), issueRefs(day))
        val alert = recordedAlerts.about(day).single()
        assertEquals(AlertLevel.INFO, alert.level)
        assertTrue("완료 — 불일치 없음" in alert.title)
        assertEquals("출금 2건 (D일 송신): 일치 2 · 자동 완료 0 · 자동 실패(E002, 환불) 0 · 재시도 중 0", alert.lines[0])
        assertTrue(alert.items.isEmpty())
    }

    @Test
    fun `완료 알림 - 불일치가 없으면 INFO, 있으면 CRITICAL 이고 긴급이 먼저`() {
        val clean = ReconTestDates.unique()
        val summary = ReconSummary(clean, 3, 3, 0, 3, 0, 0, 0, emptyMap())

        val info = summaries.completionAlert(summary)

        assertEquals(AlertLevel.INFO, info.level)
        assertTrue("완료 — 불일치 없음" in info.title)
        assertTrue(info.items.isEmpty())

        val dirty = ReconTestDates.unique()
        repo.insertIssue(ReconIssue(dirty, ReconIssueType.COMPLETED_NOT_AT_BANK, reference = "T261028NOTATBANK0001"), Instant.now())
        repo.insertIssue(ReconIssue(dirty, ReconIssueType.PAID_TWICE, reference = "T261028PAIDTWICE0001"), Instant.now())
        repo.insertIssue(ReconIssue(dirty, ReconIssueType.AUTO_COMPLETED, reference = "T261028AUTOCOMPL0001"), Instant.now())

        val critical = summaries.completionAlert(summary.copy(reconDate = dirty, mismatches = repo.issueCounts(dirty).filterKeys { it.mismatch }))

        assertEquals(AlertLevel.CRITICAL, critical.level)
        assertTrue("불일치 2건 (긴급 1)" in critical.title)
        assertEquals(2, critical.items.size)                       // 자동 확정은 목록에 없다 (요약 건수로만)
        assertTrue(critical.items[0].startsWith("[긴급] PAID_TWICE T261028PAIDTWICE0001"))
    }
}
