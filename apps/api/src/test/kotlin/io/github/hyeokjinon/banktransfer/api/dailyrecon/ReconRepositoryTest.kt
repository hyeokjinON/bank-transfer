package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.BankStatementLine
import io.github.hyeokjinon.banktransfer.transfer.ReconIssueType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.time.ZoneId
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ReconRepositoryTest : ApiIntegrationTest() {

    @Autowired private lateinit var repo: ReconRepository
    @Autowired private lateinit var jdbc: JdbcTemplate

    private val seoul = ZoneId.of("Asia/Seoul")

    private fun line(day: java.time.LocalDate, lineNo: Int, reference: String, amount: Long = 30_000) =
        BankStatementLine(day, lineNo, reference, "090", "1002123456789", amount, day.atTime(10, 15, 30).atZone(seoul).toInstant())

    @Test
    fun `은행 줄 - 같은 줄을 다시 넣어도 그대로, 거래번호의 모든 날짜 줄을 날짜 · 줄 순으로, 처리 시각은 KST 로 저장`() {
        val day = ReconTestDates.unique()
        val ref = "T" + UUID.randomUUID().toString().replace("-", "").take(19).uppercase()
        val lines = listOf(line(day, 2, ref), line(day, 3, "V" + ref.drop(1), amount = 1))

        repo.insertLines(lines, Instant.now())
        repo.insertLines(lines, Instant.now())                     // ② 재시작 — 무시
        repo.insertLines(listOf(line(day.plusDays(1), 2, ref)), Instant.now())

        assertEquals(listOf(day to 2, day.plusDays(1) to 2), repo.linesOf(ref).map { it.bankDate to it.lineNo })
        assertEquals(lines.first(), repo.linesOf(ref).first())
        assertEquals(1, repo.countLines(day, verification = false))
        assertEquals(1, repo.countLines(day, verification = true))
        assertEquals(
            "10:15:30",
            jdbc.queryForObject("SELECT DATE_FORMAT(processed_at, '%H:%i:%s') FROM bank_statement_line WHERE bank_date = ? AND line_no = 2", String::class.java, day),
        )
    }

    @Test
    fun `줄 수 - 1원인증은 V 이면서 1원인 줄만, 1원이 아닌 V 줄은 대조하는 쪽으로 센다`() {
        val day = ReconTestDates.unique()
        val v = "V" + UUID.randomUUID().toString().replace("-", "").take(19).uppercase()
        repo.insertLines(listOf(line(day, 2, v, amount = 1), line(day, 3, v.dropLast(1) + "X", amount = 1_000_000), line(day, 4, "T" + v.drop(1))), Instant.now())

        assertEquals(1, repo.countLines(day, verification = true))
        assertEquals(2, repo.countLines(day, verification = false))
    }

    @Test
    fun `이슈 - 같은 날 · 종류 · 키는 한 번만, 지갑 이슈는 user_id 가 키`() {
        val day = ReconTestDates.unique()
        val ref = "T261028ISSUE" + UUID.randomUUID().toString().take(8).uppercase()
        val issue = ReconIssue(day, ReconIssueType.PAID_TWICE, reference = ref, transferId = UUID.randomUUID(), userId = "u-1", ourValue = "COMPLETED 1000", bankValue = "1000; 1000")

        repo.insertIssue(issue, Instant.now())
        repo.insertIssue(issue, Instant.now())                     // 재시작 · 재실행 — 늘지 않는다
        repo.insertIssue(issue.copy(type = ReconIssueType.AUTO_COMPLETED), Instant.now())
        repo.insertIssue(ReconIssue(day, ReconIssueType.BALANCE_LEDGER_MISMATCH, userId = "wallet-user", ourValue = "balance=1", bankValue = "ledgerSum=0"), Instant.now())

        assertEquals(
            mapOf(ReconIssueType.PAID_TWICE to 1, ReconIssueType.AUTO_COMPLETED to 1, ReconIssueType.BALANCE_LEDGER_MISMATCH to 1),
            repo.issueCounts(day),
        )
        assertEquals(listOf(ref, ref, null), repo.issuesOf(day).map { it.reference })
        assertEquals("wallet-user", jdbc.queryForObject("SELECT ref_key FROM recon_issue WHERE recon_date = ? AND type = 'BALANCE_LEDGER_MISMATCH'", String::class.java, day))
        assertFailsWith<IllegalArgumentException> { ReconIssue(day, ReconIssueType.UNKNOWN_REFERENCE) }
    }

    @Test
    fun `잔고 ≠ 원장 합인 지갑을 찾는다 - 맞는 지갑은 없다`() {
        val healthy = fundedUser(10_000)
        val broken = fundedUser(10_000)
        // 공유 DB 의 모든 지갑을 훑는 ⑤ 가 뒤 테스트에서 이 지갑 때문에 불일치를 보고하지 않게, 깨뜨린 잔고는 반드시 되돌린다
        jdbc.update("UPDATE wallet SET balance = balance + 1 WHERE user_id = ?", broken)
        try {
            val mismatches = repo.ledgerMismatches()

            assertTrue(mismatches.contains(LedgerMismatch(broken, 10_001, 10_000)))
            assertFalse(mismatches.any { it.userId == healthy })
        } finally {
            jdbc.update("UPDATE wallet SET balance = balance - 1 WHERE user_id = ?", broken)
        }
    }
}
