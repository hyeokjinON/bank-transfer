package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.transfer.BankStatementLine
import io.github.hyeokjinon.banktransfer.transfer.ReconIssueType
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.Timestamp
import java.sql.Types
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

/** recon_issue 한 줄로 남길 것 (P5-1). 지갑 이슈는 거래번호가 없고 userId 가 키다. */
data class ReconIssue(
    val reconDate: LocalDate,
    val type: ReconIssueType,
    val reference: String? = null,
    val transferId: UUID? = null,
    val userId: String? = null,
    val ourValue: String? = null,
    val bankValue: String? = null,
) {
    init {
        require(reference != null || userId != null) { "an issue needs a reference or a user id (ref_key)" }
    }
}

/** 저장된 이슈 (알림 목록 · 보고용). 사용자 id 는 싣지 않는다 — 알림에 넣지 않는 값이다. */
data class ReconIssueRow(val id: Long, val type: ReconIssueType, val reference: String?, val ourValue: String?, val bankValue: String?)

/** 잔고 ≠ 원장 합인 지갑 (⑤). */
data class LedgerMismatch(val userId: String, val balance: Long, val ledgerSum: Long)

/**
 * 일마감 대사의 SQL (P5-1). bank_statement_line · recon_issue 는 append-only 기록이고, 같은 것을 다시 넣으면 무시한다 —
 * 잡이 어디서 다시 시작해도 결과가 같다. 시각은 Timestamp(Instant) 로 넘기고 KST 변환은 JDBC 설정이 한다 (database.md §2), 날짜는 LocalDate 그대로.
 * 쓰기는 부르는 쪽(배치 단계)의 트랜잭션에 합류한다.
 */
class ReconRepository(private val jdbc: NamedParameterJdbcTemplate) {

    fun insertLines(lines: List<BankStatementLine>, loadedAt: Instant) {
        if (lines.isEmpty()) return
        val at = Timestamp.from(loadedAt)
        jdbc.batchUpdate(
            "INSERT INTO bank_statement_line (bank_date, line_no, reference, bank_code, account_no, amount, processed_at, loaded_at) " +
                "VALUES (:bankDate, :lineNo, :reference, :bankCode, :accountNo, :amount, :processedAt, :loadedAt) " +
                // 같은 파일을 다시 넣으면(② 재시작) 그대로 둔다 — 은행이 한 번 보낸 줄은 기록이라 덮어쓰지 않는다
                "ON DUPLICATE KEY UPDATE line_no = line_no",
            lines.map { line ->
                MapSqlParameterSource()
                    .addValue("bankDate", line.bankDate)
                    .addValue("lineNo", line.lineNo)
                    .addValue("reference", line.reference)
                    .addValue("bankCode", line.bankCode)
                    .addValue("accountNo", line.accountNo)
                    .addValue("amount", line.amount)
                    .addValue("processedAt", Timestamp.from(line.processedAt))
                    .addValue("loadedAt", at)
            }.toTypedArray(),
        )
    }

    /** 이 거래번호의 **적재된 모든 날짜의** 줄 (날짜 · 줄 순). 이중 지급(⑪)과 늦게 처리된 건을 보려면 날짜를 가리지 않는다. */
    fun linesOf(reference: String): List<BankStatementLine> =
        jdbc.query(
            "SELECT bank_date, line_no, reference, bank_code, account_no, amount, processed_at FROM bank_statement_line " +
                "WHERE reference = :reference ORDER BY bank_date, line_no",
            MapSqlParameterSource("reference", reference),
            RowMapper { rs, _ ->
                BankStatementLine(
                    bankDate = rs.getObject("bank_date", LocalDate::class.java),
                    lineNo = rs.getInt("line_no"),
                    reference = rs.getString("reference"),
                    bankCode = rs.getString("bank_code"),
                    accountNo = rs.getString("account_no"),
                    amount = rs.getLong("amount"),
                    processedAt = rs.getTimestamp("processed_at").toInstant(),
                )
            },
        )

    /** 파일 [bankDate] 의 줄 수 — 출금(T) · 1원인증(V) 을 접두로 가른다. */
    fun countLines(bankDate: LocalDate, referencePrefix: String): Int =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM bank_statement_line WHERE bank_date = :bankDate AND reference LIKE :pattern",
            MapSqlParameterSource().addValue("bankDate", bankDate).addValue("pattern", "$referencePrefix%"),
            Int::class.java,
        )!!

    fun insertIssue(issue: ReconIssue, at: Instant) {
        jdbc.update(
            "INSERT INTO recon_issue (recon_date, type, reference, transfer_id, user_id, our_value, bank_value, created_at) " +
                "VALUES (:reconDate, :type, :reference, :transferId, :userId, :ourValue, :bankValue, :createdAt) " +
                // 같은 날 · 같은 종류 · 같은 키는 한 번만 (uk_recon_issue) — 재시작 · 재실행해도 늘지 않는다
                "ON DUPLICATE KEY UPDATE id = id",
            MapSqlParameterSource()
                .addValue("reconDate", issue.reconDate)
                .addValue("type", issue.type.name)
                .addValue("reference", issue.reference, Types.CHAR)
                .addValue("transferId", issue.transferId?.toString(), Types.CHAR)
                .addValue("userId", issue.userId, Types.VARCHAR)
                .addValue("ourValue", issue.ourValue?.take(VALUE_MAX), Types.VARCHAR)
                .addValue("bankValue", issue.bankValue?.take(VALUE_MAX), Types.VARCHAR)
                .addValue("createdAt", Timestamp.from(at)),
        )
    }

    fun issueCounts(reconDate: LocalDate): Map<ReconIssueType, Int> =
        jdbc.query(
            "SELECT type, COUNT(*) AS n FROM recon_issue WHERE recon_date = :reconDate GROUP BY type",
            MapSqlParameterSource("reconDate", reconDate),
            RowMapper { rs, _ -> ReconIssueType.valueOf(rs.getString("type")) to rs.getInt("n") },
        ).toMap()

    fun issuesOf(reconDate: LocalDate): List<ReconIssueRow> =
        jdbc.query(
            "SELECT id, type, reference, our_value, bank_value FROM recon_issue WHERE recon_date = :reconDate ORDER BY id",
            MapSqlParameterSource("reconDate", reconDate),
            RowMapper { rs, _ ->
                ReconIssueRow(rs.getLong("id"), ReconIssueType.valueOf(rs.getString("type")), rs.getString("reference"), rs.getString("our_value"), rs.getString("bank_value"))
            },
        )

    /** 잔고 ≠ 원장 합인 지갑 (database.md §9 "잔고 = 원장 합 검증" 과 같은 식). 전체를 훑는다 — 하루 한 번이라 감수한다. */
    fun ledgerMismatches(): List<LedgerMismatch> =
        jdbc.query(
            "SELECT w.user_id, w.balance, COALESCE(SUM(CASE l.type WHEN 'WITHDRAW' THEN -l.amount ELSE l.amount END), 0) AS ledger_sum " +
                "FROM wallet w LEFT JOIN wallet_ledger l ON l.user_id = w.user_id " +
                "GROUP BY w.user_id, w.balance HAVING w.balance <> ledger_sum",
            MapSqlParameterSource(),
            RowMapper { rs, _ -> LedgerMismatch(rs.getString("user_id"), rs.getLong("balance"), rs.getLong("ledger_sum")) },
        )

    private companion object {
        /** our_value · bank_value 열 길이. */
        const val VALUE_MAX = 200
    }
}
