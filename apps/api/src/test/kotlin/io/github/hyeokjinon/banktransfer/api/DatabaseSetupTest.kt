package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.batch.core.job.parameters.JobParametersBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.time.LocalDate
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

class DatabaseSetupTest : ApiIntegrationTest() {

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    @Test
    fun `Flyway 가 V1 테이블을 만들었다`() {
        val tables = jdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() " +
                "AND table_name IN ('account_verification', 'transfer', 'outbox_event') ORDER BY table_name",
            String::class.java,
        )
        assertEquals(listOf("account_verification", "outbox_event", "transfer"), tables)
    }

    @Test
    fun `DB 세션 시간대는 KST`() {
        assertEquals("Asia/Seoul", jdbc.queryForObject("SELECT @@session.time_zone", String::class.java))
    }

    @Test
    fun `멱등 키와 거래번호는 ascii_bin 으로 바이트 비교한다`() {
        for (column in listOf("idempotency_key", "reference")) {
            val collation = jdbc.queryForObject(
                "SELECT collation_name FROM information_schema.columns " +
                    "WHERE table_schema = DATABASE() AND table_name = 'transfer' AND column_name = ?",
                String::class.java, column,
            )
            assertEquals("ascii_bin", collation, column)
        }
    }

    @Test
    fun `V2 가 transfer 에 재발행 컬럼을 더했다 (횟수 NOT NULL 기본 0, 시각 NULL)`() {
        val columns = jdbc.queryForList(
            "SELECT column_name, is_nullable, column_default FROM information_schema.columns " +
                "WHERE table_schema = DATABASE() AND table_name = 'transfer' AND column_name IN ('republish_count', 'republished_at') " +
                "ORDER BY column_name",
        )
        assertEquals(listOf("republish_count", "republished_at"), columns.map { it["column_name"] })
        assertEquals("NO", columns[0]["is_nullable"])
        assertEquals("0", columns[0]["column_default"].toString())
        assertEquals("YES", columns[1]["is_nullable"])
    }

    @Test
    fun `V3 가 지갑 · 원장 테이블과 사용자 범위 멱등 UNIQUE 를 만들었다 (user_id 는 모두 ascii_bin)`() {
        val tables = jdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() " +
                "AND table_name IN ('wallet', 'wallet_ledger') ORDER BY table_name",
            String::class.java,
        )
        assertEquals(listOf("wallet", "wallet_ledger"), tables)

        val uniqueColumns = jdbc.queryForList(
            "SELECT column_name FROM information_schema.statistics WHERE table_schema = DATABASE() " +
                "AND table_name = 'transfer' AND index_name = 'uk_transfer_user_idempotency' ORDER BY seq_in_index",
            String::class.java,
        )
        assertEquals(listOf("user_id", "idempotency_key"), uniqueColumns)
        val oldUnique = jdbc.queryForObject(
            "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() " +
                "AND table_name = 'transfer' AND index_name = 'uk_transfer_idempotency'",
            Int::class.java,
        )
        assertEquals(0, oldUnique)   // 전역 UNIQUE 는 없어졌다 — 남의 키와 겹쳐도 된다

        for (table in listOf("wallet", "wallet_ledger", "transfer", "account_verification")) {
            val collation = jdbc.queryForObject(
                "SELECT collation_name FROM information_schema.columns " +
                    "WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'user_id'",
                String::class.java, table,
            )
            assertEquals("ascii_bin", collation, "$table.user_id")
        }
    }

    @Autowired
    private lateinit var jobRepository: JobRepository

    @Test
    fun `V4 가 Spring Batch 메타 표 · bank_statement_line · recon_issue 를 만들었다`() {
        val tables = jdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()",
            String::class.java,
        ).toSet()
        val expected = setOf(
            "BATCH_JOB_INSTANCE", "BATCH_JOB_EXECUTION", "BATCH_JOB_EXECUTION_PARAMS", "BATCH_STEP_EXECUTION",
            "BATCH_STEP_EXECUTION_CONTEXT", "BATCH_JOB_EXECUTION_CONTEXT", "BATCH_STEP_EXECUTION_SEQ", "BATCH_JOB_EXECUTION_SEQ",
            "BATCH_JOB_INSTANCE_SEQ", "bank_statement_line", "recon_issue",
        )
        assertEquals(expected, tables.intersect(expected))

        fun indexColumns(table: String, index: String) = jdbc.queryForList(
            "SELECT column_name FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ? ORDER BY seq_in_index",
            String::class.java, table, index,
        )
        assertEquals(listOf("bank_date", "line_no"), indexColumns("bank_statement_line", "PRIMARY"))
        assertEquals(listOf("recon_date", "type", "ref_key"), indexColumns("recon_issue", "uk_recon_issue"))
        for (table in listOf("bank_statement_line", "recon_issue")) {
            val collation = jdbc.queryForObject(
                "SELECT collation_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'reference'",
                String::class.java, table,
            )
            assertEquals("ascii_bin", collation, "$table.reference")
        }
    }

    @Test
    fun `잡 저장소는 JDBC 다 - 잡 인스턴스가 BATCH_JOB_INSTANCE 에 남는다 (starter-batch 만이면 메모리 저장소)`() {
        val name = "p5-1-probe-${UUID.randomUUID()}"

        jobRepository.createJobInstance(name, JobParametersBuilder().addLocalDate("reconDate", LocalDate.of(2100, 1, 1)).toJobParameters())

        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM BATCH_JOB_INSTANCE WHERE JOB_NAME = ?", Int::class.java, name))
    }
}
