package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
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
}
