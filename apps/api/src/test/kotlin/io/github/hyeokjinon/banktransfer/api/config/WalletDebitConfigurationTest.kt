package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.api.persistence.ConditionalWalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.RedisWalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.WalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.WalletLedger
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.redisson.api.RedissonClient
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.transaction.support.TransactionTemplate
import java.util.function.Supplier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * 설정값별 전략 빈 선택 (P4-2). 전략 선택만 보므로 DB 에 닿지 않는 JDBC 객체를 넣는다 (빈을 만들 때 쿼리를 하지 않는다).
 * 운영처럼 한 번에 한 전략: 요청마다 고르면 운영 API 에 실험 손잡이가 생기고 네 빈이 자원을 나눠 쓴다.
 */
class WalletDebitConfigurationTest {

    private val runner = ApplicationContextRunner()
        .withUserConfiguration(WalletDebitConfiguration::class.java)
        .withBean(NamedParameterJdbcTemplate::class.java, Supplier { NamedParameterJdbcTemplate(DriverManagerDataSource()) })
        .withBean(WalletLedger::class.java, Supplier {
            val dataSource = DriverManagerDataSource()
            WalletLedger(NamedParameterJdbcTemplate(dataSource), TransactionTemplate(DataSourceTransactionManager(dataSource)))
        })

    @Test
    fun `설정이 없으면 conditional 이고 RedissonClient 빈이 없다 (Redis 없이 뜬다)`() {
        runner.run { ctx ->
            assertNull(ctx.startupFailure)
            assertIs<ConditionalWalletDebit>(ctx.getBean(WalletDebit::class.java))
            assertEquals(0, ctx.getBeanNamesForType(RedissonClient::class.java).size)
        }
    }

    @ParameterizedTest(name = "{0} → {1}")
    @CsvSource(
        "conditional, ConditionalWalletDebit",
        "pessimistic, PessimisticWalletDebit",
        "optimistic, OptimisticWalletDebit",
        "PESSIMISTIC, PessimisticWalletDebit",
    )
    fun `설정값으로 전략을 고른다 (대소문자 무관), redis 가 아니면 RedissonClient 가 없다`(value: String, expected: String) {
        runner.withPropertyValues("wallet.debit-strategy=$value").run { ctx ->
            assertNull(ctx.startupFailure)
            assertEquals(expected, ctx.getBean(WalletDebit::class.java)::class.simpleName)
            assertEquals(0, ctx.getBeanNamesForType(RedissonClient::class.java).size)
        }
    }

    @Test
    fun `redis 면 RedisWalletDebit 과 RedissonClient - 기동 때 Redis 에 붙지 않는다 (닿지 않는 주소로도 뜬다)`() {
        runner.withPropertyValues("wallet.debit-strategy=redis", "wallet.lock.redis-address=redis://127.0.0.1:1").run { ctx ->
            assertNull(ctx.startupFailure)
            assertIs<RedisWalletDebit>(ctx.getBean(WalletDebit::class.java))
            assertEquals(1, ctx.getBeanNamesForType(RedissonClient::class.java).size)
        }
    }

    @Test
    fun `모르는 전략 이름이면 기동 실패`() {
        runner.withPropertyValues("wallet.debit-strategy=mutex").run { ctx ->
            assertNotNull(ctx.startupFailure)
        }
    }
}
