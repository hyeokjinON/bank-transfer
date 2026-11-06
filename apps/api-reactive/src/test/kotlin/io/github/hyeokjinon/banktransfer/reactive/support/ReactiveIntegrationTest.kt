package io.github.hyeokjinon.banktransfer.reactive.support

import io.github.hyeokjinon.banktransfer.account.infra.contract.VerificationErrorContract
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.web.client.RestClient
import java.time.Duration
import java.util.UUID

/**
 * apps/api-reactive 통합 테스트의 부모 (apps/api 의 ApiIntegrationTest 와 같은 원칙): 설정이 같으면 컨텍스트 · MySQL 컨테이너 · 모의 은행이 한 번만 뜬다.
 * 스키마는 apps/api 의 마이그레이션으로 Flyway 가 만든다 (build.gradle.kts 의 spring.flyway.locations). 테스트끼리 DB 를 공유하므로 사용자는 테스트마다 새로.
 * 모의 은행 지연을 바꾼 테스트가 다음 테스트에 남기지 않게 끝날 때 되돌린다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(MySqlTestContainer::class, TestClockConfiguration::class)
abstract class ReactiveIntegrationTest {

    companion object {
        val bank: MockBankServer = MockBankServer(silentAccounts = setOf(VerificationErrorContract.SILENT_ACCOUNT)).start()

        @JvmStatic
        @DynamicPropertySource
        fun bankProperties(registry: DynamicPropertyRegistry) {
            registry.add("bank.gateway.port") { bank.boundPort }
            registry.add("bank.gateway.response-timeout") { "500ms" }
        }
    }

    @Value("\${local.server.port}")
    protected var port: Int = 0

    @Autowired protected lateinit var gateway: BankGatewayClient
    @Autowired protected lateinit var jdbc: JdbcTemplate
    @Autowired protected lateinit var testClock: TestClock

    protected val client: RestClient by lazy { RestClient.create("http://localhost:$port") }

    protected fun newUserId(): String = UUID.randomUUID().toString()

    protected fun rowsOf(userId: String): Int =
        jdbc.queryForObject("SELECT COUNT(*) FROM account_verification WHERE user_id = ?", Int::class.java, userId)!!

    @BeforeEach
    fun waitForBankConnection() {
        val deadline = System.currentTimeMillis() + 5_000
        while (!gateway.isConnected) { check(System.currentTimeMillis() < deadline) { "gateway not connected" }; Thread.sleep(20) }
    }

    @AfterEach
    fun resetShared() {
        testClock.release()
        bank.responseDelay = Duration.ZERO
    }
}
