package io.github.hyeokjinon.banktransfer.api.support

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.web.client.RestClient

/**
 * 모든 Spring 통합 테스트의 부모. 설정이 같으므로 스프링 테스트 컨텍스트가 하나로 캐시되어
 * MySQL 컨테이너와 모의 은행도 한 번만 뜬다. 테스트끼리 DB 를 공유하므로 키 · 계좌번호는 테스트마다 새로 만든다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(MySqlTestContainer::class)
abstract class ApiIntegrationTest {

    companion object {
        /** 9990000000001 은 응답하지 않는 계좌 (타임아웃 재현). 컨텍스트와 수명이 같으므로 닫지 않는다. */
        val bank: MockBankServer = MockBankServer(silentAccounts = setOf("9990000000001")).start()

        @JvmStatic
        @DynamicPropertySource
        fun bankProperties(registry: DynamicPropertyRegistry) {
            registry.add("bank.gateway.port") { bank.boundPort }
            registry.add("bank.gateway.response-timeout") { "500ms" }
        }
    }

    @Value("\${local.server.port}")
    protected var port: Int = 0

    @Autowired
    protected lateinit var gateway: BankGatewayClient

    protected val client: RestClient by lazy { RestClient.create("http://localhost:$port") }

    @BeforeEach
    fun waitForBankConnection() {
        val deadline = System.currentTimeMillis() + 5_000
        while (!gateway.isConnected) {
            check(System.currentTimeMillis() < deadline) { "gateway did not connect to mock bank" }
            Thread.sleep(20)
        }
    }
}
