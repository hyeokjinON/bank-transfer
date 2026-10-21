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
 * 폴링 퍼블리셔 스케줄은 끈다: 테스트가 OutboxRelay.publishPending() 을 직접 불러 발행 시점을 정한다 (다른 테스트의 이체 상태가 갑자기 바뀌지 않게).
 * 정리 작업도 마찬가지로 끈다 (TransferReconciler.reconcile() 을 직접 부른다).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = ["transfer.outbox.relay.enabled=false", "transfer.reconcile.enabled=false"])
@Import(MySqlTestContainer::class, KafkaTestContainer::class)
abstract class ApiIntegrationTest {

    companion object {
        /** 9990000000001 = 무응답 · 처리 안 함. 9990000000002 = 처리 후 무응답 (MockBankServer 기본값). 컨텍스트와 수명이 같으므로 닫지 않는다. */
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
