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
import java.time.Duration

/**
 * 모든 Spring 통합 테스트의 부모. 설정이 같으므로 스프링 테스트 컨텍스트가 하나로 캐시되어
 * MySQL 컨테이너와 모의 은행도 한 번만 뜬다. 테스트끼리 DB 를 공유하므로 키 · 계좌번호는 테스트마다 새로 만든다.
 * 폴링 퍼블리셔 스케줄은 끈다: 테스트가 OutboxRelay.publishPending() 을 직접 불러 발행 시점을 정한다 (다른 테스트의 이체 상태가 갑자기 바뀌지 않게).
 * 정리 작업도 마찬가지로 끈다 (TransferReconciler.reconcile() 을 직접 부른다).
 * 재시도 토픽 간격은 위 프로퍼티로 줄인다. 은행 장애는 [withBankDown] 으로 만든다.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "transfer.outbox.relay.enabled=false",
        "transfer.reconcile.enabled=false",
        // 재시도 간격 0.5초 → 1초 → 2초 (운영 5 · 30 · 60초). 뒤 메시지가 막히지 않는지 볼 여유(3.5초)는 남기고 테스트는 빨리 끝나게
        "transfer.retry.delay-ms=500",
        "transfer.retry.multiplier=2.0",
        "transfer.retry.max-delay-ms=2000",
    ],
)
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
    fun waitForBankConnection() = awaitGateway(connected = true, timeout = Duration.ofSeconds(5))

    /**
     * 은행 장애를 만든다: 모의 은행을 내려(붙어 있던 연결도 끊음) [block] 을 돌리고, 끝나면 **반드시** 같은 포트로 다시 올린다.
     * 게이트웨이 재접속은 1초부터 두 배씩(최대 30초) 늘어나므로 다시 붙을 때까지 넉넉히 기다린다 — 다음 테스트가 끊긴 채 시작하지 않게.
     */
    protected fun <T> withBankDown(block: () -> T): T {
        bank.pause()
        var failure: Throwable? = null
        try {
            awaitGateway(connected = false, timeout = Duration.ofSeconds(5))
            return block()
        } catch (e: Throwable) {
            failure = e
            throw e
        } finally {
            // 정리가 실패해도 진짜 실패 원인(block 의 예외)을 가리지 않는다: 원래 예외를 주(主)로 두고 정리 실패는 suppressed 로 붙인다
            try {
                bank.resume()
                awaitGateway(connected = true, timeout = Duration.ofSeconds(40))
            } catch (cleanup: Throwable) {
                if (failure == null) throw cleanup
                failure.addSuppressed(cleanup)
            }
        }
    }

    private fun awaitGateway(connected: Boolean, timeout: Duration) {
        val deadline = System.currentTimeMillis() + timeout.toMillis()
        while (gateway.isConnected != connected) {
            check(System.currentTimeMillis() < deadline) { "gateway connected=${gateway.isConnected}, expected $connected within $timeout" }
            Thread.sleep(20)
        }
    }
}
