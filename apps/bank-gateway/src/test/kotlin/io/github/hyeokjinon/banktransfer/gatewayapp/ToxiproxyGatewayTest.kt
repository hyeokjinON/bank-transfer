package io.github.hyeokjinon.banktransfer.gatewayapp

import eu.rekawek.toxiproxy.Proxy
import eu.rekawek.toxiproxy.ToxiproxyClient
import eu.rekawek.toxiproxy.model.ToxicDirection
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import io.github.hyeokjinon.banktransfer.gateway.http.HttpGatewaySettings
import io.github.hyeokjinon.banktransfer.gateway.http.WebClientBankGateway
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.boot.WebApplicationType
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.Testcontainers
import org.testcontainers.junit.jupiter.Testcontainers as TestcontainersTest
import org.testcontainers.toxiproxy.ToxiproxyContainer
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * 실제 망 장애에 가까운 재현 (P8-1 스펙 §7): 호출자 → Toxiproxy(컨테이너) → 게이트웨이 앱(이 JVM) → 모의 은행.
 * 지연은 그만큼 늦게 성공하고, 응답이 막히면(timeout toxic) 호출자는 "모름" — 은행에는 한 번만 갔다.
 */
@TestcontainersTest(disabledWithoutDocker = true)
class ToxiproxyGatewayTest {

    companion object {
        private val bank = MockBankServer().start()
        private lateinit var app: ConfigurableApplicationContext
        private lateinit var toxiproxy: ToxiproxyContainer
        private lateinit var proxy: Proxy
        private lateinit var baseUrl: String

        @JvmStatic
        @BeforeAll
        fun up() {
            app = GatewayApps.start(WebApplicationType.REACTIVE, bank.boundPort)
            Testcontainers.exposeHostPorts(app.port())                       // 컨테이너가 host.testcontainers.internal 로 이 JVM 의 게이트웨이에 닿게
            toxiproxy = ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.9.0").also { it.start() }
            proxy = ToxiproxyClient(toxiproxy.host, toxiproxy.controlPort)
                .createProxy("gateway", "0.0.0.0:8666", "host.testcontainers.internal:${app.port()}")
            baseUrl = "http://${toxiproxy.host}:${toxiproxy.getMappedPort(8666)}"
            await.atMost(Duration.ofSeconds(10)) untilAsserted { assertTrue(app.getBean(BankGatewayClient::class.java).isConnected) }
        }

        @JvmStatic
        @AfterAll
        fun down() { toxiproxy.stop(); app.close(); bank.close() }
    }

    private val client = WebClientBankGateway(HttpGatewaySettings(baseUrl = baseUrl, responseTimeout = Duration.ofSeconds(2)))

    @AfterEach
    fun clean() { proxy.toxics().all.forEach { it.remove() }; client.close() }

    private fun ref() = "T261110" + UUID.randomUUID().toString().replace("-", "").take(13).uppercase()

    @Test
    fun `지연 300ms 를 넣으면 그만큼 늦게 성공`() {
        proxy.toxics().latency("lat", ToxicDirection.DOWNSTREAM, 300)
        val started = System.nanoTime()
        val res = client.transfer("090", "1002123456789", 1_000, "TX1", ref()).get(5, TimeUnit.SECONDS)
        assertEquals("0000", res.header.responseCode)
        assertTrue(Duration.ofNanos(System.nanoTime() - started) >= Duration.ofMillis(300))
    }

    @Test
    fun `응답이 막히면 모름 - 은행에는 한 번만 갔다`() {
        proxy.toxics().timeout("cut", ToxicDirection.DOWNSTREAM, 0)       // 응답 방향을 막는다 (요청은 통과)
        val r = ref()
        val e = (runCatching { client.transfer("090", "1002123456789", 1_000, "TX2", r).get(5, TimeUnit.SECONDS) }.exceptionOrNull() as ExecutionException).cause!!
        assertIs<BankUnavailableException>(e)
        assertTrue(e !is BankNotSentException)
        await.atMost(Duration.ofSeconds(3)) untilAsserted { assertEquals(1, bank.depositsOf("1002123456789").count { it.reference == r }) }
    }
}
