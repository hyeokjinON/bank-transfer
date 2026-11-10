package io.github.hyeokjinon.banktransfer.gatewayapp

import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.boot.WebApplicationType
import org.springframework.web.client.RestClient
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertContains

/** bankConnection health 가 은행 회선을 따라간다 (P8-3 readiness 로 쓴다). */
class BankConnectionHealthTest {
    @Test
    fun `은행이 내려가면 DOWN, 올라오면 UP`() {
        MockBankServer().start().use { bank ->
            GatewayApps.start(WebApplicationType.REACTIVE, bank.boundPort).use { ctx ->
                val rest = RestClient.create("http://127.0.0.1:${ctx.port()}")
                fun status(): String = rest.get().uri("/actuator/health/bankConnection").exchange { _, res -> res.bodyTo(String::class.java) ?: "" }
                await.atMost(Duration.ofSeconds(10)) untilAsserted { assertContains(status(), "\"UP\"") }
                bank.pause()
                await.atMost(Duration.ofSeconds(10)) untilAsserted { assertContains(status(), "\"DOWN\"") }
                bank.resume()
                await.atMost(Duration.ofSeconds(40)) untilAsserted { assertContains(status(), "\"UP\"") }
            }
        }
    }
}
