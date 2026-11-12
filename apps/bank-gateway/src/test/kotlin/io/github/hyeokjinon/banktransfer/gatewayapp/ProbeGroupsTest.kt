package io.github.hyeokjinon.banktransfer.gatewayapp

import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.boot.WebApplicationType
import org.springframework.web.client.RestClient
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * K8s 프로브 그룹 (P8-3a 스펙 §7): readiness 는 은행 회선을 따라가고 liveness 는 따라가지 않는다.
 * 은행 장애 = 트래픽에서 빼기(호출자는 연결 실패 = "안 보냄")이지 재시작 사유가 아니다 — liveness 에 넣으면 재시작 루프 (실험 B2).
 */
class ProbeGroupsTest {
    @Test
    fun `은행이 내려가면 readiness 503 · liveness 200, 올라오면 readiness 200`() {
        MockBankServer().start().use { bank ->
            GatewayApps.start(WebApplicationType.REACTIVE, bank.boundPort).use { ctx ->
                val rest = RestClient.create("http://127.0.0.1:${ctx.port()}")
                fun code(path: String): Int = rest.get().uri(path).exchange { _, res -> res.statusCode.value() }
                await.atMost(Duration.ofSeconds(10)) untilAsserted { assertEquals(200, code("/actuator/health/readiness")) }
                bank.pause()
                await.atMost(Duration.ofSeconds(10)) untilAsserted { assertEquals(503, code("/actuator/health/readiness")) }
                assertEquals(200, code("/actuator/health/liveness"))
                bank.resume()
                await.atMost(Duration.ofSeconds(40)) untilAsserted { assertEquals(200, code("/actuator/health/readiness")) }
            }
        }
    }
}
