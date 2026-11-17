package io.github.hyeokjinon.banktransfer.gatewayapp.metrics

import io.github.hyeokjinon.banktransfer.gatewayapp.GatewayApps
import io.github.hyeokjinon.banktransfer.gatewayapp.port
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.boot.WebApplicationType
import org.springframework.web.client.RestClient
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertTrue

/** P9a 노출: 게이트웨이 업무 지표 · 공통 태그. 선출이 꺼진 기본(IntelliJ · 테스트)은 기동 즉시 리더 → leader 1 · connected 1. */
class GatewayPrometheusEndpointTest {
    @Test
    fun `게이트웨이 업무 지표가 Prometheus 형식으로 나온다`() {
        MockBankServer().start().use { bank ->
            GatewayApps.start(WebApplicationType.REACTIVE, bank.boundPort).use { ctx ->
                val rest = RestClient.create("http://127.0.0.1:${ctx.port()}")
                fun scrape() = rest.get().uri("/actuator/prometheus").retrieve().body(String::class.java)!!

                await.atMost(Duration.ofSeconds(10)) untilAsserted {
                    assertTrue(Regex("""(?m)^banktransfer_gateway_bank_connected\{.*} 1\.0$""").containsMatchIn(scrape()), "은행 연결 1 이 아니다")
                }
                val text = scrape()
                listOf(
                    "banktransfer_gateway_leader{", "banktransfer_gateway_bank_request_seconds_bucket{", "banktransfer_gateway_not_sent_total{",
                    "banktransfer_gateway_leader_terms_total{", "banktransfer_gateway_tran_no_remaining{", "banktransfer_gateway_tran_no_reserve_total{",
                    "banktransfer_bank_maintenance{", "process_start_time_seconds{",
                ).forEach { assertTrue(it in text, "없음: $it") }
                assertTrue(Regex("""(?m)^banktransfer_gateway_leader\{.*} 1\.0$""").containsMatchIn(text))
                assertTrue("""application="bank-gateway"""" in text)
            }
        }
    }
}
