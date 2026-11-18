package io.github.hyeokjinon.banktransfer.gatewayapp.tracing

import io.github.hyeokjinon.banktransfer.gateway.http.GatewayPaths
import io.github.hyeokjinon.banktransfer.gatewayapp.GatewayApps
import io.github.hyeokjinon.banktransfer.gatewayapp.bankClient
import io.github.hyeokjinon.banktransfer.gatewayapp.port
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.boot.WebApplicationType
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 게이트웨이 안의 이어짐 (P9b 스펙 §4-3 · §8): 받은 traceparent 를 서버 구간이 잇고, withClient 의 `bank.request` 가 그 자식이다.
 * WebFlux 판은 spring.reactor.context-propagation=auto 에 기댄다 — 컨트롤러 · withClient 가 서버 관측을 현재로 본다.
 */
class GatewayTraceContinuityTest {
    private val bank = MockBankServer().start()

    @AfterTest
    fun close() = bank.close()

    private fun check(server: WebApplicationType) {
        val exporter = InMemorySpanExporter.create()
        GatewayApps.startTraced(server, bank.boundPort, exporter).use { ctx ->
            await.atMost(Duration.ofSeconds(10)) untilAsserted { assertTrue(ctx.bankClient().isConnected) }
            val body = """{"bankCode":"090","accountNo":"1002123456789","amount":1000,"memo":"T","reference":"$REF"}"""
            val request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:${ctx.port()}${GatewayPaths.TRANSFERS}"))
                .header("Content-Type", "application/json")
                .header("traceparent", "00-$TRACE-$PARENT-01")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build()
            val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
            assertEquals(200, response.statusCode(), response.body())

            await.atMost(Duration.ofSeconds(5)) untilAsserted {
                val trace = exporter.finishedSpanItems.filter { it.traceId == TRACE }
                val serverSpans = trace.filter { it.kind == SpanKind.SERVER }
                val bankSpans = trace.filter { it.name == BankRequestTracing.SPAN_NAME }
                assertEquals(1, serverSpans.size, "서버 구간: ${trace.map { it.name }}")
                assertEquals(1, bankSpans.size, "bank.request: ${trace.map { it.name }}")
                val serverSpan = serverSpans.first()
                val bankSpan = bankSpans.first()
                assertEquals(PARENT, serverSpan.parentSpanId)
                assertEquals("bank-gateway", serverSpan.resource.getAttribute(AttributeKey.stringKey("service.name")))
                assertEquals(serverSpan.spanId, bankSpan.parentSpanId)
                assertEquals("ok", bankSpan.attributes.get(AttributeKey.stringKey(BankRequestTracing.OUTCOME)))
                assertEquals(REF, bankSpan.attributes.get(AttributeKey.stringKey(BankRequestTracing.REFERENCE)))
            }
        }
    }

    @Test
    fun `WebFlux 판 - 받은 traceparent 를 이어 server 에서 bank request 로`() = check(WebApplicationType.REACTIVE)

    @Test
    fun `MVC 비동기 판도 같다`() = check(WebApplicationType.SERVLET)

    private companion object {
        const val TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
        const val PARENT = "00f067aa0ba902b7"
        const val REF = "T261118CONTINUITY01"
    }
}
