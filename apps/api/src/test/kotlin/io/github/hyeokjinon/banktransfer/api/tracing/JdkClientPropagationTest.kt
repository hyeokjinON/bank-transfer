package io.github.hyeokjinon.banktransfer.api.tracing

import com.sun.net.httpserver.HttpServer
import io.github.hyeokjinon.banktransfer.api.support.attr
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayJson
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayPaths
import io.github.hyeokjinon.banktransfer.gateway.http.HeaderDto
import io.github.hyeokjinon.banktransfer.gateway.http.HttpGatewaySettings
import io.github.hyeokjinon.banktransfer.gateway.http.JdkHttpBankGateway
import io.github.hyeokjinon.banktransfer.gateway.http.TransferResult
import io.micrometer.observation.ObservationHandler
import io.micrometer.observation.ObservationRegistry
import io.micrometer.observation.transport.SenderContext
import io.micrometer.tracing.handler.DefaultTracingObservationHandler
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext
import io.micrometer.tracing.otel.bridge.OtelPropagator
import io.micrometer.tracing.otel.bridge.OtelTracer
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import java.net.InetSocketAddress
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * JDK 클라이언트 판도 traceId 를 잇는다 (P9b 스펙 §8 통합 5) — 스프링 문맥 · 컨테이너 없이, 실제 OTel SDK · Micrometer 브리지 · 관측 처리기로.
 * JDK HttpClient 는 자동 계측이 없어서 JdkHttpBankGateway 의 손코드 관측이 traceparent 를 넣는지 여기서 고정한다.
 */
class JdkClientPropagationTest {
    private val exporter = InMemorySpanExporter.create()
    private val provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build()
    private val otel = provider.get("test")
    private val tracer = OtelTracer(otel, OtelCurrentTraceContext()) { }
    private val propagator = OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()), otel)
    private val registry = ObservationRegistry.create().apply {
        // Boot 와 같게 첫 번째로 맞는 것 하나만 — 둘 다 걸리면 구간이 두 개 열리고 INTERNAL 이 남는다
        observationConfig().observationHandler(
            ObservationHandler.FirstMatchingCompositeObservationHandler(
                PropagatingSenderTracingObservationHandler<SenderContext<*>>(tracer, propagator),
                DefaultTracingObservationHandler(tracer),
            ),
        )
    }

    @Volatile private var receivedTraceparent: String? = null
    private val server: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext(GatewayPaths.TRANSFERS) { ex ->
            receivedTraceparent = ex.requestHeaders.getFirst("traceparent")
            ex.requestBody.readAllBytes()
            val bytes = GatewayJson.write(
                TransferResult(HeaderDto(1, LocalDateTime.of(2026, 11, 10, 9, 0), "0000"), "090", "1002123456789", 1_000, "M", REF),
            ).toByteArray()
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, bytes.size.toLong())
            ex.responseBody.use { it.write(bytes) }
        }
        start()
    }

    @AfterTest
    fun close() {
        server.stop(0)
        provider.close()
    }

    @Test
    fun `JDK 판 호출은 부모 구간의 traceId 로 traceparent 를 보내고, CLIENT http post 구간이 부모 아래에 남는다`() {
        val parent = tracer.nextSpan().name("parent").start()
        JdkHttpBankGateway(HttpGatewaySettings(baseUrl = "http://127.0.0.1:${server.address.port}"), registry).use { gateway ->
            val res = tracer.withSpan(parent).use { gateway.transfer("090", "1002123456789", 1_000, "M", REF).get(5, TimeUnit.SECONDS) }
            assertEquals("0000", res.header.responseCode)
        }
        parent.end()

        // traceparent 는 끝 깃발을 비교하지 않는다 (OTel 1.62 는 -03)
        val received = assertNotNull(receivedTraceparent)
        assertEquals("00-${parent.context().traceId()}-", received.take(36))
        await.atMost(Duration.ofSeconds(2)) untilAsserted {
            val client = exporter.finishedSpanItems.single { it.name == "http post" }
            assertEquals(SpanKind.CLIENT, client.kind)
            assertEquals(parent.context().traceId(), client.traceId)
            assertEquals(parent.context().spanId(), client.parentSpanId)
            assertEquals(GatewayPaths.TRANSFERS, client.attr("uri"))
            assertEquals("-${client.spanId}-", received.substring(35, 35 + 18))   // 서버가 받은 부모 = 이 CLIENT 구간
        }
    }

    private companion object {
        const val REF = "T261110JDKPROP0000001"
    }
}
