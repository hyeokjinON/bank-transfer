package io.github.hyeokjinon.banktransfer.api.tracing

import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext
import io.micrometer.tracing.otel.bridge.OtelPropagator
import io.micrometer.tracing.otel.bridge.OtelTracer
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** 아웃박스를 건너는 문맥 (P9b 스펙 §4-1, E6 · E10) — 실제 OTel SDK · Micrometer 브리지로. */
class OutboxTracingTest {
    private val exporter = InMemorySpanExporter.create()
    private val provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build()
    private val otel = provider.get("test")
    private val tracer = OtelTracer(otel, OtelCurrentTraceContext()) { }
    private val tracing = OutboxTracing(tracer, OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()), otel))

    @AfterTest
    fun close() = provider.close()

    private fun publishSpans() = exporter.finishedSpanItems.filter { it.name == OutboxTracing.SPAN_NAME }

    @Test
    fun `구간 안에서는 W3C traceparent 55자, 밖에서는 null`() {
        assertNull(tracing.currentTraceParent())
        val span = tracer.nextSpan().name("http").start()
        val tp = tracer.withSpan(span).use { tracing.currentTraceParent() }
        span.end()

        // 끝 깃발: OTel 1.6x 는 sampled(01)에 random-trace-id(02)를 더해 03 으로 쓴다 — 접두와 sampled 비트만 본다
        assertEquals("00-${span.context().traceId()}-${span.context().spanId()}-", tp!!.take(53))
        assertEquals(1, tp.takeLast(2).toInt(16) and 1)
        assertEquals(55, tp.length)
    }

    @Test
    fun `저장한 traceparent 를 부모로 outbox publish 를 열고, 안에서 연 구간은 그 자식 - 끝나면 범위가 닫힌다`() {
        val tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        tracing.publishing(tp, "transfer-1", 7L) { tracer.nextSpan().name("send").start().end() }

        val publish = publishSpans().single()
        assertEquals("0af7651916cd43dd8448eb211c80319c", publish.traceId)
        assertEquals("b7ad6b7169203331", publish.parentSpanId)
        assertEquals("transfer-1", publish.attributes.get(AttributeKey.stringKey("transfer.id")))
        assertEquals("7", publish.attributes.get(AttributeKey.stringKey("outbox.event_id")))
        assertEquals(publish.spanId, exporter.finishedSpanItems.single { it.name == "send" }.parentSpanId)
        assertNull(tracer.currentSpan())   // 다음 행으로 새지 않는다 (위험 C1)
    }

    @Test
    fun `NULL · 틀린 형식 · 0 trace id 는 예외 없이 각각 새 trace`() {
        val bad = listOf(
            null, "", "garbage",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",          // 깃발 없음
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",       // 0 trace id
            "00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-01",       // 대문자 (W3C 는 소문자)
        )
        bad.forEach { tracing.publishing(it, "t", 1L) { } }

        val publishes = publishSpans()
        assertEquals(bad.size, publishes.size)
        assertTrue(publishes.none { it.parentSpanContext.isValid })
        assertEquals(bad.size, publishes.map { it.traceId }.toSet().size)
        bad.forEach { assertFalse(OutboxTracing.isValid(it), "$it") }
    }

    @Test
    fun `보내기가 던지면 구간에 남기고 그대로 던진다`() {
        assertFailsWith<IllegalStateException> {
            tracing.publishing("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "t", 1L) { error("broker down") }
        }
        assertEquals(StatusCode.ERROR, publishSpans().single().status.statusCode)
        assertNull(tracer.currentSpan())
    }
}
