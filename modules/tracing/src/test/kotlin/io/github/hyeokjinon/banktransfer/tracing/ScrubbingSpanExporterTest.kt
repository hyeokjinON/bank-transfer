package io.github.hyeokjinon.banktransfer.tracing

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** 내보내기 앞 지우개 (P9b 결정 E9). 예외 메시지 · 스택 · 상태 설명에는 사용자 id · 금액 · 계좌가 섞일 수 있다 — 종류만 남긴다. */
class ScrubbingSpanExporterTest {
    private val sink = InMemorySpanExporter.create()
    private val provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(ScrubbingSpanExporter(sink))).build()
    private val tracer = provider.get("test")

    @AfterTest
    fun close() = provider.close()

    /** 구간이 담은 모든 문자열 (이름 · 속성 · 사건 속성 · 상태 설명). */
    private fun strings(span: SpanData): List<String> =
        listOf(span.name, span.status.description) +
            span.attributes.asMap().values.map { it.toString() } +
            span.events.flatMap { e -> listOf(e.name) + e.attributes.asMap().values.map { it.toString() } }

    @Test
    fun `예외 메시지 · 스택 · 상태 설명은 지우고 예외 종류만 남긴다`() {
        val span = tracer.spanBuilder("withdraw").startSpan()
        span.recordException(IllegalStateException("balance of 'user-7f3a' is less than 7777771 (account 1002123456789)"))
        span.setStatus(StatusCode.ERROR, "balance of 'user-7f3a' is less than 7777771")
        span.end()

        val exported = sink.finishedSpanItems.single()
        val event = exported.events.single()
        assertEquals("exception", event.name)
        assertEquals("java.lang.IllegalStateException", event.attributes.get(AttributeKey.stringKey("exception.type")))
        assertEquals(1, event.attributes.size())
        assertEquals(StatusCode.ERROR, exported.status.statusCode)
        assertEquals("", exported.status.description)
        listOf("user-7f3a", "7777771", "1002123456789").forEach { secret ->
            assertTrue(strings(exported).none { secret in it }, "지운 뒤에도 '$secret' 이 남았다")
        }
    }

    @Test
    fun `예외 없는 구간은 그대로 - 속성 · 일반 사건 · 이름 유지`() {
        val span = tracer.spanBuilder("bank.request").setAttribute("bank.outcome", "ok").startSpan()
        span.addEvent("sent", Attributes.of(AttributeKey.stringKey("note"), "n"))
        span.end()

        val exported = sink.finishedSpanItems.single()
        assertEquals("bank.request", exported.name)
        assertEquals("ok", exported.attributes.get(AttributeKey.stringKey("bank.outcome")))
        assertEquals("n", exported.events.single().attributes.get(AttributeKey.stringKey("note")))
        assertEquals(StatusCode.UNSET, exported.status.statusCode)
    }

    @Test
    fun `wrapAll 은 한 번만 감싼다`() {
        val once = ScrubbingSpanExporter.wrapAll(listOf(sink))
        assertIs<ScrubbingSpanExporter>(once.single())
        assertSame(once.single(), ScrubbingSpanExporter.wrapAll(once).single())
    }
}
