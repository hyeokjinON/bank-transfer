package io.github.hyeokjinon.banktransfer.api.support

import io.github.hyeokjinon.banktransfer.tracing.ScrubbingSpanExporter
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SpanProcessor
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean

/**
 * 공유 컨텍스트가 끝낸 구간을 메모리에 모은다 (P9b). 공유 컨텍스트라 다른 테스트의 구간도 섞인다 — 추적 테스트는 [clear] 로 시작하고 속성 · traceId 로 거른다.
 * InMemorySpanExporter 를 SpanExporter 빈으로 올리지 않는다 — 올리면 Boot 의 5초 묶음 처리기에도 들어가 같은 구간이 두 번 온다.
 */
class TestSpans {
    val exporter: InMemorySpanExporter = InMemorySpanExporter.create()

    /** 지우개 **앞** 의 구간 (같은 구간, 지우기 전). 금지 속성 테스트가 "지울 것이 실제로 있었다" 를 증명하는 데만 쓴다. */
    val rawExporter: InMemorySpanExporter = InMemorySpanExporter.create()

    fun all(): List<SpanData> = exporter.finishedSpanItems

    fun raw(): List<SpanData> = rawExporter.finishedSpanItems

    fun ofTrace(traceId: String): List<SpanData> = all().filter { it.traceId == traceId }

    fun clear() {
        exporter.reset()
        rawExporter.reset()
    }
}

fun SpanData.attr(key: String): String? = attributes.get(AttributeKey.stringKey(key))

fun SpanData.service(): String? = resource.getAttribute(AttributeKey.stringKey("service.name"))

@TestConfiguration(proxyBeanMethods = false)
class TracingTestConfiguration {
    @Bean
    fun testSpans() = TestSpans()

    /** 끝나는 즉시 같은 스레드에서 넘긴다 (5초 묶음 없이). 운영과 같은 지우개를 지난다 — 금지 속성 테스트(E9)가 지운 뒤 모양을 본다. */
    @Bean
    fun testSpanProcessor(spans: TestSpans): SpanProcessor = SimpleSpanProcessor.create(ScrubbingSpanExporter(spans.exporter))

    /** 지우개를 거치지 않는 두 번째 줄 — 지우기 전 모양을 [TestSpans.raw] 로 본다 (테스트 전용). */
    @Bean
    fun testRawSpanProcessor(spans: TestSpans): SpanProcessor = SimpleSpanProcessor.create(spans.rawExporter)
}
