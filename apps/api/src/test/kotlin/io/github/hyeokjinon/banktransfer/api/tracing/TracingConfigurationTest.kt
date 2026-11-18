package io.github.hyeokjinon.banktransfer.api.tracing

import io.github.hyeokjinon.banktransfer.tracing.ScrubbingSpanExporter
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.export.SpanExporter
import org.springframework.beans.factory.support.StaticListableBeanFactory
import kotlin.test.Test
import kotlin.test.assertIs

/** Boot 의 spanExporters(@ConditionalOnMissingBean)를 대신하는 빈 — 모든 내보내기가 지우개를 지난다 (E9). */
class TracingConfigurationTest {
    @Test
    fun `모든 내보내기를 지우개로 감싼다`() {
        val factory = StaticListableBeanFactory().apply { addBean("otlp", InMemorySpanExporter.create()) }
        val exporters = TracingConfiguration().spanExporters(factory.getBeanProvider(SpanExporter::class.java))
        assertIs<ScrubbingSpanExporter>(exporters.list().single())
    }
}
