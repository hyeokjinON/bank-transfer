package io.github.hyeokjinon.banktransfer.tracing

import io.micrometer.tracing.Span
import io.micrometer.tracing.exporter.FinishedSpan
import io.micrometer.tracing.test.simple.SimpleSpan
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** actuator 요청 구간 거름 (P9b) — 헬스 프로브 · Prometheus 수집이 5초마다 trace 를 만들지 않게. 지표(P9a)는 그대로다. */
class ActuatorSpansTest {
    private fun span(kind: Span.Kind, vararg tags: Pair<String, String>): FinishedSpan {
        val base = SimpleSpan().apply { tags.forEach { (k, v) -> tag(k, v) } }
        return object : FinishedSpan by base {
            override fun getKind(): Span.Kind = kind
        }
    }

    private val filter = ActuatorSpans()

    @Test
    fun `actuator SERVER 구간은 내보내지 않는다`() {
        assertFalse(filter.isExportable(span(Span.Kind.SERVER, "uri" to "/actuator/health/{*path}")))
        assertFalse(filter.isExportable(span(Span.Kind.SERVER, "uri" to "/actuator/prometheus")))
        assertFalse(filter.isExportable(span(Span.Kind.SERVER, "uri" to "UNKNOWN", "http.url" to "/actuator/prometheus")))
    }

    @Test
    fun `업무 요청 · 서버가 아닌 구간은 내보낸다`() {
        assertTrue(filter.isExportable(span(Span.Kind.SERVER, "uri" to "/api/v1/transfers")))
        assertTrue(filter.isExportable(span(Span.Kind.CLIENT, "uri" to "/actuator/health")))
        assertTrue(filter.isExportable(span(Span.Kind.CONSUMER)))
    }
}
