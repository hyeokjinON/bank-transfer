package io.github.hyeokjinon.banktransfer.tracing

import io.micrometer.tracing.Span
import io.micrometer.tracing.exporter.FinishedSpan
import io.micrometer.tracing.exporter.SpanExportingPredicate

/**
 * actuator 요청 구간은 내보내지 않는다 (P9b). kubelet 프로브(Pod 마다 수 초) · Prometheus 수집(5초 × Pod 4)이 업무 trace 를 덮지 않게.
 * 지표가 아니라 내보내기만 거른다 — P9a 의 http_server_requests 는 그대로다. Boot 의 묶음 처리기(BatchSpanProcessor)가 이 빈을 쓴다.
 */
class ActuatorSpans : SpanExportingPredicate {
    override fun isExportable(span: FinishedSpan): Boolean {
        if (span.kind != Span.Kind.SERVER) return true
        val uri = span.tags["uri"].orEmpty()
        val url = span.tags["http.url"].orEmpty()
        return !(uri.startsWith(ACTUATOR) || url.contains(ACTUATOR))
    }

    private companion object {
        const val ACTUATOR = "/actuator"
    }
}
