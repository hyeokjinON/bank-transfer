package io.github.hyeokjinon.banktransfer.tracing

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.data.DelegatingSpanData
import io.opentelemetry.sdk.trace.data.EventData
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.data.StatusData
import io.opentelemetry.sdk.trace.export.SpanExporter

/**
 * 내보내기 앞 지우개 (P9b 결정 E9). 구간에 예외가 남을 때 브리지는 메시지 · 스택(`exception` 사건)과 상태 설명(= 메시지)을 같이 남긴다.
 * 도메인 예외 메시지에는 사용자 id · 금액이(예 `StoreConflict.InsufficientBalance`), 입력 검증 메시지에는 거절된 값(계좌번호 등)이 들어갈 수 있다 —
 * 보내기 전에 `exception.type` 만 남기고 상태 설명은 비운다. 속성 이름 · 값 자체는 코드가 붙이는 것이라 건드리지 않는다 (붙이는 쪽 규칙: 구간 표).
 * 상태 코드(ERROR)는 그대로 — "실패한 구간" 은 보인다.
 */
class ScrubbingSpanExporter(private val delegate: SpanExporter) : SpanExporter {

    override fun export(spans: Collection<SpanData>): CompletableResultCode = delegate.export(spans.map(::scrub))

    override fun flush(): CompletableResultCode = delegate.flush()

    override fun shutdown(): CompletableResultCode = delegate.shutdown()

    private class Scrubbed(span: SpanData) : DelegatingSpanData(span) {
        private val events: List<EventData> = span.events.map { e ->
            if (e.name != EXCEPTION_EVENT) e
            else EventData.create(e.epochNanos, e.name, Attributes.of(EXCEPTION_TYPE, e.attributes.get(EXCEPTION_TYPE) ?: "unknown"))
        }
        private val status: StatusData = StatusData.create(span.status.statusCode, "")

        override fun getEvents(): List<EventData> = events

        override fun getStatus(): StatusData = status
    }

    companion object {
        private const val EXCEPTION_EVENT = "exception"
        private val EXCEPTION_TYPE: AttributeKey<String> = AttributeKey.stringKey("exception.type")

        /** 지울 것이 없으면 그대로 돌려준다 (대부분의 구간 — 복사 비용을 치르지 않는다). */
        fun scrub(span: SpanData): SpanData =
            if (span.status.description.isEmpty() && span.events.none { it.name == EXCEPTION_EVENT }) span else Scrubbed(span)

        /** Boot 가 만든 내보내기(OTLP 등)를 모두 감싼다. 이미 감싼 것은 다시 감싸지 않는다. */
        fun wrapAll(exporters: List<SpanExporter>): List<SpanExporter> =
            exporters.map { if (it is ScrubbingSpanExporter) it else ScrubbingSpanExporter(it) }
    }
}
