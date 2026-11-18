package io.github.hyeokjinon.banktransfer.api.tracing

import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator

/**
 * 아웃박스를 건너는 추적 문맥 (P9b 스펙 §4-1, 결정 E6 · E10). "DB 에 쓰고 0.5초 뒤 다른 스레드가 꺼낸다" 는 틈을 프레임워크는 모른다.
 * - 쓰기: 접수(saveWithEvent) · 재발행(republish)이 행을 만들 때 [currentTraceParent] 를 outbox_event.trace_parent 에 넣는다.
 * - 읽기: 발행기가 행마다 [publishing] — 저장값을 부모로 `outbox.publish` 구간을 열고 그 범위 안에서 kafka.send → Kafka producer 구간 + traceparent 헤더.
 *   행마다 열고 닫는다 — 문맥이 다음 행으로 새지 않는다 (위험 C1).
 * - 저장값이 NULL · 형식이 틀리면 부모 없이 새 trace (예외 없음, E10). 전파가 꺼져 있으면(Propagator.NOOP) 쓰는 값도 null.
 * - 속성: transfer.id · outbox.event_id 만 (E9).
 */
class OutboxTracing(private val tracer: Tracer, private val propagator: Propagator) {

    /** 지금 구간의 W3C traceparent. 구간이 없거나 전파가 꺼져 있으면 null. 샘플링 0 이면 끝 깃발이 00 (전파는 계속된다). */
    fun currentTraceParent(): String? {
        val context = tracer.currentTraceContext().context() ?: return null
        val carrier = HashMap<String, String>()
        propagator.inject(context, carrier) { c, key, value -> c?.put(key, value) }
        return carrier[TRACEPARENT]?.takeIf(::isValid)
    }

    /** [traceParent] 를 부모로 `outbox.publish` 를 열고 그 범위 안에서 [send]. [send] 의 예외는 구간에 남기고 그대로 던진다. */
    fun <T> publishing(traceParent: String?, transferId: String, eventId: Long, send: () -> T): T {
        val builder = if (isValid(traceParent)) propagator.extract(traceParent!!, GETTER) else tracer.spanBuilder().setNoParent()
        val span = builder.name(SPAN_NAME)
            .tag(TraceKeys.TRANSFER_ID, transferId)
            .tag(TraceKeys.OUTBOX_EVENT_ID, eventId.toString())
            .start()
        return tracer.inSpan(span, send)
    }

    companion object {
        const val TRACEPARENT = "traceparent"
        const val SPAN_NAME = "outbox.publish"

        /** W3C trace-context 버전 00: 소문자 hex, trace id · span id 가 모두 0 이면 무효. */
        private val W3C = Regex("^00-(?!0{32})[0-9a-f]{32}-(?!0{16})[0-9a-f]{16}-[0-9a-f]{2}$")

        /** 운반체 = 저장된 traceparent 문자열 하나. 다른 이름(tracestate · b3 …)은 없다. */
        private val GETTER = Propagator.Getter<String> { carrier, key -> if (key == TRACEPARENT) carrier else null }

        fun isValid(traceParent: String?): Boolean = traceParent != null && W3C.matches(traceParent)

        val NOOP = OutboxTracing(Tracer.NOOP, Propagator.NOOP)
    }
}
