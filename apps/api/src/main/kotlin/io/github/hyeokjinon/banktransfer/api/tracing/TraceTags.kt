package io.github.hyeokjinon.banktransfer.api.tracing

import io.micrometer.tracing.Tracer
import java.util.UUID

/**
 * 지금 구간에 id 를 붙인다 (P9b 스펙 §5) — 자동 구간(MVC 서버 · Kafka 소비)이 모르는 업무 id. Tempo 에서 `transfer.id` 한 번 검색으로 찾게 (성공 기준 3).
 * 구간이 없으면 아무것도 하지 않는다 — 관찰은 돈 흐름을 바꾸지 않는다 (E10).
 */
class TraceTags(private val tracer: Tracer) {

    fun transferId(id: UUID) {
        tracer.currentSpan()?.tag(TraceKeys.TRANSFER_ID, id.toString())
    }

    companion object {
        val NOOP = TraceTags(Tracer.NOOP)
    }
}
