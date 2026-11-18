package io.github.hyeokjinon.banktransfer.api.tracing

import io.micrometer.tracing.Tracer
import java.util.UUID

/**
 * 지금 구간에 id 를 붙인다 (P9b 스펙 §5) — 자동 구간(MVC 서버 · Kafka 소비)이 모르는 업무 id. Tempo 에서 `transfer.id` 한 번 검색으로 찾게 (성공 기준 3).
 * 구간이 없으면 아무것도 하지 않는다 — 관찰은 돈 흐름을 바꾸지 않는다 (E10).
 */
class TraceTags(private val tracer: Tracer) {

    fun transferId(id: UUID) {
        runCatching { tracer.currentSpan()?.tag(TraceKeys.TRANSFER_ID, id.toString()) }   // 돈 흐름으로 던지지 않는다 (E10)
            .onFailure { TracingFailures.report("tags.transferId", it) }
    }

    fun verificationId(id: UUID) {
        runCatching { tracer.currentSpan()?.tag(TraceKeys.VERIFICATION_ID, id.toString()) }   // E10
            .onFailure { TracingFailures.report("tags.verificationId", it) }
    }

    /** 비동기 응답용 — 완료 콜백은 다른 스레드라 currentSpan 이 없다. 요청 스레드에서 지금 구간을 잡아 둔다 (서버 구간은 비동기 완료 때까지 열려 있다). */
    fun verificationTagger(): (UUID) -> Unit {
        val span = runCatching { tracer.currentSpan() }.onFailure { TracingFailures.report("tags.currentSpan", it) }.getOrNull()
        return { id -> runCatching { span?.tag(TraceKeys.VERIFICATION_ID, id.toString()) }.onFailure { TracingFailures.report("tags.verificationTagger", it) } }   // E10
    }

    companion object {
        val NOOP = TraceTags(Tracer.NOOP)
    }
}
