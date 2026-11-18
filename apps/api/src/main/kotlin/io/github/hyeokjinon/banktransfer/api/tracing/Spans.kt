package io.github.hyeokjinon.banktransfer.api.tracing

import io.micrometer.tracing.Span
import io.micrometer.tracing.Tracer

/**
 * [span] 을 지금 스레드의 구간으로 두고 [block] 을 돈다 (P9b). 동기 예외는 구간에 남기고 그대로 던지며 (비동기 완료 실패는 이 구간이 아니라 해당 클라이언트 구간에 남는다), 끝나면 범위를 닫고 구간을 끝낸다.
 * 범위가 새지 않게(위험 C1) 구간을 여는 곳은 이 함수 하나를 쓴다.
 */
internal inline fun <T> Tracer.inSpan(span: Span, block: () -> T): T {
    // E10: 추적 호출(범위 열기 · 닫기 · 오류 · 끝)이 던져도 block 은 정확히 한 번, 그 결과 · 예외는 그대로. 실패는 첫 번만 WARN
    val scope = runCatching { withSpan(span) }.onFailure { TracingFailures.report("inSpan.open", it) }.getOrNull()
    try {
        try {
            return block()
        } finally {
            runCatching { scope?.close() }.onFailure { TracingFailures.report("inSpan.close", it) }
        }
    } catch (e: Throwable) {
        runCatching { span.error(e) }.onFailure { TracingFailures.report("inSpan.error", it) }
        throw e
    } finally {
        runCatching { span.end() }.onFailure { TracingFailures.report("inSpan.end", it) }
    }
}
