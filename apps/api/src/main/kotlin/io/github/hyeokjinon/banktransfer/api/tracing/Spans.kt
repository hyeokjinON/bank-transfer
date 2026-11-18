package io.github.hyeokjinon.banktransfer.api.tracing

import io.micrometer.tracing.Span
import io.micrometer.tracing.Tracer

/**
 * [span] 을 지금 스레드의 구간으로 두고 [block] 을 돈다 (P9b). 동기 예외는 구간에 남기고 그대로 던지며 (비동기 완료 실패는 이 구간이 아니라 해당 클라이언트 구간에 남는다), 끝나면 범위를 닫고 구간을 끝낸다.
 * 범위가 새지 않게(위험 C1) 구간을 여는 곳은 이 함수 하나를 쓴다.
 */
internal inline fun <T> Tracer.inSpan(span: Span, block: () -> T): T {
    try {
        return withSpan(span).use { block() }
    } catch (e: Throwable) {
        span.error(e)
        throw e
    } finally {
        span.end()
    }
}
