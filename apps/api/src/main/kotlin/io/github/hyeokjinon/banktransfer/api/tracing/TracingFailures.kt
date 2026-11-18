package io.github.hyeokjinon.banktransfer.api.tracing

import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 추적 호출이 던졌을 때의 기록 한 곳 (P9b, E10). 돈 흐름 위의 추적 호출은 모두 runCatching 으로 막고 여기로 보고한다:
 * 첫 실패만 WARN (예외 클래스 · 메시지 한 줄 — 스택 없음, 업무 값은 싣지 않는다), 이후는 DEBUG (스택 포함).
 * 추적이 api 에서 통째로 고장 나도 로그에 흔적이 남게 한다.
 */
internal object TracingFailures {
    private val log = LoggerFactory.getLogger(TracingFailures::class.java)
    private val warned = AtomicBoolean(false)

    fun report(where: String, e: Throwable) {
        runCatching {
            if (warned.compareAndSet(false, true)) {
                log.warn("추적 호출 실패 — 추적 없이 계속한다 (이후 실패는 DEBUG): where={} error={}: {}", where, e.javaClass.name, e.message)
            } else {
                log.debug("추적 호출 실패 where={}", where, e)
            }
        }
    }

    /** 테스트 전용: 첫 실패 상태를 되돌린다. */
    internal fun resetForTest() = warned.set(false)
}
