package io.github.hyeokjinon.banktransfer.transfer

/**
 * 정리 작업의 건별 범위 (P9b 추적, 스펙 §4-2). 도메인은 추적 라이브러리를 모른다 — apps/api 의 ReconcileTracing 이 구현해
 * 결과조회 · 재발행 한 건을 transfer.id 가 붙은 구간으로 감싼다 (재발행이 쓰는 아웃박스 행의 trace_parent 도 그 구간이 된다).
 * 구현은 [block] 의 결과 · 예외를 그대로 돌려준다 — 정리 작업의 예외 격리 · 결과 분류는 바뀌지 않는다. 기본은 그냥 실행.
 */
interface ReconcileItemScope {
    fun <T> inquiry(transfer: Transfer, block: () -> T): T = block()

    fun <T> republish(transfer: Transfer, block: () -> T): T = block()

    companion object {
        val NONE: ReconcileItemScope = object : ReconcileItemScope {}
    }
}
