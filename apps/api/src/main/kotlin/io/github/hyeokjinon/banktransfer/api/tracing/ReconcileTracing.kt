package io.github.hyeokjinon.banktransfer.api.tracing

import io.github.hyeokjinon.banktransfer.transfer.ReconcileItemScope
import io.github.hyeokjinon.banktransfer.transfer.ReconcileResult
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.micrometer.tracing.Span
import io.micrometer.tracing.Tracer

/**
 * 정리 작업 추적 (P9b 스펙 §4-2, 결정 E7 · E10). 실행 1회 = 새 root `transfer.reconcile` — 처음 trace 에 이어 붙이지 않는다 (수 분짜리 trace 는 보기 · 샘플링이 어렵다).
 * 결과조회 · 재발행 한 건 = 자식 구간(transfer.id · transfer.reference). 재발행이 쓰는 아웃박스 행의 trace_parent 는 그 건 구간이라
 * 재발행 뒤 Kafka → 게이트웨이 구간이 이 실행 trace 에 이어진다. @Scheduled 관측은 꺼 두었다 (application.yml) — root 는 여기서만 연다.
 * 구간을 여는 · 태그를 다는 일이 실패해도 업무는 구간 없이 그대로 돈다 (E10) — [run] · [block] 의 예외는 건드리지 않고 그대로 던진다.
 * reconcile.* 건수는 숫자(long) 속성이다 — TraceQL 로 `span.reconcile.republished > 0` 비교를 하려는 의도된 예외 (문자열 규칙의 예외).
 */
class ReconcileTracing(private val tracer: Tracer) : ReconcileItemScope {

    fun reconcile(run: () -> ReconcileResult): ReconcileResult {
        val span = runCatching { tracer.spanBuilder().setNoParent().name(ROOT).start() }
            .onFailure { TracingFailures.report("reconcile.open", it) }.getOrNull() ?: return run()
        return tracer.inSpan(span) {
            val result = run()
            runCatching {
                span.tag("reconcile.republished", result.republished.toLong())
                    .tag("reconcile.abandoned", result.abandoned.toLong())
                    .tag("reconcile.marked_unknown", result.markedUnknown.toLong())
                    .tag("reconcile.completed", result.completed.toLong())
                    .tag("reconcile.failed", result.failed.toLong())
                    .tag("reconcile.still_unknown", result.stillUnknown.toLong())
                    .tag("reconcile.gave_up", result.gaveUp.toLong())
                    .tag("reconcile.errors", result.errors.toLong())
            }.onFailure { TracingFailures.report("reconcile.tag", it) }
            result
        }
    }

    override fun <T> inquiry(transfer: Transfer, block: () -> T): T = item(INQUIRY, transfer, block)

    override fun <T> republish(transfer: Transfer, block: () -> T): T = item(REPUBLISH, transfer, block)

    private fun <T> item(name: String, transfer: Transfer, block: () -> T): T {
        val span: Span = runCatching {
            tracer.nextSpan().name(name)
                .tag(TraceKeys.TRANSFER_ID, transfer.id.toString())
                .tag(TraceKeys.TRANSFER_REFERENCE, transfer.reference)
                .start()
        }.onFailure { TracingFailures.report("reconcile.item", it) }.getOrNull() ?: return block()
        return tracer.inSpan(span, block)
    }

    companion object {
        const val ROOT = "transfer.reconcile"
        const val INQUIRY = "transfer.inquiry"
        const val REPUBLISH = "transfer.republish"
        val NOOP = ReconcileTracing(Tracer.NOOP)
    }
}
