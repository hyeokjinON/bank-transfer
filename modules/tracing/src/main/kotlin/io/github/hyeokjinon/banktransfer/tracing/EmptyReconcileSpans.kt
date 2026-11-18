package io.github.hyeokjinon.banktransfer.tracing

import io.micrometer.tracing.exporter.FinishedSpan
import io.micrometer.tracing.exporter.SpanExportingPredicate

/**
 * 할 일이 없었던 정리 작업 실행의 root 구간은 내보내지 않는다 (P9b). 정리 작업은 Pod 마다 10초 주기라 빈 실행이 하루 만 단위로 쌓인다.
 * root `transfer.reconcile` 이면서 `reconcile.*` 건수가 모두 0 인 것만 — 건이 하나라도 있으면 still_unknown · errors 등 어느 하나가 0 이 아니고,
 * 건별 자식 구간이 있는 실행은 항상 그렇다. 빈 실행에는 자식이 없어 root 하나만 지우면 된다. 건수 속성이 없거나 읽을 수 없으면 내보낸다 (의심스러우면 남긴다).
 */
class EmptyReconcileSpans : SpanExportingPredicate {
    override fun isExportable(span: FinishedSpan): Boolean {
        if (span.name != ROOT || !span.parentId.isNullOrEmpty()) return true
        val counts = COUNT_KEYS.map { span.tags[it]?.toLongOrNull() ?: return true }
        return counts.any { it != 0L }
    }

    companion object {
        const val ROOT = "transfer.reconcile"
        private val COUNT_KEYS = listOf(
            "republished", "abandoned", "marked_unknown", "completed", "failed", "still_unknown", "gave_up", "errors",
        ).map { "reconcile.$it" }
    }
}
