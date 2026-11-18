package io.github.hyeokjinon.banktransfer.tracing

import io.micrometer.tracing.exporter.FinishedSpan
import io.micrometer.tracing.exporter.SpanExportingPredicate

/**
 * 할 일이 없었던 정리 작업 실행의 root 구간은 내보내지 않는다 (P9b). 정리 작업은 Pod 마다 10초 주기라 빈 실행이 하루 만 단위로 쌓인다.
 * root `transfer.reconcile` 이면서 `reconcile.*` 건수와 `reconcile.items`(이 실행이 연 건별 자식 구간 수)가 모두 0 인 것만 지운다.
 * items 를 같이 보는 까닭: 건수가 모두 0 이어도 자식이 있을 수 있다 (은행 끊김 → 결과조회 Unavailable, 다른 Pod 에 전이를 뺏김) — 그 root 를 지우면 자식만 고아로 남는다.
 * root 판정은 parentId 가 비었거나 모두 0 (OTel 브리지는 부모 없는 구간의 parentId 를 "0000000000000000" 으로 준다).
 * 건수 속성이 없거나 읽을 수 없으면 내보낸다 (의심스러우면 남긴다).
 */
class EmptyReconcileSpans : SpanExportingPredicate {
    override fun isExportable(span: FinishedSpan): Boolean {
        if (span.name != ROOT || !isRoot(span.parentId)) return true
        val counts = COUNT_KEYS.map { span.tags[it]?.toLongOrNull() ?: return true }
        return counts.any { it != 0L }
    }

    private fun isRoot(parentId: String?): Boolean = parentId.isNullOrEmpty() || parentId.all { it == '0' }

    companion object {
        const val ROOT = "transfer.reconcile"
        private val COUNT_KEYS = listOf(
            "republished", "abandoned", "marked_unknown", "completed", "failed", "still_unknown", "gave_up", "errors", "items",
        ).map { "reconcile.$it" }
    }
}
