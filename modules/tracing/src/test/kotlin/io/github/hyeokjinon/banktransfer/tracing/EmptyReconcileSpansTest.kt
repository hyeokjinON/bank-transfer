package io.github.hyeokjinon.banktransfer.tracing

import io.micrometer.tracing.exporter.FinishedSpan
import io.micrometer.tracing.test.simple.SimpleSpan
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 빈 정리 작업 root 는 내보내지 않는다 (P9b m4) — 건이 있는 실행 · 다른 구간 · 속성이 이상한 것은 남긴다. */
class EmptyReconcileSpansTest {
    private val keys = listOf("republished", "abandoned", "marked_unknown", "completed", "failed", "still_unknown", "gave_up", "errors")

    private fun span(name: String, parent: String = "", vararg tags: Pair<String, String>): FinishedSpan {
        val base = SimpleSpan().apply { name(name); tags.forEach { (k, v) -> tag(k, v) } }
        return object : FinishedSpan by base {
            override fun getParentId(): String = parent
        }
    }

    private fun counts(vararg nonZero: Pair<String, Long>): Array<Pair<String, String>> {
        val m = nonZero.toMap()
        return keys.map { "reconcile.$it" to (m[it] ?: 0L).toString() }.toTypedArray()
    }

    private val filter = EmptyReconcileSpans()

    @Test
    fun `건수가 모두 0 인 root 는 내보내지 않는다`() {
        assertFalse(filter.isExportable(span("transfer.reconcile", "", *counts())))
    }

    @Test
    fun `건수가 하나라도 0 이 아니면 내보낸다`() {
        assertTrue(filter.isExportable(span("transfer.reconcile", "", *counts("still_unknown" to 1))))
        assertTrue(filter.isExportable(span("transfer.reconcile", "", *counts("errors" to 2))))
        assertTrue(filter.isExportable(span("transfer.reconcile", "", *counts("republished" to 1))))
    }

    @Test
    fun `이름이 다르거나 root 가 아니거나 건수 속성이 없으면 내보낸다`() {
        assertTrue(filter.isExportable(span("transfer.inquiry", "", *counts())))
        assertTrue(filter.isExportable(span("transfer.reconcile", "abc123", *counts())))
        assertTrue(filter.isExportable(span("transfer.reconcile", "")))
        assertTrue(filter.isExportable(span("transfer.reconcile", "", "reconcile.republished" to "x")))
    }
}
