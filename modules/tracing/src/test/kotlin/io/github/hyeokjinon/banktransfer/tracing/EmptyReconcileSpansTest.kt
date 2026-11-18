package io.github.hyeokjinon.banktransfer.tracing

import io.micrometer.tracing.exporter.FinishedSpan
import io.micrometer.tracing.otel.bridge.OtelFinishedSpan
import io.micrometer.tracing.test.simple.SimpleSpan
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 빈 정리 작업 root 는 내보내지 않는다 (P9b m4) — 건이 있는 실행 · 다른 구간 · 속성이 이상한 것은 남긴다. */
class EmptyReconcileSpansTest {
    private val keys = listOf("republished", "abandoned", "marked_unknown", "completed", "failed", "still_unknown", "gave_up", "errors", "items")

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
    fun `OTel 의 root 표기(parentId 모두 0)도 root 로 본다`() {
        assertFalse(filter.isExportable(span("transfer.reconcile", "0000000000000000", *counts())))
    }

    @Test
    fun `건수가 모두 0 이어도 건별 구간을 열었으면 내보낸다 - 은행 끊김 결과조회`() {
        assertTrue(filter.isExportable(span("transfer.reconcile", "0000000000000000", *counts("items" to 3))))
    }

    @Test
    fun `실제 OTel 브리지가 만든 구간 - 빈 root 는 거르고 자식이 있던 root 는 남긴다`() {
        val exporter = InMemorySpanExporter.create()
        val tracer = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build().get("test")
        fun root(items: Long) = tracer.spanBuilder("transfer.reconcile").setNoParent().startSpan().apply {
            keys.forEach { setAttribute("reconcile.$it", if (it == "items") items else 0L) }
        }.end()
        root(0)
        root(1)
        val (empty, withItem) = exporter.finishedSpanItems.map { OtelFinishedSpan.fromOtel(it) }
        assertFalse(filter.isExportable(empty), "parentId=${empty.parentId} tags=${empty.tags}")
        assertTrue(filter.isExportable(withItem))
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
