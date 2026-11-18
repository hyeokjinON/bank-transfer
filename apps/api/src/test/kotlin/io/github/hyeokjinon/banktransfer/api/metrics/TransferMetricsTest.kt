package io.github.hyeokjinon.banktransfer.api.metrics

import io.github.hyeokjinon.banktransfer.api.messaging.DltKind
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.transfer.BankTransferOutcome
import io.github.hyeokjinon.banktransfer.transfer.InquiryResolution
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals

/** api 사건 지표 (P9a 스펙 §4-1). 태그 조합은 처음부터 0, 미전송은 점검 창이면 maintenance. */
class TransferMetricsTest {
    private val registry = SimpleMeterRegistry()
    private val window = BankMaintenanceWindow(LocalTime.of(23, 50), LocalTime.of(0, 10), ZoneId.of("Asia/Seoul"))
    private val noonKst = Instant.parse("2026-11-17T03:00:00Z")
    private val inMaintenance = Instant.parse("2026-11-17T14:55:00Z")   // KST 23:55

    private fun metrics(now: Instant) = TransferMetrics(registry, window, Clock.fixed(now, ZoneOffset.UTC))
    private fun count(name: String, key: String, value: String) = registry.get(name).tag(key, value).counter().count()

    @Test
    fun `모든 태그 조합이 처음부터 0 - increase 가 첫 증가를 놓치지 않게`() {
        metrics(noonKst)

        assertEquals(2, registry.find(TransferMetrics.ACCEPTED).counters().size)
        assertEquals(5, registry.find(TransferMetrics.BANK_RESULT).counters().size)
        assertEquals(2, registry.find(TransferMetrics.TRANSFER_COMPLETED).timers().size)
        assertEquals(3, registry.find(TransferMetrics.DLT).counters().size)
        assertEquals(3, registry.find(TransferMetrics.RECONCILED).counters().size)
        assertEquals(2, registry.find(TransferMetrics.OUTBOX_PUBLISHED).counters().size)
        assertEquals(0.0, registry.get(TransferMetrics.GAVE_UP).counter().count())
        assertEquals(0.0, registry.get(TransferMetrics.REPUBLISHED).counter().count())
    }

    @Test
    fun `은행 결과 - 미전송은 점검 창이면 maintenance, 아니면 not_sent`() {
        metrics(noonKst).bankResult(BankTransferOutcome.NotSent)
        metrics(inMaintenance).bankResult(BankTransferOutcome.NotSent)
        metrics(noonKst).bankResult(BankTransferOutcome.Completed)
        metrics(noonKst).bankResult(BankTransferOutcome.Rejected("1001"))
        metrics(noonKst).bankResult(BankTransferOutcome.ResultUnknown)

        listOf("not_sent", "maintenance", "ok", "rejected", "unknown").forEach {
            assertEquals(1.0, count(TransferMetrics.BANK_RESULT, "outcome", it), it)
        }
    }

    @Test
    fun `확정 - 접수 → 확정 시간을 상태별 Timer 로, 접수 · 재요청 · DLT · 결과조회 · 아웃박스`() {
        val m = metrics(noonKst)
        m.finalized(TransferStatus.COMPLETED, noonKst.minusSeconds(3), noonKst)
        m.accepted(created = true)
        m.accepted(created = false)
        m.deadLettered(DltKind.BANK_NOT_CONNECTED)
        m.inquired(InquiryResolution.STILL_UNKNOWN)
        m.abandoned()
        m.republished()
        m.outboxPublished(3)
        m.outboxPublished(0)
        m.outboxFailed()

        val timer = registry.get(TransferMetrics.TRANSFER_COMPLETED).tag("status", "COMPLETED").timer()
        assertEquals(1, timer.count())
        assertEquals(Duration.ofSeconds(3).toNanos().toDouble(), timer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS))
        assertEquals(1.0, count(TransferMetrics.ACCEPTED, "result", "created"))
        assertEquals(1.0, count(TransferMetrics.ACCEPTED, "result", "replayed"))
        assertEquals(1.0, count(TransferMetrics.DLT, "kind", "bank_not_connected"))
        assertEquals(1.0, count(TransferMetrics.RECONCILED, "result", "still_unknown"))
        assertEquals(1.0, registry.get(TransferMetrics.GAVE_UP).counter().count())
        assertEquals(1.0, registry.get(TransferMetrics.REPUBLISHED).counter().count())
        assertEquals(3.0, count(TransferMetrics.OUTBOX_PUBLISHED, "result", "ok"))
        assertEquals(1.0, count(TransferMetrics.OUTBOX_PUBLISHED, "result", "error"))
    }

    @Test
    fun `점검 Gauge 는 KST 점검 창을 따른다`() {
        val noon = SimpleMeterRegistry().also { TransferMetrics(it, window, Clock.fixed(noonKst, ZoneOffset.UTC)) }
        val night = SimpleMeterRegistry().also { TransferMetrics(it, window, Clock.fixed(inMaintenance, ZoneOffset.UTC)) }

        assertEquals(0.0, noon.get(TransferMetrics.BANK_MAINTENANCE).gauge().value())
        assertEquals(1.0, night.get(TransferMetrics.BANK_MAINTENANCE).gauge().value())
    }

    /** 관찰자 계약 (TransferObserver KDoc): 예외를 던지지 않는다 — 돈 처리 흐름을 바꾸지 않는다. */
    @Test
    fun `예상 밖 입력에도 예외를 던지지 않는다 - 계약`() {
        val brokenClock = object : Clock() {
            override fun getZone() = ZoneOffset.UTC
            override fun withZone(zone: ZoneId) = this
            override fun instant(): Instant = error("clock down")
        }
        val m = TransferMetrics(SimpleMeterRegistry(), window, brokenClock)

        m.bankResult(BankTransferOutcome.NotSent)                                 // 시계 예외
        m.bankResult(BankTransferOutcome.Rejected(""))
        m.finalized(TransferStatus.RECEIVED, noonKst, noonKst)                    // 미리 만들지 않은 상태
        m.finalized(TransferStatus.UNKNOWN, noonKst, noonKst)
        m.finalized(TransferStatus.COMPLETED, noonKst.plusSeconds(10), noonKst)   // 시각 역전
        m.finalized(TransferStatus.FAILED, Instant.MIN, Instant.MAX)              // 간격 계산 넘침
        m.outboxPublished(-5)
        m.outboxPublished(Int.MAX_VALUE)
        m.accepted(created = true)
        m.deadLettered(DltKind.OTHER)
        m.inquired(InquiryResolution.COMPLETED)
        m.republished()
        m.abandoned()
        m.outboxFailed()
    }
}
