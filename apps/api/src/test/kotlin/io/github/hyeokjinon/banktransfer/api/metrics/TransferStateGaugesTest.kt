package io.github.hyeokjinon.banktransfer.api.metrics

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 상태 Gauge (P9a 스펙 §4-2): 조회 전 · 실패는 NaN (0 이 아니다 — 위험 A6), 이전 값을 들고 있지 않는다. */
class TransferStateGaugesTest {
    private val registry = SimpleMeterRegistry()
    private val snapshot = TransferStateSnapshot(
        open = mapOf(RECEIVED to 3L, SENDING to 1L, UNKNOWN to 2L),
        oldestSeconds = mapOf(RECEIVED to 12.5, SENDING to 0.5, UNKNOWN to 40.0),
        unknownExhausted = 1, outboxUnpublished = 7, outboxOldestSeconds = 1.5,
    )
    private var read: () -> TransferStateSnapshot = { snapshot }
    private var nanos = 0L
    private val gauges = TransferStateGauges(registry, staleAfter = Duration.ofSeconds(30), nanoTime = { nanos }) { read() }

    private fun open(status: String) = registry.get(TransferStateGauges.OPEN).tag("status", status).gauge().value()
    private fun oldest(status: String) = registry.get(TransferStateGauges.OPEN_OLDEST).tag("status", status).gauge().value()
    private fun gauge(name: String) = registry.get(name).gauge().value()
    private fun queries(result: String) = registry.get(TransferStateGauges.DB_QUERY).tag("result", result).timer().count()

    @Test
    fun `첫 조회 전에는 NaN - 0 이면 "막힌 건 없음" 으로 읽힌다`() {
        assertTrue(open("RECEIVED").isNaN())
        assertTrue(gauge(TransferStateGauges.OUTBOX_UNPUBLISHED).isNaN())
    }

    @Test
    fun `조회하면 상태별 값, 조회 시간은 ok`() {
        gauges.refresh()

        assertEquals(3.0, open("RECEIVED"))
        assertEquals(40.0, oldest("UNKNOWN"))
        assertEquals(1.0, gauge(TransferStateGauges.UNKNOWN_EXHAUSTED))
        assertEquals(7.0, gauge(TransferStateGauges.OUTBOX_UNPUBLISHED))
        assertEquals(1.5, gauge(TransferStateGauges.OUTBOX_OLDEST))
        assertEquals(1, queries("ok"))
    }

    @Test
    fun `조회가 실패하면 모든 값이 NaN 이고 error 를 센다 - 다시 성공하면 돌아온다`() {
        gauges.refresh()
        read = { error("db down") }
        gauges.refresh()

        assertTrue(open("SENDING").isNaN())
        assertTrue(oldest("RECEIVED").isNaN())
        assertTrue(gauge(TransferStateGauges.UNKNOWN_EXHAUSTED).isNaN())
        assertEquals(1, queries("error"))

        read = { snapshot }
        gauges.refresh()
        assertEquals(1.0, open("SENDING"))
    }

    @Test
    fun `조회가 끝나지 않으면 마지막 값을 들고 있지 않고 3 주기 뒤 NaN - 멈춘 쿼리 · 서지 않는 스케줄`() {
        gauges.refresh()
        assertEquals(3.0, open("RECEIVED"))

        val hang = CountDownLatch(1)
        read = { hang.await(); snapshot }
        val stuck = thread(isDaemon = true) { gauges.refresh() }   // 끝나지 않는 조회
        try {
            nanos = Duration.ofSeconds(30).toNanos()
            assertEquals(3.0, open("RECEIVED"), "30초 이내는 아직 신선")
            nanos = Duration.ofSeconds(30).toNanos() + 1
            assertTrue(open("RECEIVED").isNaN())
            assertTrue(oldest("UNKNOWN").isNaN())
            assertTrue(gauge(TransferStateGauges.OUTBOX_OLDEST).isNaN())
        } finally {
            hang.countDown()
            stuck.join(5_000)
        }
    }
}
