package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.metrics.TransferMetrics
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Clock
import java.time.LocalTime
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals

/** 발행기 지표 (P9a): publishPending 이 돌아온 뒤(커밋 뒤) 발행 건수를, 실패하면 실패한 주기를 센다. */
class OutboxRelaySchedulerTest {
    private val registry = SimpleMeterRegistry()
    private val metrics = TransferMetrics(registry, BankMaintenanceWindow(LocalTime.of(23, 50), LocalTime.of(0, 10), ZoneId.of("Asia/Seoul")), Clock.systemUTC())
    private fun count(result: String) = registry.get(TransferMetrics.OUTBOX_PUBLISHED).tag("result", result).counter().count()

    @Test
    fun `발행 건수는 ok, 브로커 장애 주기는 error - 예외는 밖으로 나가지 않는다`() {
        OutboxRelayScheduler({ 3 }, metrics).run()
        OutboxRelayScheduler({ error("broker down") }, metrics).run()

        assertEquals(3.0, count("ok"))
        assertEquals(1.0, count("error"))
    }
}
