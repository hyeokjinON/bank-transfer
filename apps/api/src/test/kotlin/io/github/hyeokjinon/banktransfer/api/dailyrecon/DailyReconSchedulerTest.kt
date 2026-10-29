package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals

/** 매시 회차 (P5-1): 시계는 UTC, 날짜 · 마지막 회차 판단은 KST. 실행기는 함수로 바꿔 끼운다. */
class DailyReconSchedulerTest {

    private val calls = mutableListOf<Pair<LocalDate, ReconTrigger>>()
    private val alerts = mutableListOf<Alert>()

    private fun scheduler(nowUtc: String, run: (LocalDate, ReconTrigger) -> ReconRun = { date, trigger -> calls += date to trigger; ReconRun.AlreadyCompleted(date) }) =
        DailyReconScheduler(run, Clock.fixed(Instant.parse(nowUtc), ZoneOffset.UTC), "0 0 1-6 * * *", "Asia/Seoul", AlertNotifier { alerts += it })

    @Test
    fun `01시 회차는 전날(KST) 대사 - 마지막 회차가 아니다`() {
        scheduler("2026-10-29T16:00:00Z").run()   // KST 10-30 01:00

        assertEquals(listOf(LocalDate.of(2026, 10, 29) to ReconTrigger.SCHEDULE), calls)
    }

    @Test
    fun `05시는 마지막이 아니고 06시는 마지막 회차 - 같은 cron 의 다음 실행이 다음 날`() {
        scheduler("2026-10-29T20:00:00Z").run()   // KST 05:00
        scheduler("2026-10-29T21:00:00Z").run()   // KST 06:00

        assertEquals(listOf(ReconTrigger.SCHEDULE, ReconTrigger.SCHEDULE_FINAL), calls.map { it.second })
        assertEquals(setOf(LocalDate.of(2026, 10, 29)), calls.map { it.first }.toSet())
    }

    @Test
    fun `실행이 예외를 던져도 스케줄러는 던지지 않고 긴급 알림`() {
        scheduler("2026-10-29T16:00:00Z") { _, _ -> error("db down") }.run()

        assertEquals(AlertLevel.CRITICAL, alerts.single().level)
    }
}
