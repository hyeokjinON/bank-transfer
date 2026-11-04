package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.io.ClassPathResource
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 06:30 스케줄 (P5-2): 시계는 UTC, 날짜는 KST. 실행기는 함수로 바꿔 끼운다. */
class DataRetentionSchedulerTest {

    private val calls = mutableListOf<LocalDate>()
    private val alerts = mutableListOf<Alert>()

    private fun scheduler(nowUtc: String, run: (LocalDate) -> RetentionRun = { date -> calls += date; RetentionRun.AlreadyCompleted(date) }) =
        DataRetentionScheduler(run, Clock.fixed(Instant.parse(nowUtc), ZoneOffset.UTC), "Asia/Seoul", AlertNotifier { alerts += it })

    @Test
    fun `오늘(KST) 날짜로 실행한다 - KST 06시 30분은 UTC 로 아직 전날`() {
        scheduler("2026-11-01T21:30:00Z").run()   // KST 11-02 06:30

        assertEquals(listOf(LocalDate.of(2026, 11, 2)), calls)
        assertTrue(alerts.isEmpty())
    }

    @Test
    fun `실행이 예외를 던져도(시작조차 못 함) 스케줄러는 던지지 않고 WARN - 내부 메시지는 싣지 않는다`() {
        scheduler("2026-11-01T21:30:00Z") { _ -> error("db down") }.run()

        val alert = alerts.single()
        assertEquals(AlertLevel.WARN, alert.level)
        assertEquals("보존 배치 2026-11-02 실패", alert.title)
        assertTrue(alert.lines.any { IllegalStateException::class.java.name in it })
        assertFalse(alert.lines.any { "db down" in it })
    }

    @Test
    fun `실패 결과는 실행기가 이미 알렸으므로 다시 알리지 않는다`() {
        scheduler("2026-11-01T21:30:00Z") { date -> RetentionRun.Failed(date, RetentionFailure("ERROR", "x")) }.run()

        assertTrue(alerts.isEmpty())
    }

    @Test
    fun `스케줄 풀은 4 - 밀린 행이 많은 첫 실행이 길어져도 발행기 · 정리 작업 · 대사를 굶기지 않는다`() {
        val yml = Binder(ConfigurationPropertySources.from(YamlPropertySourceLoader().load("application", ClassPathResource("application.yml"))))

        assertEquals(4, yml.bind("spring.task.scheduling.pool.size", Int::class.javaObjectType).get())
    }
}
