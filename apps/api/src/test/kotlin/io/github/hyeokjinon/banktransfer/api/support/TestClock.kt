package io.github.hyeokjinon.banktransfer.api.support

import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

/**
 * 통합 테스트의 앱 시계 (P5-1). 평소에는 진짜 시각이다.
 * - [fix]: 점검 시간 · 대사 테스트가 시각을 정한다. [ApiIntegrationTest] 가 테스트마다 [release] 로 되돌린다.
 * - 진짜 시각이 점검 시간(기본 23:50 ~ 00:10 KST)에 걸리면 점검 직전으로 비켜 간다 — 밤에 CI · 개발 PC 에서 도는 다른 테스트가 503 으로 흔들리지 않게.
 *   비켜 가는 동안 시각은 (점검 길이 + 1분) 만큼 앞의 같은 간격을 지나므로, 점검 시작 · 끝 순간에 걸친 테스트만 시각이 그만큼 뒤로 · 앞으로 뛴다.
 * - [withZone] 은 Clock.systemUTC() 처럼 다른 시간대의 시계를 돌려준다. 고정 상태는 공유하므로 파생 시계도 같은 [fix] 를 따른다.
 */
class TestClock private constructor(
    private val window: BankMaintenanceWindow,
    private val zone: ZoneId,
    private val state: State,
) : Clock() {

    constructor(window: BankMaintenanceWindow) : this(window, ZoneOffset.UTC, State())

    private class State {
        @Volatile
        var fixed: Instant? = null
    }

    fun fix(at: Instant) {
        state.fixed = at
    }

    fun release() {
        state.fixed = null
    }

    override fun instant(): Instant =
        state.fixed ?: Instant.now().let { now -> if (window.isActive(now)) now.minus(window.length).minusSeconds(60) else now }

    override fun getZone(): ZoneId = zone

    override fun withZone(zone: ZoneId): Clock = if (zone == this.zone) this else TestClock(window, zone, state)
}

/** 앱의 Clock 빈(Clock.systemUTC())보다 먼저 주입된다. 진짜 시각을 따르되 테스트가 고정할 수 있다. */
@TestConfiguration(proxyBeanMethods = false)
class TestClockConfiguration {

    @Bean
    @Primary
    fun testClock(window: BankMaintenanceWindow) = TestClock(window)
}
