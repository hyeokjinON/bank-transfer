package io.github.hyeokjinon.banktransfer.gateway

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 은행 점검 시간 (P5-1). 기본 23:50 ~ 00:10 (KST), 시작 포함 · 끝 제외, 자정을 넘는다. */
class BankMaintenanceWindowTest {

    private val seoul = ZoneId.of("Asia/Seoul")
    private val window = BankMaintenanceWindow(LocalTime.of(23, 50), LocalTime.of(0, 10), seoul)

    private fun kst(h: Int, m: Int, s: Int = 0, nanos: Int = 0): Instant =
        LocalDate.of(2026, 10, 28).atTime(h, m, s, nanos).atZone(seoul).toInstant()

    @Test
    fun `시작 포함 · 끝 제외`() {
        assertFalse(window.isActive(kst(23, 49, 59, 999_999_999)))
        assertTrue(window.isActive(kst(23, 50)))
        assertTrue(window.isActive(kst(0, 9, 59, 999_999_999)))
        assertFalse(window.isActive(kst(0, 10)))
    }

    @Test
    fun `자정을 넘는 구간 - 자정 앞뒤 모두 점검 중, 낮은 아님`() {
        assertTrue(window.isActive(kst(23, 59, 59)))
        assertTrue(window.isActive(kst(0, 0)))
        assertFalse(window.isActive(kst(12, 0)))
        assertEquals(Duration.ofMinutes(20), window.length)
    }

    @Test
    fun `자정을 넘지 않는 구간도 된다`() {
        val night = BankMaintenanceWindow(LocalTime.of(3, 0), LocalTime.of(3, 20), seoul)

        assertTrue(night.isActive(kst(3, 10)))
        assertFalse(night.isActive(kst(3, 20)))
        assertFalse(night.isActive(kst(2, 59)))
        assertEquals(Duration.ofMinutes(20), night.length)
    }

    @Test
    fun `남은 시간 - 자정 전이면 다음 날 끝까지, Retry-After 초는 올림, 점검이 아니면 0`() {
        assertEquals(Duration.ofSeconds(899, 500_000_000), window.remaining(kst(23, 55, 0, 500_000_000)))
        assertEquals(900, window.remainingSeconds(kst(23, 55, 0, 500_000_000)))
        assertEquals(Duration.ofMinutes(5), window.remaining(kst(0, 5)))
        assertEquals(300, window.remainingSeconds(kst(0, 5)))
        assertEquals(Duration.ZERO, window.remaining(kst(12, 0)))
        assertEquals(0, window.remainingSeconds(kst(12, 0)))
    }

    @Test
    fun `꺼져 있으면 언제나 점검 아님`() {
        val off = BankMaintenanceWindow(LocalTime.of(23, 50), LocalTime.of(0, 10), seoul, enabled = false)

        assertFalse(off.isActive(kst(23, 55)))
        assertEquals(Duration.ZERO, off.remaining(kst(23, 55)))
    }

    @Test
    fun `시계는 UTC 로 주고 판정은 KST - LocalTime_now(clock) 을 그대로 쓰는 실수를 잡는다`() {
        val utc1450 = Clock.fixed(Instant.parse("2026-10-28T14:50:00Z"), ZoneOffset.UTC)   // = KST 23:50
        val utc2350 = Clock.fixed(Instant.parse("2026-10-28T23:50:00Z"), ZoneOffset.UTC)   // = KST 다음 날 08:50

        assertTrue(window.isActive(utc1450.instant()))
        assertFalse(window.isActive(utc2350.instant()))
        assertEquals(LocalTime.of(14, 50), LocalTime.now(utc1450))   // 시계의 시간대(UTC)로 읽으면 점검이 아닌 것처럼 보인다
    }

    @Test
    fun `시작과 끝이 같으면 거부`() {
        assertFailsWith<IllegalArgumentException> { BankMaintenanceWindow(LocalTime.of(1, 0), LocalTime.of(1, 0), seoul) }
    }
}
