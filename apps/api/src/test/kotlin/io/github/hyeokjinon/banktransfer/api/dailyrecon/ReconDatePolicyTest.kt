package io.github.hyeokjinon.banktransfer.api.dailyrecon

import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * 수동 대사가 받는 날짜 (ADR-0010): 마감된 날(어제 이전, KST)만. 오늘을 대사하면 방금 보내 아직 은행 파일에 없는 출금을
 * "은행이 처리하지 않았다" 로 보고 E002 환불하는데, 은행은 그 출금을 아직 처리할 수 있다 → 이중 지급.
 * daily-recon.allow-today(손 확인용)일 때만 오늘을 받는다. 내일 이후는 언제나 거부.
 */
class ReconDatePolicyTest {

    private val today = LocalDate.of(2026, 10, 30)

    /** 오늘(KST) 12:00. */
    private val noon: Instant = today.atTime(12, 0).atZone(DailyRecon.KST).toInstant()

    private fun rejected(date: LocalDate, now: Instant, allowToday: Boolean) =
        assertFailsWith<ReconDateNotClosedException> { DailyRecon.requireClosed(date, now, allowToday) }

    @Test
    fun `기본 - 어제 이전만 받는다, 오늘 · 내일은 거부`() {
        DailyRecon.requireClosed(today.minusDays(1), noon, allowToday = false)
        DailyRecon.requireClosed(today.minusDays(30), noon, allowToday = false)

        val sameDay = rejected(today, noon, allowToday = false)
        rejected(today.plusDays(1), noon, allowToday = false)

        assertEquals("recon date 2026-10-30 is not closed yet (today is 2026-10-30, KST) - only past days can be reconciled", sameDay.message)
    }

    @Test
    fun `allow-today (손 확인용) - 오늘도 받는다, 내일 이후는 그래도 거부`() {
        DailyRecon.requireClosed(today, noon, allowToday = true)
        DailyRecon.requireClosed(today.minusDays(1), noon, allowToday = true)

        rejected(today.plusDays(1), noon, allowToday = true)
    }

    @Test
    fun `KST 기준 - KST 00시 30분(UTC 전날 15시 30분)이면 KST 오늘은 거부, KST 어제(UTC 로는 오늘)는 받는다`() {
        val justAfterMidnight = today.atTime(0, 30).atZone(DailyRecon.KST).toInstant()
        assertEquals(Instant.parse("2026-10-29T15:30:00Z"), justAfterMidnight)

        rejected(today, justAfterMidnight, allowToday = false)
        DailyRecon.requireClosed(today.minusDays(1), justAfterMidnight, allowToday = false)
        // 손 확인용이어도 "오늘" 은 KST 오늘이다 — UTC 날짜(10-29)로 보면 KST 오늘(10-30)을 내일로 거부하게 된다
        DailyRecon.requireClosed(today, justAfterMidnight, allowToday = true)
        rejected(today.plusDays(1), justAfterMidnight, allowToday = true)
    }
}
