package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.retention.DataRetention
import io.github.hyeokjinon.banktransfer.api.retention.RetentionTestDates
import io.github.hyeokjinon.banktransfer.api.retention.RetentionTestSupport
import io.github.hyeokjinon.banktransfer.api.support.ApiResponse
import io.github.hyeokjinon.banktransfer.api.support.send
import java.time.LocalDate
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 수동 실행 API (P5-2, 설계 §4): 200 (실패해도 200 + FAILED + 이유), 409 두 가지, 내일 이후 400. 인증은 범위 밖. */
class RetentionApiTest : RetentionTestSupport() {

    private val seoul = ZoneId.of("Asia/Seoul")

    private fun post(date: String?): ApiResponse =
        send(client.post().uri(if (date == null) "/admin/retention-runs" else "/admin/retention-runs?date=$date"))

    /** 앱 시계를 [today] 정오(KST)에 둔다. [RetentionTestDates] 는 먼 과거라 시계를 두지 않아도 지난 날이지만, 내일 · 오늘을 재려면 시계가 그 날이어야 한다. */
    private fun todayIs(today: LocalDate) = testClock.fix(today.atTime(12, 0).atZone(seoul).toInstant())

    @Suppress("UNCHECKED_CAST")
    private fun ApiResponse.deleted() = body["deleted"] as Map<String, Any?>

    @Suppress("UNCHECKED_CAST")
    private fun ApiResponse.reason() = body["reason"] as Map<String, Any?>

    @Test
    fun `완료하면 200 COMPLETED + 건수 (reason 은 null 로 있다), 다시 보내면 409 RETENTION_ALREADY_COMPLETED`() {
        val day = RetentionTestDates.next()
        outboxRows(2, outboxCutoff(day).minusSeconds(1))
        verificationRows(1, "EXPIRED", verificationCutoff(day).minusSeconds(1))

        val first = post(day.toString())

        assertEquals(200 to "COMPLETED", first.status to first.body["status"])
        assertEquals(day.toString(), first.body["date"])
        assertEquals(mapOf<String, Any?>("outboxEvents" to 2, "verifications" to 1), first.deleted())
        assertTrue("reason" in first.body)
        assertEquals(null, first.body["reason"])

        val again = post(day.toString())

        assertEquals(409 to "RETENTION_ALREADY_COMPLETED", again.status to again.body["code"])
    }

    @Test
    fun `실행 중이면 409 RETENTION_RUNNING - 도중에 JVM 이 죽어 STARTED 로 남은 날짜도 같다`() {
        val day = RetentionTestDates.next()
        val stuck = startedExecution(day)
        try {
            val response = post(day.toString())

            assertEquals(409 to "RETENTION_RUNNING", response.status to response.body["code"])
        } finally {
            abandon(stuck)
        }
    }

    @Test
    fun `실패하면 200 FAILED + 이유, deleted 는 null - 요청은 처리됐고 결과가 실패다`() {
        val day = RetentionTestDates.next()
        jobRepository.createJobInstance(DataRetention.JOB_NAME, DataRetention.parameters(day))   // 처음 시작하다 죽은 곳

        val response = post(day.toString())

        assertEquals(200 to "FAILED", response.status to response.body["status"])
        assertEquals("ERROR" to IllegalStateException::class.java.name, response.reason()["code"] to response.reason()["message"])
        assertTrue("deleted" in response.body)
        assertEquals(null, response.body["deleted"])
    }

    @Test
    fun `내일 이후는 400 RETENTION_DATE_IN_FUTURE, 오늘과 지난 날은 실행한다`() {
        val today = RetentionTestDates.next()
        todayIs(today)

        val tomorrow = post(today.plusDays(1).toString())
        val sameDay = post(today.toString())
        val yesterday = post(today.minusDays(1).toString())

        assertEquals(400 to "RETENTION_DATE_IN_FUTURE", tomorrow.status to tomorrow.body["code"])
        assertEquals(200 to "COMPLETED", sameDay.status to sameDay.body["status"])
        assertEquals(200 to "COMPLETED", yesterday.status to yesterday.body["status"])
    }

    @Test
    fun `오늘은 KST 로 정한다 - KST 00시 30분(UTC 전날 15시 30분)이면 KST 오늘은 실행, KST 내일은 400`() {
        val today = RetentionTestDates.next()
        testClock.fix(today.atTime(0, 30).atZone(seoul).toInstant())   // UTC 로는 아직 어제

        val sameDay = post(today.toString())
        val tomorrow = post(today.plusDays(1).toString())

        assertEquals(200 to "COMPLETED", sameDay.status to sameDay.body["status"])
        assertEquals(400 to "RETENTION_DATE_IN_FUTURE", tomorrow.status to tomorrow.body["code"])
    }

    @Test
    fun `날짜가 없거나 형식이 틀리면 400 INVALID_REQUEST`() {
        assertEquals(400 to "INVALID_REQUEST", post(null).let { it.status to it.body["code"] })
        assertEquals(400 to "INVALID_REQUEST", post("2026-13-01").let { it.status to it.body["code"] })
        assertEquals(400 to "INVALID_REQUEST", post("20261102").let { it.status to it.body["code"] })
    }
}
