package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyReconLauncher
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyReconTestSupport
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconTestDates
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconTrigger
import io.github.hyeokjinon.banktransfer.api.support.ApiResponse
import io.github.hyeokjinon.banktransfer.api.support.send
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import org.springframework.beans.factory.annotation.Autowired
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

/** 관리 API (P5-1, 설계 §5-2): 200 {status, summary} (실패해도 200 + FAILED + 이유), 409 두 가지, 내일 이후 400. 인증은 범위 밖. */
class ReconciliationApiTest : DailyReconTestSupport() {

    @Autowired private lateinit var launcher: DailyReconLauncher

    private fun post(date: String?): ApiResponse =
        send(client.post().uri(if (date == null) "/admin/reconciliations" else "/admin/reconciliations?date=$date"))

    /**
     * 대사일 하나를 고르고 앱 시계를 그다음 날 01시(KST, 첫 회차 시각)에 둔다. [ReconTestDates] 는 먼 미래의 날짜라
     * 진짜 시각 그대로면 컨트롤러가 '내일 이후' 400 으로 거른다.
     */
    private fun reconDay(): LocalDate =
        ReconTestDates.unique().also { testClock.fix(it.plusDays(1).atTime(1, 0).atZone(DailyRecon.KST).toInstant()) }

    @Suppress("UNCHECKED_CAST")
    private fun ApiResponse.summary() = body["summary"] as Map<String, Any?>

    @Suppress("UNCHECKED_CAST")
    private fun ApiResponse.reason() = body["reason"] as Map<String, Any?>

    @Test
    fun `완료하면 200 COMPLETED + 요약, 다시 보내면 409 RECON_ALREADY_COMPLETED`() {
        val day = reconDay()
        val paid = seed(fundedUser(), TransferStatus.UNKNOWN, at(day))
        writeBankFile(day, listOf(lineOf(paid)))

        val first = post(day.toString())

        assertEquals(200, first.status)
        assertEquals("COMPLETED", first.body["status"])
        assertEquals(day.toString(), first.body["date"])
        assertEquals(1, first.summary()["transfers"])
        assertEquals(1, first.summary()["autoCompleted"])
        assertEquals(TransferStatus.COMPLETED, status(paid))

        val again = post(day.toString())

        assertEquals(409 to "RECON_ALREADY_COMPLETED", again.status to again.body["code"])
    }

    @Test
    fun `파일이 없으면 200 FAILED + 이유 FILE_NOT_READY (요청은 처리됐고 결과가 실패)`() {
        val response = post(reconDay().toString())

        assertEquals(200, response.status)
        assertEquals("FAILED", response.body["status"])
        assertEquals("FILE_NOT_READY", response.reason()["code"])
        assertEquals(null, response.body["summary"])
    }

    @Test
    fun `내일 이후 날짜는 400 RECON_DATE_IN_FUTURE - 오늘(KST)은 손 확인용으로 허용, 점검 시간에도 그대로`() {
        val today = ReconTestDates.unique()
        testClock.fix(today.atTime(23, 55).atZone(DailyRecon.KST).toInstant())   // 점검 시간 — 관리 API 는 막지 않는다

        val tomorrow = post(today.plusDays(1).toString())
        val sameDay = post(today.toString())

        assertEquals(400 to "RECON_DATE_IN_FUTURE", tomorrow.status to tomorrow.body["code"])
        assertEquals(200 to "FAILED", sameDay.status to sameDay.body["status"])   // 파일이 없을 뿐 실행은 됐다
    }

    @Test
    fun `날짜가 없거나 형식이 틀리면 400 INVALID_REQUEST`() {
        assertEquals(400 to "INVALID_REQUEST", post(null).let { it.status to it.body["code"] })
        assertEquals(400 to "INVALID_REQUEST", post("2026-13-01").let { it.status to it.body["code"] })
        assertEquals(400 to "INVALID_REQUEST", post("20261028").let { it.status to it.body["code"] })
    }

    @Test
    fun `실행 중이면 409 RECON_RUNNING`() {
        val day = reconDay()
        writeBankFile(day, emptyList())

        val (_, response) = whileReportHeld(day, { launcher.run(day, ReconTrigger.SCHEDULE) }) { post(day.toString()) }

        assertEquals(409 to "RECON_RUNNING", response.status to response.body["code"])
    }
}
