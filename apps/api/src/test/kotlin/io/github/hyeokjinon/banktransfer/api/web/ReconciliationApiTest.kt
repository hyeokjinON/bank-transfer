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

/** 관리 API (P5-1, 설계 §5-2): 200 {status, summary} (실패해도 200 + FAILED + 이유), 409 두 가지, 마감 전(오늘 이후, KST) 400. 인증은 범위 밖. */
class ReconciliationApiTest : DailyReconTestSupport() {

    @Autowired private lateinit var launcher: DailyReconLauncher

    private fun post(date: String?): ApiResponse =
        send(client.post().uri(if (date == null) "/admin/reconciliations" else "/admin/reconciliations?date=$date"))

    /**
     * 대사일 하나를 고르고 앱 시계를 그다음 날 01시(KST, 첫 회차 시각)에 둔다. [ReconTestDates] 는 먼 미래의 날짜라
     * 진짜 시각 그대로면 컨트롤러가 '마감 전' 400 으로 거른다.
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
    fun `마감 전 날짜는 400 RECON_DATE_NOT_CLOSED - 오늘(KST)도 기본은 거부, 어제는 실행, 점검 시간에도 그대로`() {
        val today = ReconTestDates.unique()
        testClock.fix(today.atTime(23, 55).atZone(DailyRecon.KST).toInstant())   // 점검 시간 — 관리 API 는 막지 않는다

        val tomorrow = post(today.plusDays(1).toString())
        val sameDay = post(today.toString())
        val yesterday = post(today.minusDays(1).toString())

        assertEquals(400 to "RECON_DATE_NOT_CLOSED", tomorrow.status to tomorrow.body["code"])
        // 오늘을 대사하면 아직 파일에 없는 방금 보낸 출금을 E002 환불한다 — 은행이 그 뒤 처리하면 이중 지급 (ADR-0010). 손 확인은 allow-today 로만
        assertEquals(400 to "RECON_DATE_NOT_CLOSED", sameDay.status to sameDay.body["code"])
        assertEquals(200 to "FAILED", yesterday.status to yesterday.body["status"])   // 파일이 없을 뿐 실행은 됐다
    }

    @Test
    fun `오늘은 KST 로 정한다 - KST 00시 30분(UTC 전날 15시 30분)이면 KST 오늘은 400, KST 어제는 실행`() {
        val today = ReconTestDates.unique()
        testClock.fix(today.atTime(0, 30).atZone(DailyRecon.KST).toInstant())   // UTC 로는 아직 어제

        val sameDay = post(today.toString())
        val yesterday = post(today.minusDays(1).toString())

        assertEquals(400 to "RECON_DATE_NOT_CLOSED", sameDay.status to sameDay.body["code"])
        assertEquals(200 to "FAILED", yesterday.status to yesterday.body["status"])
    }

    @Test
    fun `날짜가 없거나 형식이 틀리면 400 INVALID_REQUEST`() {
        assertEquals(400 to "INVALID_REQUEST", post(null).let { it.status to it.body["code"] })
        assertEquals(400 to "INVALID_REQUEST", post("2026-13-01").let { it.status to it.body["code"] })
        assertEquals(400 to "INVALID_REQUEST", post("20261028").let { it.status to it.body["code"] })
    }

    @Test
    fun `잡 인스턴스만 남아 있어도(처음 시작하다 죽은 곳) 500 이 아니라 200 FAILED + 이유 ERROR`() {
        val day = reconDay()
        jobRepository.createJobInstance(DailyRecon.JOB_NAME, DailyRecon.parameters(day))

        val response = post(day.toString())

        assertEquals(200 to "FAILED", response.status to response.body["status"])
        assertEquals("ERROR", response.reason()["code"])
    }

    @Test
    fun `실행 중이면 409 RECON_RUNNING`() {
        val day = reconDay()
        writeBankFile(day, emptyList())

        val (_, response) = whileReportHeld(day, { launcher.run(day, ReconTrigger.SCHEDULE) }) { post(day.toString()) }

        assertEquals(409 to "RECON_RUNNING", response.status to response.body["code"])
    }
}
