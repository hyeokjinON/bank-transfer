package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyReconLauncher
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconAlreadyCompletedException
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconDateInFutureException
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconFailure
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconRun
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconRunningException
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconSummary
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconTrigger
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Clock
import java.time.LocalDate

/** 대사 실행 결과. status = COMPLETED 면 summary, FAILED 면 reason. */
data class ReconRunResponse(val date: LocalDate, val status: String, val reason: ReconFailure?, val summary: ReconSummary?)

/**
 * 일마감 대사 수동 실행 (P5-1, 관리용 — 인증은 다른 API 와 같이 범위 밖). 점검 시간에도 그대로 받는다.
 * 이 요청 스레드에서 끝까지 돌리고 결과를 돌려준다. 실패해도 200 (status = FAILED + 이유) — 요청은 처리됐고 결과가 실패다.
 * 이미 끝난 날짜 · 실행 중이면 409, 내일 이후(KST)면 400. 오늘은 손 확인용으로 허용한다 (그 뒤 출금은 오늘 파일에 없다).
 */
@RestController
@RequestMapping("/admin/reconciliations")
class ReconciliationController(private val launcher: DailyReconLauncher, private val clock: Clock) {

    @PostMapping
    fun run(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) date: LocalDate): ReconRunResponse {
        val today = LocalDate.ofInstant(clock.instant(), DailyRecon.KST)
        if (date.isAfter(today)) throw ReconDateInFutureException(date, today)
        return when (val run = launcher.run(date, ReconTrigger.MANUAL)) {
            is ReconRun.Completed -> ReconRunResponse(date, "COMPLETED", null, run.summary)
            is ReconRun.Failed -> ReconRunResponse(date, "FAILED", run.failure, null)
            is ReconRun.AlreadyCompleted -> throw ReconAlreadyCompletedException(date)
            is ReconRun.Running -> throw ReconRunningException(date)
        }
    }
}
