package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.retention.DataRetentionLauncher
import io.github.hyeokjinon.banktransfer.api.retention.RetentionAlreadyCompletedException
import io.github.hyeokjinon.banktransfer.api.retention.RetentionDeleted
import io.github.hyeokjinon.banktransfer.api.retention.RetentionFailure
import io.github.hyeokjinon.banktransfer.api.retention.RetentionPolicy
import io.github.hyeokjinon.banktransfer.api.retention.RetentionRun
import io.github.hyeokjinon.banktransfer.api.retention.RetentionRunningException
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Clock
import java.time.LocalDate

/** 보존 배치 실행 결과. status = COMPLETED 면 deleted(이 날짜 인스턴스의 모든 실행의 합), FAILED 면 reason. */
data class RetentionRunResponse(val date: LocalDate, val status: String, val reason: RetentionFailure?, val deleted: RetentionDeleted?)

/**
 * 보존 배치 수동 실행 (P5-2, 관리용 — 인증은 다른 관리 API 와 같이 범위 밖). 이 요청 스레드에서 끝까지 돌린다.
 * 실패해도 200 (status = FAILED + 이유) — 요청은 처리됐고 결과가 실패다. 이미 끝난 날짜 · 실행 중이면 409,
 * 내일 이후(KST)면 400 — 기준 시각이 앞당겨져 정책보다 일찍 지우게 된다 ([RetentionPolicy.requireNotFuture]).
 */
@RestController
@RequestMapping("/admin/retention-runs")
class RetentionController(
    private val launcher: DataRetentionLauncher,
    private val clock: Clock,
) {

    @PostMapping
    fun run(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) date: LocalDate): RetentionRunResponse {
        RetentionPolicy.requireNotFuture(date, clock.instant())
        return when (val run = launcher.run(date)) {
            is RetentionRun.Completed -> RetentionRunResponse(date, "COMPLETED", null, run.deleted)
            is RetentionRun.Failed -> RetentionRunResponse(date, "FAILED", run.failure, null)
            is RetentionRun.AlreadyCompleted -> throw RetentionAlreadyCompletedException(date)
            is RetentionRun.Running -> throw RetentionRunningException(date)
        }
    }
}
