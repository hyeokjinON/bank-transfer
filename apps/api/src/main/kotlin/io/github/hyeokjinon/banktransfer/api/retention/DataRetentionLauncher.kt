package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobLauncher
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobStart
import io.github.hyeokjinon.banktransfer.api.batch.failureCauses
import org.slf4j.LoggerFactory
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.repository.JobRepository
import java.time.LocalDate

/** 표별 삭제 건수. 이 날짜 인스턴스의 모든 실행을 더한 값이라 재시작해도 맞다. */
data class RetentionDeleted(val outboxEvents: Long, val verifications: Long)

/** 실패 이유. code = ERROR · RESTART_REFUSED. 예외 종류만 싣는다 (내부 메시지는 로그에). */
data class RetentionFailure(val code: String, val message: String)

/** 실행 한 번의 결과. */
sealed interface RetentionRun {
    val date: LocalDate

    data class Completed(override val date: LocalDate, val deleted: RetentionDeleted) : RetentionRun
    data class Failed(override val date: LocalDate, val failure: RetentionFailure) : RetentionRun
    data class AlreadyCompleted(override val date: LocalDate) : RetentionRun
    data class Running(override val date: LocalDate) : RetentionRun
}

/**
 * 날짜 하나의 보존 배치를 이 스레드에서 끝까지 돌린다 (P5-2, 설계 §4). 시작 경합 처리는 [DatedJobLauncher] 가 한다.
 * 실패: 멈춘 단계 · 지운 만큼 그대로 두고 WARN — 삭제가 하루 밀려도 급하지 않다. 자동 재시도는 없다: 다음 날 실행은 새 인스턴스이고 기준 시각이 뒤로 가 밀린 행까지 지운다.
 * 성공은 건수를 로그로만 남긴다 — 대사 요약이 이미 매일 간다.
 */
class DataRetentionLauncher(
    jobOperator: JobOperator,
    job: Job,
    private val jobRepository: JobRepository,
    private val alerts: AlertNotifier,
    pauseBeforeRetry: () -> Unit = { Thread.sleep(DatedJobLauncher.RETRY_PAUSE_MS) },
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val starter = DatedJobLauncher(jobOperator, job, jobRepository, DataRetention::parameters, pauseBeforeRetry)

    fun run(date: LocalDate): RetentionRun =
        when (val start = starter.start(date)) {
            is DatedJobStart.Ended -> outcomeOf(date, start.execution)
            DatedJobStart.AlreadyCompleted -> {
                log.info("data retention {} is already completed, skipped", date)
                RetentionRun.AlreadyCompleted(date)
            }
            DatedJobStart.Running -> {
                log.info("data retention {} is already running elsewhere, skipped", date)
                RetentionRun.Running(date)
            }
            is DatedJobStart.RestartRefused -> {
                log.error("data retention {} could not be restarted: {}", date, start.message)
                failed(date, RetentionFailure("RESTART_REFUSED", "지난 실행이 UNKNOWN 으로 끝나 재시작을 거부했다 (OPERATION §10)"))
            }
            is DatedJobStart.NotStarted -> {
                log.error("data retention {} could not be started", date, start.cause)
                failed(date, RetentionFailure("ERROR", start.cause.javaClass.name))
            }
        }

    private fun outcomeOf(date: LocalDate, execution: JobExecution): RetentionRun {
        if (execution.status == BatchStatus.COMPLETED) {
            val deleted = deletedOf(execution)
            log.info("data retention {} completed: {} outbox events, {} verifications deleted", date, deleted.outboxEvents, deleted.verifications)
            return RetentionRun.Completed(date, deleted)
        }
        val cause = execution.failureCauses().firstOrNull()
        log.error("data retention {} failed", date, cause)
        return failed(date, RetentionFailure("ERROR", cause?.javaClass?.name ?: execution.exitStatus.exitCode))
    }

    /** 재시작하면 멈춘 단계에 새 StepExecution 이 건수 0 으로 생긴다 → 같은 단계의 모든 실행을 더한다. getLastStepExecution 만 보면 모자란다. */
    private fun deletedOf(execution: JobExecution): RetentionDeleted {
        val steps = jobRepository.getJobExecutions(execution.jobInstance).flatMap { it.stepExecutions }
        fun written(step: String) = steps.filter { it.stepName == step }.sumOf { it.writeCount }
        return RetentionDeleted(written(DataRetention.PURGE_OUTBOX), written(DataRetention.PURGE_VERIFICATIONS))
    }

    private fun failed(date: LocalDate, failure: RetentionFailure): RetentionRun {
        alerts.notifySafely(
            Alert(
                AlertLevel.WARN, "보존 배치 $date 실패",
                listOf("이유: ${failure.code} ${failure.message}", "다음 날 실행이 밀린 행까지 지운다. 바로 돌리려면 POST /admin/retention-runs?date=$date (OPERATION §10)"),
            ),
        )
        log.warn("data retention {} failed: {} {}", date, failure.code, failure.message)
        return RetentionRun.Failed(date, failure)
    }
}
