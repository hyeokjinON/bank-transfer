package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobLauncher
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobRunningException
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobSingleRunGuard
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobStart
import io.github.hyeokjinon.banktransfer.api.batch.failureCauses
import org.slf4j.LoggerFactory
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.repository.JobRepository
import java.time.LocalDate

/** 누가 실행했나 — 실패 알림이 갈린다. */
enum class ReconTrigger { SCHEDULE, SCHEDULE_FINAL, MANUAL }

/** 실패 이유. code = FILE_NOT_READY · FILE_INVALID · RESTART_REFUSED · ERROR. ERROR 는 예외 종류만 싣는다 (내부 메시지는 로그에). */
data class ReconFailure(val code: String, val message: String)

/** 실행 한 번의 결과. */
sealed interface ReconRun {
    val date: LocalDate

    data class Completed(override val date: LocalDate, val summary: ReconSummary) : ReconRun
    data class Failed(override val date: LocalDate, val failure: ReconFailure) : ReconRun
    data class AlreadyCompleted(override val date: LocalDate) : ReconRun
    data class Running(override val date: LocalDate) : ReconRun
}

class ReconRunningException(date: LocalDate) : DatedJobRunningException("daily reconciliation for $date is already running")

class ReconAlreadyCompletedException(date: LocalDate) : RuntimeException("daily reconciliation for $date is already completed")

/** 아직 마감되지 않은 날(오늘 이후, KST)의 수동 대사 — 지난 날만 대사할 수 있다 ([DailyRecon.requireClosed]). */
class ReconDateNotClosedException(date: LocalDate, today: LocalDate) :
    RuntimeException("recon date $date is not closed yet (today is $today, KST) - only past days can be reconciled")

/**
 * 대사일 하나의 잡을 이 스레드에서 끝까지 돌리고 대사의 말로 옮긴다 (P5-1). 시작 경합 처리는 [DatedJobLauncher] 가 한다.
 *
 * 실패 알림 (설계 §8): 스케줄의 마지막 회차는 이유와 무관하게 "대사 미완료" CRITICAL — 실패든 "아직 돌고 있음"(Running)이든,
 * 그 전 회차 · 수동의 FILE_NOT_READY 는 알리지 않음 (은행 파일은 보통 00:05 에 온다), 그 밖 실패는 CRITICAL. 완료 알림은 잡의 ⑥ 보고가 보낸다.
 */
class DailyReconLauncher(
    jobOperator: JobOperator,
    job: Job,
    jobRepository: JobRepository,
    private val summaries: ReconSummaryReader,
    private val alerts: AlertNotifier,
    /** 실행 없는 인스턴스를 만났을 때 다시 시도하기 전 기다림 (테스트는 이 사이에 실행을 만들어 끼운다). */
    pauseBeforeRetry: () -> Unit = { Thread.sleep(DatedJobLauncher.RETRY_PAUSE_MS) },
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val starter = DatedJobLauncher(jobOperator, job, jobRepository, DailyRecon::parameters, pauseBeforeRetry)

    fun run(date: LocalDate, trigger: ReconTrigger): ReconRun =
        when (val start = starter.start(date)) {
            is DatedJobStart.Ended -> outcomeOf(date, start.execution, trigger)
            DatedJobStart.AlreadyCompleted -> {
                log.info("daily reconciliation {} is already completed, skipped ({})", date, trigger)
                ReconRun.AlreadyCompleted(date)
            }
            DatedJobStart.Running -> running(date, trigger)
            // 지난 실행이 UNKNOWN(롤백할 수 없는 실패)으로 끝났다 — 사람이 BATCH_* 상태를 확인해야 한다 (OPERATION §9)
            is DatedJobStart.RestartRefused -> failed(date, ReconFailure("RESTART_REFUSED", start.message), trigger)
            // 다시 시도해도 실행 없는 인스턴스다 — 만들던 쪽이 그 사이에 죽었다. 이 날짜는 사람이 BATCH_JOB_INSTANCE 를 정리해야 돈다 (OPERATION §9)
            is DatedJobStart.NotStarted -> {
                log.error("daily reconciliation {} could not be started ({})", date, trigger, start.cause)
                failed(date, ReconFailure("ERROR", start.cause.javaClass.name), trigger)
            }
        }

    private fun outcomeOf(date: LocalDate, execution: JobExecution, trigger: ReconTrigger): ReconRun {
        if (execution.status == BatchStatus.COMPLETED) return ReconRun.Completed(date, summaries.read(execution.jobInstance, date))
        val causes = execution.failureCauses()
        val failure = causes.firstNotNullOfOrNull { it as? ReconFileException }?.let { ReconFailure(it.code, it.message.orEmpty()) }
            ?: ReconFailure("ERROR", causes.firstOrNull()?.javaClass?.name ?: execution.exitStatus.exitCode)
        return failed(date, failure, trigger)
    }

    private fun running(date: LocalDate, trigger: ReconTrigger): ReconRun {
        log.info("daily reconciliation {} is already running elsewhere, skipped ({})", date, trigger)
        // 스펙 §8 "06:00 회차까지 완료하지 못하면 알림" 은 "돌고 있음" 도 예외로 두지 않는다 — 대사 도중 JVM 이 죽어 실행이 STARTED 로 남으면
        // 회차마다 Running 만 돌아오고 아무도 모른다
        if (trigger == ReconTrigger.SCHEDULE_FINAL) {
            alerts.notifySafely(
                incompleteAlert(
                    date,
                    ReconFailure("RUNNING", "다른 실행이 아직 돌고 있다 — 곧 끝나면 완료 알림이 따로 온다. 오지 않으면 STARTED 로 남은 실행을 확인"),
                ),
            )
        }
        return ReconRun.Running(date)
    }

    private fun incompleteAlert(date: LocalDate, failure: ReconFailure) = Alert(
        AlertLevel.CRITICAL, "일마감 대사 $date 미완료 — 마지막 회차까지 끝내지 못함",
        listOf("이유: ${failure.code} ${failure.message}", "확인 뒤 POST /admin/reconciliations?date=$date 로 다시 실행 (OPERATION §9)"),
    )

    private fun failed(date: LocalDate, failure: ReconFailure, trigger: ReconTrigger): ReconRun {
        when {
            trigger == ReconTrigger.SCHEDULE_FINAL -> alerts.notifySafely(incompleteAlert(date, failure))
            failure.code == DailyRecon.FILE_NOT_READY ->
                log.info("recon file for {} is not ready yet ({}), the next round retries", date, trigger)
            else -> alerts.notifySafely(
                Alert(
                    AlertLevel.CRITICAL, "일마감 대사 $date 실패",
                    listOf("이유: ${failure.code} ${failure.message}", "멈춘 단계부터 다시: 다음 정각 회차 또는 POST /admin/reconciliations?date=$date"),
                ),
            )
        }
        log.warn("daily reconciliation {} failed ({}): {} {}", date, trigger, failure.code, failure.message)
        return ReconRun.Failed(date, failure)
    }
}

/**
 * 대사 잡의 동시 실행 가드. 나중 실행은 [ReconRunningException] 으로 멈춘다.
 * 이름이 클래스처럼 대문자인 이유: 일반화하기 전의 호출(`SingleRunGuard(jobRepository)` — 잡 구성 · DailyReconLauncherTest)을 그대로 두려고.
 */
fun SingleRunGuard(jobRepository: JobRepository) =
    DatedJobSingleRunGuard(jobRepository, DailyRecon.RECON_DATE, ::ReconRunningException)
