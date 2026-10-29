package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import org.slf4j.LoggerFactory
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.core.job.parameters.JobParameters
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.launch.JobRestartException
import org.springframework.batch.core.listener.JobExecutionListener
import org.springframework.batch.core.repository.JobRepository
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.PessimisticLockingFailureException
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

class ReconRunningException(date: LocalDate) : RuntimeException("daily reconciliation for $date is already running")

class ReconAlreadyCompletedException(date: LocalDate) : RuntimeException("daily reconciliation for $date is already completed")

class ReconDateInFutureException(date: LocalDate, today: LocalDate) : RuntimeException("recon date $date is after today ($today, KST)")

/**
 * 대사일 하나의 잡을 이 스레드에서 끝까지 돌린다 (P5-1). Spring Batch 6 의 JobOperator 는 기본이 동기 실행(SyncTaskExecutor)이다.
 * 같은 날짜는 잡 저장소가 인스턴스 하나로 둔다: 끝났으면 [ReconRun.AlreadyCompleted], 돌고 있으면 [ReconRun.Running], 실패했으면 멈춘 곳부터 다시.
 * 처음 시작하는 날짜를 두 곳이 동시에 시작하면 진 쪽은 잡 인스턴스 UNIQUE 에서 DuplicateKeyException(직렬화 격리라 데드락일 수도)을 받는다 → Running.
 * 인스턴스와 실행 생성 사이에 온 쪽은 "실행 없는 인스턴스" 를 본다 → 잠깐 기다려 한 번 더 ([startOrRetry]).
 * 실패한 날짜를 두 곳이 동시에 재시작하는 경우는 [SingleRunGuard] 가 나중 실행을 멈춘다 → Running.
 *
 * 실패 알림 (설계 §8): 스케줄의 마지막 회차는 이유와 무관하게 "대사 미완료" CRITICAL — 실패든 "아직 돌고 있음"(Running)이든,
 * 그 전 회차 · 수동의 FILE_NOT_READY 는 알리지 않음 (은행 파일은 보통 00:05 에 온다), 그 밖 실패는 CRITICAL. 완료 알림은 잡의 ⑥ 보고가 보낸다.
 */
class DailyReconLauncher(
    private val jobOperator: JobOperator,
    private val job: Job,
    private val jobRepository: JobRepository,
    private val summaries: ReconSummaryReader,
    private val alerts: AlertNotifier,
    /** 실행 없는 인스턴스를 만났을 때 다시 시도하기 전 기다림 (테스트는 이 사이에 실행을 만들어 끼운다). */
    private val pauseBeforeRetry: () -> Unit = { Thread.sleep(RETRY_PAUSE_MS) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun run(date: LocalDate, trigger: ReconTrigger): ReconRun {
        val execution = try {
            startOrRetry(DailyRecon.parameters(date))
        } catch (e: JobInstanceAlreadyCompleteException) {
            log.info("daily reconciliation {} is already completed, skipped ({})", date, trigger)
            return ReconRun.AlreadyCompleted(date)
        } catch (e: JobExecutionAlreadyRunningException) {
            return running(date, trigger)
        } catch (e: DuplicateKeyException) {
            return running(date, trigger)
        } catch (e: PessimisticLockingFailureException) {
            return running(date, trigger)
        } catch (e: JobRestartException) {
            // 지난 실행이 UNKNOWN(롤백할 수 없는 실패)으로 끝났다 — 사람이 BATCH_* 상태를 확인해야 한다 (OPERATION §9)
            return failed(date, ReconFailure("RESTART_REFUSED", e.message.orEmpty()), trigger)
        } catch (e: IllegalStateException) {
            // 다시 시도해도 실행 없는 인스턴스다 — 만들던 쪽이 그 사이에 죽었다. 이 날짜는 사람이 BATCH_JOB_INSTANCE 를 정리해야 돈다 (OPERATION §9)
            log.error("daily reconciliation {} could not be started ({})", date, trigger, e)
            return failed(date, ReconFailure("ERROR", e.javaClass.name), trigger)
        }
        return outcomeOf(date, execution, trigger)
    }

    /**
     * Spring Batch 6.0.5 의 시작은 인스턴스와 실행을 서로 다른 트랜잭션에서 만든다(TaskExecutorJobLauncher.createJobExecution).
     * 처음 시작하는 날짜를 다른 곳이 막 시작해 인스턴스만 보이는 순간에 오면 IllegalStateException("Cannot find any job execution …") 이다 —
     * 여러 인스턴스의 같은 정각 회차가 흔한 경우. 인스턴스가 있으면 잠깐 기다려 한 번 더: 그때는 "돌고 있음"(→ Running)이나 "이미 완료" 다.
     */
    private fun startOrRetry(params: JobParameters): JobExecution =
        try {
            jobOperator.start(job, params)
        } catch (e: IllegalStateException) {
            if (jobRepository.getJobInstance(job.name, params) == null) throw e
            log.info("daily reconciliation instance {} has no execution yet, retrying once", params)
            pauseBeforeRetry()
            jobOperator.start(job, params)
        }

    private fun outcomeOf(date: LocalDate, execution: JobExecution, trigger: ReconTrigger): ReconRun {
        if (execution.status == BatchStatus.COMPLETED) return ReconRun.Completed(date, summaries.read(execution.jobInstance, date))
        val causes = execution.allFailureExceptions.flatMap { generateSequence(it) { cause -> cause.cause }.toList() }
        if (causes.any { it is ReconRunningException }) return running(date, trigger)
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

    private companion object {
        /** 인스턴스 커밋과 실행 커밋 사이는 보통 수 ms — 넉넉히. */
        const val RETRY_PAUSE_MS = 500L
    }
}

/**
 * 같은 날짜의 잡이 동시에 둘 돌지 않게 하는 마지막 확인 (P5-1, 계획 "스펙과 다른 점" 1).
 * Spring Batch 6.0.5 의 시작 경로는 "돌고 있는 실행이 있나" 를 본 뒤 별도 트랜잭션에서 실행을 만든다 — 실패한 날짜를 두 곳이 동시에
 * 재시작하면 둘 다 확인을 통과할 수 있다(새 날짜는 잡 인스턴스 UNIQUE 가 막는다). 그래서 실행이 시작된 직후, 같은 인스턴스에 먼저 만든(작은 id)
 * 실행이 아직 돌고 있으면 이 실행을 멈춘다 — 이 실행만 FAILED 로 끝나고 [DailyReconLauncher] 는 "실행 중" 으로 답한다.
 * 먼저 실행이 끝난 뒤의 재시작은 막지 않는다 (돌고 있는 실행이 없다).
 */
class SingleRunGuard(private val jobRepository: JobRepository) : JobExecutionListener {

    override fun beforeJob(jobExecution: JobExecution) {
        val earlierRunning = jobRepository.findRunningJobExecutions(jobExecution.jobInstance.jobName)
            .any { it.jobInstanceId == jobExecution.jobInstanceId && it.id < jobExecution.id }
        if (earlierRunning) {
            throw ReconRunningException(checkNotNull(jobExecution.jobParameters.getLocalDate(DailyRecon.RECON_DATE)))
        }
    }
}
