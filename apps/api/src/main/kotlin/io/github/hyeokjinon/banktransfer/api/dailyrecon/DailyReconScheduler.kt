package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.scheduling.support.CronExpression
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime

/**
 * 매일 01:00 ~ 06:00 매시 정각(KST)에 전날 대사를 돌린다 (P5-1, 설계 §5-2). 파일이 아직 없으면 다음 정각에 다시(멈춘 곳부터),
 * 이미 끝났으면 조용히 넘어간다. 마지막 회차 = 같은 cron 의 다음 실행이 다음 날인 회차 — cron 을 바꿔도 따로 고칠 값이 없다.
 * 대사는 이 스케줄 스레드에서 동기로 돈다 (스케줄 풀 3 — 발행기 · 정리 작업과 나눠 쓴다). 테스트는 이 빈을 끄고 실행기를 직접 부른다.
 */
class DailyReconScheduler(
    /** 실행기 (테스트는 함수로 바꿔 끼운다). */
    private val launch: (LocalDate, ReconTrigger) -> ReconRun,
    private val clock: Clock,
    cron: String,
    zone: String,
    private val alerts: AlertNotifier,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val cron = CronExpression.parse(cron)
    private val zone = ZoneId.of(zone)

    @Scheduled(cron = "\${daily-recon.schedule.cron:0 0 1-6 * * *}", zone = "\${daily-recon.schedule.zone:Asia/Seoul}")
    fun run() {
        val now = ZonedDateTime.ofInstant(clock.instant(), zone)
        val date = now.toLocalDate().minusDays(1)
        try {
            val result = launch(date, if (isFinalRound(now)) ReconTrigger.SCHEDULE_FINAL else ReconTrigger.SCHEDULE)
            log.info("daily reconciliation round {}: {}", now.toLocalTime(), result)
        } catch (e: Exception) {
            // 실행기는 실패를 결과로 돌려준다. 여기 오는 것은 시작조차 못 한 경우(DB 장애 등)다
            log.error("daily reconciliation {} could not be started", date, e)
            alerts.notifySafely(Alert(AlertLevel.CRITICAL, "일마감 대사 $date 를 시작하지 못함", listOf(e.javaClass.name)))
        }
    }

    internal fun isFinalRound(now: ZonedDateTime): Boolean = cron.next(now)?.toLocalDate() != now.toLocalDate()
}
