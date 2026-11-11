package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneId

/**
 * 매일 06:30(KST)에 오늘 날짜로 보존 배치를 돌린다 (P5-2, 설계 §4). 대사 마지막 회차(06:00) 뒤. 재시도 회차는 없다.
 * 이 스케줄 스레드에서 동기로 돈다 (스케줄 풀 4 — 밀린 행이 많은 첫 실행이 길어져도 발행기 · 정리 작업 · 대사를 굶기지 않게). 테스트는 이 빈을 끄고 실행기를 직접 부른다.
 * api 가 여러 대면 한 대만 돈다 (P8-2 ShedLock "data-retention", 최대 30분 · 최소 1분). open: ShedLock 이 CGLIB 프록시로 감싼다.
 */
open class DataRetentionScheduler(
    /** 실행기 (테스트는 함수로 바꿔 끼운다). */
    private val launch: (LocalDate) -> RetentionRun,
    private val clock: Clock,
    zone: String,
    private val alerts: AlertNotifier,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val zone = ZoneId.of(zone)

    @Scheduled(cron = "\${data-retention.schedule.cron:0 30 6 * * *}", zone = "\${data-retention.schedule.zone:Asia/Seoul}")
    @SchedulerLock(name = "data-retention", lockAtMostFor = "PT30M", lockAtLeastFor = "PT1M")
    open fun run() {
        val date = LocalDate.ofInstant(clock.instant(), zone)
        try {
            log.info("data retention round: {}", launch(date))
        } catch (e: Exception) {
            // 실행기는 실패를 결과로 돌려준다(알림도 거기서). 여기 오는 것은 시작조차 못 한 경우(DB 장애 등)다
            log.error("data retention {} could not be started", date, e)
            alerts.notifySafely(Alert(AlertLevel.WARN, "보존 배치 $date 실패", listOf("시작하지 못함: ${e.javaClass.name}")))
        }
    }
}
