package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.batch.DatedJobRunningException
import org.springframework.batch.core.job.parameters.JobParameters
import org.springframework.batch.core.job.parameters.JobParametersBuilder
import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

/** 보존 배치의 이름 · 상수 (P5-2). 운영 문서 · 테스트가 이름을 여기서 본다. */
object DataRetention {
    const val JOB_NAME = "dataRetentionJob"
    const val RETENTION_DATE = "retentionDate"

    const val PURGE_OUTBOX = "purgeOutbox"
    const val PURGE_VERIFICATIONS = "purgeVerifications"

    /** 날짜마다 잡 인스턴스 하나 (식별 파라미터 retentionDate). */
    fun parameters(date: LocalDate): JobParameters = JobParametersBuilder().addLocalDate(RETENTION_DATE, date).toJobParameters()
}

/** 무엇을 언제부터 지워도 되나 (P5-2, 설계 §4). 시계를 보지 않고 날짜로만 정해 재시작해도 기준이 같다. */
object RetentionPolicy {
    private val KST: ZoneId = ZoneId.of("Asia/Seoul")

    /** 이 시각보다 이전이면 지운다. [date] 00:00(KST) − [retention]. */
    fun cutoff(date: LocalDate, retention: Duration): Instant = date.atStartOfDay(KST).toInstant().minus(retention)

    /**
     * 수동 실행이 받는 날짜: 오늘과 지난 날(KST). 내일 이후를 받으면 기준 시각이 앞당겨져 정책보다 일찍 지우게 된다.
     * 지난 날은 기준이 더 과거라 지우는 범위가 좁아질 뿐이다.
     */
    fun requireNotFuture(date: LocalDate, now: Instant) {
        val today = LocalDate.ofInstant(now, KST)
        if (date.isAfter(today)) throw RetentionDateInFutureException(date, today)
    }
}

class RetentionRunningException(date: LocalDate) : DatedJobRunningException("data retention for $date is already running")

class RetentionAlreadyCompletedException(date: LocalDate) : RuntimeException("data retention for $date is already completed")

/** 내일 이후(KST)의 수동 실행 ([RetentionPolicy.requireNotFuture]). */
class RetentionDateInFutureException(date: LocalDate, today: LocalDate) :
    RuntimeException("retention date $date is in the future (today is $today, KST)")

/**
 * 보존 배치 설정 (P5-2, 설계 §5). 범위를 벗어나면 기동에 실패한다 — 0d 같은 실수로 방금 발행한 행이나 막 만료된 인증을 지우는 일을 막는다.
 * 7일 · 30일은 법이 아니라 이 프로젝트의 운영 판단이다 (database.md §4-9).
 */
@ConfigurationProperties("data-retention")
data class DataRetentionProperties(
    /** 발행 뒤 이만큼 지난 outbox_event 를 지운다. 브로커 기본 보존과 같아 그동안 Kafka 메시지와 대조할 수 있다. */
    val outboxRetention: Duration = Duration.ofDays(7),
    /** 만료(expires_at) 뒤 이만큼 지난 VERIFIED 가 아닌 account_verification 을 지운다. */
    val verificationRetention: Duration = Duration.ofDays(30),
    /** 트랜잭션 하나에 지우는 최대 행 수. */
    val batchSize: Int = 1000,
    val schedule: Schedule = Schedule(),
) {
    init {
        require(outboxRetention >= MIN_RETENTION) { "data-retention.outbox-retention must be at least $MIN_RETENTION: $outboxRetention" }
        require(verificationRetention >= MIN_RETENTION) { "data-retention.verification-retention must be at least $MIN_RETENTION: $verificationRetention" }
        require(batchSize in 1..MAX_BATCH_SIZE) { "data-retention.batch-size must be within 1..$MAX_BATCH_SIZE: $batchSize" }
    }

    data class Schedule(
        val enabled: Boolean = true,
        /** 매일 06:30 — 대사 마지막 회차(06:00) 뒤. 재시도 회차는 없다: 다음 날 실행이 밀린 행까지 지운다. */
        val cron: String = "0 30 6 * * *",
        val zone: String = "Asia/Seoul",
    )

    private companion object {
        val MIN_RETENTION: Duration = Duration.ofDays(1)
        const val MAX_BATCH_SIZE = 10_000
    }
}
