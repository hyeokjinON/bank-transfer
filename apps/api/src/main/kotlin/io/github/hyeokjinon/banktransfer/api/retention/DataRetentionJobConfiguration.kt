package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.batch.DatedJobSingleRunGuard
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.job.parameters.DefaultJobParametersValidator
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.interceptor.DefaultTransactionAttribute
import java.time.Duration

/**
 * 보존 배치 조립 (P5-2, 설계 §4). ① purgeOutbox ② purgeVerifications.
 * 잡 저장소(JDBC)가 날짜마다 인스턴스 하나를 두어 끝난 날짜는 거부하고, 실패하면 멈춘 단계부터 잇는다.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DataRetentionProperties::class)
class DataRetentionJobConfiguration {

    @Bean
    fun retentionRepository(jdbc: NamedParameterJdbcTemplate) = RetentionRepository(jdbc)

    @Bean
    fun dataRetentionJob(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager,
        props: DataRetentionProperties,
        repo: RetentionRepository,
    ): Job = buildDataRetentionJob(jobRepository, transactionManager, props, repo::deleteOutboxEvents, repo::deleteVerifications)
}

/** 잡 모양. 삭제 함수를 받는 이유: 재시작 · 동시 실행 테스트가 같은 잡에 실패 · 멈춤을 끼운다 (행 락으로 만들면 묶음 순서가 옵티마이저에 달려 흔들린다). */
internal fun buildDataRetentionJob(
    jobRepository: JobRepository,
    transactionManager: PlatformTransactionManager,
    props: DataRetentionProperties,
    purgeOutbox: PurgeQuery,
    purgeVerifications: PurgeQuery,
): Job {
    fun purgeStep(name: String, retention: Duration, query: PurgeQuery) =
        StepBuilder(name, jobRepository)
            // 트랜잭션 관리자를 넘기지 않으면 Spring Batch 6 은 트랜잭션 없이 돈다 (P5-1 과 같은 함정)
            .tasklet(PurgeTasklet(retention, props.batchSize, query), transactionManager)
            .transactionAttribute(purgeTransaction())
            .build()

    return JobBuilder(DataRetention.JOB_NAME, jobRepository)
        .validator(DefaultJobParametersValidator(arrayOf(DataRetention.RETENTION_DATE), emptyArray()))
        // 실패한 날짜의 동시 재시작 — Spring Batch 6 의 시작 확인 사이의 틈을 막는다
        .listener(DatedJobSingleRunGuard(jobRepository, DataRetention.RETENTION_DATE, ::RetentionRunningException))
        .start(purgeStep(DataRetention.PURGE_OUTBOX, props.outboxRetention, purgeOutbox))
        .next(purgeStep(DataRetention.PURGE_VERIFICATIONS, props.verificationRetention, purgeVerifications))
        .build()
}

/**
 * READ COMMITTED: MySQL 기본 REPEATABLE READ 의 범위 삭제는 next-key 락으로 "마지막 미발행 행과 첫 발행 행 사이" 를 잠가,
 * 새 미발행 행(NULL, 가장 큰 id)을 넣는 출금 접수가 삭제 트랜잭션을 기다릴 수 있다. 발행기가 READ COMMITTED 인 이유와 같은 뿌리다 (MessagingConfiguration, 재현은 RetentionLockTest).
 *
 * rollbackOn 을 true 로 두는 이유: TaskletStep 의 기본 속성이 그렇다. DefaultTransactionAttribute 그대로면 검사 예외에 false 라
 * 단계가 그 예외를 삼키고 CONTINUABLE 로 같은 호출을 끝없이 돈다 (Spring Batch 6.0.5 TaskletStep.ChunkTransactionCallback).
 */
private fun purgeTransaction() = object : DefaultTransactionAttribute() {
    init {
        isolationLevel = TransactionDefinition.ISOLATION_READ_COMMITTED
    }

    override fun rollbackOn(ex: Throwable) = true
}
