package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.CHECK_LEDGER
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.JOB_NAME
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.LOAD_CHUNK_SIZE
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.LOAD_STATEMENT
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.MATCH_BANK_LINES
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.MATCH_TRANSFERS
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.RECON_DATE
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.REPORT
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.VALIDATE_FILE
import io.github.hyeokjinon.banktransfer.transfer.BankStatementLine
import io.github.hyeokjinon.banktransfer.transfer.StatementSettler
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.job.parameters.DefaultJobParametersValidator
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.infrastructure.item.ItemStreamReader
import org.springframework.batch.infrastructure.item.ItemStreamWriter
import org.springframework.batch.infrastructure.item.database.Order
import org.springframework.batch.infrastructure.item.database.builder.JdbcPagingItemReaderBuilder
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.interceptor.DefaultTransactionAttribute
import java.sql.Timestamp
import java.time.Clock
import java.time.LocalDate
import java.util.UUID
import javax.sql.DataSource

/**
 * 일마감 대사 잡 (P5-1, 설계 §5). API 앱 안의 Spring Batch — 확정 · 환불에 기존 TransferStore 경로를 그대로 쓴다.
 * ① validateFile ② loadStatement(청크 500) ③ matchTransfers(청크 100) ④ matchBankLines(청크 100) ⑤ checkLedger ⑥ report.
 * 잡 저장소(JDBC)가 날짜마다 인스턴스 하나를 두어 끝난 날짜는 거부하고, 실패하면 멈춘 단계 · 마지막 커밋 청크부터 잇는다.
 *
 * Spring Batch 6 주의 (계획의 "버전 · API 확인 결과" 6 · 8):
 * - chunk(size) 의 트랜잭션 관리자 기본값은 ResourcelessTransactionManager(트랜잭션 없음)다 → 모든 단계에 JPA 의 transactionManager 를 넘긴다.
 * - 스텝 범위(@StepScope) 빈은 CGLIB 프록시라 Kotlin(final) 클래스를 그대로 돌려줄 수 없다 → 반환형을 인터페이스(ItemStreamReader/Writer)로 둔다.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DailyReconProperties::class)
class DailyReconJobConfiguration {

    @Bean
    fun reconRepository(jdbc: NamedParameterJdbcTemplate) = ReconRepository(jdbc)

    @Bean
    fun statementSettler(store: TransferStore, clock: Clock) = StatementSettler(store, clock)

    @Bean
    fun reconSummaryReader(jobRepository: JobRepository, repo: ReconRepository) = ReconSummaryReader(jobRepository, repo)

    @Bean
    fun dailyReconciliationJob(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager,
        props: DailyReconProperties,
        repo: ReconRepository,
        summaries: ReconSummaryReader,
        alerts: AlertNotifier,
        clock: Clock,
        @Qualifier("statementFileReader") statementFile: ItemStreamReader<BankStatementLine>,
        @Qualifier("transferIdReader") transferIds: ItemStreamReader<UUID>,
        @Qualifier("transferSettlingWriter") transferWriter: ItemStreamWriter<UUID>,
        @Qualifier("bankLineReader") bankLines: ItemStreamReader<BankLineRef>,
        @Qualifier("bankLineSettlingWriter") bankLineWriter: ItemStreamWriter<BankLineRef>,
    ): Job {
        // ③ ④ 는 READ COMMITTED: 조건부 전이가 0건이면(그 사이 정리 작업 등이 바꿈) 다시 읽어 판정하는데, MySQL 기본 REPEATABLE READ 는
        // 청크 첫 읽기(리더)의 스냅숏을 다시 보여 줘 바뀐 상태를 보지 못한다 (발행기와 같은 이유 — MessagingConfiguration)
        val readCommitted = DefaultTransactionAttribute().apply { isolationLevel = TransactionDefinition.ISOLATION_READ_COMMITTED }

        val validateFile = StepBuilder(VALIDATE_FILE, jobRepository).tasklet(ValidateFileTasklet(props), transactionManager).build()
        val loadStatement = StepBuilder(LOAD_STATEMENT, jobRepository).chunk<BankStatementLine, BankStatementLine>(LOAD_CHUNK_SIZE)
            .reader(statementFile)
            .writer { chunk -> repo.insertLines(chunk.items, clock.instant()) }
            .transactionManager(transactionManager)
            .build()
        val matchTransfers = StepBuilder(MATCH_TRANSFERS, jobRepository).chunk<UUID, UUID>(props.chunkSize)
            .reader(transferIds)
            .writer(transferWriter)
            .transactionManager(transactionManager)
            .transactionAttribute(readCommitted)
            .build()
        val matchBankLines = StepBuilder(MATCH_BANK_LINES, jobRepository).chunk<BankLineRef, BankLineRef>(props.chunkSize)
            .reader(bankLines)
            .writer(bankLineWriter)
            .transactionManager(transactionManager)
            .transactionAttribute(readCommitted)
            .build()
        val checkLedger = StepBuilder(CHECK_LEDGER, jobRepository).tasklet(CheckLedgerTasklet(repo, clock), transactionManager).build()
        val report = StepBuilder(REPORT, jobRepository).tasklet(ReportTasklet(summaries, alerts), transactionManager).build()

        return JobBuilder(JOB_NAME, jobRepository)
            .validator(DefaultJobParametersValidator(arrayOf(RECON_DATE), emptyArray()))
            .listener(SingleRunGuard(jobRepository))   // 실패한 날짜의 동시 재시작 — Spring Batch 6 의 시작 확인 사이의 틈을 막는다
            .start(validateFile)
            .next(loadStatement)
            .next(matchTransfers)
            .next(matchBankLines)
            .next(checkLedger)
            .next(report)
            .build()
    }

    /** ② 파일 D 의 상세. */
    @Bean
    @StepScope
    fun statementFileReader(
        props: DailyReconProperties,
        @Value("#{jobParameters['reconDate']}") reconDate: LocalDate,
    ): ItemStreamReader<BankStatementLine> = StatementFileReader(props.reconFile(reconDate), reconDate)

    /** ③ sending_at 이 D일(KST)인 출금 id, id 순 페이징. sending_at = 마지막으로 은행에 보내려 한 시각. */
    @Bean
    @StepScope
    fun transferIdReader(
        dataSource: DataSource,
        props: DailyReconProperties,
        @Value("#{jobParameters['reconDate']}") reconDate: LocalDate,
    ): ItemStreamReader<UUID> {
        val (from, to) = DailyRecon.dayRange(reconDate)
        return JdbcPagingItemReaderBuilder<UUID>()
            .name("transferIdReader")
            .dataSource(dataSource)
            .selectClause("id")
            .fromClause("transfer")
            // 시각은 Timestamp(Instant) 로 넘긴다 — KST 벽시계 변환은 JDBC 설정(connectionTimeZone)이 한다 (database.md §2)
            .whereClause("sending_at >= :from AND sending_at < :to")
            .parameterValues(mapOf<String, Any>("from" to Timestamp.from(from), "to" to Timestamp.from(to)))
            .sortKeys(mapOf("id" to Order.ASCENDING))
            .pageSize(props.chunkSize)
            .rowMapper { rs, _ -> UUID.fromString(rs.getString("id")) }
            .build()
    }

    /**
     * ③ 판정 · 확정. 거래번호로 적재된 모든 날짜의 줄을 찾는다.
     * 확정은 "sending_at 이 아직 D일 안" 을 같은 UPDATE 의 조건으로 건다 — 리더가 읽은 뒤 다시 보내진 출금(sending_at = 지금)을 E002 로 환불하지 않게 (ADR-0010).
     */
    @Bean
    @StepScope
    fun transferSettlingWriter(
        settler: StatementSettler,
        repo: ReconRepository,
        clock: Clock,
        @Value("#{jobParameters['reconDate']}") reconDate: LocalDate,
    ): ItemStreamWriter<UUID> {
        val (from, to) = DailyRecon.dayRange(reconDate)
        return SettlingWriter(reconDate, repo, clock) { id: UUID -> settler.settle(id, from..<to, repo::linesOf) }
    }

    /**
     * ④ 파일 D 의 1원인증(V)이 아닌 줄 중 ③ 이 다루지 않은 것: 우리에게 없는 거래번호, 다른 날 보낸(또는 보낸 적 없는) 출금.
     * T 로 시작하지 않는 낯선 거래번호도 여기서 UNKNOWN_REFERENCE 가 된다 — 은행이 내보낸 돈을 접두 때문에 놓치지 않게.
     * 같은 거래번호가 여러 줄이면(이중 지급) 첫 줄 하나만 읽는다 — 판정은 그 거래번호의 모든 줄로 한 번.
     */
    @Bean
    @StepScope
    fun bankLineReader(
        dataSource: DataSource,
        props: DailyReconProperties,
        @Value("#{jobParameters['reconDate']}") reconDate: LocalDate,
    ): ItemStreamReader<BankLineRef> {
        val (from, to) = DailyRecon.dayRange(reconDate)
        return JdbcPagingItemReaderBuilder<BankLineRef>()
            .name("bankLineReader")
            .dataSource(dataSource)
            .selectClause("l.line_no, l.reference, t.id AS transfer_id")
            .fromClause("bank_statement_line l LEFT JOIN transfer t ON t.reference = l.reference")
            .whereClause(
                "l.bank_date = :reconDate AND l.reference NOT LIKE 'V%' " +
                    "AND (t.id IS NULL OR t.sending_at IS NULL OR t.sending_at < :from OR t.sending_at >= :to) " +
                    "AND l.line_no = (SELECT MIN(f.line_no) FROM bank_statement_line f WHERE f.bank_date = l.bank_date AND f.reference = l.reference)",
            )
            .parameterValues(mapOf<String, Any>("reconDate" to reconDate, "from" to Timestamp.from(from), "to" to Timestamp.from(to)))
            .sortKeys(mapOf("line_no" to Order.ASCENDING))
            .pageSize(props.chunkSize)
            .rowMapper { rs, _ -> BankLineRef(rs.getString("reference"), rs.getString("transfer_id")?.let(UUID::fromString)) }
            .build()
    }

    /** ④ 판정 · 확정: 우리 출금이 있으면 ③ 과 같은 판정, 없으면 UNKNOWN_REFERENCE. */
    @Bean
    @StepScope
    fun bankLineSettlingWriter(
        settler: StatementSettler,
        repo: ReconRepository,
        clock: Clock,
        @Value("#{jobParameters['reconDate']}") reconDate: LocalDate,
    ): ItemStreamWriter<BankLineRef> = SettlingWriter(reconDate, repo, clock) { ref: BankLineRef ->
        val lines = repo.linesOf(ref.reference)
        ref.transferId?.let { id -> settler.settle(id) { lines } } ?: settler.unknownReference(lines)
    }

    @Bean
    fun dailyReconLauncher(jobOperator: JobOperator, job: Job, jobRepository: JobRepository, summaries: ReconSummaryReader, alerts: AlertNotifier) =
        DailyReconLauncher(jobOperator, job, jobRepository, summaries, alerts)

    @Bean
    @ConditionalOnProperty(prefix = "daily-recon.schedule", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun dailyReconScheduler(launcher: DailyReconLauncher, clock: Clock, props: DailyReconProperties, alerts: AlertNotifier) =
        DailyReconScheduler(launcher::run, clock, props.schedule.cron, props.schedule.zone, alerts)
}
