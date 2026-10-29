package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.FILE_INVALID
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.FILE_NOT_READY
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.KST
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.RECON_DATE
import io.github.hyeokjinon.banktransfer.protocol.ProtocolException
import io.github.hyeokjinon.banktransfer.protocol.ReconFileFormat
import io.github.hyeokjinon.banktransfer.protocol.ReconLine
import io.github.hyeokjinon.banktransfer.transfer.BankStatementLine
import io.github.hyeokjinon.banktransfer.transfer.ReconIssueType
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict
import io.github.hyeokjinon.banktransfer.transfer.Settlement
import org.slf4j.LoggerFactory
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.StepContribution
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.infrastructure.item.Chunk
import org.springframework.batch.infrastructure.item.ExecutionContext
import org.springframework.batch.infrastructure.item.ItemStreamWriter
import org.springframework.batch.infrastructure.item.support.AbstractItemCountingItemStreamItemReader
import org.springframework.batch.infrastructure.repeat.RepeatStatus
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.LocalDate
import java.util.EnumMap
import java.util.UUID

/** ① 이 판정한 대사 파일 문제 (P5-1). [code] 가 실패 이유로 그대로 나간다. */
sealed class ReconFileException(val code: String, message: String) : RuntimeException(message)

/** 파일이 아직 없다 — 은행 파일은 보통 00:05 에 온다. 다음 정각에 다시 한다. */
class ReconFileNotReadyException(date: LocalDate) : ReconFileException(FILE_NOT_READY, "recon file for $date is not there yet")

/** 파일이 있지만 믿을 수 없다 (날짜 · 건수 · 합계 · 길이). 사람이 봐야 한다. */
class ReconFileInvalidException(date: LocalDate, reason: String) : ReconFileException(FILE_INVALID, "recon file for $date is invalid: $reason")

internal fun reconDateOf(chunkContext: ChunkContext): LocalDate =
    checkNotNull(chunkContext.stepContext.stepExecution.jobParameters.getLocalDate(RECON_DATE)) { "job parameter $RECON_DATE is missing" }

/** ① 대사 파일이 있고 형식이 맞는지 (헤더 날짜 = D, 트레일러 건수 · 합계 = 상세). 헤더의 은행코드는 기록만 하고 보지 않는다. */
class ValidateFileTasklet(private val props: DailyReconProperties) : Tasklet {

    override fun execute(contribution: StepContribution, chunkContext: ChunkContext): RepeatStatus {
        val date = reconDateOf(chunkContext)
        val file = props.reconFile(date)
        if (!Files.isRegularFile(file)) throw ReconFileNotReadyException(date)
        try {
            ReconFileFormat.read(Files.readAllBytes(file), date)
        } catch (e: ProtocolException) {
            throw ReconFileInvalidException(date, e.message ?: "unreadable")
        }
        return RepeatStatus.FINISHED
    }
}

/**
 * ② 파일 D 의 상세를 읽는다 (1원인증 V 줄도 적재한다 — 대조하지 않고 건수만 보고). ① 이 검증했지만 열 때 다시 검증한다
 * (그 사이 파일이 바뀌었으면 여기서 실패). 재시작하면 읽은 건수만큼 건너뛴다 (AbstractItemCountingItemStreamItemReader).
 */
class StatementFileReader(private val file: Path, private val reconDate: LocalDate) : AbstractItemCountingItemStreamItemReader<BankStatementLine>() {
    private var lines: List<BankStatementLine> = emptyList()

    init {
        setName("statementFileReader")
    }

    override fun doOpen() {
        lines = ReconFileFormat.read(Files.readAllBytes(file), reconDate).lines.map { it.toStatementLine(reconDate) }
    }

    override fun doRead(): BankStatementLine? = lines.getOrNull(currentItemCount - 1)

    override fun doClose() {
        lines = emptyList()
    }
}

/** 파일의 처리시각(HHmmss, KST)을 대사일의 Instant 로. */
internal fun ReconLine.toStatementLine(bankDate: LocalDate) = BankStatementLine(
    bankDate = bankDate,
    lineNo = lineNo,
    reference = detail.reference,
    bankCode = detail.bankCode,
    accountNo = detail.accountNo,
    amount = detail.amount,
    processedAt = bankDate.atTime(detail.processedAt).atZone(KST).toInstant(),
)

/** ④ 의 한 건: 파일 D 의 T 줄 중 ③ 이 다루지 않은 거래번호 (거래번호마다 첫 줄 하나)와 그 거래번호의 우리 출금 id (없으면 null). */
data class BankLineRef(val reference: String, val transferId: UUID?)

/** 판정별 건수. 단계의 ExecutionContext 에 `verdict.<판정>` 으로 저장한다 — 청크가 커밋될 때만 저장되므로 재시작해도 두 번 세지 않는다. */
class VerdictCounts {
    private val counts = EnumMap<ReconVerdict, Int>(ReconVerdict::class.java)

    fun add(verdict: ReconVerdict) {
        counts[verdict] = this[verdict] + 1
    }

    operator fun get(verdict: ReconVerdict): Int = counts[verdict] ?: 0

    val total: Int get() = counts.values.sum()

    fun saveTo(context: ExecutionContext) = ReconVerdict.entries.forEach { context.putInt(key(it), this[it]) }

    fun restore(context: ExecutionContext) {
        counts.clear()
        ReconVerdict.entries.forEach { verdict -> context.getInt(key(verdict), 0).takeIf { it > 0 }?.let { counts[verdict] = it } }
    }

    companion object {
        fun key(verdict: ReconVerdict) = "verdict.${verdict.name}"

        fun from(context: ExecutionContext?): VerdictCounts = VerdictCounts().also { counts -> context?.let { counts.restore(it) } }
    }
}

/**
 * ③ · ④ 의 쓰기: 한 건마다 [settle] 로 판정 · (미확정만) 확정하고, 이슈가 있으면 recon_issue 에 남긴다 (같은 것은 무시).
 * 확정 · 환불 · 이슈는 모두 청크 트랜잭션에 합류한다 — 청크가 실패하면 함께 롤백되고 재시작 때 같은 결과로 다시 쓴다 (이중 환불 없음).
 * 판정별 건수는 [ItemStreamWriter.update] 로 청크마다 단계 ExecutionContext 에 저장한다.
 */
class SettlingWriter<T : Any>(
    private val reconDate: LocalDate,
    private val repo: ReconRepository,
    private val clock: Clock,
    private val settle: (T) -> Settlement,
) : ItemStreamWriter<T> {
    private val counts = VerdictCounts()

    override fun open(executionContext: ExecutionContext) = counts.restore(executionContext)

    override fun update(executionContext: ExecutionContext) = counts.saveTo(executionContext)

    override fun write(chunk: Chunk<out T>) {
        for (item in chunk.items) {
            val settlement = settle(item)
            counts.add(settlement.verdict)
            settlement.verdict.issue?.let { repo.insertIssue(reconIssueOf(reconDate, it, settlement), clock.instant()) }
        }
    }
}

/**
 * 판정 하나를 이슈 한 줄로. our_value = 판정 시점의 우리 상태 · 금액 · 계좌, bank_value = 이 거래번호의 은행 줄들(날짜#줄).
 * DB 에는 계좌 원문을 둔다(운영자가 찾는다) — 알림으로 나갈 때 AlertText 가 가린다.
 */
fun reconIssueOf(reconDate: LocalDate, type: ReconIssueType, settlement: Settlement): ReconIssue {
    val transfer = settlement.transfer
    return ReconIssue(
        reconDate = reconDate,
        type = type,
        reference = transfer?.reference ?: settlement.lines.first().reference,
        transferId = transfer?.id,
        userId = transfer?.userId,
        ourValue = transfer?.let { "${it.status} ${it.amount} ${it.bankCode}-${it.accountNo}" },
        bankValue = settlement.lines.takeIf { it.isNotEmpty() }
            ?.joinToString("; ") { "${it.amount} ${it.bankCode}-${it.accountNo} ${it.bankDate}#${it.lineNo}" },
    )
}

/** ⑤ 잔고 ≠ 원장 합인 지갑을 이슈로 (대사일과 무관하게 지금 전체). 고칠 때까지 날마다 다시 나온다. */
class CheckLedgerTasklet(private val repo: ReconRepository, private val clock: Clock) : Tasklet {

    override fun execute(contribution: StepContribution, chunkContext: ChunkContext): RepeatStatus {
        val date = reconDateOf(chunkContext)
        for (mismatch in repo.ledgerMismatches()) {
            repo.insertIssue(
                ReconIssue(date, ReconIssueType.BALANCE_LEDGER_MISMATCH, userId = mismatch.userId, ourValue = "balance=${mismatch.balance}", bankValue = "ledgerSum=${mismatch.ledgerSum}"),
                clock.instant(),
            )
        }
        return RepeatStatus.FINISHED
    }
}

/** ⑥ 단계별 건수 · recon_issue 로 요약을 만들어 알린다. 알림 실패가 대사를 실패시키거나 되돌리지 않는다 — 기록의 원본은 recon_issue. */
class ReportTasklet(private val summaries: ReconSummaryReader, private val alerts: AlertNotifier) : Tasklet {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun execute(contribution: StepContribution, chunkContext: ChunkContext): RepeatStatus {
        val date = reconDateOf(chunkContext)
        val summary = summaries.read(chunkContext.stepContext.stepExecution.jobExecution.jobInstance, date)
        log.info("daily reconciliation {} completed: {}", date, summary)
        alerts.notifySafely(summaries.completionAlert(summary))
        return RepeatStatus.FINISHED
    }
}
