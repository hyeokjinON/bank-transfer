package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.protocol.ReconDetail
import io.github.hyeokjinon.banktransfer.protocol.ReconFileFormat
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.repository.JobRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.nio.file.Files
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * 대사 테스트 공용 (P5-1, 공유 DB). 테스트마다 [ReconTestDates.unique] 로 다른 대사일을 쓰고, 그날 보낸 출금을 심고, 은행 파일을 쓴다.
 * 심은 출금은 아웃박스를 발행된 것으로 표시한다 — 다른 테스트의 발행기가 집어 가 은행으로 보내지 않게.
 */
abstract class DailyReconTestSupport : ApiIntegrationTest() {

    @Autowired protected lateinit var store: TransferStore
    @Autowired protected lateinit var jdbc: JdbcTemplate
    @Autowired protected lateinit var props: DailyReconProperties
    @Autowired protected lateinit var jobOperator: JobOperator
    @Autowired protected lateinit var job: Job
    @Autowired protected lateinit var jobRepository: JobRepository

    private val references = RandomReferenceGenerator()

    /** 대사일 [date] 의 [hour] 시 (KST). */
    protected fun at(date: LocalDate, hour: Int = 10): Instant = date.atTime(hour, 0).atZone(DailyRecon.KST).toInstant()

    /**
     * [status] 인 출금 하나를 [sentAt] 에 은행에 보낸 것으로 심는다 (접수 차감 · WITHDRAW 포함, FAILED 는 환불까지).
     * RECEIVED 는 "보내려다 못 보냄"(sending_at 있음). 계좌는 모의 은행에 없는 번호 — 혹시 처리돼도 공용 계좌에 입금이 생기지 않는다.
     */
    protected fun seed(user: String, status: TransferStatus, sentAt: Instant, amount: Long = 10_000, id: UUID = UUID.randomUUID()): Transfer {
        val transfer = Transfer(
            id = id, userId = user, idempotencyKey = UUID.randomUUID().toString(), reference = references.next(), requestHash = "h".repeat(64),
            bankCode = "090", accountNo = unregisteredAccount(), amount = amount, memo = "대사",
            status = TransferStatus.RECEIVED, createdAt = sentAt, updatedAt = sentAt,
        )
        store.saveWithEvent(transfer, TransferEvent.TransferRequested(transfer.id, sentAt))
        jdbc.update("UPDATE outbox_event SET published_at = NOW(6) WHERE aggregate_id = ?", transfer.id.toString())
        store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.SENDING, sentAt)
        when (status) {
            TransferStatus.RECEIVED -> store.transition(transfer.id, TransferStatus.SENDING, TransferStatus.RECEIVED, sentAt)
            TransferStatus.SENDING -> Unit
            TransferStatus.UNKNOWN -> store.transition(transfer.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, sentAt)
            TransferStatus.COMPLETED -> store.transition(transfer.id, TransferStatus.SENDING, TransferStatus.COMPLETED, sentAt, "0000")
            TransferStatus.FAILED -> store.fail(transfer.id, TransferStatus.SENDING, sentAt, "1001")
        }
        return store.findById(transfer.id)!!
    }

    /** 은행이 이 출금을 처리했다는 상세 한 줄 (금액만 바꿔 불일치를 만든다). */
    protected fun lineOf(transfer: Transfer, amount: Long = transfer.amount) =
        ReconDetail(transfer.reference, transfer.bankCode, transfer.accountNo, amount, LocalTime.of(10, 0, 1))

    protected fun writeBankFile(date: LocalDate, details: List<ReconDetail>) {
        val file = props.reconFile(date)
        Files.createDirectories(file.parent)
        Files.write(file, ReconFileFormat.write(date, "090", details))
    }

    protected fun runJob(date: LocalDate): JobExecution = jobOperator.start(job, DailyRecon.parameters(date))

    protected fun status(transfer: Transfer): TransferStatus = store.findById(transfer.id)!!.status

    /**
     * [date] 의 잡을 [start] 로 다른 스레드에서 시작해 ⑥ 보고(완료 알림)에서 붙잡아 둔 채 [whileRunning] 을 부르고, 끝나면 놓아준다
     * — 잡이 돌고 있는 동안 두 번째 실행이 오는 경우. 돌려주는 값 = (붙잡았던 실행의 결과, whileRunning 의 결과).
     */
    protected fun <S, T> whileReportHeld(date: LocalDate, start: () -> S, whileRunning: () -> T): Pair<S, T> {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        recordedAlerts.onNotify = { if (date.toString() in it.title) { entered.countDown(); release.await(20, TimeUnit.SECONDS) } }
        try {
            val held = CompletableFuture.supplyAsync(start)
            check(entered.await(20, TimeUnit.SECONDS)) { "the job for $date did not reach its report step" }
            val result = whileRunning()
            release.countDown()
            return held.get(20, TimeUnit.SECONDS) to result
        } finally {
            release.countDown()
            recordedAlerts.onNotify = null
        }
    }

    /** 이슈 종류 → 거래번호 목록 (지갑 이슈는 null). */
    protected fun issueRefs(date: LocalDate): Map<String, List<String?>> =
        jdbc.queryForList("SELECT type, reference FROM recon_issue WHERE recon_date = ? ORDER BY id", date)
            .groupBy({ it["type"] as String }, { it["reference"] as String? })
}
