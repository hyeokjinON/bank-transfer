package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.transfer.BankStatementLine
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict
import io.github.hyeokjinon.banktransfer.transfer.Settlement
import io.github.hyeokjinon.banktransfer.transfer.StatementSettler
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * 경합 (P5-1, 설계 §10): 판정 직전에 다른 쪽(정리 작업)이 상태를 바꾸면 다시 읽어 바뀐 상태로 판정한다 — 실제 MySQL · JPA 저장소로.
 * ③ ④ 청크 트랜잭션을 READ COMMITTED 로 두는 이유도 같이 보인다: REPEATABLE READ 면 청크 첫 읽기의 스냅숏 때문에 바뀐 상태를 못 본다.
 */
class StatementSettlerIsolationTest : DailyReconTestSupport() {

    @Autowired private lateinit var transactionManager: PlatformTransactionManager

    /** 첫 확정 시도 직전에 다른 스레드 · 트랜잭션이 이 출금을 COMPLETED 로 확정하고 커밋한다 (결과조회 확정 흉내). */
    private fun racingStore(): TransferStore = object : TransferStore by store {
        private var raced = false

        override fun transition(
            id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?, sentWithin: OpenEndRange<Instant>?,
        ): Boolean {
            if (!raced) {
                raced = true
                CompletableFuture.runAsync { store.transition(id, TransferStatus.UNKNOWN, TransferStatus.COMPLETED, Instant.now(), "0000") }
                    .get(10, TimeUnit.SECONDS)
            }
            return store.transition(id, from, to, at, bankResponseCode, sentWithin)
        }
    }

    /**
     * 첫 실패 확정 직전에 다른 스레드 · 트랜잭션이 이 출금을 다시 보내고 커밋한다 (컨슈머의 SENDING → RECEIVED, 재발행 뒤 RECEIVED → SENDING).
     * 상태는 다시 SENDING 이지만 sending_at 은 [resentAt] 이다.
     */
    private fun resendingStore(resentAt: Instant): TransferStore = object : TransferStore by store {
        private var raced = false

        override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String, sentWithin: OpenEndRange<Instant>?): Boolean {
            if (!raced) {
                raced = true
                CompletableFuture.runAsync {
                    store.transition(id, TransferStatus.SENDING, TransferStatus.RECEIVED, resentAt)
                    store.transition(id, TransferStatus.RECEIVED, TransferStatus.SENDING, resentAt)
                }.get(10, TimeUnit.SECONDS)
            }
            return store.fail(id, from, at, bankResponseCode, sentWithin)
        }
    }

    /** 청크 트랜잭션 흉내: [isolation] 으로 열고, 첫 읽기로 스냅숏을 만든 뒤 판정한다. */
    private fun settleInChunk(isolation: Int, transferId: UUID, line: BankStatementLine): Settlement {
        val settler = StatementSettler(racingStore(), Clock.systemUTC())
        return TransactionTemplate(transactionManager).apply { isolationLevel = isolation }.execute {
            store.findById(transferId)
            settler.settle(transferId, null) { listOf(line) }
        }
    }

    private fun unknownWithLine(): Pair<UUID, BankStatementLine> {
        val day = ReconTestDates.unique()
        val transfer = seed(fundedUser(), TransferStatus.UNKNOWN, at(day))
        return transfer.id to BankStatementLine(day, 2, transfer.reference, transfer.bankCode, transfer.accountNo, transfer.amount, at(day))
    }

    @Test
    fun `READ COMMITTED - 확정이 0건이면 다시 읽어 바뀐 상태(COMPLETED)로 판정한다 = 일치, 두 번 확정하지 않는다`() {
        val (id, line) = unknownWithLine()

        val settlement = settleInChunk(TransactionDefinition.ISOLATION_READ_COMMITTED, id, line)

        assertEquals(ReconVerdict.MATCHED, settlement.verdict)
        assertEquals(TransferStatus.COMPLETED, settlement.transfer!!.status)
        assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
    }

    @Test
    fun `③ 판정 뒤 확정 전에 다시 보내지면(sending_at D+1) E002 실패가 0건 - 환불하지 않고 RESENT_LATER (실제 MySQL)`() {
        val day = ReconTestDates.unique()
        val user = fundedUser()
        val transfer = seed(user, TransferStatus.SENDING, at(day))
        val balance = wallets.balanceOf(user)
        val (from, to) = DailyRecon.dayRange(day)
        val settler = StatementSettler(resendingStore(at(day.plusDays(1), 1)), Clock.systemUTC())

        val settlement = TransactionTemplate(transactionManager).apply { isolationLevel = TransactionDefinition.ISOLATION_READ_COMMITTED }.execute {
            store.findById(transfer.id)
            settler.settle(transfer.id, from..<to) { emptyList() }
        }!!

        assertEquals(ReconVerdict.RESENT_LATER, settlement.verdict)
        val after = store.findById(transfer.id)!!
        assertEquals(TransferStatus.SENDING, after.status)                 // 은행이 지금 처리 중일 수 있는 출금을 실패로 확정하지 않았다
        assertEquals(at(day.plusDays(1), 1), after.sendingAt)
        assertEquals(listOf("WITHDRAW"), ledgerTypes(transfer.id))          // 환불 없음 — 이중 지급의 길이 막혔다
        assertEquals(balance, wallets.balanceOf(user))
    }

    @Test
    fun `REPEATABLE READ 였다면 스냅숏 때문에 바뀐 상태를 못 보고 3번 뒤 포기한다 - ③ ④ 를 READ COMMITTED 로 두는 이유`() {
        val (id, line) = unknownWithLine()

        assertFailsWith<IllegalStateException> { settleInChunk(TransactionDefinition.ISOLATION_REPEATABLE_READ, id, line) }
        assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)   // 다른 쪽의 확정은 그대로다
    }
}
