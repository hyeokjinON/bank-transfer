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

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean {
            if (!raced) {
                raced = true
                CompletableFuture.runAsync { store.transition(id, TransferStatus.UNKNOWN, TransferStatus.COMPLETED, Instant.now(), "0000") }
                    .get(10, TimeUnit.SECONDS)
            }
            return store.transition(id, from, to, at, bankResponseCode)
        }
    }

    /** 청크 트랜잭션 흉내: [isolation] 으로 열고, 첫 읽기로 스냅숏을 만든 뒤 판정한다. */
    private fun settleInChunk(isolation: Int, transferId: UUID, line: BankStatementLine): Settlement {
        val settler = StatementSettler(racingStore(), Clock.systemUTC())
        return TransactionTemplate(transactionManager).apply { isolationLevel = isolation }.execute {
            store.findById(transferId)
            settler.settle(transferId) { listOf(line) }
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
    fun `REPEATABLE READ 였다면 스냅숏 때문에 바뀐 상태를 못 보고 3번 뒤 포기한다 - ③ ④ 를 READ COMMITTED 로 두는 이유`() {
        val (id, line) = unknownWithLine()

        assertFailsWith<IllegalStateException> { settleInChunk(TransactionDefinition.ISOLATION_REPEATABLE_READ, id, line) }
        assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)   // 다른 쪽의 확정은 그대로다
    }
}
