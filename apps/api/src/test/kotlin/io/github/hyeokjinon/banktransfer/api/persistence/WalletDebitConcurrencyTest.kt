package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.concurrently
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.StoreConflict
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 차감 전략마다 같은 정확성 기준 (P4-2 스펙 §6): 잔고 10만 원에 1만 원 출금 100건이 동시에 와도
 * 잔고는 음수가 되지 않고, 잔고 = 원장 합, 출금 행 수 = 원장 WITHDRAW 수 = 성공 수. 측정은 이 테스트가 통과한 전략만 한다.
 * 전략마다 컨텍스트를 띄우면 MySQL · Kafka 컨테이너가 전략 수만큼 뜬다 → 캐시된 컨텍스트 하나에서 저장소를 전략별로 직접 만든다.
 * 조건부 · 비관적은 진 쪽이 기다렸다가 바뀐 잔고를 보므로 정확히 10건, 낙관적 · Redis 는 409 · 503 으로 포기하는 건이 있어 1~10건이다.
 * 동시성 테스트는 확률적이다 — 실제로 경합이 났는지는 Task 3 의 변이 검사(FOR UPDATE 빼기)로 한 번 확인한다.
 * 계좌는 모의 은행에 없는 번호다: 남은 RECEIVED 를 나중에 다른 테스트가 처리해도 1001 거절 → 이 사용자에게 환불될 뿐이다.
 */
class WalletDebitConcurrencyTest : ApiIntegrationTest() {

    @Autowired private lateinit var transfers: TransferJpaRepository
    @Autowired private lateinit var outbox: OutboxEventJpaRepository
    @Autowired private lateinit var tx: TransactionTemplate
    @Autowired private lateinit var namedJdbc: NamedParameterJdbcTemplate
    @Autowired private lateinit var jdbc: JdbcTemplate

    private val references = RandomReferenceGenerator()

    private fun debitFor(strategy: String): WalletDebit = when (strategy) {
        "conditional" -> ConditionalWalletDebit(namedJdbc, wallets)
        "pessimistic" -> PessimisticWalletDebit(namedJdbc, wallets)
        "optimistic" -> OptimisticWalletDebit(namedJdbc, wallets)
        else -> error("unknown strategy $strategy")
    }

    private fun newTransfer(userId: String, accountNo: String): Transfer {
        val now = Instant.now()
        return Transfer(
            id = UUID.randomUUID(), userId = userId, idempotencyKey = UUID.randomUUID().toString(), reference = references.next(),
            requestHash = "h".repeat(64), bankCode = "090", accountNo = accountNo, amount = 10_000, memo = "동시",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
    }

    private fun count(sql: String, userId: String): Int = jdbc.queryForObject(sql, Int::class.java, userId)!!

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = ["conditional", "pessimistic", "optimistic"])
    fun `잔고 10만 원에 1만 원 출금 100건 동시 - 음수 없음, 잔고 = 원장 합, 출금 = WITHDRAW = 성공 수`(strategy: String) {
        val store = JpaTransferStore(transfers, outbox, tx, wallets, debitFor(strategy))
        val user = fundedUser(100_000)
        val account = unregisteredAccount()

        val outcomes = concurrently(100) {
            val t = newTransfer(user, account)
            try {
                store.saveWithEvent(t, TransferEvent.TransferRequested(t.id, t.createdAt))
                "OK"
            } catch (e: StoreConflict.InsufficientBalance) {
                "INSUFFICIENT_BALANCE"
            } catch (e: StoreConflict.ConcurrentUpdate) {
                "CONCURRENT_UPDATE"
            } catch (e: StoreConflict.WalletBusy) {
                "WALLET_BUSY"
            }
        }

        // 그 밖의 예외(= API 라면 500)는 concurrently 가 그대로 던져 여기까지 오지 않는다 → 성공 + 422 + 409 + 503 = 100
        val counts = outcomes.groupingBy { it }.eachCount()
        val succeeded = counts["OK"] ?: 0
        assertEquals(100, outcomes.size)
        assertTrue(succeeded in 1..10, "$strategy: $counts")
        if (strategy in EXACTLY_TEN) assertEquals(10, succeeded, "$strategy: $counts")
        val balance = wallets.balanceOf(user)
        assertEquals(100_000L - 10_000L * succeeded, balance, "$strategy: $counts")
        assertTrue(balance >= 0)
        assertEquals(balance, ledgerSum(user))
        assertEquals(succeeded, count("SELECT COUNT(*) FROM transfer WHERE user_id = ?", user))
        assertEquals(succeeded, count("SELECT COUNT(*) FROM wallet_ledger WHERE user_id = ? AND type = 'WITHDRAW'", user))
    }

    private companion object {
        /** 진 쪽이 기다렸다가 바뀐 잔고를 보는 전략 — 포기가 없으니 정확히 잔고 몫(10건)만 성공한다. */
        val EXACTLY_TEN = setOf("conditional", "pessimistic")
    }
}
