package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.StoreConflict
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 낙관적 락 전략 (P4-2): 락 없이 잔고 · version 을 읽고, version 이 그대로일 때만 쓴다. 바뀌었으면 트랜잭션 전체를 처음부터 다시.
 * 충돌은 훅(beforeWrite)에서 다른 트랜잭션이 version 을 올려 결정적으로 만든다. 재시도 대기는 주입한 sleeper 로 기록만 한다 (테스트가 실제로 자지 않게).
 */
class OptimisticWalletDebitTest : ApiIntegrationTest() {

    @Autowired private lateinit var transfers: TransferJpaRepository
    @Autowired private lateinit var outbox: OutboxEventJpaRepository
    @Autowired private lateinit var tx: TransactionTemplate
    @Autowired private lateinit var namedJdbc: NamedParameterJdbcTemplate
    @Autowired private lateinit var jdbc: JdbcTemplate

    private val references = RandomReferenceGenerator()
    private val sleeps = CopyOnWriteArrayList<Duration>()

    private fun storeWith(beforeWrite: (String) -> Unit) = JpaTransferStore(
        transfers, outbox, tx, wallets,
        OptimisticWalletDebit(
            namedJdbc, wallets, maxAttempts = 5, backoffMin = Duration.ofMillis(10), backoffMax = Duration.ofMillis(50),
            sleeper = { sleeps += it }, beforeWrite = beforeWrite,
        ),
    )

    /** 다른 트랜잭션(다른 스레드 · 다른 커넥션)이 지갑 version 을 올린다 — 환불 · 충전 · 다른 차감이 사이에 끼어든 것과 같다. */
    private fun bumpVersionElsewhere(userId: String) {
        CompletableFuture.runAsync { jdbc.update("UPDATE wallet SET version = version + 1 WHERE user_id = ?", userId) }
            .get(10, TimeUnit.SECONDS)
    }

    private fun newTransfer(userId: String, amount: Long = 10_000): Transfer {
        val now = Instant.now()
        return Transfer(
            id = UUID.randomUUID(), userId = userId, idempotencyKey = UUID.randomUUID().toString(), reference = references.next(),
            requestHash = "h".repeat(64), bankCode = "090", accountNo = unregisteredAccount(), amount = amount, memo = "낙관",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
    }

    private fun requested(t: Transfer) = TransferEvent.TransferRequested(t.id, t.createdAt)

    private fun transferCount(userId: String) = jdbc.queryForObject("SELECT COUNT(*) FROM transfer WHERE user_id = ?", Int::class.java, userId)!!

    @Test
    fun `첫 시도 사이에 다른 트랜잭션이 version 을 올리면 처음부터 다시 해 성공한다 (대기 1번)`() {
        val user = fundedUser(30_000)
        var calls = 0
        val t = newTransfer(user)

        storeWith { if (++calls == 1) bumpVersionElsewhere(it) }.saveWithEvent(t, requested(t))

        assertEquals(2, calls)
        assertEquals(1, sleeps.size)
        assertTrue(sleeps.all { it in Duration.ofMillis(10)..Duration.ofMillis(50) }, sleeps.toString())
        assertEquals(20_000L, wallets.balanceOf(user))
        assertEquals(listOf("WITHDRAW"), ledgerTypes(t.id))                   // 첫 시도의 출금 · 원장은 롤백됐다 — 한 번만 남는다
        assertEquals(1, transferCount(user))
        assertEquals(1, outbox.countByAggregateId(t.id.toString()))
        assertEquals(wallets.balanceOf(user), ledgerSum(user))
    }

    @Test
    fun `5번 모두 충돌하면 ConcurrentUpdate 이고 출금 · 원장 · 아웃박스 모두 남지 않는다 (대기 4번)`() {
        val user = fundedUser(30_000)
        var calls = 0
        val t = newTransfer(user)

        assertFailsWith<StoreConflict.ConcurrentUpdate> {
            storeWith { calls++; bumpVersionElsewhere(it) }.saveWithEvent(t, requested(t))
        }

        assertEquals(5, calls)
        assertEquals(4, sleeps.size)
        assertEquals(30_000L, wallets.balanceOf(user))
        assertNull(transfers.findById(t.id.toString()).orElse(null))
        assertEquals(emptyList(), ledgerTypes(t.id))
        assertEquals(0, outbox.countByAggregateId(t.id.toString()))
    }

    @Test
    fun `잔고가 모자라면 다시 하지 않고 InsufficientBalance (쓰기 전에 판단)`() {
        val user = fundedUser(9_999)
        var calls = 0
        val t = newTransfer(user)

        assertFailsWith<StoreConflict.InsufficientBalance> { storeWith { calls++ }.saveWithEvent(t, requested(t)) }

        assertEquals(0, calls)
        assertEquals(0, sleeps.size)
        assertEquals(9_999L, wallets.balanceOf(user))
    }
}
