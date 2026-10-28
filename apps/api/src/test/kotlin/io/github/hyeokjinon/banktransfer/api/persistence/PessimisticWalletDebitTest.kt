package io.github.hyeokjinon.banktransfer.api.persistence

import com.zaxxer.hikari.HikariDataSource
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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** 비관적 락 전략 (P4-2): SELECT … FOR UPDATE 로 지갑 행을 잡고, 읽은 값에서 빼 쓴다. 락 대기 한도를 넘으면 WalletBusy(503). */
class PessimisticWalletDebitTest : ApiIntegrationTest() {

    @Autowired private lateinit var transfers: TransferJpaRepository
    @Autowired private lateinit var outbox: OutboxEventJpaRepository
    @Autowired private lateinit var tx: TransactionTemplate
    @Autowired private lateinit var namedJdbc: NamedParameterJdbcTemplate
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var dataSource: DataSource

    private val references = RandomReferenceGenerator()
    private val pessimistic by lazy { PessimisticWalletDebit(namedJdbc, wallets) }
    private val store by lazy { JpaTransferStore(transfers, outbox, tx, wallets, pessimistic) }

    private fun newTransfer(userId: String, amount: Long = 10_000): Transfer {
        val now = Instant.now()
        return Transfer(
            id = UUID.randomUUID(), userId = userId, idempotencyKey = UUID.randomUUID().toString(), reference = references.next(),
            requestHash = "h".repeat(64), bankCode = "090", accountNo = unregisteredAccount(), amount = amount, memo = "비관",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
    }

    private fun requested(t: Transfer) = TransferEvent.TransferRequested(t.id, t.createdAt)

    @Test
    fun `모든 커넥션의 락 대기 한도는 5초, 풀은 20개 - 네 전략에 같은 조건`() {
        assertEquals(5L, jdbc.queryForObject("SELECT @@SESSION.innodb_lock_wait_timeout", Long::class.java))
        assertEquals(20, (dataSource as HikariDataSource).maximumPoolSize)
    }

    @Test
    fun `FOR UPDATE 로 읽은 잔고에서 빼고 version 을 올리며, 원장 balance_after 는 그 값`() {
        val user = fundedUser(30_000)
        val t = newTransfer(user)

        store.saveWithEvent(t, requested(t))

        assertEquals(20_000L, wallets.balanceOf(user))
        assertEquals(1L, jdbc.queryForObject("SELECT version FROM wallet WHERE user_id = ?", Long::class.java, user))   // 첫 충전으로 만들 때 0
        assertEquals(
            20_000L,
            jdbc.queryForObject("SELECT balance_after FROM wallet_ledger WHERE transfer_id = ? AND type = 'WITHDRAW'", Long::class.java, t.id.toString()),
        )
        assertEquals(1, outbox.countByAggregateId(t.id.toString()))
    }

    @Test
    fun `잔고가 모자라거나 지갑이 없으면 InsufficientBalance 이고 아무것도 남지 않는다`() {
        val poor = fundedUser(9_999)
        val short = newTransfer(poor)
        val noWallet = newTransfer(newUserId())

        assertFailsWith<StoreConflict.InsufficientBalance> { store.saveWithEvent(short, requested(short)) }
        assertFailsWith<StoreConflict.InsufficientBalance> { store.saveWithEvent(noWallet, requested(noWallet)) }

        assertEquals(9_999L, wallets.balanceOf(poor))
        assertNull(store.findById(short.id))
        assertNull(store.findById(noWallet.id))
        assertEquals(emptyList(), ledgerTypes(short.id))
    }

    @Test
    fun `다른 트랜잭션이 지갑 행을 잡고 놓지 않으면 락 대기 한도 뒤 WalletBusy, 출금 · 원장 · 아웃박스 없음`() {
        val user = fundedUser(30_000)
        val locked = CountDownLatch(1)
        val release = CountDownLatch(1)
        val holder = CompletableFuture.runAsync {
            tx.execute {
                jdbc.queryForObject("SELECT balance FROM wallet WHERE user_id = ? FOR UPDATE", Long::class.java, user)
                locked.countDown()
                release.await(30, TimeUnit.SECONDS)
            }
        }
        // 락 대기 한도를 이 트랜잭션의 커넥션만 1초로 줄인다 (테스트를 5초 기다리지 않게). 1205 → CannotAcquireLockException → WalletBusy
        // 경로는 5초일 때와 같고, 5초 설정 자체는 `모든 커넥션의 …` 테스트가 본다. 커넥션은 풀로 돌아가므로 끝나면 5초로 되돌린다
        val shortWait = object : WalletDebit {
            override fun withdraw(userId: String, transferId: UUID, amount: Long, at: Instant): Boolean {
                namedJdbc.jdbcOperations.execute("SET SESSION innodb_lock_wait_timeout = 1")
                try {
                    return pessimistic.withdraw(userId, transferId, amount, at)
                } finally {
                    namedJdbc.jdbcOperations.execute("SET SESSION innodb_lock_wait_timeout = 5")
                }
            }
        }
        val t = newTransfer(user)
        try {
            assertTrue(locked.await(10, TimeUnit.SECONDS))
            val started = System.nanoTime()

            assertFailsWith<StoreConflict.WalletBusy> { JpaTransferStore(transfers, outbox, tx, wallets, shortWait).saveWithEvent(t, requested(t)) }

            val waited = Duration.ofNanos(System.nanoTime() - started)
            assertTrue(waited >= Duration.ofMillis(900) && waited < Duration.ofSeconds(4), "waited $waited")
        } finally {
            release.countDown()
            holder.get(30, TimeUnit.SECONDS)
        }
        assertNull(store.findById(t.id))
        assertEquals(emptyList(), ledgerTypes(t.id))
        assertEquals(0, outbox.countByAggregateId(t.id.toString()))
        assertEquals(30_000L, wallets.balanceOf(user))
    }
}
