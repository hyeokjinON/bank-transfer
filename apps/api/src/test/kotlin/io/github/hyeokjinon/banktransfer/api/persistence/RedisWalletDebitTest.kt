package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.StoreConflict
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import org.redisson.Redisson
import org.redisson.api.RLock
import org.redisson.api.RedissonClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Redis 분산락 전략 (P4-2): 락은 DB 앞에서 같은 사용자 요청을 줄 세우는 문으로만 쓰고, 정확성은 안쪽의 조건부 UPDATE + CHECK 가 지킨다.
 * 락은 접수 트랜잭션 바깥에서 잡고 커밋이 끝난 뒤 푼다 — 먼저 풀면 다음 요청이 커밋 전 잔고를 보고 줄 세우기가 무너진다.
 */
class RedisWalletDebitTest : ApiIntegrationTest() {

    @Autowired private lateinit var transfers: TransferJpaRepository
    @Autowired private lateinit var outbox: OutboxEventJpaRepository
    @Autowired private lateinit var tx: TransactionTemplate
    @Autowired private lateinit var namedJdbc: NamedParameterJdbcTemplate
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var redisson: RedissonClient

    private val references = RandomReferenceGenerator()
    private val conditional by lazy { ConditionalWalletDebit(namedJdbc, wallets) }

    private fun storeWith(client: RedissonClient, waitTime: Duration = Duration.ofSeconds(3)) =
        JpaTransferStore(transfers, outbox, tx, wallets, RedisWalletDebit(client, conditional, waitTime))

    private fun newTransfer(userId: String, amount: Long = 10_000): Transfer {
        val now = Instant.now()
        return Transfer(
            id = UUID.randomUUID(), userId = userId, idempotencyKey = UUID.randomUUID().toString(), reference = references.next(),
            requestHash = "h".repeat(64), bankCode = "090", accountNo = unregisteredAccount(), amount = amount, memo = "분산락",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
    }

    private fun requested(t: Transfer) = TransferEvent.TransferRequested(t.id, t.createdAt)

    /** RLock.unlock() 이 불릴 때 [onUnlock] 을 먼저 부르는 클라이언트 (나머지는 진짜 클라이언트에 그대로 넘긴다). */
    private fun unlockSpy(real: RedissonClient, onUnlock: () -> Unit): RedissonClient =
        Proxy.newProxyInstance(RedissonClient::class.java.classLoader, arrayOf(RedissonClient::class.java)) { _, method, args ->
            val result = forward(method, real, args)
            if (method.name == "getLock") {
                val lock = result as RLock
                Proxy.newProxyInstance(RLock::class.java.classLoader, arrayOf(RLock::class.java)) { _, m, a ->
                    if (m.name == "unlock" && m.parameterCount == 0) onUnlock()
                    forward(m, lock, a)
                }
            } else {
                result
            }
        } as RedissonClient

    private fun forward(method: Method, target: Any, args: Array<out Any?>?): Any? =
        try {
            method.invoke(target, *(args ?: emptyArray()))
        } catch (e: InvocationTargetException) {
            throw e.targetException
        }

    @Test
    fun `락 안에서 조건부 UPDATE 로 차감하고, 끝나면 락이 풀려 있다`() {
        val user = fundedUser(30_000)
        val t = newTransfer(user)

        storeWith(redisson).saveWithEvent(t, requested(t))

        assertEquals(20_000L, wallets.balanceOf(user))
        assertEquals(listOf("WITHDRAW"), ledgerTypes(t.id))
        assertFalse(redisson.getLock(RedisWalletDebit.lockKey(user)).isLocked)
    }

    @Test
    fun `잔고가 모자라면 InsufficientBalance 이고 락은 풀린다`() {
        val user = fundedUser(9_999)
        val t = newTransfer(user)

        assertFailsWith<StoreConflict.InsufficientBalance> { storeWith(redisson).saveWithEvent(t, requested(t)) }

        assertFalse(redisson.getLock(RedisWalletDebit.lockKey(user)).isLocked)
        assertEquals(9_999L, wallets.balanceOf(user))
    }

    @Test
    fun `다른 쪽이 락을 쥐고 있으면 대기 시간 뒤 WalletBusy, 아무것도 남지 않는다`() {
        val user = fundedUser(30_000)
        val key = RedisWalletDebit.lockKey(user)
        // 다른 스레드가 쥔다 — 같은 스레드면 Redisson 락은 재진입이라 바로 잡혀 버린다
        CompletableFuture.runAsync { redisson.getLock(key).lock(10, TimeUnit.SECONDS) }.get(10, TimeUnit.SECONDS)
        val t = newTransfer(user)
        try {
            val started = System.nanoTime()

            // 기다림을 0.3초로 줄인다 (운영 3초 — 대기 길이만 다르고 경로는 같다)
            assertFailsWith<StoreConflict.WalletBusy> { storeWith(redisson, Duration.ofMillis(300)).saveWithEvent(t, requested(t)) }

            val waited = Duration.ofNanos(System.nanoTime() - started)
            assertTrue(waited >= Duration.ofMillis(300) && waited < Duration.ofSeconds(3), "waited $waited")
        } finally {
            redisson.getLock(key).forceUnlock()
        }
        assertNull(transfers.findById(t.id.toString()).orElse(null))
        assertEquals(30_000L, wallets.balanceOf(user))
        assertEquals(emptyList(), ledgerTypes(t.id))
    }

    @Test
    fun `unlock 할 때는 접수 트랜잭션이 이미 끝나 커밋돼 있다`() {
        val user = fundedUser(30_000)
        val t = newTransfer(user)
        var txActiveAtUnlock: Boolean? = null
        var committedAtUnlock: Int? = null
        val spying = unlockSpy(redisson) {
            txActiveAtUnlock = TransactionSynchronizationManager.isActualTransactionActive()
            // 다른 스레드(다른 커넥션)에서 보이면 커밋된 것이다
            committedAtUnlock = CompletableFuture.supplyAsync {
                jdbc.queryForObject("SELECT COUNT(*) FROM transfer WHERE id = ?", Int::class.java, t.id.toString())
            }.get(10, TimeUnit.SECONDS)
        }

        storeWith(spying).saveWithEvent(t, requested(t))

        assertEquals(false, txActiveAtUnlock)
        assertEquals(1, committedAtUnlock)
    }

    @Test
    fun `Redis 에 닿지 않으면 WalletBusy (fail-closed) 이고 아무것도 남지 않는다`() {
        // 아무것도 듣지 않는 포트. lazyInitialization 이라 만들 때는 붙지 않고 첫 명령(tryLock)에서 실패한다
        val unreachable = Redisson.create(redissonConfig("redis://127.0.0.1:1"))
        val user = fundedUser(30_000)
        val t = newTransfer(user)
        try {
            val started = System.nanoTime()

            assertFailsWith<StoreConflict.WalletBusy> { storeWith(unreachable).saveWithEvent(t, requested(t)) }

            val took = Duration.ofNanos(System.nanoTime() - started)
            assertTrue(took < Duration.ofSeconds(15), "took $took")   // 연결 · 명령 대기 1초, 재시도 1번 → 수 초 안에 503
        } finally {
            unreachable.shutdown()
        }
        assertNull(transfers.findById(t.id.toString()).orElse(null))
        assertEquals(30_000L, wallets.balanceOf(user))
    }
}
