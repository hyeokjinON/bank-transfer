package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.DataAccessException
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 지갑 · 원장 SQL (P4-1). 차감 · 환불은 호출자 트랜잭션 안에서만 부르므로 테스트도 tx.execute 로 감싼다. */
class WalletLedgerTest : ApiIntegrationTest() {

    @Autowired private lateinit var tx: TransactionTemplate
    @Autowired private lateinit var jdbc: JdbcTemplate

    private fun ledgerRows(userId: String) = jdbc.queryForList(
        "SELECT type, amount, balance_after, transfer_id FROM wallet_ledger WHERE user_id = ? ORDER BY id", userId,
    )

    @Test
    fun `충전 - 지갑이 없으면 만들고 있으면 더한다, 원장 DEPOSIT 에 충전 뒤 잔고`() {
        val user = newUserId()
        assertEquals(0L, wallets.balanceOf(user))   // 지갑이 없으면 0

        assertEquals(30_000L, wallets.deposit(user, 30_000))
        assertEquals(50_000L, wallets.deposit(user, 20_000))

        assertEquals(50_000L, wallets.balanceOf(user))
        val rows = ledgerRows(user)
        assertEquals(listOf("DEPOSIT", "DEPOSIT"), rows.map { it["type"] })
        assertEquals(listOf(30_000L, 50_000L), rows.map { (it["balance_after"] as Number).toLong() })
        assertEquals(listOf(null, null), rows.map { it["transfer_id"] })
    }

    @Test
    fun `차감 - 잔고가 충분하면 빼고 WITHDRAW, 모자라거나 지갑이 없으면 0건이고 아무것도 쓰지 않는다`() {
        val user = fundedUser(10_000)
        val first = UUID.randomUUID()
        val now = Instant.now()

        assertTrue(tx.execute { wallets.withdraw(user, first, 7_000, now) }!!)
        assertFalse(tx.execute { wallets.withdraw(user, UUID.randomUUID(), 3_001, now) }!!)
        assertFalse(tx.execute { wallets.withdraw(newUserId(), UUID.randomUUID(), 1, now) }!!)   // 지갑 없음 = 잔고 0

        assertEquals(3_000L, wallets.balanceOf(user))
        val rows = ledgerRows(user)
        assertEquals(listOf("DEPOSIT", "WITHDRAW"), rows.map { it["type"] })
        assertEquals(first.toString(), rows[1]["transfer_id"])
        assertEquals(3_000L, (rows[1]["balance_after"] as Number).toLong())
    }

    @Test
    fun `차감 · 환불은 호출자 트랜잭션 밖에서 부르면 거부한다`() {
        val user = fundedUser(10_000)

        assertFailsWith<IllegalStateException> { wallets.withdraw(user, UUID.randomUUID(), 1, Instant.now()) }
        assertFailsWith<IllegalStateException> { wallets.refund(UUID.randomUUID(), Instant.now()) }

        assertEquals(10_000L, wallets.balanceOf(user))
    }

    @Test
    fun `환불 - 원장의 WITHDRAW 금액을 돌려주고 REFUND, 차감 기록이 없으면 false`() {
        val user = fundedUser(10_000)
        val transferId = UUID.randomUUID()
        val now = Instant.now()
        tx.execute { wallets.withdraw(user, transferId, 4_000, now) }

        assertTrue(tx.execute { wallets.refund(transferId, now) }!!)
        assertFalse(tx.execute { wallets.refund(UUID.randomUUID(), now) }!!)   // P3 시절 출금: 뺀 적이 없으니 돌려줄 것도 없다

        assertEquals(10_000L, wallets.balanceOf(user))
        assertEquals(listOf("DEPOSIT", "WITHDRAW", "REFUND"), ledgerRows(user).map { it["type"] })
    }

    @Test
    fun `같은 출금에 REFUND 를 두 번 쓰면 UNIQUE 가 막고 그 트랜잭션의 잔고 증가도 롤백된다`() {
        val user = fundedUser(10_000)
        val transferId = UUID.randomUUID()
        val now = Instant.now()
        tx.execute { wallets.withdraw(user, transferId, 4_000, now) }
        tx.execute { wallets.refund(transferId, now) }

        assertFailsWith<DuplicateKeyException> { tx.execute { wallets.refund(transferId, now) } }

        assertEquals(10_000L, wallets.balanceOf(user))   // 이중 환불의 2차 방어선
    }

    @Test
    fun `CHECK 제약이 음수 잔고를 거부한다 (조건부 UPDATE 를 우회한 SQL 에 대한 2차 방어선)`() {
        val user = fundedUser(1_000)

        // MySQL 3819 (SQLSTATE HY000) 은 Spring 오류 코드표에 없어 UncategorizedSQLException 이 된다 → 상위 타입 + 제약 이름으로 본다
        val e = assertFailsWith<DataAccessException> {
            jdbc.update("UPDATE wallet SET balance = balance - 1001 WHERE user_id = ?", user)
        }

        assertTrue("ck_wallet_balance_non_negative" in e.mostSpecificCause.message.orEmpty(), e.toString())
        assertEquals(1_000L, wallets.balanceOf(user))
    }

    @Test
    fun `잔고 = 원장 합 (충전 - 출금 + 환불), 원장 시각은 KST 로 저장된다`() {
        val user = fundedUser(50_000)
        val refunded = UUID.randomUUID()
        val at = Instant.parse("2026-10-26T00:00:00Z")
        tx.execute {
            wallets.withdraw(user, UUID.randomUUID(), 20_000, at)
            wallets.withdraw(user, refunded, 5_000, at)
            wallets.refund(refunded, at)
        }

        assertEquals(30_000L, wallets.balanceOf(user))
        assertEquals(wallets.balanceOf(user), ledgerSum(user))
        val kst = jdbc.queryForObject(
            "SELECT DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') FROM wallet_ledger WHERE transfer_id = ? AND type = 'REFUND'",
            String::class.java, refunded.toString(),
        )
        assertEquals("2026-10-26 09:00:00", kst)   // 테스트 JVM 은 UTC — 변환은 JDBC 설정(connectionTimeZone)이 한다
    }
}
