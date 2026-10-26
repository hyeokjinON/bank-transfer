package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.concurrently
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * P4 의 정합성 기준선: 같은 사용자의 출금이 동시에 몰려도 잔고는 음수가 되지 않고, 잔고 = 원장 합이다 (P4-1).
 * 차감은 조건부 UPDATE 한 문장 + CHECK. P4-2 에서 락 전략을 바꿔도 이 테스트가 먼저 통과해야 한다.
 * 계좌는 모의 은행에 없는 번호다: 남긴 RECEIVED 를 나중에 다른 테스트가 처리해도 1001 거절 → 이 사용자에게 환불될 뿐이다.
 */
class WalletConcurrencyTest : ApiIntegrationTest() {

    @Autowired private lateinit var jdbc: JdbcTemplate

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun transfer(userId: String, key: String, accountNo: String, amount: Long): Pair<Int, Map<String, Any?>> =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .header("Idempotency-Key", key)
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to amount, "memo" to "동시"))
            .exchange { _, r -> r.statusCode.value() to (r.bodyTo(mapType) ?: emptyMap()) }

    private fun count(sql: String, userId: String): Int = jdbc.queryForObject(sql, Int::class.java, userId)!!

    @Test
    fun `잔고 10만 원에 1만 원 출금 100건이 동시에 와도 정확히 10건 접수, 90건 INSUFFICIENT_BALANCE, 잔고 0, 원장 합 = 잔고`() {
        val user = fundedUser(100_000)
        val account = verifiedBy(user, unregisteredAccount())

        val results = concurrently(100) { transfer(user, UUID.randomUUID().toString(), account, 10_000) }

        val byStatus = results.groupingBy { it.first }.eachCount()
        assertEquals(10, results.count { it.first == 202 }, byStatus.toString())
        assertEquals(90, results.count { it.first == 422 && it.second["code"] == "INSUFFICIENT_BALANCE" }, byStatus.toString())
        assertEquals(0L, wallets.balanceOf(user))
        assertEquals(0L, ledgerSum(user))                                   // 잔고 = 원장 합 (충전 10만 - 출금 10만)
        assertEquals(10, count("SELECT COUNT(*) FROM transfer WHERE user_id = ?", user))
        assertEquals(10, count("SELECT COUNT(*) FROM wallet_ledger WHERE user_id = ? AND type = 'WITHDRAW'", user))
        assertEquals(100_000L, wallets.pendingOf(user))                     // 접수된 10건은 결과 전이라 보류로 묶여 있다
    }

    @Test
    fun `같은 사용자 · 같은 키로 20건이 동시에 와도 출금 1건 · 차감 1번`() {
        val user = fundedUser(100_000)
        val account = verifiedBy(user, unregisteredAccount())
        val key = UUID.randomUUID().toString()

        val results = concurrently(20) { transfer(user, key, account, 10_000) }

        assertEquals(1, results.count { it.first == 202 }, results.map { it.first }.toString())
        assertEquals(19, results.count { it.first == 200 })                 // 진 쪽은 UNIQUE 에서 멈춰 지갑을 건드리지 않고 기존 건을 받는다
        assertEquals(1, results.map { it.second["id"] }.toSet().size)
        assertEquals(90_000L, wallets.balanceOf(user))
        assertEquals(1, count("SELECT COUNT(*) FROM wallet_ledger WHERE user_id = ? AND type = 'WITHDRAW'", user))
        assertEquals(90_000L, ledgerSum(user))
    }
}
