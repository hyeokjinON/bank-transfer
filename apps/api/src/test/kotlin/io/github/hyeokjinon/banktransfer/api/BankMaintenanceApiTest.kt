package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.send
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import java.time.LocalDate
import java.time.ZoneId
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * 점검 시간의 웹 (P5-1): 은행을 건드리는 요청(출금 접수, 1원인증 요청 · 확인)만 503 BANK_MAINTENANCE + Retry-After(남은 초, 올림).
 * 조회 · 지갑 충전은 그대로. 시각은 23:55:00.5 (KST) 로 고정 → 00:10 까지 899.5초 → Retry-After 900.
 */
class BankMaintenanceApiTest : ApiIntegrationTest() {

    @Autowired private lateinit var jdbc: JdbcTemplate

    private val seoul = ZoneId.of("Asia/Seoul")

    private fun at(h: Int, m: Int, s: Int = 0, millis: Long = 0) =
        testClock.fix(LocalDate.now(seoul).atTime(h, m, s).atZone(seoul).toInstant().plusMillis(millis))

    private fun postTransfer(user: String, account: String) = send(
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 10_000, "memo" to "점검")),
    )

    @Test
    fun `점검 중 출금 접수는 503 BANK_MAINTENANCE + Retry-After 900 - 출금도 차감도 없다`() {
        val user = fundedUser(100_000)
        val account = verifiedBy(user, unregisteredAccount())
        at(23, 55, 0, millis = 500)

        val response = postTransfer(user, account)

        assertEquals(503, response.status)
        assertEquals("BANK_MAINTENANCE", response.body["code"])
        assertEquals("900", response.retryAfter)
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM transfer WHERE user_id = ?", Int::class.java, user))
        assertEquals(100_000L, wallets.balanceOf(user))
    }

    @Test
    fun `점검 중 1원인증 요청 · 확인도 503 + Retry-After`() {
        val user = newUserId()
        at(0, 5)

        val request = send(
            client.post().uri("/api/v1/account-verifications")
                .contentType(MediaType.APPLICATION_JSON)
                .header(USER_ID_HEADER, user)
                .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동")),
        )
        val confirm = send(
            client.post().uri("/api/v1/account-verifications/${UUID.randomUUID()}/confirm")
                .contentType(MediaType.APPLICATION_JSON)
                .header(USER_ID_HEADER, user)
                .body(mapOf("code" to "1234")),
        )

        assertEquals(503 to "BANK_MAINTENANCE", request.status to request.body["code"])
        assertEquals("300", request.retryAfter)
        assertEquals(503 to "BANK_MAINTENANCE", confirm.status to confirm.body["code"])
    }

    @Test
    fun `점검 중에도 잔고 조회 · 충전 · 출금 조회는 그대로`() {
        val user = newUserId()
        at(23, 59)

        val deposit = send(
            client.post().uri("/api/v1/wallet/deposits").contentType(MediaType.APPLICATION_JSON)
                .header(USER_ID_HEADER, user).body(mapOf("amount" to 5_000)),
        )
        val wallet = send(client.get().uri("/api/v1/wallet").header(USER_ID_HEADER, user))
        val transfer = send(client.get().uri("/api/v1/transfers/${UUID.randomUUID()}").header(USER_ID_HEADER, user))

        assertEquals(200, deposit.status)
        assertEquals(200 to 5_000, wallet.status to wallet.body["balance"])
        assertEquals(404 to "TRANSFER_NOT_FOUND", transfer.status to transfer.body["code"])
        assertNull(transfer.retryAfter)
    }

    @Test
    fun `점검 직전 · 끝나는 순간(끝 제외)에는 접수된다`() {
        val user = fundedUser(100_000)
        val account = verifiedBy(user, unregisteredAccount())

        at(23, 49, 59, millis = 999)
        assertEquals(202, postTransfer(user, account).status)
        at(0, 10)
        assertEquals(202, postTransfer(user, account).status)
    }
}
