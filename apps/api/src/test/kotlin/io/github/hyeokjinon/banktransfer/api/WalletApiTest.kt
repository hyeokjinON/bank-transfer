package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

/** 지갑 API (P4-1). 충전은 관리 · 테스트용이다 (실제로는 은행 입금 통지). */
class WalletApiTest : ApiIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private data class Response(val status: Int, val body: Map<String, Any?>) {
        fun long(field: String) = (body[field] as Number).toLong()
    }

    private fun deposit(userId: String?, amount: Any): Response =
        client.post().uri("/api/v1/wallet/deposits")
            .contentType(MediaType.APPLICATION_JSON)
            .apply { if (userId != null) header(USER_ID_HEADER, userId) }
            .body(mapOf("amount" to amount))
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap()) }

    /** 직렬화를 거치지 않고 JSON 문자열을 그대로 보낸다 (소수 · 문자열 · 누락 금액 같은 잘못된 형식을 재현). */
    private fun depositRaw(userId: String, json: String): Response =
        client.post().uri("/api/v1/wallet/deposits")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .body(json)
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap()) }

    private fun wallet(userId: String): Response =
        client.get().uri("/api/v1/wallet").header(USER_ID_HEADER, userId)
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap()) }

    private fun transfer(userId: String, accountNo: String, amount: Long): Response =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to amount, "memo" to "지갑"))
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap()) }

    @Test
    fun `지갑이 없으면 잔고 0 · 보류 0`() {
        val user = newUserId()

        val w = wallet(user)

        assertEquals(200, w.status)
        assertEquals(user, w.body["userId"])
        assertEquals(0L, w.long("balance"))
        assertEquals(0L, w.long("pending"))
    }

    @Test
    fun `충전하면 응답에 충전 뒤 잔고, 조회 잔고와 원장 합이 같다`() {
        val user = newUserId()

        assertEquals(30_000L, deposit(user, 30_000).long("balance"))
        val second = deposit(user, 20_000)

        assertEquals(200, second.status)
        assertEquals(50_000L, second.long("balance"))
        assertEquals(50_000L, wallet(user).long("balance"))
        assertEquals(50_000L, ledgerSum(user))
    }

    @Test
    fun `출금을 접수하면 잔고에서 빠지고 결과가 날 때까지 보류로 보인다`() {
        val user = newUserId()
        deposit(user, 100_000)
        val account = verifiedBy(user, unregisteredAccount())   // 남는 RECEIVED 를 나중에 누가 처리해도 1001 거절 → 환불뿐

        assertEquals(202, transfer(user, account, 30_000).status)

        val w = wallet(user)
        assertEquals(70_000L, w.long("balance"))
        assertEquals(30_000L, w.long("pending"))
    }

    @Test
    fun `충전 금액이 1 ~ 1억 원 밖이면 400 INVALID_REQUEST 이고 잔고는 그대로`() {
        val user = newUserId()
        for (amount in listOf(0L, -1L, 100_000_001L)) {
            val r = deposit(user, amount)
            assertEquals(400, r.status, "amount=$amount")
            assertEquals("INVALID_REQUEST", r.body["code"], "amount=$amount")
        }

        assertEquals(200, deposit(user, 100_000_000).status)   // 한도 정확히는 받는다
        assertEquals(100_000_000L, wallet(user).long("balance"))
    }

    @Test
    fun `충전 금액이 소수이거나 문자열이거나 없으면 400 INVALID_REQUEST 이고 잔고는 그대로`() {
        val user = newUserId()
        for (json in listOf("""{"amount": 50000.5}""", """{"amount": "30000"}""", "{}")) {
            val r = depositRaw(user, json)
            assertEquals(400, r.status, json)
            assertEquals("INVALID_REQUEST", r.body["code"], json)
        }

        assertEquals(0L, wallet(user).long("balance"))
        assertEquals(0L, ledgerSum(user))
    }

    @Test
    fun `X-User-Id 가 없으면 충전도 400 USER_ID_REQUIRED`() {
        val r = deposit(null, 1_000)

        assertEquals(400, r.status)
        assertEquals("USER_ID_REQUIRED", r.body["code"])
    }
}
