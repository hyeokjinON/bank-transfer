package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * 실제 HTTP -> Spring MVC -> 도메인 -> Netty 게이트웨이 -> 모의 은행(TCP) 까지 한 번에 검증한다.
 * 인증 건은 요청자(X-User-Id)의 것이다 — 확인도 본인만 (P4-1).
 */
class AccountVerificationApiTest : ApiIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private val user = newUserId()

    private fun post(path: String, body: Map<String, String>, userId: String = user): Pair<Int, Map<String, Any?>> =
        client.post().uri(path)
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .body(body)
            .exchange { _, response -> response.statusCode.value() to (response.bodyTo(mapType) ?: emptyMap()) }

    @Test
    fun `1원 인증 요청 후 통장 적요의 코드를 입력하면 VERIFIED`() {
        val (status, created) = post(
            "/api/v1/account-verifications",
            mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"),
        )
        assertEquals(201, status)
        assertEquals("SENT", created["status"])

        // 사용자가 자기 통장에서 1원 입금 적요("BT1234")를 확인하는 단계
        val code = bank.depositsOf("1002123456789").last().memo.removePrefix("BT")

        val (confirmStatus, confirmed) = post("/api/v1/account-verifications/${created["id"]}/confirm", mapOf("code" to code))
        assertEquals(200, confirmStatus)
        assertEquals("VERIFIED", confirmed["status"])
    }

    @Test
    fun `남의 인증 건을 확인하면 404 VERIFICATION_NOT_FOUND, 본인은 그 뒤에도 확인할 수 있다`() {
        val (_, created) = post(
            "/api/v1/account-verifications",
            mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"),
        )
        val code = bank.depositsOf("1002123456789").last().memo.removePrefix("BT")

        val (otherStatus, other) = post("/api/v1/account-verifications/${created["id"]}/confirm", mapOf("code" to code), userId = newUserId())
        assertEquals(404, otherStatus)
        assertEquals("VERIFICATION_NOT_FOUND", other["code"])

        val (mineStatus, mine) = post("/api/v1/account-verifications/${created["id"]}/confirm", mapOf("code" to code))
        assertEquals(200, mineStatus)                // 남의 시도는 실패 횟수도 쓰지 않았다
        assertEquals("VERIFIED", mine["status"])
    }

    @Test
    fun `예금주명이 다르면 422 HOLDER_NAME_MISMATCH`() {
        val (status, body) = post(
            "/api/v1/account-verifications",
            mapOf("bankCode" to "090", "accountNo" to "3333012345678", "holderName" to "홍길동"),
        )
        assertEquals(422, status)
        assertEquals("HOLDER_NAME_MISMATCH", body["code"])
    }

    @Test
    fun `은행이 응답하지 않으면 504 BANK_TIMEOUT`() {
        val (status, body) = post(
            "/api/v1/account-verifications",
            mapOf("bankCode" to "090", "accountNo" to "9990000000001", "holderName" to "홍길동"),
        )
        assertEquals(504, status)
        assertEquals("BANK_TIMEOUT", body["code"])
    }

    @Test
    fun `형식이 틀리면 400 INVALID_REQUEST`() {
        val (status, body) = post(
            "/api/v1/account-verifications",
            mapOf("bankCode" to "9", "accountNo" to "abc", "holderName" to ""),
        )
        assertEquals(400, status)
        assertEquals("INVALID_REQUEST", body["code"])
    }
}
