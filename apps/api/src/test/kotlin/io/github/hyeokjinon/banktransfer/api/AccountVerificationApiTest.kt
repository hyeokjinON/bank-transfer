package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * 실제 HTTP -> Spring MVC -> 도메인 -> Netty 게이트웨이 -> 모의 은행(TCP) 까지 한 번에 검증한다.
 */
class AccountVerificationApiTest : ApiIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun post(path: String, body: Map<String, String>): Pair<Int, Map<String, Any?>> =
        client.post().uri(path)
            .contentType(MediaType.APPLICATION_JSON)
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
