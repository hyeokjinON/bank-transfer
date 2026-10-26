package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

/** X-User-Id 는 웹 계층 한 곳(RequestUserResolver)에서 꺼내고 검사한다 (P4-1). 은행 · DB 까지 가기 전에 400 이다. */
class UserIdHeaderApiTest : ApiIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun call(method: HttpMethod, path: String, userId: String?, body: Any? = null): Pair<Int, Any?> =
        client.method(method).uri(path)
            .apply { if (userId != null) header(USER_ID_HEADER, userId) }
            .apply { if (body != null) contentType(MediaType.APPLICATION_JSON).body(body) }
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .exchange { _, r -> r.statusCode.value() to r.bodyTo(mapType)?.get("code") }

    private val transferBody = mapOf("bankCode" to "090", "accountNo" to "1002123456789", "amount" to 1_000, "memo" to "헤더")
    private val verificationBody = mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동")

    @Test
    fun `X-User-Id 가 없으면 출금 접수 · 조회 · 1원인증 · 확인 모두 400 USER_ID_REQUIRED`() {
        val expected = 400 to "USER_ID_REQUIRED"
        assertEquals(expected, call(HttpMethod.POST, "/api/v1/transfers", null, transferBody))
        assertEquals(expected, call(HttpMethod.GET, "/api/v1/transfers/${UUID.randomUUID()}", null))
        assertEquals(expected, call(HttpMethod.POST, "/api/v1/account-verifications", null, verificationBody))
        assertEquals(expected, call(HttpMethod.POST, "/api/v1/account-verifications/${UUID.randomUUID()}/confirm", null, mapOf("code" to "1234")))
    }

    @Test
    fun `X-User-Id 형식이 틀리면 (공백 · 밑줄 · 37자 · 빈 값) 400 USER_ID_REQUIRED`() {
        for (userId in listOf("user 1", "user_1", "a".repeat(37), "")) {
            assertEquals(400 to "USER_ID_REQUIRED", call(HttpMethod.GET, "/api/v1/transfers/${UUID.randomUUID()}", userId), "[$userId]")
        }
    }
}
