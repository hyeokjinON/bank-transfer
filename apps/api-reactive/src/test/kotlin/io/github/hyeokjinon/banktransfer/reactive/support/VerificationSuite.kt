package io.github.hyeokjinon.banktransfer.reactive.support

import io.github.hyeokjinon.banktransfer.account.infra.contract.VerificationErrorContract
import io.github.hyeokjinon.banktransfer.reactive.web.ReactiveExceptionHandler
import io.github.hyeokjinon.banktransfer.reactive.web.USER_ID_HEADER
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import kotlin.test.Test
import kotlin.test.assertEquals

/** WebFlux 두 판이 같이 지켜야 하는 것 (P6-2). 판마다 이 클래스를 상속한 테스트 클래스가 하나씩 있다 (CoroutineModeTest · ReactorModeTest). */
abstract class VerificationSuite : ReactiveIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    @Test
    fun `오류 계약 - HTTP 로 재현되는 경우 상태와 code 가 apps-api 와 같은 표를 따른다`() {
        for (case in VerificationErrorContract.httpCases) {
            val (status, body) = client.post().uri(VerificationErrorContract.PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .headers { h -> case.userId?.let { h.set(USER_ID_HEADER, it) } }
                .body(case.body)
                .exchange { _, res -> res.statusCode.value() to (res.bodyTo(mapType) ?: emptyMap()) }
            assertEquals(case.status, status, "${case.name}: $body")
            if (case.code == null) {
                assertEquals(VerificationErrorContract.SUCCESS_FIELDS, body.keys, "${case.name}: 성공 응답 키 (인증코드 없음)")
                assertEquals("SENT", body["status"])
            } else {
                assertEquals(case.code, body["code"], "${case.name}: $body")
            }
        }
    }

    @Test
    fun `오류 계약 - 예외에서 상태 · code · Retry-After 로`() {
        val handler = ReactiveExceptionHandler()
        for (case in VerificationErrorContract.exceptionCases) {
            val res = handler.handle(case.exception)
            assertEquals(case.status, res.statusCode.value(), case.exception.toString())
            assertEquals(case.code, res.body!!.properties?.get("code"), case.exception.toString())
            assertEquals(case.retryAfter, res.headers.getFirst(HttpHeaders.RETRY_AFTER), case.exception.toString())
            if (case.status == 500) assertEquals("internal error", res.body!!.detail)
        }
    }
}
