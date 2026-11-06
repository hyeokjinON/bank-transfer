package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.account.infra.contract.VerificationErrorContract
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.web.method.annotation.ExceptionHandlerMethodResolver
import kotlin.test.Test
import kotlin.test.assertEquals

/** 1원인증 오류 계약 — MVC 쪽 (P6-2, 설계 §4-4). 같은 표를 apps/api-reactive 의 계약 테스트가 읽는다. */
class VerificationErrorContractTest : ApiIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    @Test
    fun `HTTP 로 재현되는 경우 - 상태와 code 가 표와 같다`() {
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
    fun `예외에서 상태 · code · Retry-After 로 - 표와 같다`() {
        val handler = ApiExceptionHandler()
        // 직접 호출만으로는 실제 요청이 이 메서드로 오는지 모른다 — 프레임워크와 같은 해석기로 라우팅을 확인한다 (예상 못 한 예외가 Boot 기본 500 으로 새던 구멍)
        val resolver = ExceptionHandlerMethodResolver(ApiExceptionHandler::class.java)
        for (case in VerificationErrorContract.exceptionCases) {
            assertEquals("handle", resolver.resolveMethod(case.exception)?.name, case.exception.toString())
            val res = handler.handle(case.exception)
            assertEquals(case.status, res.statusCode.value(), case.exception.toString())
            assertEquals(case.code, res.body!!.properties?.get("code"), case.exception.toString())
            assertEquals(case.retryAfter, res.headers.getFirst(HttpHeaders.RETRY_AFTER), case.exception.toString())
            if (case.status == 500) assertEquals("internal error", res.body!!.detail)
        }
    }
}
