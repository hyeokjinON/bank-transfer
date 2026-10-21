package io.github.hyeokjinon.banktransfer.api.web

import org.springframework.http.ProblemDetail
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.web.HttpRequestMethodNotSupportedException
import org.springframework.web.context.request.ServletWebRequest
import org.springframework.web.context.request.async.AsyncRequestTimeoutException
import org.springframework.web.bind.MissingServletRequestParameterException
import kotlin.test.Test
import kotlin.test.assertEquals

/** Spring MVC 표준 예외의 code 규칙: 5xx 는 전부 INTERNAL_ERROR, 400 은 INVALID_REQUEST, 그 밖의 4xx 는 상태 이름. */
class ApiExceptionHandlerTest {

    private val handler = ApiExceptionHandler()
    private val request = ServletWebRequest(MockHttpServletRequest())

    private fun codeOf(e: Exception): Pair<Int, Any?> {
        val response = handler.handleException(e, request)!!
        return response.statusCode.value() to (response.body as ProblemDetail).properties?.get("code")
    }

    @Test
    fun `5xx 는 상태 이름이 아니라 INTERNAL_ERROR`() {
        assertEquals(503 to "INTERNAL_ERROR", codeOf(AsyncRequestTimeoutException()))
    }

    @Test
    fun `400 은 INVALID_REQUEST`() {
        assertEquals(400 to "INVALID_REQUEST", codeOf(MissingServletRequestParameterException("q", "String")))
    }

    @Test
    fun `그 밖의 4xx 는 상태 이름`() {
        assertEquals(405 to "METHOD_NOT_ALLOWED", codeOf(HttpRequestMethodNotSupportedException("PATCH")))
    }
}
