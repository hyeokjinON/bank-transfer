package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconAlreadyCompletedException
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconDateNotClosedException
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconRunningException
import io.github.hyeokjinon.banktransfer.transfer.TransferException
import org.springframework.http.HttpHeaders
import org.springframework.http.ProblemDetail
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.web.HttpRequestMethodNotSupportedException
import org.springframework.web.context.request.ServletWebRequest
import org.springframework.web.context.request.async.AsyncRequestTimeoutException
import org.springframework.web.bind.MissingServletRequestParameterException
import java.time.LocalDate
import java.util.concurrent.CompletionException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

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

    @Test
    fun `잔고를 지금 잡지 못하면 503 WALLET_BUSY + Retry-After 1`() {
        val response = handler.handle(TransferException.WalletBusy())

        assertEquals(503, response.statusCode.value())
        assertEquals("WALLET_BUSY", response.body!!.properties?.get("code"))
        assertEquals("1", response.headers.getFirst(HttpHeaders.RETRY_AFTER))
    }

    @Test
    fun `낙관적 재시도를 다 쓰면 409 CONCURRENT_UPDATE + Retry-After 1`() {
        val response = handler.handle(TransferException.ConcurrentUpdate())

        assertEquals(409, response.statusCode.value())
        assertEquals("CONCURRENT_UPDATE", response.body!!.properties?.get("code"))
        assertEquals("1", response.headers.getFirst(HttpHeaders.RETRY_AFTER))
    }

    @Test
    fun `다시 보내도 같은 결과인 오류에는 Retry-After 를 붙이지 않는다`() {
        val response = handler.handle(TransferException.InsufficientBalance())

        assertEquals(422, response.statusCode.value())
        assertNull(response.headers.getFirst(HttpHeaders.RETRY_AFTER))
    }

    @Test
    fun `비동기 경로(CompletionException)로 감싸져 와도 같은 응답`() {
        val response = handler.handle(CompletionException(TransferException.WalletBusy()))

        assertEquals(503, response.statusCode.value())
        assertEquals("1", response.headers.getFirst(HttpHeaders.RETRY_AFTER))
    }

    @Test
    fun `점검 시간이면 503 BANK_MAINTENANCE + Retry-After 는 점검이 끝날 때까지 남은 초`() {
        val response = handler.handle(BankMaintenanceException(900))

        assertEquals(503, response.statusCode.value())
        assertEquals("BANK_MAINTENANCE", response.body!!.properties?.get("code"))
        assertEquals("900", response.headers.getFirst(HttpHeaders.RETRY_AFTER))
    }

    @Test
    fun `대사 - 이미 완료 409 RECON_ALREADY_COMPLETED, 실행 중 409 RECON_RUNNING, 마감 전(오늘 이후) 400 RECON_DATE_NOT_CLOSED`() {
        val day = LocalDate.of(2026, 10, 28)

        val completed = handler.handle(ReconAlreadyCompletedException(day))
        val running = handler.handle(ReconRunningException(day))
        val notClosed = handler.handle(ReconDateNotClosedException(day, day))

        assertEquals(409 to "RECON_ALREADY_COMPLETED", completed.statusCode.value() to completed.body!!.properties?.get("code"))
        assertEquals(409 to "RECON_RUNNING", running.statusCode.value() to running.body!!.properties?.get("code"))
        assertEquals(400 to "RECON_DATE_NOT_CLOSED", notClosed.statusCode.value() to notClosed.body!!.properties?.get("code"))
        assertNull(running.headers.getFirst(HttpHeaders.RETRY_AFTER))
    }
}
