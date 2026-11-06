package io.github.hyeokjinon.banktransfer.reactive.web

import io.github.hyeokjinon.banktransfer.account.AccountVerificationException
import io.github.hyeokjinon.banktransfer.account.infra.BankRejectedException
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletionException

/**
 * 예외 → 상태 + ProblemDetail (P6-2). apps/api 의 ApiExceptionHandler 에서 1원인증 요청에 해당하는 부분만 같은 규칙으로 옮겼다 —
 * 같은 요청에 같은 상태 · code 인지는 계약 테스트(VerificationErrorContract)가 본다.
 * CancellationException(클라이언트 끊김)은 여기서 다루지 않는다: 응답을 보낼 곳이 없고, 오류로 바꾸면 취소가 사라진다.
 */
@RestControllerAdvice
class ReactiveExceptionHandler : ResponseEntityExceptionHandler() {
    private val log = LoggerFactory.getLogger(javaClass)

    /** WebFlux 표준 예외(본문 깨짐 · 검증 실패 …)에도 code — 400 = INVALID_REQUEST, 그 밖 4xx = 상태 이름, 5xx = INTERNAL_ERROR (apps/api 와 같다). */
    override fun handleExceptionInternal(
        ex: Exception,
        body: Any?,
        headers: HttpHeaders?,
        status: HttpStatusCode,
        exchange: ServerWebExchange,
    ): Mono<ResponseEntity<Any>> =
        super.handleExceptionInternal(ex, body, headers, status, exchange).map { response ->
            val problem = response.body as? ProblemDetail
            if (problem != null && problem.properties?.containsKey("code") != true) {
                problem.setProperty(
                    "code",
                    when {
                        status.is5xxServerError -> "INTERNAL_ERROR"
                        status.value() == 400 -> "INVALID_REQUEST"
                        else -> HttpStatus.resolve(status.value())?.name ?: "ERROR"
                    },
                )
            }
            response
        }

    /**
     * 목록 끝의 Exception::class 는 예상 못 한 예외를 아래 else(500 INTERNAL_ERROR)로 모으려는 것이다. 없으면 Boot 기본 오류 모양이 나간다.
     * Spring 표준 예외는 부모 ResponseEntityExceptionHandler 의 더 구체적인 핸들러가 계속 받는다 (가까운 타입이 이긴다).
     */
    @ExceptionHandler(
        CompletionException::class,
        AccountVerificationException::class,
        BankTimeoutException::class,
        BankUnavailableException::class,
        BankRejectedException::class,
        BankMaintenanceException::class,
        UserIdRequiredException::class,
        Exception::class,
    )
    fun handle(e: Exception): ResponseEntity<ProblemDetail> {
        // Exception 을 받게 되면서 취소도 여기로 온다. 취소는 오류 응답으로 바꾸지 않고 그대로 다시 던진다 (설계 §4-3) —
        // 500 으로 바꾸면 끊긴 요청이 오류로 집계되고 취소 전파가 끊긴다. CompletionException 에 싸여 온 취소도 같다.
        if (e is CancellationException || (e is CompletionException && e.cause is CancellationException)) throw e
        val cause = if (e is CompletionException) e.cause ?: e else e
        val (status, code) = when (cause) {
            is AccountVerificationException.AccountNotFound -> HttpStatus.NOT_FOUND to "ACCOUNT_NOT_FOUND"
            is AccountVerificationException.HolderNameMismatch -> HttpStatus.UNPROCESSABLE_CONTENT to "HOLDER_NAME_MISMATCH"
            is BankTimeoutException -> HttpStatus.GATEWAY_TIMEOUT to "BANK_TIMEOUT"
            is BankUnavailableException -> HttpStatus.SERVICE_UNAVAILABLE to "BANK_UNAVAILABLE"
            is BankRejectedException -> HttpStatus.BAD_GATEWAY to "BANK_REJECTED"
            is BankMaintenanceException -> HttpStatus.SERVICE_UNAVAILABLE to "BANK_MAINTENANCE"
            is UserIdRequiredException -> HttpStatus.BAD_REQUEST to "USER_ID_REQUIRED"
            else -> {
                log.error("unexpected error", cause)
                HttpStatus.INTERNAL_SERVER_ERROR to "INTERNAL_ERROR"
            }
        }
        // 500 은 내부 메시지(SQL · 경로 등)를 응답에 싣지 않는다
        val detail = if (status == HttpStatus.INTERNAL_SERVER_ERROR) "internal error" else cause.message ?: code
        val problem = ProblemDetail.forStatusAndDetail(status, detail).apply { setProperty("code", code) }
        return ResponseEntity.status(status)
            .headers { h -> (cause as? BankMaintenanceException)?.let { h.set(HttpHeaders.RETRY_AFTER, it.retryAfterSeconds.toString()) } }
            .body(problem)
    }
}
