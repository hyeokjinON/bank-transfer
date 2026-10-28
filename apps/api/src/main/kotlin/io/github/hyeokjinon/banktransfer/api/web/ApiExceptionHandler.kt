package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.account.AccountVerificationException
import io.github.hyeokjinon.banktransfer.api.config.BankRejectedException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import io.github.hyeokjinon.banktransfer.transfer.TransferException
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.context.request.WebRequest
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler
import java.util.concurrent.CompletionException

/**
 * 예외 -> HTTP 상태 + RFC 9457 ProblemDetail.
 * 비동기(CompletableFuture) 경로에서는 예외가 CompletionException 으로 한 번 감싸져 오므로 벗겨서 처리한다.
 */
@RestControllerAdvice
class ApiExceptionHandler : ResponseEntityExceptionHandler() {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Spring MVC 가 던지는 표준 예외(본문 JSON 깨짐, 검증 실패, 경로 변수 타입 오류 …)의 ProblemDetail 에도 code 를 붙인다.
     * 클라이언트는 도메인 오류든 형식 오류든 code 하나로 분기할 수 있다.
     * 상태별 code: 400 = INVALID_REQUEST, 그 밖의 4xx = HttpStatus 이름, 5xx = INTERNAL_ERROR.
     * (Boot 의 기본 ProblemDetail 핸들러는 ResponseEntityExceptionHandler 빈이 있으면 물러나므로 이 지점이 유일한 공통 경로다.)
     */
    override fun handleExceptionInternal(
        ex: Exception,
        body: Any?,
        headers: HttpHeaders,
        statusCode: HttpStatusCode,
        request: WebRequest,
    ): ResponseEntity<Any>? {
        val response = super.handleExceptionInternal(ex, body, headers, statusCode, request)
        // 검증 실패 등은 body 가 null 로 들어오고 부모가 ErrorResponse 에서 ProblemDetail 을 만들어 채운다. 그래서 결과의 body 에 붙인다.
        val problem = response?.body as? ProblemDetail
        if (problem != null && problem.properties?.containsKey("code") != true) {
            // 5xx 는 어느 경로로 오든 도메인 catch-all 과 같은 INTERNAL_ERROR (클라이언트가 서버 오류를 한 code 로 분기한다)
            problem.setProperty(
                "code",
                when {
                    statusCode.is5xxServerError -> "INTERNAL_ERROR"
                    statusCode.value() == 400 -> "INVALID_REQUEST"
                    else -> HttpStatus.resolve(statusCode.value())?.name ?: "ERROR"
                },
            )
        }
        return response
    }

    @ExceptionHandler(
        CompletionException::class,
        AccountVerificationException::class,
        BankTimeoutException::class,
        BankUnavailableException::class,
        BankRejectedException::class,
        TransferException::class,
        UserIdRequiredException::class,
    )
    fun handle(e: Exception): ResponseEntity<ProblemDetail> {
        val cause = if (e is CompletionException && e.cause != null) e.cause!! else e
        val (status, code) = when (cause) {
            is AccountVerificationException.AccountNotFound -> HttpStatus.NOT_FOUND to "ACCOUNT_NOT_FOUND"
            is AccountVerificationException.HolderNameMismatch -> HttpStatus.UNPROCESSABLE_CONTENT to "HOLDER_NAME_MISMATCH"
            is AccountVerificationException.VerificationNotFound -> HttpStatus.NOT_FOUND to "VERIFICATION_NOT_FOUND"
            is AccountVerificationException.Expired -> HttpStatus.GONE to "VERIFICATION_EXPIRED"
            is AccountVerificationException.Locked -> HttpStatus.LOCKED to "VERIFICATION_LOCKED"
            is AccountVerificationException.WrongCode -> HttpStatus.BAD_REQUEST to "WRONG_CODE"
            is BankTimeoutException -> HttpStatus.GATEWAY_TIMEOUT to "BANK_TIMEOUT"
            is BankUnavailableException -> HttpStatus.SERVICE_UNAVAILABLE to "BANK_UNAVAILABLE"
            is BankRejectedException -> HttpStatus.BAD_GATEWAY to "BANK_REJECTED"
            is UserIdRequiredException -> HttpStatus.BAD_REQUEST to "USER_ID_REQUIRED"
            is TransferException.IdempotencyKeyRequired -> HttpStatus.BAD_REQUEST to "IDEMPOTENCY_KEY_REQUIRED"
            is TransferException.InvalidAmount -> HttpStatus.BAD_REQUEST to "INVALID_AMOUNT"
            is TransferException.InvalidMemo -> HttpStatus.BAD_REQUEST to "INVALID_MEMO"
            // 컨트롤러의 @Pattern 이 먼저 거르지만, 도메인을 직접 거치는 경로를 위해 같은 400 으로 매핑한다.
            is TransferException.InvalidAccount -> HttpStatus.BAD_REQUEST to "INVALID_ACCOUNT"
            is TransferException.AccountNotVerified -> HttpStatus.UNPROCESSABLE_CONTENT to "ACCOUNT_NOT_VERIFIED"
            is TransferException.InsufficientBalance -> HttpStatus.UNPROCESSABLE_CONTENT to "INSUFFICIENT_BALANCE"
            // P4-2: 요청은 옳은데 지금 잔고를 잡지 못했다. 잔고 부족(422 — 다시 보내도 같다)과 구분한다
            is TransferException.ConcurrentUpdate -> HttpStatus.CONFLICT to "CONCURRENT_UPDATE"
            is TransferException.WalletBusy -> HttpStatus.SERVICE_UNAVAILABLE to "WALLET_BUSY"
            is TransferException.IdempotencyKeyReused -> HttpStatus.UNPROCESSABLE_CONTENT to "IDEMPOTENCY_KEY_REUSED"
            is TransferException.TransferNotFound -> HttpStatus.NOT_FOUND to "TRANSFER_NOT_FOUND"
            is TransferException.IllegalTransition -> HttpStatus.CONFLICT to "ILLEGAL_TRANSITION"
            else -> {
                log.error("unexpected error", cause)
                HttpStatus.INTERNAL_SERVER_ERROR to "INTERNAL_ERROR"
            }
        }
        // 500 은 내부 메시지(SQL · 경로 등)를 응답에 싣지 않는다. 원인은 위 log.error 로 남아 있다.
        val detail = if (status == HttpStatus.INTERNAL_SERVER_ERROR) "internal error" else cause.message ?: code
        val problem = ProblemDetail.forStatusAndDetail(status, detail).apply {
            setProperty("code", code)
            if (cause is AccountVerificationException.WrongCode) setProperty("remainingAttempts", cause.remainingAttempts)
        }
        // 헤더를 붙이려고 ResponseEntity 로 감싼다. 본문 · 상태 · problem+json 처리는 ProblemDetail 을 바로 돌려줄 때와 같다
        return ResponseEntity.status(status)
            .headers { headers -> if (cause.isRetryable()) headers.set(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS) }
            .body(problem)
    }

    /**
     * 같은 요청을 잠시 뒤 다시 보내면 될 수 있는 오류 (P4-2). 클라이언트에게 언제 다시 해도 되는지 알린다.
     * Redis 장애와 락 경합을 구분하지 않는다 — 클라이언트가 할 일(잠시 뒤 다시)이 같다. 운영자는 로그 수준으로 구분한다.
     */
    private fun Throwable.isRetryable() = this is TransferException.WalletBusy || this is TransferException.ConcurrentUpdate

    private companion object {
        /** 초 단위. 바로 다시 부딪히지 않을 만큼만 — 락 대기(5초) · 분산락 대기(3초)보다 짧다. */
        const val RETRY_AFTER_SECONDS = "1"
    }
}
