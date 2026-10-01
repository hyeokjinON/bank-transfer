package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.account.AccountVerificationException
import io.github.hyeokjinon.banktransfer.api.config.BankRejectedException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import java.util.concurrent.CompletionException

/**
 * 예외 -> HTTP 상태 + RFC 9457 ProblemDetail.
 * 비동기(CompletableFuture) 경로에서는 예외가 CompletionException 으로 한 번 감싸져 오므로 벗겨서 처리한다.
 */
@RestControllerAdvice
class ApiExceptionHandler {
    private val log = LoggerFactory.getLogger(javaClass)

    @ExceptionHandler(
        CompletionException::class,
        AccountVerificationException::class,
        BankTimeoutException::class,
        BankUnavailableException::class,
        BankRejectedException::class,
    )
    fun handle(e: Exception): ProblemDetail {
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
            else -> {
                log.error("unexpected error", cause)
                HttpStatus.INTERNAL_SERVER_ERROR to "INTERNAL_ERROR"
            }
        }
        return ProblemDetail.forStatusAndDetail(status, cause.message ?: code).apply {
            setProperty("code", code)
            if (cause is AccountVerificationException.WrongCode) setProperty("remainingAttempts", cause.remainingAttempts)
        }
    }
}
