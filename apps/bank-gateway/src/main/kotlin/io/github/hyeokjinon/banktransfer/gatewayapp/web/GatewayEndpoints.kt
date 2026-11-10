package io.github.hyeokjinon.banktransfer.gatewayapp.web

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayError
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayErrorCodes
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayJson
import io.github.hyeokjinon.banktransfer.gateway.http.InquiryBody
import io.github.hyeokjinon.banktransfer.gateway.http.NameInquiryBody
import io.github.hyeokjinon.banktransfer.gateway.http.TransferBody
import io.github.hyeokjinon.banktransfer.gateway.http.toResult
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException

/**
 * 두 서버 판(WebFlux · MVC 비동기)이 같이 쓰는 엔드포인트 본문 (P8-1). 판별 컨트롤러는 이것을 감싸기만 한다 —
 * 그래서 두 판의 차이가 "Mono 로 감싸나 / future 를 그대로 돌려주나" 한 줄로 보인다.
 *
 * 돌려주는 future 는 은행 future 의 의존 future(handle)다. 호출자가 끊겨 프레임워크가 이것을 취소해도
 * CompletableFuture 의 취소는 원본으로 번지지 않는다 — 은행 송신은 그대로 간다 (스펙 §4-4).
 */
object GatewayEndpoints {
    private val log = LoggerFactory.getLogger(GatewayEndpoints::class.java)

    fun nameInquiry(bank: BankGateway, json: String): CompletableFuture<ResponseEntity<String>> =
        call(json, NameInquiryBody::class.java) { b -> bank.inquireName(b.bankCode, b.accountNo).thenApply { GatewayJson.write(it.toResult()) } }

    fun transfer(bank: BankGateway, json: String): CompletableFuture<ResponseEntity<String>> =
        call(json, TransferBody::class.java) { b ->
            bank.transfer(b.bankCode, b.accountNo, b.amount, b.memo, b.reference).thenApply { GatewayJson.write(it.toResult()) }
        }

    fun inquiry(bank: BankGateway, json: String): CompletableFuture<ResponseEntity<String>> =
        call(json, InquiryBody::class.java) { b -> bank.inquire(b.reference).thenApply { GatewayJson.write(it.toResult()) } }

    private fun <B : Any> call(json: String, type: Class<B>, send: (B) -> CompletableFuture<String>): CompletableFuture<ResponseEntity<String>> {
        val body = try {
            GatewayJson.read(json, type)
        } catch (e: Exception) {
            return CompletableFuture.completedFuture(error(HttpStatus.BAD_REQUEST, GatewayError(GatewayErrorCodes.INVALID_REQUEST, "unreadable request body")))
        }
        return send(body).handle { okJson, failure -> if (failure == null) ok(okJson) else fromFailure(failure, type.simpleName) }
    }

    private fun fromFailure(failure: Throwable, what: String): ResponseEntity<String> {
        val cause = if (failure is CompletionException && failure.cause != null) failure.cause!! else failure
        val (status, error) = when (cause) {
            is BankNotSentException -> HttpStatus.SERVICE_UNAVAILABLE to GatewayError(GatewayErrorCodes.BANK_NOT_SENT, cause.message ?: "not sent")
            is BankTimeoutException -> HttpStatus.GATEWAY_TIMEOUT to
                GatewayError(GatewayErrorCodes.BANK_TIMEOUT, cause.message ?: "timeout", tranNo = cause.tranNo, timeoutMs = cause.timeout.toMillis())
            is BankUnavailableException -> HttpStatus.BAD_GATEWAY to GatewayError(GatewayErrorCodes.BANK_UNAVAILABLE, cause.message ?: "unavailable")
            else -> HttpStatus.INTERNAL_SERVER_ERROR to GatewayError(GatewayErrorCodes.INTERNAL, cause.javaClass.simpleName)
        }
        log.warn("bank call failed: request={} status={} code={} cause={}", what, status.value(), error.code, cause.javaClass.simpleName)
        return error(status, error)
    }

    private fun ok(json: String): ResponseEntity<String> = ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json)

    private fun error(status: HttpStatus, error: GatewayError): ResponseEntity<String> =
        ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(GatewayJson.write(error))
}
