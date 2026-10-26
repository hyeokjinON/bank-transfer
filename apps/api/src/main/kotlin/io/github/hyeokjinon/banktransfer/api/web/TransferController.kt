package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.transfer.RequestTransfer
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferService
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import jakarta.validation.Valid
import jakarta.validation.constraints.Pattern
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.net.URI
import java.time.Instant
import java.util.UUID

data class TransferRequestBody(
    @field:Pattern(regexp = "\\d{3}", message = "은행코드는 숫자 3자리")
    val bankCode: String,
    @field:Pattern(regexp = "\\d{10,16}", message = "계좌번호는 숫자 10~16자리")
    val accountNo: String,
    /** 금액 범위(1 ~ 1천만원)는 도메인이 검사한다 → INVALID_AMOUNT */
    val amount: Long,
    /** 적요 길이(EUC-KR 20바이트)는 도메인이 검사한다 → INVALID_MEMO */
    val memo: String = "",
)

data class TransferResponse(
    val id: UUID,
    val status: TransferStatus,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    val memo: String,
    val bankResponseCode: String?,
    val createdAt: Instant,
    val updatedAt: Instant,
) {
    companion object {
        fun from(t: Transfer) = TransferResponse(
            t.id, t.status, t.bankCode, t.accountNo, t.amount, t.memo, t.bankResponseCode, t.createdAt, t.updatedAt,
        )
    }
}

@RestController
@RequestMapping("/api/v1/transfers")
class TransferController(private val service: TransferService) {

    /**
     * 접수만 하고 바로 돌려준다(202 Accepted). 은행 처리는 P3-2 의 Kafka 컨슈머가 한다.
     * 같은 Idempotency-Key 로 같은 내용을 다시 보내면 새로 만들지 않고 기존 건을 200 으로 돌려준다.
     * 헤더가 없으면 빈 문자열로 넘겨 도메인이 IDEMPOTENCY_KEY_REQUIRED 로 거절하게 한다 (규칙은 한 곳에).
     * 출금은 요청자(X-User-Id)의 지갑에서 빠진다. 잔고가 모자라면 422 INSUFFICIENT_BALANCE (P4-1).
     */
    @PostMapping
    fun request(
        user: RequestUser,
        @RequestHeader("Idempotency-Key", required = false) idempotencyKey: String?,
        @Valid @RequestBody body: TransferRequestBody,
    ): ResponseEntity<TransferResponse> {
        val result = service.request(
            RequestTransfer(user.id, idempotencyKey.orEmpty(), body.bankCode, body.accountNo, body.amount, body.memo),
        )
        val response = TransferResponse.from(result.transfer)
        return if (result.created) {
            ResponseEntity.accepted().location(URI.create("/api/v1/transfers/${response.id}")).body(response)
        } else {
            ResponseEntity.ok(response)
        }
    }

    /** 요청자의 출금이 아니면 404 TRANSFER_NOT_FOUND — 존재 여부도 숨긴다 (P4-1). */
    @GetMapping("/{id}")
    fun get(user: RequestUser, @PathVariable id: UUID): TransferResponse = TransferResponse.from(service.get(user.id, id))
}
