// apps/api 의 같은 DTO — 웹 계층은 공유하지 않고 계약 테스트(VerificationErrorContract)로 맞춘다 (P6-2)
package io.github.hyeokjinon.banktransfer.reactive.web

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size
import java.time.Instant
import java.util.UUID

data class VerificationRequest(
    @field:Pattern(regexp = "\\d{3}", message = "은행코드는 숫자 3자리")
    val bankCode: String,
    @field:Pattern(regexp = "\\d{10,16}", message = "계좌번호는 숫자 10~16자리")
    val accountNo: String,
    @field:NotBlank @field:Size(max = 20)
    val holderName: String,
)

/** 응답에는 인증코드를 절대 넣지 않는다. 코드는 통장 적요로만 전달된다. */
data class VerificationResponse(
    val id: UUID,
    val status: VerificationStatus,
    val expiresAt: Instant,
    val verifiedAt: Instant?,
) {
    companion object {
        fun from(v: AccountVerification) = VerificationResponse(v.id, v.status, v.expiresAt, v.verifiedAt)
    }
}
