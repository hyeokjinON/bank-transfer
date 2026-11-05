package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard
import jakarta.validation.Valid
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
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

data class ConfirmRequest(
    @field:Pattern(regexp = "\\d{4}", message = "인증코드는 숫자 4자리")
    val code: String,
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

/** 1원인증 확인 (P1 · P4-1 · P5-1). 요청(POST)은 판(account.verification.web-mode)에 따라 [AsyncVerificationRequestController] 또는 [BlockingVerificationRequestController] 가 받는다 (P6-2). */
@RestController
@RequestMapping("/api/v1/account-verifications")
class AccountVerificationController(private val service: AccountVerificationService, private val maintenance: BankMaintenanceGuard) {

    /** 남의 인증 건이면 404 VERIFICATION_NOT_FOUND (실패 횟수도 늘지 않는다, P4-1). 점검 중이면 503 (P5-1 — 스펙 §4: 요청 · 확인 모두). */
    @PostMapping("/{id}/confirm")
    fun confirm(user: RequestUser, @PathVariable id: UUID, @Valid @RequestBody body: ConfirmRequest): VerificationResponse {
        maintenance.ensureOpen()
        return VerificationResponse.from(service.confirm(user.id, id, body.code))
    }
}
