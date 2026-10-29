package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import jakarta.validation.Valid
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture

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

@RestController
@RequestMapping("/api/v1/account-verifications")
class AccountVerificationController(private val service: AccountVerificationService, private val maintenance: BankMaintenanceGuard) {

    /**
     * CompletableFuture 를 돌려주면 Spring MVC 가 서블릿 비동기 모드로 처리한다.
     * 은행 응답을 기다리는 동안 Tomcat 요청 스레드는 풀로 돌아간다. 인증 건에는 요청자(X-User-Id)를 남긴다 (P4-1).
     */
    @PostMapping
    fun request(user: RequestUser, @Valid @RequestBody body: VerificationRequest): CompletableFuture<ResponseEntity<VerificationResponse>> {
        maintenance.ensureOpen()   // 점검 중에는 성명조회 · 1원 이체를 보내지 않는다 (P5-1) — 비동기로 넘기기 전에 거절한다
        return service.request(RequestVerification(user.id, body.bankCode, body.accountNo, body.holderName))
            .thenApply { ResponseEntity.status(HttpStatus.CREATED).body(VerificationResponse.from(it)) }
    }

    /** 남의 인증 건이면 404 VERIFICATION_NOT_FOUND (실패 횟수도 늘지 않는다, P4-1). 점검 중이면 503 (P5-1 — 스펙 §4: 요청 · 확인 모두). */
    @PostMapping("/{id}/confirm")
    fun confirm(user: RequestUser, @PathVariable id: UUID, @Valid @RequestBody body: ConfirmRequest): VerificationResponse {
        maintenance.ensureOpen()
        return VerificationResponse.from(service.confirm(user.id, id, body.code))
    }
}
