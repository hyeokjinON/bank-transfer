package io.github.hyeokjinon.banktransfer.reactive.web

import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard
import io.github.hyeokjinon.banktransfer.reactive.config.ReactiveVerificationProperties
import io.github.hyeokjinon.banktransfer.reactive.flow.CoroutineVerificationFlow
import jakarta.validation.Valid
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/** 1원인증 요청 — WebFlux 코루틴 판 (기본, account.verification.web-mode = coroutine). 응답 · 오류는 apps/api 와 같다. */
@RestController
@RequestMapping("/api/v1/account-verifications")
@ConditionalOnProperty(prefix = "account.verification", name = ["web-mode"], havingValue = ReactiveVerificationProperties.COROUTINE, matchIfMissing = true)
class CoroutineVerificationController(private val flow: CoroutineVerificationFlow, private val maintenance: BankMaintenanceGuard) {

    @PostMapping
    suspend fun request(user: RequestUser, @Valid @RequestBody body: VerificationRequest): ResponseEntity<VerificationResponse> {
        maintenance.ensureOpen()   // 점검 중에는 은행에 보내지 않는다 (P5-1)
        val verification = flow.request(RequestVerification(user.id, body.bankCode, body.accountNo, body.holderName))
        return ResponseEntity.status(HttpStatus.CREATED).body(VerificationResponse.from(verification))
    }
}
