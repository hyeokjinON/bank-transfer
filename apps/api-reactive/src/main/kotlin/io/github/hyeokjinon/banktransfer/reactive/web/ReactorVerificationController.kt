package io.github.hyeokjinon.banktransfer.reactive.web

import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard
import io.github.hyeokjinon.banktransfer.reactive.config.ReactiveVerificationProperties
import io.github.hyeokjinon.banktransfer.reactive.flow.ReactorVerificationFlow
import jakarta.validation.Valid
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

/** 1원인증 요청 — WebFlux Reactor 판 (account.verification.web-mode = reactor). 응답 · 오류는 코루틴 판 · apps/api 와 같다. */
@RestController
@RequestMapping("/api/v1/account-verifications")
@ConditionalOnProperty(prefix = "account.verification", name = ["web-mode"], havingValue = ReactiveVerificationProperties.REACTOR)
class ReactorVerificationController(private val flow: ReactorVerificationFlow, private val maintenance: BankMaintenanceGuard) {

    /** 점검 검사는 Mono 를 만들기 전에 던진다 — 처리기가 받아 503 (구독 뒤 오류로 흘려도 같은 응답이지만, 은행 호출을 만들지도 않게). */
    @PostMapping
    fun request(user: RequestUser, @Valid @RequestBody body: VerificationRequest): Mono<ResponseEntity<VerificationResponse>> {
        maintenance.ensureOpen()
        return flow.request(RequestVerification(user.id, body.bankCode, body.accountNo, body.holderName))
            .map { ResponseEntity.status(HttpStatus.CREATED).body(VerificationResponse.from(it)) }
    }
}
