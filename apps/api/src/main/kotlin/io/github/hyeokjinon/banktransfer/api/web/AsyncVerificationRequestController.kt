package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.api.config.VerificationProperties
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard
import jakarta.validation.Valid
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.concurrent.CompletableFuture

/** 1원인증 요청 — 기본 판 (account.verification.web-mode = async). */
@RestController
@RequestMapping("/api/v1/account-verifications")
@ConditionalOnProperty(prefix = "account.verification", name = ["web-mode"], havingValue = VerificationProperties.ASYNC, matchIfMissing = true)
class AsyncVerificationRequestController(private val service: AccountVerificationService, private val maintenance: BankMaintenanceGuard) {

    /**
     * CompletableFuture 를 돌려주면 Spring MVC 가 서블릿 비동기 모드로 처리한다.
     * 은행 응답을 기다리는 동안 Tomcat 요청 스레드는 풀로 돌아간다. 인증 건에는 요청자(X-User-Id)를 남긴다 (P4-1).
     * 클라이언트가 응답 전에 끊어도 future 는 끝까지 간다 — 1원 이체 · 저장까지 (P6-2 실험 E2, WebFlux 판은 취소된다).
     */
    @PostMapping
    fun request(user: RequestUser, @Valid @RequestBody body: VerificationRequest): CompletableFuture<ResponseEntity<VerificationResponse>> {
        maintenance.ensureOpen()   // 점검 중에는 성명조회 · 1원 이체를 보내지 않는다 (P5-1) — 비동기로 넘기기 전에 거절한다
        return service.request(RequestVerification(user.id, body.bankCode, body.accountNo, body.holderName))
            .thenApply { ResponseEntity.status(HttpStatus.CREATED).body(VerificationResponse.from(it)) }
    }
}
