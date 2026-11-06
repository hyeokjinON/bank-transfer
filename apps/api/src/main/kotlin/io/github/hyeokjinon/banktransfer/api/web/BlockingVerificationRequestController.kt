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

/**
 * 1원인증 요청 — 대조군 (P6-2, account.verification.web-mode = blocking, 측정 전용 · 운영에서 쓰지 않는다).
 * 같은 서비스를 부르고 요청 스레드(Tomcat)가 join() 으로 은행 두 번(성명조회 · 1원 이체)을 끝까지 기다린다 —
 * 동시에 기다릴 수 있는 요청 수가 Tomcat 최대 스레드 수(기본 200)에 묶인다. 비동기 판과 다른 것은 이 join 하나뿐이다.
 */
@RestController
@RequestMapping("/api/v1/account-verifications")
@ConditionalOnProperty(prefix = "account.verification", name = ["web-mode"], havingValue = VerificationProperties.BLOCKING)
class BlockingVerificationRequestController(private val service: AccountVerificationService, private val maintenance: BankMaintenanceGuard) {

    /** join 이 던지는 CompletionException 은 ApiExceptionHandler 가 벗긴다 — 오류 응답은 비동기 판과 같다. */
    @PostMapping
    fun request(user: RequestUser, @Valid @RequestBody body: VerificationRequest): ResponseEntity<VerificationResponse> {
        maintenance.ensureOpen()
        val verification = service.request(RequestVerification(user.id, body.bankCode, body.accountNo, body.holderName)).join()
        return ResponseEntity.status(HttpStatus.CREATED).body(VerificationResponse.from(verification))
    }
}
