package io.github.hyeokjinon.banktransfer.account.infra.contract

import io.github.hyeokjinon.banktransfer.account.AccountVerificationException
import io.github.hyeokjinon.banktransfer.account.infra.BankRejectedException
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceException
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import java.time.Duration

/**
 * 1원인증 요청 오류 계약 (P6-2, 설계 §4-4). apps/api(MVC) 와 apps/api-reactive(WebFlux) 가 같은 요청에 같은 상태 · code 를 돌려줘야 한다 —
 * 웹 계층 코드는 공유하지 않으므로(API 가 다르다) 이 표 하나를 두 앱의 테스트가 같이 읽어 갈라짐을 잡는다.
 * - [httpCases]: 모의 은행으로 HTTP 재현이 되는 경우. 테스트 모의 은행은 [SILENT_ACCOUNT] 를 silentAccounts 로 받고, 게이트웨이 응답 시간 초과는 짧게(500ms) 둔다.
 * - [exceptionCases]: 모의 은행으로 재현이 어렵거나 시계가 필요한 경우 — 각 앱의 예외 처리기를 직접 불러 본다.
 */
object VerificationErrorContract {

    const val PATH = "/api/v1/account-verifications"
    const val SILENT_ACCOUNT = "9990000000001"
    const val USER = "contract-user-1"

    /** 성공 응답 본문의 키 — 인증코드는 없어야 한다. */
    val SUCCESS_FIELDS = setOf("id", "status", "expiresAt", "verifiedAt")

    data class HttpCase(val name: String, val userId: String?, val body: String, val status: Int, val code: String?)

    data class ExceptionCase(val exception: Exception, val status: Int, val code: String, val retryAfter: String? = null)

    private fun body(accountNo: String, holderName: String, bankCode: String = "090") =
        """{"bankCode":"$bankCode","accountNo":"$accountNo","holderName":"$holderName"}"""

    val httpCases: List<HttpCase> = listOf(
        HttpCase("X-User-Id 없음", null, body("1002123456789", "홍길동"), 400, "USER_ID_REQUIRED"),
        HttpCase("X-User-Id 형식 틀림", "bad id!", body("1002123456789", "홍길동"), 400, "USER_ID_REQUIRED"),
        HttpCase("헤더 없음 + 본문 틀림 → 헤더 오류가 먼저", null, body("1002123456789", "홍길동", bankCode = "9"), 400, "USER_ID_REQUIRED"),
        HttpCase("은행코드 형식 틀림", USER, body("1002123456789", "홍길동", bankCode = "9"), 400, "INVALID_REQUEST"),
        HttpCase("JSON 깨짐", USER, "{", 400, "INVALID_REQUEST"),
        HttpCase("없는 계좌", USER, body("5599000000001", "홍길동"), 404, "ACCOUNT_NOT_FOUND"),
        HttpCase("예금주 불일치", USER, body("3333012345678", "홍길동"), 422, "HOLDER_NAME_MISMATCH"),
        HttpCase("은행 무응답", USER, body(SILENT_ACCOUNT, "무응답"), 504, "BANK_TIMEOUT"),
        HttpCase("성공 (공백 무시)", USER, body("1002123456789", "홍 길동"), 201, null),
    )

    val exceptionCases: List<ExceptionCase> = listOf(
        ExceptionCase(AccountVerificationException.AccountNotFound(), 404, "ACCOUNT_NOT_FOUND"),
        ExceptionCase(AccountVerificationException.HolderNameMismatch(), 422, "HOLDER_NAME_MISMATCH"),
        ExceptionCase(BankRejectedException("9999"), 502, "BANK_REJECTED"),
        ExceptionCase(BankUnavailableException("bank connection is not active"), 503, "BANK_UNAVAILABLE"),
        ExceptionCase(BankNotSentException("bank maintenance window"), 503, "BANK_UNAVAILABLE"),
        ExceptionCase(BankMaintenanceException(120), 503, "BANK_MAINTENANCE", retryAfter = "120"),
        ExceptionCase(BankTimeoutException(1, Duration.ofSeconds(3)), 504, "BANK_TIMEOUT"),
        // 500 은 내부 메시지를 응답에 싣지 않는다 (detail = "internal error")
        ExceptionCase(IllegalStateException("jdbc:mysql://secret-host/db"), 500, "INTERNAL_ERROR"),
    )
}
