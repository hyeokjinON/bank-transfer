package io.github.hyeokjinon.banktransfer.account.infra

import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse

/** account-infra 의 엔티티 · 저장소 패키지. 두 앱이 @AutoConfigurationPackage 로 JPA 스캔에 더한다 (앱 패키지 밖이라).
 *
 * @AutoConfigurationPackage 에 앱 클래스(basePackageClasses)도 같이 적을 것 — basePackages 만 쓰면 앱 패키지가 빠진다. */
const val ACCOUNT_INFRA_PACKAGE = "io.github.hyeokjinon.banktransfer.account.infra"

/** 은행이 처리를 거절한 경우(응답코드가 0000 이 아님). */
class BankRejectedException(val responseCode: String) : RuntimeException("bank rejected with code $responseCode")

/**
 * 1원인증 성명조회 응답 해석 (P6-2 — MVC 어댑터 · WebFlux 두 흐름이 같이 쓴다). 0000 이면 예금주명, 계좌 없음이면 null, 그 밖은 거절.
 * 막는 일을 하지 않는 짧은 계산이라 어느 스레드(게이트웨이 이벤트 루프 포함)에서 불러도 된다.
 */
fun holderNameOf(res: NameInquiryResponse): String? = when (res.header.responseCode) {
    ResponseCodes.OK -> res.holderName
    ResponseCodes.ACCOUNT_NOT_FOUND -> null
    else -> throw BankRejectedException(res.header.responseCode)
}

/** 1원 이체 응답이 0000 이 아니면 거절. */
fun requireOk(res: TransferResponse) {
    if (!res.header.isOk) throw BankRejectedException(res.header.responseCode)
}

/** 로그용 계좌 표시 — 끝 4자리만 (CLAUDE.md: 로그에 계좌 원문을 남기지 않는다). */
fun maskAccount(accountNo: String): String = "****" + accountNo.takeLast(4)
