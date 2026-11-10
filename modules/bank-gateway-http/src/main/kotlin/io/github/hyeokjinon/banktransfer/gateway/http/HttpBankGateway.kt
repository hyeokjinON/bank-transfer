package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.gateway.BankGateway

/** 게이트웨이 앱을 HTTP 로 부르는 BankGateway (P8-1). 연결 풀 · 스레드를 가지므로 닫아야 한다. */
interface HttpBankGateway : BankGateway, AutoCloseable {
    companion object {
        const val WEBCLIENT = "webclient"

        /** 고를 수 있는 클라이언트 종류 — 설정 검증이 여기를 본다 (종류를 더하면 이 한 곳만 고친다). */
        val KINDS: List<String> = listOf(WEBCLIENT)

        fun create(kind: String, settings: HttpGatewaySettings): HttpBankGateway = when (kind) {
            WEBCLIENT -> WebClientBankGateway(settings)
            else -> throw IllegalArgumentException("unknown http client '$kind' (webclient)")
        }
    }
}
