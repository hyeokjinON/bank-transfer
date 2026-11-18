package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.micrometer.observation.ObservationRegistry

/** 게이트웨이 앱을 HTTP 로 부르는 BankGateway (P8-1). 연결 풀 · 스레드를 가지므로 닫아야 한다. */
interface HttpBankGateway : BankGateway, AutoCloseable {
    companion object {
        const val WEBCLIENT = "webclient"
        const val JDK = "jdk"

        /** 고를 수 있는 클라이언트 종류 — 설정 검증이 여기를 본다 (종류를 더하면 이 한 곳만 고친다). */
        val KINDS: List<String> = listOf(WEBCLIENT, JDK)

        /** [observations] = P9b 게이트웨이 호출 관측 (api 는 Boot 의 레지스트리를 넘긴다 → 클라이언트 구간 + traceparent). 기본 NOOP (api-reactive · 테스트). */
        fun create(kind: String, settings: HttpGatewaySettings, observations: ObservationRegistry = ObservationRegistry.NOOP): HttpBankGateway = when (kind) {
            WEBCLIENT -> WebClientBankGateway(settings, observations)
            JDK -> JdkHttpBankGateway(settings, observations)
            else -> throw IllegalArgumentException("unknown http client '$kind' (${KINDS.joinToString(" | ")})")
        }
    }
}
