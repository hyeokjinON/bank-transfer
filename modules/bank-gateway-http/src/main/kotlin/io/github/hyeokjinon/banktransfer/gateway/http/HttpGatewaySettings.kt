package io.github.hyeokjinon.banktransfer.gateway.http

import java.time.Duration

/**
 * 게이트웨이 HTTP 호출 설정 (P8-1, 스펙 §4-4 · §6). 시간 순서: 연결 · 연결 풀 대기 < 게이트웨이 은행 응답(3s) < [responseTimeout].
 * api 가 게이트웨이보다 먼저 끊으면 게이트웨이는 답을 받았는데 api 는 "모름" 이 되는 건이 쓸데없이 는다.
 */
data class HttpGatewaySettings(
    val baseUrl: String,
    /** WebClient 만. Reactor Netty 기본은 max(코어, 8) × 2 (이 PC 16코어 = 32) — 측정의 동시 대기(수백)보다 작아 새 천장이 된다. */
    val maxConnections: Int = 500,
    val connectTimeout: Duration = Duration.ofSeconds(1),
    /** WebClient 만. 기본 45초면 연결을 못 얻은 컨슈머가 그만큼 묶인다 — 빨리 "안 보냄" 으로 끝낸다. */
    val pendingAcquireTimeout: Duration = Duration.ofSeconds(1),
    val responseTimeout: Duration = Duration.ofSeconds(5),
) {
    init {
        require(maxConnections >= 1) { "maxConnections must be >= 1: $maxConnections" }
    }
    companion object {
        /** 응답 시간 초과 권장 하한 — 게이트웨이의 은행 응답 시간 초과(3s) + 여유. 이보다 짧으면 "모름" 이 쓸데없이 는다. */
        val RECOMMENDED_MIN_RESPONSE_TIMEOUT: Duration = Duration.ofSeconds(5)
    }
}
