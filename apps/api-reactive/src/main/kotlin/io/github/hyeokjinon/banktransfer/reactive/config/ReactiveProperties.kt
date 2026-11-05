package io.github.hyeokjinon.banktransfer.reactive.config

import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import java.time.LocalTime
import java.time.ZoneId

/** 1원인증 (P6-2). [webMode] = coroutine(기본) | reactor — 소문자 그대로만 (apps/api 의 VerificationProperties 와 같은 이유). */
@ConfigurationProperties("account.verification")
data class ReactiveVerificationProperties(
    val ttl: Duration = Duration.ofMinutes(5),
    val webMode: String = COROUTINE,
) {
    init {
        require(webMode in WEB_MODES) { "account.verification.web-mode must be one of $WEB_MODES (lower case): '$webMode'" }
    }

    companion object {
        const val COROUTINE = "coroutine"
        const val REACTOR = "reactor"
        val WEB_MODES = listOf(COROUTINE, REACTOR)
    }
}

/** apps/api 의 같은 키 · 같은 기본값 (결정(계획): 20줄짜리라 모듈로 빼지 않고 앱마다 둔다). */
@ConfigurationProperties("bank.gateway")
data class BankGatewayProperties(
    val host: String = "localhost",
    val port: Int = 9090,
    val responseTimeout: Duration = Duration.ofSeconds(3),
    val pollInterval: Duration = Duration.ofSeconds(30),
)

/** apps/api 와 같다 — KST 벽시계 "HH:mm", 시작 포함 · 끝 제외 (YAML 에서는 따옴표 필수). */
@ConfigurationProperties("bank.maintenance")
data class BankMaintenanceProperties(
    val enabled: Boolean = true,
    val start: String = "23:50",
    val end: String = "00:10",
) {
    fun toWindow() = BankMaintenanceWindow(LocalTime.parse(start), LocalTime.parse(end), ZoneId.of("Asia/Seoul"), enabled)
}
