package io.github.hyeokjinon.banktransfer.reactive.config

import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.gateway.http.HttpBankGateway
import io.github.hyeokjinon.banktransfer.gateway.http.HttpGatewaySettings
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

/**
 * apps/api 의 같은 키 · 같은 기본값 (결정(계획): 20줄짜리라 모듈로 빼지 않고 앱마다 둔다). 은행 게이트웨이 (P8-1). mode: direct(기본 — 이 프로세스가 Netty 로 FEP 에 직결) | http(게이트웨이 앱을 HTTP 로 부름, apps/bank-gateway).
 * host · port · response-timeout · poll-interval 은 direct 일 때만 쓴다. http.* 는 http 일 때만 (스펙 §6).
 */
@ConfigurationProperties("bank.gateway")
data class BankGatewayProperties(
    val mode: String = DIRECT,
    val host: String = "localhost",
    val port: Int = 9090,
    val responseTimeout: Duration = Duration.ofSeconds(3),
    val pollInterval: Duration = Duration.ofSeconds(30),
    val http: Http = Http(),
) {
    init {
        require(mode in MODES) { "bank.gateway.mode must be one of $MODES (lower case): '$mode'" }
    }

    data class Http(
        val baseUrl: String = "http://localhost:8082",
        val client: String = HttpBankGateway.WEBCLIENT,
        val maxConnections: Int = 500,
        val connectTimeout: Duration = Duration.ofSeconds(1),
        val pendingAcquireTimeout: Duration = Duration.ofSeconds(1),
        val responseTimeout: Duration = Duration.ofSeconds(5),
    ) {
        init {
            require(client in CLIENTS) { "bank.gateway.http.client must be one of $CLIENTS (lower case): '$client'" }
        }

        fun toSettings() = HttpGatewaySettings(baseUrl, maxConnections, connectTimeout, pendingAcquireTimeout, responseTimeout)
    }

    companion object {
        const val DIRECT = "direct"
        const val HTTP = "http"
        val MODES = listOf(DIRECT, HTTP)
        val CLIENTS = listOf(HttpBankGateway.WEBCLIENT)
    }
}

/** apps/api 와 같다 — KST 벽시계 "HH:mm", 시작 포함 · 끝 제외 (YAML 에서는 따옴표 필수). */
@ConfigurationProperties("bank.maintenance")
data class BankMaintenanceProperties(
    val enabled: Boolean = true,
    val start: String = "23:50",
    val end: String = "00:10",
) {
    fun toWindow() = BankMaintenanceWindow(LocalTime.parse(start), LocalTime.parse(end), ZoneId.of("Asia/Seoul"), enabled)
}
