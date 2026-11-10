package io.github.hyeokjinon.banktransfer.gatewayapp.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.gatewayapp.health.BankConnectionHealthIndicator
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock
import java.time.Duration
import java.time.LocalTime
import java.time.ZoneId

@ConfigurationProperties("bank.gateway")
data class GatewayBankProperties(
    val host: String = "localhost",
    val port: Int = 9090,
    val responseTimeout: Duration = Duration.ofSeconds(3),
    val pollInterval: Duration = Duration.ofSeconds(30),
)

/** api 와 같은 규칙 (KST 벽시계 "HH:mm", 시작 포함 · 끝 제외, 따옴표 필수). */
@ConfigurationProperties("bank.maintenance")
data class GatewayMaintenanceProperties(
    val enabled: Boolean = true,
    val start: String = "23:50",
    val end: String = "00:10",
) {
    fun toWindow() = BankMaintenanceWindow(LocalTime.parse(start), LocalTime.parse(end), ZoneId.of("Asia/Seoul"), enabled)
}

/** 조립은 여기서만. 은행 연결은 이 앱에 하나뿐이다 (스펙 §2 결정 7). */
@Configuration(proxyBeanMethods = false)
class GatewayConfiguration {

    @Bean
    fun clock(): Clock = Clock.system(ZoneId.of("Asia/Seoul"))   // 저장소 시간대 정책은 KST (docs/database.md)

    /** 점검 중에는 전문을 보내지 않는다 (P5-1) — 503 BANK_NOT_SENT 로 돌려준다 (호출자에게 "안 보냄"). */
    @Bean(destroyMethod = "close")
    fun bankGatewayClient(props: GatewayBankProperties, maintenance: GatewayMaintenanceProperties, clock: Clock): BankGatewayClient {
        val window = maintenance.toWindow()
        return BankGatewayClient(
            BankGatewayConfig(host = props.host, port = props.port, responseTimeout = props.responseTimeout, pollInterval = props.pollInterval),
            clock,
            sendGate = { if (window.isActive(clock.instant())) "bank maintenance window" else null },
        ).start()
    }

    @Bean
    fun bankConnectionHealthIndicator(client: BankGatewayClient) = BankConnectionHealthIndicator(client)
}
