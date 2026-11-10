package io.github.hyeokjinon.banktransfer.gatewayapp.health

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator

/**
 * 은행 회선 상태 — /actuator/health/bankConnection (P8-1). P8-3 에서 K8s readiness 로 쓴다:
 * 회선이 죽은 게이트웨이는 트래픽을 받지 않고, 호출자는 연결 실패 = "안 보냄" 으로 해석한다 (스펙 §4-3 과 맞물림).
 */
class BankConnectionHealthIndicator(private val client: BankGatewayClient) : HealthIndicator {
    override fun health(): Health =
        if (client.isConnected) {
            Health.up().withDetail("pending", client.pendingCount).build()
        } else {
            Health.down().withDetail("reason", "bank channel not connected").build()
        }
}
