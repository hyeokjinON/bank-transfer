package io.github.hyeokjinon.banktransfer.gatewayapp.health

import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator

/**
 * 은행 회선 상태 — /actuator/health/bankConnection (P8-1). K8s readiness 그룹에 들어간다 (P8-3a).
 * P8-3b: 대기자(리더 아님)는 UP(`role: standby`) — 둘 다 Ready 여야 롤링(`maxUnavailable 0`) · `rollout status` · PDB 가 돈다 (스펙 §16).
 * 트래픽은 readiness 가 아니라 리더 라벨 + Service selector 가 고른다. 리더인데 회선이 끊기면 DOWN (P8-3a B1 그대로 — 트래픽에서 빠진다).
 */
class BankConnectionHealthIndicator(private val owner: BankConnectionOwner) : HealthIndicator {
    override fun health(): Health {
        val client = owner.current() ?: return Health.up().withDetail("role", "standby").build()
        return if (client.isConnected) {
            Health.up().withDetail("role", "leader").withDetail("pending", client.pendingCount).build()
        } else {
            Health.down().withDetail("role", "leader").withDetail("reason", "bank channel not connected").build()
        }
    }
}
