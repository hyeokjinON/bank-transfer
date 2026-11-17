package io.github.hyeokjinon.banktransfer.gatewayapp.metrics

import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.gateway.TranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.MeterBinder
import java.time.Clock

/**
 * 게이트웨이 상태 Gauge (P9a 스펙 §4-3). 값은 읽을 때마다 Owner · 번호 출처 · 점검 창에서 바로 계산한다 (따로 들고 있지 않는다).
 * Boot 가 MeterBinder 빈을 레지스트리에 묶는다.
 */
class GatewayGauges(
    private val owner: BankConnectionOwner,
    private val tranNoSource: TranNoSource,
    private val maintenance: BankMaintenanceWindow,
    private val clock: Clock,
) : MeterBinder {
    override fun bindTo(registry: MeterRegistry) {
        Gauge.builder(GatewayMetrics.LEADER, owner) { if (it.isLeader) 1.0 else 0.0 }
            .description("이 Pod 가 은행 연결 주인(리더)인가 — 두 Pod 합이 1 이어야 정상").register(registry)
        Gauge.builder(GatewayMetrics.BANK_CONNECTED, owner) { if (it.current()?.isConnected == true) 1.0 else 0.0 }
            .description("은행 TCP 회선이 붙어 있나 (리더가 아니면 0)").register(registry)
        Gauge.builder(GatewayMetrics.TRAN_NO_REMAINING, tranNoSource) { it.remaining()?.toDouble() ?: Double.NaN }
            .description("예약해 둔 남은 전문번호 (현재 + 미리 예약). 메모리 출처는 NaN").register(registry)
        Gauge.builder(GatewayMetrics.BANK_MAINTENANCE, maintenance) { if (it.isActive(clock.instant())) 1.0 else 0.0 }
            .description("은행 점검 시간인가 (KST 창 — 규칙이 이것으로 알림을 뺀다, 결정 D8)").register(registry)
    }
}
