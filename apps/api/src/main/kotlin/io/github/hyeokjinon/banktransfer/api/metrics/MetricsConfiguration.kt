package io.github.hyeokjinon.banktransfer.api.metrics

import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

/** P9a 지표 조립. MeterRegistry 는 actuator 자동 구성 (Prometheus 레지스트리는 build.gradle.kts 의 runtimeOnly). */
@Configuration(proxyBeanMethods = false)
class MetricsConfiguration {

    @Bean
    fun transferMetrics(registry: MeterRegistry, window: BankMaintenanceWindow, clock: Clock) = TransferMetrics(registry, window, clock)
}
