package io.github.hyeokjinon.banktransfer.api.metrics

import io.github.hyeokjinon.banktransfer.api.reconcile.ReconcileProperties
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Clock

/**
 * P9a 지표 조립. MeterRegistry 는 actuator 자동 구성 (Prometheus 레지스트리는 build.gradle.kts 의 runtimeOnly).
 * (MetricsProperties 는 @ConfigurationPropertiesScan 도 등록하지만, 이 클래스만 떼어 쓰는 테스트를 위해 여기서도 켠다 — 같은 이름이라 한 번만 등록된다.)
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(MetricsProperties::class)
class MetricsConfiguration {

    @Bean
    fun transferMetrics(registry: MeterRegistry, window: BankMaintenanceWindow, clock: Clock) = TransferMetrics(registry, window, clock)

    /** 결과조회 소진 기준은 정리 작업과 같은 값 (transfer.reconcile.max-inquiries). */
    @Bean
    fun transferStateReader(jdbc: JdbcTemplate, reconcile: ReconcileProperties) = TransferStateReader(jdbc, reconcile.maxInquiries)

    @Bean
    fun transferStateGauges(registry: MeterRegistry, reader: TransferStateReader) = TransferStateGauges(registry, reader::read)

    @Bean
    fun transferStatePoller(gauges: TransferStateGauges, properties: MetricsProperties) = TransferStatePoller(gauges, properties)
}
