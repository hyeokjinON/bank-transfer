package io.github.hyeokjinon.banktransfer.api.metrics

import io.github.hyeokjinon.banktransfer.api.reconcile.ReconcileProperties
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Clock
import javax.sql.DataSource

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
    fun transferStateReader(dataSource: DataSource, reconcile: ReconcileProperties, properties: MetricsProperties): TransferStateReader {
        // 상태 조회는 자기 JdbcTemplate 으로: 멈춘 쿼리(메타데이터 락 · 반쯤 닫힌 소켓)가 조회 주기보다 오래 붙잡지 못하게 (위험 A6). 시간 초과는 실패 = NaN + error
        val jdbc = JdbcTemplate(dataSource).apply { queryTimeout = (properties.dbPollInterval.seconds / 2).toInt().coerceAtLeast(1) }
        return TransferStateReader(jdbc, reconcile.maxInquiries)
    }

    @Bean
    fun transferStateGauges(registry: MeterRegistry, reader: TransferStateReader, properties: MetricsProperties) =
        TransferStateGauges(registry, staleAfter = properties.dbPollInterval.multipliedBy(3), read = reader::read)   // 3 주기 동안 새 값이 없으면 NaN

    @Bean
    fun transferStatePoller(gauges: TransferStateGauges, properties: MetricsProperties) = TransferStatePoller(gauges, properties)
}
