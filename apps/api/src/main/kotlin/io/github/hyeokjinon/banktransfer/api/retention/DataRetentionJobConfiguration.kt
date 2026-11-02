package io.github.hyeokjinon.banktransfer.api.retention

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

/** 보존 배치 조립 (P5-2, 설계 §4). */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DataRetentionProperties::class)
class DataRetentionJobConfiguration {

    @Bean
    fun retentionRepository(jdbc: NamedParameterJdbcTemplate) = RetentionRepository(jdbc)
}
