package io.github.hyeokjinon.banktransfer.api.dailyrecon

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

/** 일마감 대사 조립 (P5-1). 잡 · 단계는 Task 10, 실행기 · 스케줄은 Task 11 에서 더한다. */
@Configuration(proxyBeanMethods = false)
class DailyReconJobConfiguration {

    @Bean
    fun reconRepository(jdbc: NamedParameterJdbcTemplate) = ReconRepository(jdbc)
}
