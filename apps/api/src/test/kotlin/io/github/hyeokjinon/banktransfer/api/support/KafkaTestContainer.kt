package io.github.hyeokjinon.banktransfer.api.support

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.kafka.KafkaContainer

/** 통합 테스트용 Kafka. infra/docker-compose.yml 과 같은 apache/kafka:4.1.0 (KRaft). spring.kafka.bootstrap-servers 를 바꿔 준다. */
@TestConfiguration(proxyBeanMethods = false)
class KafkaTestContainer {

    @Bean
    @ServiceConnection
    fun kafka(): KafkaContainer = KafkaContainer("apache/kafka:4.1.0")
}
