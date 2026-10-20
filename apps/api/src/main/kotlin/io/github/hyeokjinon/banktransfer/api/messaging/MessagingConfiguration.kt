package io.github.hyeokjinon.banktransfer.api.messaging

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.transaction.support.TransactionTemplate

object TransferTopics {
    /** 출금 접수 신호. key = 이체 id → 같은 이체의 메시지는 같은 파티션(순서 보장). */
    const val REQUESTED = "transfer.requested"
}

/**
 * Kafka 쪽 조립. 토픽은 코드로 선언한다 (브로커의 자동 생성은 꺼 두었다 — 오타 토픽 방지).
 * OutboxRelayProperties 는 BankTransferApplication 의 @ConfigurationPropertiesScan 이 등록한다.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
class MessagingConfiguration {

    @Bean
    fun transferRequestedTopic(): NewTopic = TopicBuilder.name(TransferTopics.REQUESTED).partitions(3).replicas(1).build()

    @Bean
    fun outboxRelay(
        jdbc: JdbcTemplate,
        tx: TransactionTemplate,
        kafka: KafkaTemplate<String, String>,
        props: OutboxRelayProperties,
    ) = OutboxRelay(jdbc, tx, kafka, props.batchSize)

    @Bean
    @ConditionalOnProperty(prefix = "transfer.outbox.relay", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun outboxRelayScheduler(relay: OutboxRelay) = OutboxRelayScheduler(relay)
}
