package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.config.BankTransferAdapter
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.transfer.TransferProcessor
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock

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

    @Bean
    fun transferProcessor(store: TransferStore, client: BankGatewayClient) =
        TransferProcessor(store, BankTransferAdapter(client), Clock.systemUTC())

    @Bean
    fun transferRequestedListener(processor: TransferProcessor) = TransferRequestedListener(processor::process)

    /**
     * 컨슈머 예외 처리 (P3-2): 1초부터 두 배씩, 최대 10초 간격으로 5번 다시 시도한 뒤 로그를 남기고 넘어간다.
     * 그동안 같은 파티션의 다음 메시지는 기다린다 (블로킹 재시도).
     * 다 실패한 이체는 RECEIVED 로 남는다 → P3-3 에서 재시도 토픽 + DLT 로 바꾼다.
     */
    @Bean
    fun kafkaErrorHandler(): DefaultErrorHandler =
        DefaultErrorHandler(
            ExponentialBackOffWithMaxRetries(5).apply {
                initialInterval = 1_000
                multiplier = 2.0
                maxInterval = 10_000
            },
        )
}
