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
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
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

    /**
     * 발행기는 공용 TransactionTemplate 대신 READ COMMITTED 전용 템플릿을 쓴다 (빈으로 올리지 않는다: 공용 템플릿 자동 구성이 물러난다).
     * 이유: MySQL 기본 REPEATABLE READ 에서는 `SELECT … FOR UPDATE SKIP LOCKED` 가 next-key/gap 락까지 잡아,
     * 발행기가 Kafka 응답을 기다리는 동안 접수의 outbox INSERT 가 락 대기(ERROR 1205)에 걸린다.
     * READ COMMITTED 는 고른 행만 잠그고 gap 락이 없어 새 INSERT 가 막히지 않는다. 행 락 + SKIP LOCKED 라 이중 발행 방지는 그대로다.
     */
    @Bean
    fun outboxRelay(
        jdbc: JdbcTemplate,
        transactionManager: PlatformTransactionManager,
        kafka: KafkaTemplate<String, String>,
        props: OutboxRelayProperties,
    ): OutboxRelay {
        val readCommitted = TransactionTemplate(transactionManager).apply {
            isolationLevel = TransactionDefinition.ISOLATION_READ_COMMITTED
        }
        return OutboxRelay(jdbc, readCommitted, kafka, props.batchSize)
    }

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
     * 넘어간 레코드의 오프셋은 커밋한다(isCommitRecovered): manual_immediate 에서는 기본값이 커밋하지 않아,
     * 재시작 뒤 그 메시지가 다시 올지 말지가 우연에 달린다. "넘김 = 커밋"으로 정해 둔다. 이체는 P3-3 까지 RECEIVED 로 남는다.
     */
    @Bean
    fun kafkaErrorHandler(): DefaultErrorHandler =
        DefaultErrorHandler(
            ExponentialBackOffWithMaxRetries(5).apply {
                initialInterval = 1_000
                multiplier = 2.0
                maxInterval = 10_000
            },
        ).apply { setCommitRecovered(true) }
}
