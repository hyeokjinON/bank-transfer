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
import org.springframework.kafka.retrytopic.RetryTopicSchedulerWrapper
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock

object TransferTopics {
    /** 출금 접수 신호. key = 이체 id → 같은 이체의 메시지는 같은 파티션(순서 보장). */
    const val REQUESTED = "transfer.requested"

    /** 원래 토픽 · 재시도 토픽 · DLT 의 파티션 수. DLT 로 보낼 때 원래 파티션 번호를 그대로 쓰므로 모두 같아야 해서 한 곳에서 정한다. */
    const val PARTITIONS = 3

    /** 애너테이션(@RetryableTopic numPartitions)은 문자열만 받는다. [PARTITIONS] 와 같은 값. */
    const val PARTITIONS_STR = "3"

    // @RetryableTopic 이 만드는 토픽 (인덱스 접미사 — 간격 설정을 바꿔도 이름이 같다). 운영 문서 · 테스트가 이름을 여기서 본다
    const val RETRY_0 = "$REQUESTED-retry-0"
    const val RETRY_1 = "$REQUESTED-retry-1"
    const val RETRY_2 = "$REQUESTED-retry-2"
    const val DLT = "$REQUESTED-dlt"
}

/**
 * Kafka 쪽 조립. 토픽은 코드로 선언한다 (브로커의 자동 생성은 꺼 두었다 — 오타 토픽 방지). 재시도 · DLT 토픽은 리스너의 @RetryableTopic 이 만든다.
 * OutboxRelayProperties 는 BankTransferApplication 의 @ConfigurationPropertiesScan 이 등록한다.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
class MessagingConfiguration {

    @Bean
    fun transferRequestedTopic(): NewTopic = TopicBuilder.name(TransferTopics.REQUESTED).partitions(TransferTopics.PARTITIONS).replicas(1).build()

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
     * 재시도 토픽에서 "아직 때가 안 된" 파티션을 멈췄다가 다시 여는 스케줄러. Spring Kafka 는 이 빈을 TaskScheduler 보다 먼저 찾는다.
     * 따로 두는 이유: 없으면 공용 스케줄러(스레드 2개)를 쓰는데, 발행기와 정리 작업(은행이 느리면 한 주기가 길다)이 둘 다 잡고 있으면
     * 파티션 재개가 늦어져 재시도 간격이 설정보다 길어진다.
     */
    @Bean
    fun retryTopicSchedulerWrapper() = RetryTopicSchedulerWrapper(
        ThreadPoolTaskScheduler().apply {
            poolSize = 1
            setThreadNamePrefix("retry-topic-")
        },
    )
}
