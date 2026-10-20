package io.github.hyeokjinon.banktransfer.api.messaging

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

object TransferTopics {
    /** 출금 접수 신호. key = 이체 id → 같은 이체의 메시지는 같은 파티션(순서 보장). */
    const val REQUESTED = "transfer.requested"
}

/** Kafka 쪽 조립. 토픽은 코드로 선언한다 (브로커의 자동 생성은 꺼 두었다 — 오타 토픽 방지). */
@Configuration(proxyBeanMethods = false)
class MessagingConfiguration {

    @Bean
    fun transferRequestedTopic(): NewTopic = TopicBuilder.name(TransferTopics.REQUESTED).partitions(3).replicas(1).build()
}
