package io.github.hyeokjinon.banktransfer.api.messaging

import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.transaction.support.TransactionTemplate
import java.util.concurrent.TimeUnit

@ConfigurationProperties("transfer.outbox.relay")
data class OutboxRelayProperties(
    val enabled: Boolean = true,
    val intervalMs: Long = 500,
    val batchSize: Int = 100,
)

/**
 * 폴링 퍼블리셔: outbox_event 의 미발행 행을 Kafka 로 보내고 published_at 을 기록한다.
 * - FOR UPDATE SKIP LOCKED: 앱이 여러 대여도 같은 행을 두 번 보내지 않고, 서로 기다리지도 않는다.
 * - 브로커가 받았다고 확인한(get) 뒤에 published_at 을 쓴다. 그 사이에 죽으면 다음 주기에 다시 보낸다 → 적어도 한 번.
 *   중복은 컨슈머의 조건부 상태 변경(RECEIVED 일 때만 SENDING)이 막는다.
 * - 메시지 key = 이체 id, value = payload 에 eventId(outbox_event.id)를 붙인 JSON.
 */
class OutboxRelay(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
    private val kafka: KafkaTemplate<String, String>,
    private val batchSize: Int,
) {

    /** 최대 batchSize 건을 발행하고, 발행한 건수를 돌려준다. */
    fun publishPending(): Int =
        tx.execute {
            val events = jdbc.query(
                """
                SELECT id, aggregate_id, JSON_SET(payload, '$.eventId', id) AS message
                  FROM outbox_event
                 WHERE published_at IS NULL
                 ORDER BY id
                 LIMIT ?
                   FOR UPDATE SKIP LOCKED
                """.trimIndent(),
                { rs, _ -> PendingEvent(rs.getLong("id"), rs.getString("aggregate_id"), rs.getString("message")) },
                batchSize,
            )
            events.forEach { event ->
                kafka.send(TransferTopics.REQUESTED, event.aggregateId, event.message).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                jdbc.update("UPDATE outbox_event SET published_at = NOW(6) WHERE id = ?", event.id)
            }
            events.size
        }!!

    private data class PendingEvent(val id: Long, val aggregateId: String, val message: String)

    private companion object {
        const val SEND_TIMEOUT_SECONDS = 5L
    }
}

/** 스케줄 실행은 따로 둔다: 테스트는 이 빈을 끄고 OutboxRelay.publishPending() 을 직접 부른다. */
class OutboxRelayScheduler(private val relay: OutboxRelay) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${transfer.outbox.relay.interval-ms:500}")
    fun run() {
        try {
            relay.publishPending()
        } catch (e: Exception) {
            // 브로커 장애 등. 트랜잭션은 롤백됐고 published_at 도 그대로라 다음 주기에 다시 보낸다.
            log.warn("outbox relay failed, retrying next round: {}", e.message)
        }
    }
}
