package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.transfer.ProcessResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import java.util.UUID

/** 은행에 보내지 못해 나중에 다시 처리해야 한다. 에러 핸들러가 이 예외를 보고 재시도한다. */
class RetryLaterException(transferId: UUID) : RuntimeException("bank not reachable, transfer $transferId will be retried")

/**
 * transfer.requested 컨슈머. key = 이체 id. 처리가 끝난 뒤에만 오프셋을 커밋한다 (수동 ack):
 * 처리 중에 죽으면 같은 메시지를 다시 받고, 조건부 상태 변경이 중복 처리를 막는다.
 * 은행에 보내지 못한 경우(RETRY_LATER)만 ack 하지 않고 예외를 던진다.
 */
class TransferRequestedListener(private val process: (UUID) -> ProcessResult) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = [TransferTopics.REQUESTED], groupId = "transfer-processor", concurrency = "3")
    fun on(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val transferId = UUID.fromString(record.key())
        val result = process(transferId)
        if (result == ProcessResult.RETRY_LATER) throw RetryLaterException(transferId)
        log.info("transfer {} processed: {} (partition {}, offset {})", transferId, result, record.partition(), record.offset())
        ack.acknowledge()
    }
}
