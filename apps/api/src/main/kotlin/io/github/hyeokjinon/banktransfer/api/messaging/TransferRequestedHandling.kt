package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import io.github.hyeokjinon.banktransfer.transfer.ProcessResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.springframework.kafka.support.KafkaHeaders
import java.nio.ByteBuffer
import java.util.UUID

/** 은행에 보내지 못해 나중에 다시 처리해야 한다. 재시도 토픽으로 넘어간다 (P3-3b). */
class RetryLaterException(transferId: UUID) : RuntimeException("bank not reachable, transfer $transferId will be retried")

/** key 가 이체 id(UUID)가 아니다. 몇 번 다시 해도 같은 결과라 재시도 없이 DLT 로 보낸다 (@RetryableTopic exclude). */
class InvalidTransferKeyException : RuntimeException("message key is not a transfer id (UUID)")   // 키 값은 담지 않는다: 외부 입력이라 DLT 핸들러가 정리해서 한 번만 남긴다

/**
 * transfer.requested 리스너 두 판(블로킹 TransferRequestedListener · 코루틴 SuspendingTransferRequestedListener)의 @RetryableTopic · @KafkaListener 값 (P6-1).
 * 애너테이션은 상수만 받고 메타 애너테이션으로 묶을 수 없어 값을 여기 한 곳에 둔다 — 두 판의 재시도 단계 · 그룹이 따로 놀지 않게 (테스트가 두 애너테이션이 같은지 본다).
 */
object TransferListenerSettings {
    const val ATTEMPTS = "\${transfer.retry.attempts:4}"
    const val DELAY_MS = "\${transfer.retry.delay-ms:5000}"
    const val MULTIPLIER = "\${transfer.retry.multiplier:6.0}"
    const val MAX_DELAY_MS = "\${transfer.retry.max-delay-ms:60000}"

    /** 그룹 · 동시성은 application.yml 한 곳에서 정한다 (이 값은 그것을 가리킬 뿐이다 — 재시도 · DLT 그룹이 접미사로 나뉘게). */
    const val GROUP_ID = "\${spring.kafka.consumer.group-id}"
}

/**
 * transfer.requested 컨슈머의 모드 공통 부분 (P6-1 에서 TransferRequestedListener 에서 뽑았다): key 읽기 · 처리 결과 마무리 · DLT.
 * 두 판의 경고 · 알림 · DLT 분기가 같아야 해서 한 곳에 둔다. 로그는 부르는 리스너의 로거로 남긴다 — 운영 로그와 테스트가 리스너 이름으로 찾는다.
 * 사람이 봐야 하는 두 경우(CONFLICT, 은행 장애가 아닌 이유의 DLT)는 로그와 함께 WARN 알림을 보낸다 (P5-1). 알림 실패는 처리 결과를 바꾸지 않는다.
 */
internal class TransferRequestedHandling(
    private val alerts: AlertNotifier,
    private val log: Logger,
) {

    fun transferIdOf(key: String?): UUID {
        if (key == null) throw InvalidTransferKeyException()
        return try {
            UUID.fromString(key)
        } catch (e: IllegalArgumentException) {
            throw InvalidTransferKeyException()
        }
    }

    /**
     * 처리 결과로 메시지를 마무리한다. RETRY_LATER 면 [RetryLaterException] 을 던진다 → 재시도 토픽 (리스너는 ack 하지 않는다).
     * CONFLICT 는 재시도로 풀 수 없어 경고 · 알림만 남기고 넘긴다. 그 밖은 처리 로그 한 줄.
     */
    fun conclude(record: ConsumerRecord<String, String>, transferId: UUID, result: ProcessResult) {
        if (result == ProcessResult.RETRY_LATER) throw RetryLaterException(transferId)
        if (result == ProcessResult.CONFLICT) {
            // 재시도로 풀 수 없다: 은행이 확정한 결과가 이미 기록된 최종 상태와 다르다. 오프셋은 넘기고 사람이 확인하게 경고만 남긴다.
            // 잔고도 자동으로 고치지 않는다 (P4-1): 예컨대 FAILED 로 확정 · 환불한 뒤 은행이 완료라고 답했다면 환불된 돈이 실제로 나간 상태다 → 수동 확인 · P5 대사
            log.warn("transfer {} conflict: bank result differs from the recorded final status — wallet balance left as is, needs manual check", transferId)
            alerts.notifySafely(
                Alert(
                    AlertLevel.WARN, "출금 확정과 은행 결과가 어긋남 (CONFLICT)",
                    listOf("transferId=$transferId", "잔고는 그대로 — 수동 확인, 일마감 대사가 다음 날 다시 맞춘다"),
                ),
            )
        }
        log.info("transfer {} processed: {} ({} partition {}, offset {})", transferId, result, record.topic(), record.partition(), record.offset())
    }

    /**
     * 재시도를 다 쓴 메시지 · 재시도하지 않는 메시지. 로그만 남긴다 — ack 는 리스너가 항상 한다 (안 하면 DLT 오프셋이 커밋되지 않아 재시작마다 다시 온다).
     * DLT 에 온 이유에 따라 이체의 다음이 다르므로 로그 수준도 다르다 ([dltKind]):
     * - 은행 연결 없음: 은행에 보내려 한 흔적(sending_at)이 있어 5분 뒤 정리 작업이 아웃박스로 다시 발행한다 → WARN
     * - 잘못된 key: 이체가 없다. 보낸 쪽 버그 → WARN
     * - 그 밖(DB 장애 등): 재발행 대상이 아니다 (sending_at 이 NULL 이면 정리 작업이 건드리지 않는다) → ERROR, 사람이 확인
     * 한 줄로만 남긴다: 예외 헤더는 줄바꿈이 섞일 수 있어 정리하고 길이를 자른다. 메시지 값 · 계좌번호는 남기지 않는다. 은행 장애가 아닌 이유는 WARN 알림도 보낸다 (P5-1).
     */
    fun onDlt(record: ConsumerRecord<String, String>) {
        val causeFqcn = record.headerText(KafkaHeaders.EXCEPTION_CAUSE_FQCN) ?: record.headerText(KafkaHeaders.EXCEPTION_FQCN)
        val original = "${record.headerText(KafkaHeaders.ORIGINAL_TOPIC)}-${record.headerInt(KafkaHeaders.ORIGINAL_PARTITION)}@${record.headerLong(KafkaHeaders.ORIGINAL_OFFSET)}"
        when (dltKind(causeFqcn)) {
            DltKind.BANK_NOT_CONNECTED -> log.warn(
                "transfer {} moved to DLT: bank not connected. stays RECEIVED, reconcile will republish it. original={}",
                record.key(), original,
            )
            // 키가 UUID 가 아니라 이체 id 로 쓸 수 없다. 키는 남길 값이 이것뿐이라 한 번만, 정리해서 짧게
            DltKind.INVALID_KEY -> log.warn(
                "message moved to DLT: key is not a transfer id (UUID), no transfer is affected. key={} original={}",
                logSafe(record.key(), KEY_LOG_MAX), original,
            )
            DltKind.OTHER -> {
                log.error(
                    "transfer {} moved to DLT and is not retried automatically - check transfer status. " +
                        "republish covers only bank-not-connected: a RECEIVED transfer with sending_at NULL is skipped. exception={} cause={} original={}",
                    record.key(), causeFqcn, logSafe(rootMessage(record.headerText(KafkaHeaders.EXCEPTION_MESSAGE)), MESSAGE_LOG_MAX), original,
                )
                // 자동으로 다시 하지 않는 유일한 DLT 경우라 사람에게 알린다 (P5-1). 메시지 값 · 계좌번호는 싣지 않는다
                alerts.notifySafely(
                    Alert(
                        AlertLevel.WARN, "DLT: 자동으로 다시 처리하지 않는 출금",
                        listOf("transferId=${logSafe(record.key(), KEY_LOG_MAX)}", "exception=$causeFqcn", "출금 상태를 확인하고 손으로 다시 처리한다 (OPERATION §6)"),
                    ),
                )
            }
        }
    }
}

private fun ConsumerRecord<*, *>.headerText(name: String): String? =
    headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8)

private fun ConsumerRecord<*, *>.headerInt(name: String): Int? =
    headers().lastHeader(name)?.value()?.takeIf { it.size == Int.SIZE_BYTES }?.let { ByteBuffer.wrap(it).int }

private fun ConsumerRecord<*, *>.headerLong(name: String): Long? =
    headers().lastHeader(name)?.value()?.takeIf { it.size == Long.SIZE_BYTES }?.let { ByteBuffer.wrap(it).long }

private const val KEY_LOG_MAX = 64
private const val MESSAGE_LOG_MAX = 200

/** DLT 로 온 이유. 로그 수준 · 안내가 갈린다 (테스트에서 분기만 따로 확인할 수 있게 순수 함수로 뺐다). */
internal enum class DltKind { BANK_NOT_CONNECTED, INVALID_KEY, OTHER }

internal fun dltKind(causeFqcn: String?): DltKind = when (causeFqcn) {
    RetryLaterException::class.java.name -> DltKind.BANK_NOT_CONNECTED
    InvalidTransferKeyException::class.java.name -> DltKind.INVALID_KEY
    else -> DltKind.OTHER
}

/** 로그에 남길 외부 입력: 줄바꿈 · 제어문자를 `?` 로 바꾸고 길이를 자른다 (로그 위조 · 한 줄 깨짐 방지). */
internal fun logSafe(text: String?, max: Int): String {
    if (text == null) return "null"
    val cleaned = text.map { if (it.isISOControl()) '?' else it }.joinToString("")
    return if (cleaned.length <= max) cleaned else cleaned.take(max) + "..."
}

/**
 * DLT 헤더 메시지에서 원래 예외의 메시지만 남긴다.
 * spring-kafka 4.1 은 "<리스너 래퍼 메시지>; <원래 예외 메시지>" 로 붙이고, 래퍼 메시지는
 * "Listener method '…' threw exception\nEndpoint handler details:\nMethod […]\nBean […]" 처럼 길어서
 * 그대로 자르면 원인이 잘려 나간다. 원래 메시지 안에도 "; " 가 있을 수 있으니 마지막이 아니라 `Bean [` 뒤의 첫 "]; " 로 가른다.
 */
internal fun rootMessage(message: String?): String? {
    if (message == null) return null
    val bean = message.indexOf("\nBean [")
    val separator = if (bean < 0) -1 else message.indexOf("]; ", bean)
    return if (separator < 0) message else message.substring(separator + 3)
}
