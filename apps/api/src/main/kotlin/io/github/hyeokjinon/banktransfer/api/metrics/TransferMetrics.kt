package io.github.hyeokjinon.banktransfer.api.metrics

import io.github.hyeokjinon.banktransfer.api.messaging.DltKind
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.transfer.BankTransferOutcome
import io.github.hyeokjinon.banktransfer.transfer.InquiryResolution
import io.github.hyeokjinon.banktransfer.transfer.TransferObserver
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * api 사건 지표 (P9a 스펙 §4-1). 도메인 사건은 [TransferObserver] 로 받는다 (저장이 커밋된 뒤에만 불린다 — 위험 A4).
 * 접수 · DLT · 아웃박스는 웹 · 메시징 계층이 직접 부른다. 라벨은 결과 · 상태 · 종류뿐 (사용자 · 계좌 · 거래번호 · 금액 없음, 결정 D7).
 * 카운터는 근사치다 — Kafka 최소 한 번 · Pod 재시작으로 정확한 건수가 아니다. 돈의 정답은 DB (위험 B3).
 * 모든 태그 조합을 처음에 0 으로 만든다 (Prometheus increase() 가 첫 증가를 놓치지 않게).
 *
 * 어떤 메서드도 예외를 던지지 않는다 ([TransferObserver] 계약 — 관찰은 돈 처리 흐름을 바꾸지 않는다):
 * 미리 만든 미터는 없는 값이면 건너뛰고, 그래도 나는 예외(시계 · 시각 계산 · 레지스트리)는 [safely] 가 삼킨다.
 * 삼킨 것은 미터 이름별로 처음 한 번 WARN, 그 뒤는 DEBUG 로 남긴다 (계좌 · 사용자 · 거래번호 없이 미터 이름과 예외 종류만).
 */
class TransferMetrics(
    private val registry: MeterRegistry,
    private val maintenance: BankMaintenanceWindow,
    private val clock: Clock,
) : TransferObserver {

    private val log = LoggerFactory.getLogger(javaClass)
    private val warned: MutableSet<String> = ConcurrentHashMap.newKeySet()

    private val acceptedCounters: Map<Boolean, Counter> = mapOf(
        true to counter(ACCEPTED, "result", "created", "출금 접수 — 새로 만든 건"),
        false to counter(ACCEPTED, "result", "replayed", "출금 접수 — 같은 멱등키 재요청 (기존 건을 돌려줌)"),
    )
    private val bankResults: Map<String, Counter> = BANK_RESULT_TAGS.associateWith {
        counter(BANK_RESULT, "outcome", it, "컨슈머의 은행 호출 결과 (maintenance = 점검 창 안의 미전송)")
    }
    private val completed: Map<TransferStatus, Timer> = listOf(TransferStatus.COMPLETED, TransferStatus.FAILED).associateWith {
        Timer.builder(TRANSFER_COMPLETED).description("접수(created_at) → 확정 시간 — 재시도 · 결과조회를 거치면 분 단위")
            .tag("status", it.name).serviceLevelObjectives(*COMPLETED_BUCKETS).register(registry)
    }
    // 이 "포기" 는 재발행 소진(RECEIVED → FAILED E001, 알림 TransferGaveUp) — 도메인 ReconcileResult.gaveUp(결과조회 소진)과 다른 것이다.
    private val gaveUp: Counter = Counter.builder(GAVE_UP).description("재발행 한도 소진 FAILED(E001) — 사용자가 다시 요청해야 한다").register(registry)
    private val deadLetters: Map<DltKind, Counter> = DltKind.entries.associateWith {
        counter(DLT, "kind", it.name.lowercase(), "DLT 로 온 메시지 (bank_not_connected 는 정리 작업이 재발행한다)")
    }
    private val republishedCount: Counter = Counter.builder(REPUBLISHED).description("정리 작업의 재발행").register(registry)
    private val reconciled: Map<InquiryResolution, Counter> = InquiryResolution.entries.associateWith {
        counter(RECONCILED, "result", it.name.lowercase(), "UNKNOWN 결과조회 결과")
    }
    private val outbox: Map<String, Counter> = listOf("ok", "error").associateWith {
        counter(OUTBOX_PUBLISHED, "result", it, "아웃박스 발행 — ok = 발행 건수, error = 실패한 주기 수")
    }

    init {
        Gauge.builder(BANK_MAINTENANCE, maintenance) {
            // 시계 · 창 계산이 실패해도 스크랩을 깨지 않는다 — 값을 모르면 NaN (0 = 점검 아님 이라고 말하지 않는다)
            try {
                if (it.isActive(clock.instant())) 1.0 else 0.0
            } catch (e: Exception) {
                Double.NaN
            }
        }.description("은행 점검 시간인가 (KST 창 — 규칙이 이것으로 알림을 뺀다, 결정 D8)").register(registry)
    }

    /** 접수 API 가 돌아온 뒤 (저장 = 커밋 뒤). 예외(잔고 부족 · 점검 503 …)면 부르지 않는다. */
    fun accepted(created: Boolean) = safely(ACCEPTED) { acceptedCounters[created]?.increment() }

    /** 미전송은 지금이 점검 창이면 maintenance — 점검 보류도 SendGate 의 BankNotSentException 으로 오기 때문이다 ("계획에서 정한 것" 1). */
    override fun bankResult(outcome: BankTransferOutcome) = safely(BANK_RESULT) {
        val tag = when (outcome) {
            BankTransferOutcome.Completed -> "ok"
            is BankTransferOutcome.Rejected -> "rejected"
            BankTransferOutcome.ResultUnknown -> "unknown"
            BankTransferOutcome.NotSent -> if (maintenance.isActive(clock.instant())) "maintenance" else "not_sent"
        }
        bankResults[tag]?.increment()
    }

    /** COMPLETED · FAILED 만 센다 (다른 상태는 미리 만든 Timer 가 없어 건너뛴다). 시각이 거꾸로면 0 으로 본다. */
    override fun finalized(status: TransferStatus, createdAt: Instant, at: Instant) = safely(TRANSFER_COMPLETED) {
        completed[status]?.record(Duration.between(createdAt, at).coerceAtLeast(Duration.ZERO))
    }

    override fun republished() = safely(REPUBLISHED) { republishedCount.increment() }

    override fun abandoned() = safely(GAVE_UP) { gaveUp.increment() }

    override fun inquired(resolution: InquiryResolution) = safely(RECONCILED) { reconciled[resolution]?.increment() }

    fun deadLettered(kind: DltKind) = safely(DLT) { deadLetters[kind]?.increment() }

    /** publishPending 이 돌아온 뒤 (발행기 트랜잭션 커밋 뒤). 0 이하는 세지 않는다. */
    fun outboxPublished(count: Int) = safely(OUTBOX_PUBLISHED) {
        if (count > 0) outbox["ok"]?.increment(count.toDouble())
    }

    fun outboxFailed() = safely(OUTBOX_PUBLISHED) { outbox["error"]?.increment() }

    private fun counter(name: String, key: String, value: String, description: String): Counter =
        Counter.builder(name).description(description).tag(key, value).register(registry)

    private inline fun safely(meter: String, block: () -> Unit) {
        try {
            block()
        } catch (e: Exception) {
            if (warned.add(meter)) {
                log.warn("metric {} skipped, observer must not break the money flow: {}", meter, e.javaClass.name)
            } else {
                log.debug("metric {} skipped: {}", meter, e.javaClass.name)
            }
        }
    }

    companion object {
        const val ACCEPTED = "banktransfer.transfer.accepted"
        const val BANK_RESULT = "banktransfer.transfer.bank.result"
        const val TRANSFER_COMPLETED = "banktransfer.transfer.completed"
        const val GAVE_UP = "banktransfer.transfer.gave_up"
        const val DLT = "banktransfer.transfer.dlt"
        const val REPUBLISHED = "banktransfer.transfer.republished"
        const val RECONCILED = "banktransfer.transfer.reconciled"
        const val OUTBOX_PUBLISHED = "banktransfer.outbox.published"
        const val BANK_MAINTENANCE = "banktransfer.bank.maintenance"

        private val BANK_RESULT_TAGS = listOf("ok", "rejected", "not_sent", "unknown", "maintenance")

        /** 정상(1초 안) · 재시도 토픽(5 · 30 · 60초) · 재발행(5분 간격) · 포기(약 26분)를 가르는 버킷 (위험 B2). */
        private val COMPLETED_BUCKETS: Array<Duration> = arrayOf(
            Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(5),
            Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofMinutes(1), Duration.ofMinutes(5), Duration.ofMinutes(30),
        )
    }
}
