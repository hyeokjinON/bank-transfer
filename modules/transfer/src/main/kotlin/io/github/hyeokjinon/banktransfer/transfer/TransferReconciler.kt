package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Clock
import java.time.Duration

/** 한 번 돈 정리 작업의 결과 (로그 · 테스트용). */
data class ReconcileResult(
    val markedUnknown: Int,
    val completed: Int,
    val failed: Int,
    val stillUnknown: Int,
    val gaveUp: Int,
)

/**
 * 결과를 모르는 이체를 은행에 물어서 확정한다 (P3-3a). 은행에 다시 보내지 않는다 — 묻기만 한다.
 * 1. [sendingTimeout] 보다 오래 SENDING 인 건 → UNKNOWN. 컨슈머가 은행 호출 중 또는 결과 기록 전에 죽은 경우다.
 *    대상은 status = SENDING 으로만 고른다: 미전송(NotSent)으로 RECEIVED 에 되돌린 건에도 sendingAt 이 남기 때문이다.
 * 2. sending_at 이 [sendingTimeout] 보다 오래된 UNKNOWN 마다 결과조회 (①과 같은 유예). 컨슈머가 3초 타임아웃으로 적은 UNKNOWN 도
 *    마찬가지다 — 은행이 조금 느릴 뿐 아직 처리 중이면 조회가 2001(처리 안 됨)을 돌려줘 FAILED 로 잘못 확정할 수 있기 때문이다.
 *    결과조회: 처리됨 → COMPLETED(0000), 처리 안 됨 → FAILED(은행 코드), 모름 → 시도 횟수 +1.
 *    [maxInquiries] 번 모르면 더 묻지 않고 [onGiveUp] 으로 알린다 → 일마감 대사(P5)에서 맞춘다.
 * 상태 변경은 모두 조건부라, 그 사이 컨슈머가 은행의 늦은 응답으로 먼저 확정했다면 아무것도 바꾸지 않는다.
 */
class TransferReconciler(
    private val store: TransferStore,
    private val bank: BankInquiryPort,
    private val clock: Clock = Clock.systemUTC(),
    private val sendingTimeout: Duration = Duration.ofSeconds(30),
    private val maxInquiries: Int = 10,
    private val batchSize: Int = 100,
    private val onGiveUp: (Transfer) -> Unit = {},
) {

    fun reconcile(): ReconcileResult {
        val now = clock.instant()
        val cutoff = now.minus(sendingTimeout)   // ① ② 가 같은 기준 — 방금 UNKNOWN 이 된 건은 이미 cutoff 보다 오래라 같은 주기에 바로 묻는다
        val markedUnknown = store.findSendingBefore(cutoff, batchSize)
            .count { store.transition(it.id, SENDING, UNKNOWN, now) }

        var completed = 0
        var failed = 0
        var stillUnknown = 0
        var gaveUp = 0
        for (transfer in store.findUnknownForInquiry(maxInquiries, cutoff, batchSize)) {
            val outcome = try {
                bank.inquire(transfer.reference)
            } catch (e: Exception) {
                InquiryOutcome.Unknown   // 포트가 분류하지 못한 예외도 "모름" — 다음 주기에 다시 묻는다
            }
            when (outcome) {
                InquiryOutcome.Processed ->
                    if (store.transition(transfer.id, UNKNOWN, COMPLETED, now, BANK_OK)) completed++
                is InquiryOutcome.NotProcessed ->
                    if (store.transition(transfer.id, UNKNOWN, FAILED, now, outcome.responseCode)) failed++
                InquiryOutcome.Unknown ->
                    if (store.recordInquiryAttempt(transfer.id, now)) {
                        val attempts = transfer.inquiryAttempts + 1
                        if (attempts >= maxInquiries) {
                            gaveUp++
                            onGiveUp(transfer.copy(inquiryAttempts = attempts))
                        } else {
                            stillUnknown++
                        }
                    }
            }
        }
        return ReconcileResult(markedUnknown, completed, failed, stillUnknown, gaveUp)
    }

    private companion object {
        const val BANK_OK = "0000"
    }
}
