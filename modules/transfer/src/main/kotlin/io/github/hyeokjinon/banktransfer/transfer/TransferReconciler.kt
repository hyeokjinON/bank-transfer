package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Clock
import java.time.Duration
import java.time.Instant

/** 한 번 돈 정리 작업의 결과 (로그 · 테스트용). */
data class ReconcileResult(
    val markedUnknown: Int,
    val completed: Int,
    val failed: Int,
    val stillUnknown: Int,
    val gaveUp: Int,
    /** ⓪ 아웃박스로 다시 발행한 건 (P3-3b). */
    val republished: Int,
    /** ⓪ 재발행 한도를 다 쓰고 FAILED(E001) 로 포기한 건 (P3-3b). */
    val abandoned: Int,
)

/**
 * 정리 작업. 은행에 다시 "보내는" 일은 하지 않는다 — 재발행은 아웃박스에 신호를 넣을 뿐이고, 은행 호출은 늘 컨슈머의 조건부 전이 뒤에 한 번이다.
 * ⓪ (P3-3b) 은행에 보내지도 못한 채 [republishAfter] 넘게 RECEIVED 로 남은 건 → 아웃박스로 다시 발행, [maxRepublish] 번을 다 쓰면 FAILED(E001).
 *    대상은 "마지막 재발행 이후 은행에 보내려 한 흔적(sendingAt > republishedAt)" 이 있는 것만이다: Kafka · 컨슈머 장애로 시도조차 못 한 건은
 *    복구되면 밀린 메시지가 처리되므로 횟수를 쓰지 않는다. 재시도 토픽 체인(약 95초)이 끝난 뒤에 보도록 대기는 5분.
 * ① [sendingTimeout] 보다 오래 SENDING 인 건 → UNKNOWN. 컨슈머가 은행 호출 중 또는 결과 기록 전에 죽은 경우다.
 *    대상은 status = SENDING 으로만 고른다: 미전송(NotSent)으로 RECEIVED 에 되돌린 건에도 sendingAt 이 남기 때문이다.
 * ② sending_at 이 [sendingTimeout] 보다 오래된 UNKNOWN 마다 결과조회 (①과 같은 유예). 컨슈머가 3초 타임아웃으로 적은 UNKNOWN 도
 *    마찬가지다 — 은행이 조금 느릴 뿐 아직 처리 중이면 조회가 2001(처리 안 됨)을 돌려줘 FAILED 로 잘못 확정할 수 있기 때문이다.
 *    결과조회: 처리됨 → COMPLETED(0000), 처리 안 됨 → FAILED(은행 코드), 모름 → 시도 횟수 +1,
 *    연결 없음(Unavailable) → 이번 주기의 남은 조회를 멈추고 횟수를 세지 않는다 (긴 은행 점검에도 한도에 닿지 않게).
 *    [maxInquiries] 번 모르면 더 묻지 않고 [onGiveUp] 으로 알린다 → 일마감 대사(P5)에서 맞춘다.
 * 실패 확정(② 처리 안 됨, ⓪ 포기)은 [TransferStore.fail] 로 한다 — 상태 전이와 환불이 한 트랜잭션이다 (P4-1).
 * 상태 변경은 모두 조건부라, 그 사이 컨슈머가 먼저 바꿨다면 아무것도 바꾸지 않는다.
 */
class TransferReconciler(
    private val store: TransferStore,
    private val bank: BankInquiryPort,
    private val clock: Clock = Clock.systemUTC(),
    private val sendingTimeout: Duration = Duration.ofSeconds(30),
    private val maxInquiries: Int = 10,
    private val batchSize: Int = 100,
    private val republishAfter: Duration = Duration.ofMinutes(5),
    private val maxRepublish: Int = 3,
    private val onGiveUp: (Transfer) -> Unit = {},
    private val onAbandon: (Transfer) -> Unit = {},
) {

    fun reconcile(): ReconcileResult {
        val now = clock.instant()
        val (republished, abandoned) = republishOrAbandon(now)

        val cutoff = now.minus(sendingTimeout)   // ① ② 가 같은 기준 — 방금 UNKNOWN 이 된 건은 이미 cutoff 보다 오래라 같은 주기에 바로 묻는다
        val markedUnknown = store.findSendingBefore(cutoff, batchSize)
            .count { store.transition(it.id, SENDING, UNKNOWN, now) }

        var completed = 0
        var failed = 0
        var stillUnknown = 0
        var gaveUp = 0
        inquiries@ for (transfer in store.findUnknownForInquiry(maxInquiries, cutoff, batchSize)) {
            val outcome = try {
                bank.inquire(transfer.reference)
            } catch (e: Exception) {
                InquiryOutcome.Unknown   // 포트가 분류하지 못한 예외도 "모름" — 다음 주기에 다시 묻는다
            }
            when (outcome) {
                InquiryOutcome.Processed ->
                    if (store.transition(transfer.id, UNKNOWN, COMPLETED, now, BANK_OK)) completed++
                is InquiryOutcome.NotProcessed ->
                    if (store.fail(transfer.id, UNKNOWN, now, outcome.responseCode)) failed++
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
                // 연결이 없으면 남은 건도 똑같다: 건마다 기다리지 않고 다음 주기에 다시 묻는다. 시도로 세지 않는다
                InquiryOutcome.Unavailable -> break@inquiries
            }
        }
        return ReconcileResult(markedUnknown, completed, failed, stillUnknown, gaveUp, republished, abandoned)
    }

    /** ⓪ 재발행 · 포기. 둘 다 조건부라 인스턴스가 여럿이어도 한 번만 반영된다. 돌려주는 값 = (재발행 수, 포기 수). */
    private fun republishOrAbandon(now: Instant): Pair<Int, Int> {
        var republished = 0
        var abandoned = 0
        for (transfer in store.findRepublishable(now.minus(republishAfter), batchSize)) {
            if (transfer.republishCount < maxRepublish) {
                if (store.republish(transfer.id, transfer.republishCount, now)) republished++
            } else if (store.fail(transfer.id, RECEIVED, now, NOT_SENT)) {
                abandoned++
                onAbandon(transfer.copy(status = FAILED, bankResponseCode = NOT_SENT, updatedAt = now))
            }
        }
        return republished to abandoned
    }

    private companion object {
        const val BANK_OK = "0000"

        /** 은행에 보내지 못한 채 포기 (이 서비스의 자체 코드 — 은행 응답코드가 아니다). 돈이 움직이지 않았다는 뜻이다. */
        const val NOT_SENT = "E001"
    }
}
