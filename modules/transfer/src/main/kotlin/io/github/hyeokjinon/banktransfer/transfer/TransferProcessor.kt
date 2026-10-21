package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Clock
import java.util.UUID

/** 처리 결과. 컨슈머는 [RETRY_LATER] 일 때만 메시지를 다시 처리하게 한다. */
enum class ProcessResult {
    COMPLETED,
    FAILED,
    UNKNOWN,
    ALREADY_HANDLED,

    /** 은행이 준 확정 결과가 이미 확정된 상태와 다르다 (예: 결과조회로 FAILED 확정 뒤 은행의 늦은 응답이 완료) → 사람이 확인해야 한다. */
    CONFLICT,
    NOT_FOUND,
    RETRY_LATER,
}

/**
 * TransferRequested 처리 (P3-2).
 * 1. RECEIVED -> SENDING 을 조건부로 바꾼다. false 면 다른 소비자가 이미 집어 갔거나 끝난 건 → 은행에 보내지 않는다.
 * 2. 은행 호출은 DB 트랜잭션 밖에서 한다 (응답을 기다리는 동안 커넥션을 붙잡지 않는다).
 * 3. 결과를 기록한다. 결과를 모르면 UNKNOWN — 다시 보내지 않는다 (P3-3 결과조회로 확정).
 *    보내지도 못했으면 RECEIVED 로 되돌려 재시도하게 한다.
 * 은행 응답 뒤 DB 기록이 실패하면 SENDING 으로 남는다 — 정리 작업(TransferReconciler)이 30초 넘게 SENDING 인 건을 UNKNOWN 으로 바꾸고 결과조회로 확정한다.
 */
class TransferProcessor(
    private val store: TransferStore,
    private val bank: BankTransferPort,
    private val clock: Clock = Clock.systemUTC(),
) {

    fun process(transferId: UUID): ProcessResult {
        val transfer = store.findById(transferId) ?: return ProcessResult.NOT_FOUND
        if (!store.transition(transferId, RECEIVED, SENDING, clock.instant())) return ProcessResult.ALREADY_HANDLED

        val outcome = try {
            bank.transfer(transfer.bankCode, transfer.accountNo, transfer.amount, transfer.memo, transfer.reference)
        } catch (e: Exception) {
            // 포트가 결과를 분류하지 못하고 예외를 던졌다 = 은행이 받았는지 모른다 → 다시 보내지 않고 UNKNOWN (P3-3 결과조회로 확정)
            BankTransferOutcome.ResultUnknown
        }
        return when (outcome) {
            BankTransferOutcome.Completed -> record(transferId, COMPLETED, BANK_OK, ProcessResult.COMPLETED)
            is BankTransferOutcome.Rejected -> record(transferId, FAILED, outcome.responseCode, ProcessResult.FAILED)
            BankTransferOutcome.ResultUnknown -> record(transferId, UNKNOWN, null, ProcessResult.UNKNOWN)
            BankTransferOutcome.NotSent -> record(transferId, RECEIVED, null, ProcessResult.RETRY_LATER)
        }
    }

    /**
     * SENDING 에서 결과로 옮긴다. false 면 정리 작업이 먼저 SENDING → UNKNOWN 으로 바꾼 것이다 (은행 응답이 30초 넘게 늦은 경우).
     * 그때 은행이 직접 준 확정 결과(완료 · 거절)는 결과조회보다 정확하므로 UNKNOWN 에서 바로 확정한다.
     * 그 밖(모름 · 미전송)은 정리 작업이 결과조회로 맡으므로 손대지 않는다.
     * 확정 결과를 UNKNOWN 에서도 옮기지 못했는데 현재 상태가 이미 다른 확정(완료 ↔ 거절)이면 [ProcessResult.CONFLICT] 다:
     * 결과조회 결과와 은행의 확정 결과가 엇갈린 것이라 조용히 넘기면 DB 와 은행이 어긋난 채 남는다.
     * 같은 확정 결과였거나 아직 확정 전이면 [ProcessResult.ALREADY_HANDLED] 다.
     */
    private fun record(id: UUID, to: TransferStatus, bankResponseCode: String?, result: ProcessResult): ProcessResult {
        val now = clock.instant()
        if (store.transition(id, SENDING, to, now, bankResponseCode)) return result
        if ((to == COMPLETED || to == FAILED) && store.transition(id, UNKNOWN, to, now, bankResponseCode)) return result
        if (to == COMPLETED || to == FAILED) {
            val current = store.findById(id)?.status
            if ((current == COMPLETED || current == FAILED) && current != to) return ProcessResult.CONFLICT
        }
        return ProcessResult.ALREADY_HANDLED
    }

    private companion object {
        const val BANK_OK = "0000"
    }
}
