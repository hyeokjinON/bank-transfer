package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Clock
import java.util.UUID

/** 처리 결과. 컨슈머는 [RETRY_LATER] 일 때만 메시지를 다시 처리하게 한다. */
enum class ProcessResult { COMPLETED, FAILED, UNKNOWN, ALREADY_HANDLED, NOT_FOUND, RETRY_LATER }

/**
 * TransferRequested 처리 (P3-2).
 * 1. RECEIVED -> SENDING 을 조건부로 바꾼다. false 면 다른 소비자가 이미 집어 갔거나 끝난 건 → 은행에 보내지 않는다.
 * 2. 은행 호출은 DB 트랜잭션 밖에서 한다 (응답을 기다리는 동안 커넥션을 붙잡지 않는다).
 * 3. 결과를 기록한다. 결과를 모르면 UNKNOWN — 다시 보내지 않는다 (P3-3 결과조회로 확정).
 *    보내지도 못했으면 RECEIVED 로 되돌려 재시도하게 한다.
 * 은행 응답 뒤 DB 기록이 실패하면 SENDING 으로 남긴다 — P3-3 정리 작업이 30초 넘게 SENDING 인 건을 UNKNOWN 으로 바꾼다.
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
            bank.transfer(transfer.bankCode, transfer.accountNo, transfer.amount, transfer.memo)
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

    /** SENDING 은 이 처리만 만든다. 여기서 false 면 누군가 규칙을 어기고 상태를 바꾼 것이다. */
    private fun record(id: UUID, to: TransferStatus, bankResponseCode: String?, result: ProcessResult): ProcessResult {
        check(store.transition(id, SENDING, to, clock.instant(), bankResponseCode)) { "transfer $id left SENDING unexpectedly" }
        return result
    }

    private companion object {
        const val BANK_OK = "0000"
    }
}
