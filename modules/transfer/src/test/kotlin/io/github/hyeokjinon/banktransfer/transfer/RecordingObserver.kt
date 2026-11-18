package io.github.hyeokjinon.banktransfer.transfer

import java.time.Duration
import java.time.Instant

/** 불린 사건을 순서대로 남긴다 (P9a). 확정은 "final:<상태>:<접수 → 확정 초>". */
class RecordingObserver : TransferObserver {
    val events = mutableListOf<String>()

    override fun bankResult(outcome: BankTransferOutcome) {
        events += "bank:${outcome::class.simpleName}"
    }

    override fun finalized(status: TransferStatus, createdAt: Instant, at: Instant) {
        events += "final:$status:${Duration.between(createdAt, at).seconds}"
    }

    override fun republished() {
        events += "republished"
    }

    override fun abandoned() {
        events += "abandoned"
    }

    override fun inquired(resolution: InquiryResolution) {
        events += "inquired:$resolution"
    }
}
