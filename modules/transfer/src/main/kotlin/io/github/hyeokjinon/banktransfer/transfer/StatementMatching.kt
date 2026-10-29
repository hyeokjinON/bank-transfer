package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

/** 은행 대사 파일의 상세 한 줄 (P5-1). 같은 거래번호가 여러 날짜 · 여러 줄에 있을 수 있다 (이중 지급). */
data class BankStatementLine(
    val bankDate: LocalDate,
    val lineNo: Int,
    val reference: String,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    val processedAt: Instant,
) {
    override fun toString(): String =
        "BankStatementLine(bankDate=$bankDate, lineNo=$lineNo, reference=$reference, bankCode=$bankCode, accountNo=${maskAccountNo(accountNo)}, amount=$amount)"
}

/**
 * 일마감 대사가 남기는 기록의 종류 (recon_issue.type, P5-1).
 * [mismatch] = 사람이 봐야 하는 어긋남 (자동 확정 AUTO_* 는 감사 기록일 뿐 어긋남이 아니다). [urgent] = 돈이 두 번 나갔거나 나갈 수 있다.
 */
enum class ReconIssueType(val mismatch: Boolean, val urgent: Boolean = false) {
    AUTO_COMPLETED(mismatch = false),
    AUTO_FAILED(mismatch = false),
    AMOUNT_OR_ACCOUNT_MISMATCH(mismatch = true),
    COMPLETED_NOT_AT_BANK(mismatch = true),
    REFUNDED_BUT_PAID(mismatch = true),
    PAID_BUT_RECEIVED(mismatch = true, urgent = true),
    UNKNOWN_REFERENCE(mismatch = true),
    BALANCE_LEDGER_MISMATCH(mismatch = true),
    PAID_TWICE(mismatch = true, urgent = true),
}

/** 거래번호로 짝지은 판정 (설계 §6 표, 주석의 번호가 표의 #). [issue] 가 있으면 recon_issue 에 남긴다. */
enum class ReconVerdict(val issue: ReconIssueType?) {
    /** 1 COMPLETED + 같은 줄, 2 FAILED + 없음. */
    MATCHED(null),

    /** 3 UNKNOWN · SENDING + 같은 줄 → COMPLETED. */
    AUTO_COMPLETE(ReconIssueType.AUTO_COMPLETED),

    /** 4 UNKNOWN · SENDING + 없음 → FAILED E002 + 환불. */
    AUTO_FAIL(ReconIssueType.AUTO_FAILED),

    /** 5 어느 상태든 금액 · 은행코드 · 계좌가 다름 — 3 이어도 확정하지 않는다. */
    AMOUNT_OR_ACCOUNT_MISMATCH(ReconIssueType.AMOUNT_OR_ACCOUNT_MISMATCH),

    /** 6 COMPLETED + 없음. */
    COMPLETED_NOT_AT_BANK(ReconIssueType.COMPLETED_NOT_AT_BANK),

    /** 7 FAILED + 있음 — 환불한 돈이 나갔다. */
    REFUNDED_BUT_PAID(ReconIssueType.REFUNDED_BUT_PAID),

    /** 8 RECEIVED + 있음 — 재발행되면 이중 지급 (긴급). */
    PAID_BUT_RECEIVED(ReconIssueType.PAID_BUT_RECEIVED),

    /** — RECEIVED + 없음: 재시도 중 (재발행 · E001 이 맡는다). */
    RETRYING(null),

    /** 9 우리에게 없는 거래번호의 T 줄. */
    UNKNOWN_REFERENCE(ReconIssueType.UNKNOWN_REFERENCE),

    /** 11 같은 거래번호 2줄 이상 (긴급). */
    PAID_TWICE(ReconIssueType.PAID_TWICE),
}

/**
 * 일마감 대사의 판정 (P5-1, 설계 §6). 순수 함수 — 배치는 읽기 · 쓰기 · 연결만 한다.
 * [lines] = 이 거래번호의 **적재된 모든 날짜의** 은행 줄. 순서: 줄 2개 이상(이중 지급)이 금액 비교보다 먼저다 — 둘 중 하나만 맞아도 돈은 두 번 나갔다.
 */
object StatementMatcher {

    fun judge(transfer: Transfer?, lines: List<BankStatementLine>): ReconVerdict {
        if (transfer == null) {
            require(lines.isNotEmpty()) { "nothing to judge: no transfer and no bank line" }
            return ReconVerdict.UNKNOWN_REFERENCE
        }
        require(lines.all { it.reference == transfer.reference }) { "bank lines of another reference than ${transfer.reference}" }
        if (lines.size >= 2) return ReconVerdict.PAID_TWICE
        val line = lines.singleOrNull()
            ?: return when (transfer.status) {
                COMPLETED -> ReconVerdict.COMPLETED_NOT_AT_BANK
                FAILED -> ReconVerdict.MATCHED
                UNKNOWN, SENDING -> ReconVerdict.AUTO_FAIL
                RECEIVED -> ReconVerdict.RETRYING
            }
        if (line.amount != transfer.amount || line.bankCode != transfer.bankCode || line.accountNo != transfer.accountNo) {
            return ReconVerdict.AMOUNT_OR_ACCOUNT_MISMATCH
        }
        return when (transfer.status) {
            COMPLETED -> ReconVerdict.MATCHED
            UNKNOWN, SENDING -> ReconVerdict.AUTO_COMPLETE
            FAILED -> ReconVerdict.REFUNDED_BUT_PAID
            RECEIVED -> ReconVerdict.PAID_BUT_RECEIVED
        }
    }
}

/** 판정 하나와 그때의 우리 출금 · 은행 줄 (기록 · 알림용). [transfer] 는 판정한 시점의 상태다 — 자동 확정 전. */
data class Settlement(val verdict: ReconVerdict, val transfer: Transfer?, val lines: List<BankStatementLine>)

/**
 * 판정하고, **결과를 모르는 출금(UNKNOWN · SENDING)만** 확정한다 (P5-1, 설계 §2 "자동 보정 범위").
 * 이미 확정된 건을 뒤집으면 보정이 새 사고가 된다(환불한 돈을 다시 빼려는데 잔고 부족 등) → 판정만 돌려주고 기록 · 알림은 부르는 쪽이 한다.
 * 확정은 기존 조건부 전이(→ COMPLETED, 0000)와 [TransferStore.fail](→ FAILED E002 + 환불, 한 트랜잭션)을 그대로 쓴다.
 * 0건이면(그 사이 정리 작업 · 컨슈머가 바꿈) 다시 읽어 바뀐 상태로 다시 판정한다. [maxAttempts] 번 넘게 계속 바뀌면 예외 —
 * 배치 단계가 실패하고 재시작으로 이어간다 (저장소가 옛 스냅숏을 보는 설정이면 여기서 드러난다).
 */
class StatementSettler(
    private val store: TransferStore,
    private val clock: Clock = Clock.systemUTC(),
    private val maxAttempts: Int = 3,
) {

    fun settle(transferId: UUID, linesOf: (String) -> List<BankStatementLine>): Settlement {
        var transfer = read(transferId)
        val lines = linesOf(transfer.reference)
        repeat(maxAttempts) {
            val verdict = StatementMatcher.judge(transfer, lines)
            val settled = when (verdict) {
                ReconVerdict.AUTO_COMPLETE -> store.transition(transfer.id, transfer.status, COMPLETED, clock.instant(), BANK_OK)
                ReconVerdict.AUTO_FAIL -> store.fail(transfer.id, transfer.status, clock.instant(), NOT_AT_BANK)
                else -> true
            }
            if (settled) return Settlement(verdict, transfer, lines)
            transfer = read(transferId)
        }
        error("transfer $transferId kept changing during daily reconciliation ($maxAttempts attempts) — the step fails and resumes on restart")
    }

    /** 우리에게 없는 거래번호의 은행 줄 (④). 저장소를 보지 않는다. */
    fun unknownReference(lines: List<BankStatementLine>): Settlement = Settlement(StatementMatcher.judge(null, lines), null, lines)

    private fun read(id: UUID): Transfer = store.findById(id) ?: error("transfer $id vanished during daily reconciliation")

    companion object {
        /** 일마감 대사로 "은행이 처리하지 않았다" 를 확정 (이 서비스의 자체 코드 — E001 재발행 포기와 구분). 돈은 움직이지 않았다. */
        const val NOT_AT_BANK = "E002"
        private const val BANK_OK = "0000"
    }
}
