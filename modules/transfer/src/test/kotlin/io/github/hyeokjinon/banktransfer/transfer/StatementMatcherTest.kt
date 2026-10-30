package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.AMOUNT_OR_ACCOUNT_MISMATCH
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.AUTO_COMPLETE
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.AUTO_FAIL
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.COMPLETED_NOT_AT_BANK
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.MATCHED
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.PAID_BUT_RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.PAID_TWICE
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.REFUNDED_BUT_PAID
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.RETRYING
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.UNKNOWN_REFERENCE
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Instant
import java.time.LocalDate
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** 설계 §6 의 표를 줄마다 그대로 (P5-1). 번호는 표의 # 이다. */
class StatementMatcherTest {

    private val day = LocalDate.of(2026, 10, 28)

    private fun transfer(status: TransferStatus) = Transfer(
        id = UUID.randomUUID(), userId = "u", idempotencyKey = "k", reference = REF, requestHash = "h",
        bankCode = "090", accountNo = "1002123456789", amount = 30_000, memo = "m",
        status = status, createdAt = Instant.EPOCH, updatedAt = Instant.EPOCH,
    )

    private fun line(amount: Long = 30_000, accountNo: String = "1002123456789", bankCode: String = "090", lineNo: Int = 2, reference: String = REF) =
        BankStatementLine(day, lineNo, reference, bankCode, accountNo, amount, Instant.EPOCH)

    private fun judge(transfer: Transfer?, vararg lines: BankStatementLine) = StatementMatcher.judge(transfer, lines.toList())

    @Test
    fun `1 COMPLETED + 같은 줄 = 일치`() = assertEquals(MATCHED, judge(transfer(COMPLETED), line()))

    @Test
    fun `2 FAILED + 없음 = 일치`() = assertEquals(MATCHED, judge(transfer(FAILED)))

    @Test
    fun `3 UNKNOWN · SENDING + 같은 줄 = 자동 완료`() {
        assertEquals(AUTO_COMPLETE, judge(transfer(UNKNOWN), line()))
        assertEquals(AUTO_COMPLETE, judge(transfer(SENDING), line()))
    }

    @Test
    fun `4 UNKNOWN · SENDING + 없음 = 자동 실패`() {
        assertEquals(AUTO_FAIL, judge(transfer(UNKNOWN)))
        assertEquals(AUTO_FAIL, judge(transfer(SENDING)))
    }

    @Test
    fun `5 어느 상태든 금액이 다르면 불일치 - UNKNOWN 이어도 확정하지 않는다`() {
        for (status in TransferStatus.entries) {
            assertEquals(AMOUNT_OR_ACCOUNT_MISMATCH, judge(transfer(status), line(amount = 30_001)), "$status")
        }
    }

    @Test
    fun `5 계좌번호가 다르면 불일치`() {
        for (status in TransferStatus.entries) {
            assertEquals(AMOUNT_OR_ACCOUNT_MISMATCH, judge(transfer(status), line(accountNo = "1002123456780")), "$status")
        }
    }

    @Test
    fun `5 은행코드가 다르면 불일치`() = assertEquals(AMOUNT_OR_ACCOUNT_MISMATCH, judge(transfer(UNKNOWN), line(bankCode = "091")))

    @Test
    fun `6 COMPLETED + 없음 = COMPLETED_NOT_AT_BANK`() = assertEquals(COMPLETED_NOT_AT_BANK, judge(transfer(COMPLETED)))

    @Test
    fun `7 FAILED + 있음 = REFUNDED_BUT_PAID (환불한 돈이 나감)`() = assertEquals(REFUNDED_BUT_PAID, judge(transfer(FAILED), line()))

    @Test
    fun `8 RECEIVED + 있음 = PAID_BUT_RECEIVED (긴급 - 재발행되면 이중 지급)`() {
        assertEquals(PAID_BUT_RECEIVED, judge(transfer(RECEIVED), line()))
        assertTrue(PAID_BUT_RECEIVED.issue!!.urgent)
    }

    @Test
    fun `RECEIVED + 없음 = 재시도 중 - 넘어간다 (이슈 없음)`() {
        assertEquals(RETRYING, judge(transfer(RECEIVED)))
        assertNull(RETRYING.issue)
    }

    @Test
    fun `9 우리에게 없는 거래번호 = UNKNOWN_REFERENCE - 줄이 여럿이어도`() {
        assertEquals(UNKNOWN_REFERENCE, judge(null, line()))
        assertEquals(UNKNOWN_REFERENCE, judge(null, line(), line(lineNo = 3)))
    }

    @Test
    fun `11 같은 거래번호 2줄 이상 = PAID_TWICE (긴급) - 어느 상태든, 금액이 달라도`() {
        for (status in TransferStatus.entries) {
            assertEquals(PAID_TWICE, judge(transfer(status), line(), line(lineNo = 3, amount = 1)), "$status")
        }
        assertTrue(PAID_TWICE.issue!!.urgent)
    }

    @Test
    fun `일치 · 재시도 중 · 다른 날 재송신만 이슈가 없고, 자동 확정은 감사 기록(불일치 아님), 나머지는 불일치`() {
        val withoutIssue = ReconVerdict.entries.filter { it.issue == null }.toSet()
        assertEquals(setOf(MATCHED, RETRYING, ReconVerdict.RESENT_LATER), withoutIssue)
        assertFalse(AUTO_COMPLETE.issue!!.mismatch)
        assertFalse(AUTO_FAIL.issue!!.mismatch)
        assertTrue(ReconVerdict.entries.filter { it.issue != null && it != AUTO_COMPLETE && it != AUTO_FAIL }.all { it.issue!!.mismatch })
        assertTrue(ReconIssueType.BALANCE_LEDGER_MISMATCH.mismatch)
    }

    @Test
    fun `판정할 것이 없거나 다른 거래번호의 줄이면 거부 (호출 쪽 버그)`() {
        assertFailsWith<IllegalArgumentException> { judge(null) }
        assertFailsWith<IllegalArgumentException> { judge(transfer(UNKNOWN), line(reference = "T261028OTHERREF00001")) }
    }

    private companion object {
        const val REF = "T261028MATCHER000001"
    }
}
