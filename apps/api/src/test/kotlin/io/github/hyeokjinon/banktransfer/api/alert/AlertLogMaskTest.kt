package io.github.hyeokjinon.banktransfer.api.alert

import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconIssueRow
import io.github.hyeokjinon.banktransfer.api.dailyrecon.alertLine
import io.github.hyeokjinon.banktransfer.api.dailyrecon.reconIssueOf
import io.github.hyeokjinon.banktransfer.api.reconcile.inquiryGiveUpAlert
import io.github.hyeokjinon.banktransfer.transfer.BankStatementLine
import io.github.hyeokjinon.banktransfer.transfer.ReconIssueType
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict
import io.github.hyeokjinon.banktransfer.transfer.Settlement
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import java.time.Instant
import java.time.LocalDate
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 앱 알림의 로그 사본은 금액 · 잔고를 가리고 찾기 키는 남긴다 (P9c, 스펙 §14). 텔레그램 본문은 그대로. */
@ExtendWith(OutputCaptureExtension::class)
class AlertLogMaskTest {
    private val transferId = UUID.fromString("0f8c2e9a-1b2c-4d5e-8f90-a1b2c3d4e5f6")
    private val reference = "T261028GIVEUP0000001"
    private val reconDate = LocalDate.parse("2026-10-28")

    private fun transfer(amount: Long) = Transfer(
        id = transferId, userId = "user-secret-id", idempotencyKey = "k", reference = reference,
        requestHash = "h", bankCode = "090", accountNo = "1002123456789", amount = amount, memo = "m",
        status = TransferStatus.UNKNOWN, inquiryAttempts = 10, createdAt = Instant.EPOCH, updatedAt = Instant.EPOCH,
    )

    private fun bankLine(lineNo: Int, amount: Long) =
        BankStatementLine(reconDate, lineNo, reference, "090", "1002123456789", amount, Instant.EPOCH)

    /** DailyReconSteps 가 만드는 이슈 → 저장된 행 → 알림 항목 (실제 모양 그대로). */
    private fun reconMismatchAlert(): Alert {
        val issue = reconIssueOf(
            reconDate, ReconIssueType.AMOUNT_OR_ACCOUNT_MISMATCH,
            Settlement(ReconVerdict.AMOUNT_OR_ACCOUNT_MISMATCH, transfer(31_234), listOf(bankLine(3, 32_345), bankLine(7, 33_456))),
        )
        val row = ReconIssueRow(41, issue.type, issue.reference, issue.ourValue, issue.bankValue)
        return Alert(AlertLevel.CRITICAL, "일마감 대사 $reconDate 완료 — 불일치 1건", items = listOf(row.alertLine()))
    }

    private fun ledgerMismatchAlert(): Alert {
        val row = ReconIssueRow(57, ReconIssueType.BALANCE_LEDGER_MISMATCH, null, "balance=-45678", "ledgerSum=56789")
        return Alert(AlertLevel.CRITICAL, "일마감 대사 $reconDate 완료 — 불일치 1건", items = listOf(row.alertLine()))
    }

    @Test
    fun `결과조회 한도 소진 알림 - 로그 사본에 금액이 없고 출금 id · 거래번호 · 가린 계좌는 남는다`(output: CapturedOutput) {
        LogNotifier().notify(inquiryGiveUpAlert(transfer(30_123)))

        val logged = output.out.lines().single { reference in it }
        assertFalse("30123" in logged, logged)
        assertTrue("amount=***" in logged, logged)
        assertTrue("transferId=$transferId" in logged, logged)
        assertTrue("090-1002****6789" in logged, logged)
    }

    @Test
    fun `대사 불일치 - 우리 · 은행 값의 금액이 없고 거래번호 · 날짜 · 줄 번호 · 가린 계좌는 남는다`(output: CapturedOutput) {
        LogNotifier().notify(reconMismatchAlert())

        val logged = output.out.lines().single { reference in it }
        listOf("31234", "32345", "33456").forEach { assertFalse(it in logged, "$it 가 남았다: $logged") }
        assertTrue("AMOUNT_OR_ACCOUNT_MISMATCH $reference 우리=UNKNOWN *** 090-1002****6789" in logged, logged)
        assertTrue("은행=*** 090-1002****6789 2026-10-28#3; *** 090-1002****6789 2026-10-28#7" in logged, logged)
        assertTrue("일마감 대사 2026-10-28" in logged, logged)
    }

    @Test
    fun `잔고 원장 불일치 - 잔고 · 원장 합(음수 포함)이 없고 issue 번호 · 날짜는 남는다`(output: CapturedOutput) {
        LogNotifier().notify(ledgerMismatchAlert())

        val logged = output.out.lines().single { "BALANCE_LEDGER_MISMATCH" in it }
        assertFalse("45678" in logged, logged)
        assertFalse("56789" in logged, logged)
        assertTrue("지갑(issue #57) 우리=balance=*** 은행=ledgerSum=***" in logged, logged)
        assertTrue("일마감 대사 2026-10-28" in logged, logged)
    }

    @Test
    fun `텔레그램 본문(AlertText render)은 금액을 그대로 싣는다 - 가리는 것은 로그 사본뿐`() {
        val text = AlertText.render(reconMismatchAlert())

        assertTrue("우리=UNKNOWN 31234 090-1002****6789" in text, text)
        assertTrue("은행=32345 090-1002****6789 2026-10-28#3; 33456 090-1002****6789 2026-10-28#7" in text, text)
        assertTrue("amount=30123" in AlertText.render(inquiryGiveUpAlert(transfer(30_123))))
    }

    @Test
    fun `요약의 건수 · 자른 줄 끝의 금액 - 건수는 그대로, 4096자에서 잘린 금액도 가린다`() {
        assertEquals("출금 5건 (D일 송신): 일치 3 · 재시도 중 2", AlertLogMask.mask("출금 5건 (D일 송신): 일치 3 · 재시도 중 2"))
        assertEquals("- PAID_TWICE T1 우리=COMPLETED ***…", AlertLogMask.mask("- PAID_TWICE T1 우리=COMPLETED 300…"))
    }
}
