package io.github.hyeokjinon.banktransfer.api.reconcile

import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertText
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class InquiryGiveUpAlertTest {

    @Test
    fun `결과조회 한도 소진 알림 - WARN, 거래번호 · 금액 · 가린 계좌, 사용자 id 는 없다`() {
        val transfer = Transfer(
            id = UUID.randomUUID(), userId = "user-secret-id", idempotencyKey = "k", reference = "T261028GIVEUP0000001",
            requestHash = "h", bankCode = "090", accountNo = "1002123456789", amount = 30_000, memo = "m",
            status = TransferStatus.UNKNOWN, inquiryAttempts = 10, createdAt = Instant.EPOCH, updatedAt = Instant.EPOCH,
        )

        val alert = inquiryGiveUpAlert(transfer)
        val text = AlertText.render(alert)

        assertEquals(AlertLevel.WARN, alert.level)
        assertTrue("10회" in alert.title)
        assertTrue("T261028GIVEUP0000001" in text)
        assertTrue("090-1002****6789" in text)
        assertFalse("1002123456789" in text)
        assertFalse("user-secret-id" in text)
    }
}
