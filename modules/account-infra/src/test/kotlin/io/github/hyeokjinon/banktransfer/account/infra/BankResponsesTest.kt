package io.github.hyeokjinon.banktransfer.account.infra

import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class BankResponsesTest {

    private fun header(biz: BizType, code: String) = Header(MessageClass.RESPONSE, biz, 1, LocalDateTime.of(2026, 11, 5, 9, 0, 0), code)

    @Test
    fun `성명조회 0000 이면 예금주명`() {
        assertEquals("홍길동", holderNameOf(NameInquiryResponse(header(BizType.NAME_INQUIRY, ResponseCodes.OK), "090", "1002123456789", "홍길동")))
    }

    @Test
    fun `성명조회 계좌 없음이면 null`() {
        assertNull(holderNameOf(NameInquiryResponse(header(BizType.NAME_INQUIRY, ResponseCodes.ACCOUNT_NOT_FOUND), "090", "5599000000001", "")))
    }

    @Test
    fun `성명조회 그 밖의 코드는 BankRejectedException`() {
        val e = assertFailsWith<BankRejectedException> {
            holderNameOf(NameInquiryResponse(header(BizType.NAME_INQUIRY, "9999"), "090", "1002123456789", ""))
        }
        assertEquals("9999", e.responseCode)
    }

    @Test
    fun `1원 이체가 0000 이 아니면 BankRejectedException, 0000 이면 지나간다`() {
        requireOk(TransferResponse(header(BizType.TRANSFER, ResponseCodes.OK), "090", "1002123456789", 1, "BT1234", "V2026110500000000001"))
        assertFailsWith<BankRejectedException> {
            requireOk(TransferResponse(header(BizType.TRANSFER, "1001"), "090", "1002123456789", 1, "BT1234", "V2026110500000000001"))
        }
    }

    @Test
    fun `계좌는 끝 4자리만 남긴다`() {
        assertEquals("****6789", maskAccount("1002123456789"))
    }
}
