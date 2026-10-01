package io.github.hyeokjinon.banktransfer.protocol

import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class BankMessageSerializerTest {
    private val now = LocalDateTime.of(2026, 10, 1, 9, 30, 15)
    private fun header(biz: BizType, cls: MessageClass = MessageClass.REQUEST, tranNo: Long = 42) =
        Header(cls, biz, tranNo, now)

    @Test
    fun `성명조회 응답은 직렬화 후 다시 읽어도 같다`() {
        val original = NameInquiryResponse(header(BizType.NAME_INQUIRY, MessageClass.RESPONSE), "090", "1002123456789", "홍길동")

        val bytes = BankMessageSerializer.serialize(original)

        assertEquals(Header.LENGTH + 3 + 16 + 20, bytes.size)
        assertEquals(original, BankMessageSerializer.deserialize(bytes))
    }

    @Test
    fun `헤더는 33바이트이고 숫자는 0, 문자는 공백으로 채운다`() {
        val bytes = BankMessageSerializer.serialize(PollRequest(header(BizType.POLL, MessageClass.ADMIN_REQUEST, tranNo = 7)))

        assertEquals("0800PLL0000000720261001093015" + "0000", String(bytes, Charsets.US_ASCII))
    }

    @Test
    fun `한글은 EUC-KR 2바이트로 계산한다`() {
        val transfer = TransferRequest(header(BizType.TRANSFER), "090", "1002123456789", 1, "BT1234홍길동")
        val bytes = BankMessageSerializer.serialize(transfer)

        // 적요 20바이트 = "BT1234"(6) + "홍길동"(6) + 공백 8
        val memoBytes = bytes.copyOfRange(bytes.size - 20, bytes.size)
        assertEquals(6 + 6, String(memoBytes, BankCharsets.EUC_KR).trimEnd().toByteArray(BankCharsets.EUC_KR).size)
        assertEquals(transfer, BankMessageSerializer.deserialize(bytes))
    }

    @Test
    fun `길이를 넘는 한글은 글자 단위로 잘라 깨진 글자를 만들지 않는다`() {
        val longName = "가나다라마바사아자차카" // 11글자 = 22바이트, 필드는 20바이트
        val res = NameInquiryResponse(header(BizType.NAME_INQUIRY, MessageClass.RESPONSE), "090", "1", longName)

        val decoded = BankMessageSerializer.deserialize(BankMessageSerializer.serialize(res)) as NameInquiryResponse

        assertEquals("가나다라마바사아자차", decoded.holderName)
    }

    @Test
    fun `금액이 13자리를 넘으면 예외`() {
        val tooBig = TransferRequest(header(BizType.TRANSFER), "090", "1", 10_000_000_000_000, "")
        assertFailsWith<ProtocolException> { BankMessageSerializer.serialize(tooBig) }
    }

    @Test
    fun `길이가 모자란 전문은 예외`() {
        val bytes = BankMessageSerializer.serialize(NameInquiryRequest(header(BizType.NAME_INQUIRY), "090", "1"))
        assertFailsWith<ProtocolException> { BankMessageSerializer.deserialize(bytes.copyOf(bytes.size - 1)) }
    }

    @Test
    fun `요청과 응답은 전문구분코드로 구분한다`() {
        val req = BankMessageSerializer.serialize(PollRequest(header(BizType.POLL, MessageClass.ADMIN_REQUEST)))
        val res = BankMessageSerializer.serialize(PollResponse(header(BizType.POLL, MessageClass.ADMIN_RESPONSE)))

        assertIs<PollRequest>(BankMessageSerializer.deserialize(req))
        assertIs<PollResponse>(BankMessageSerializer.deserialize(res))
    }
}
