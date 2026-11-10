package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals

/** 전문 응답 → JSON → 전문 응답이 그대로 돌아온다 (게이트웨이 앱이 쓰고 클라이언트가 읽는 길). */
class GatewayContractTest {
    private val at = LocalDateTime.of(2026, 11, 10, 9, 30, 15)
    private fun header(biz: BizType, code: String = "0000") = Header(MessageClass.RESPONSE, biz, 1234, at, code)

    @Test
    fun `성명조회 응답 왕복`() {
        val original = NameInquiryResponse(header(BizType.NAME_INQUIRY), "090", "1002123456789", "홍길동")
        val json = GatewayJson.write(original.toResult())
        assertEquals(original, GatewayJson.read(json, NameInquiryResult::class.java).toResponse())
    }

    @Test
    fun `이체 응답 왕복 - 거절 응답코드도 그대로`() {
        val original = TransferResponse(header(BizType.TRANSFER, "1001"), "090", "1002123456789", 30_000, "적요", "T261110ABCDEFGHIJKLM")
        val json = GatewayJson.write(original.toResult())
        assertEquals(original, GatewayJson.read(json, TransferResult::class.java).toResponse())
    }

    @Test
    fun `결과조회 응답 왕복`() {
        val original = InquiryResponse(header(BizType.INQUIRY), "T261110ABCDEFGHIJKLM", "2001")
        val json = GatewayJson.write(original.toResult())
        assertEquals(original, GatewayJson.read(json, InquiryResult::class.java).toResponse())
    }
}
