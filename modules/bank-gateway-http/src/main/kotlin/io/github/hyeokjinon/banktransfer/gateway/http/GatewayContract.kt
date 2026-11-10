package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.time.LocalDateTime

/** 게이트웨이 앱 내부 API 경로 (P8-1). 모두 POST · JSON · 내부망 전용. */
object GatewayPaths {
    const val NAME_INQUIRIES = "/internal/v1/bank/name-inquiries"
    const val TRANSFERS = "/internal/v1/bank/transfers"
    const val INQUIRIES = "/internal/v1/bank/inquiries"
}

/** 오류 본문의 code. 503 이라도 BANK_NOT_SENT 일 때만 "안 보냄" 이다 (스펙 §4-3). */
object GatewayErrorCodes {
    const val BANK_NOT_SENT = "BANK_NOT_SENT"         // 503
    const val BANK_TIMEOUT = "BANK_TIMEOUT"           // 504
    const val BANK_UNAVAILABLE = "BANK_UNAVAILABLE"   // 502
    const val INVALID_REQUEST = "INVALID_REQUEST"     // 400
    const val INTERNAL = "INTERNAL"                   // 500
}

data class GatewayError(val code: String, val message: String, val tranNo: Long? = null, val timeoutMs: Long? = null)

/** 은행 응답 헤더 중 호출자가 쓰는 것 (전문구분 · 업무구분은 경로가 정한다). */
data class HeaderDto(val tranNo: Long, val tranDateTime: LocalDateTime, val responseCode: String)

data class NameInquiryBody(val bankCode: String, val accountNo: String)
data class NameInquiryResult(val header: HeaderDto, val bankCode: String, val accountNo: String, val holderName: String)
data class TransferBody(val bankCode: String, val accountNo: String, val amount: Long, val memo: String, val reference: String)
data class TransferResult(val header: HeaderDto, val bankCode: String, val accountNo: String, val amount: Long, val memo: String, val reference: String)
data class InquiryBody(val reference: String)
data class InquiryResult(val header: HeaderDto, val reference: String, val result: String)

private fun Header.toDto() = HeaderDto(tranNo, tranDateTime, responseCode)
private fun HeaderDto.toHeader(biz: BizType) = Header(MessageClass.RESPONSE, biz, tranNo, tranDateTime, responseCode)

fun NameInquiryResponse.toResult() = NameInquiryResult(header.toDto(), bankCode, accountNo, holderName)
fun NameInquiryResult.toResponse() = NameInquiryResponse(header.toHeader(BizType.NAME_INQUIRY), bankCode, accountNo, holderName)
fun TransferResponse.toResult() = TransferResult(header.toDto(), bankCode, accountNo, amount, memo, reference)
fun TransferResult.toResponse() = TransferResponse(header.toHeader(BizType.TRANSFER), bankCode, accountNo, amount, memo, reference)
fun InquiryResponse.toResult() = InquiryResult(header.toDto(), reference, result)
fun InquiryResult.toResponse() = InquiryResponse(header.toHeader(BizType.INQUIRY), reference, result)
