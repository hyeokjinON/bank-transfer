package io.github.hyeokjinon.banktransfer.protocol

import java.time.LocalDateTime

/** 전문구분코드. 업무 요청/응답과 관리(폴링) 요청/응답을 나눈다. */
enum class MessageClass(val code: String, val isRequest: Boolean) {
    REQUEST("0200", true),
    RESPONSE("0210", false),
    ADMIN_REQUEST("0800", true),
    ADMIN_RESPONSE("0810", false);

    fun toResponse(): MessageClass = when (this) {
        REQUEST -> RESPONSE
        ADMIN_REQUEST -> ADMIN_RESPONSE
        else -> throw ProtocolException("$this is already a response")
    }

    companion object {
        fun of(code: String): MessageClass =
            entries.firstOrNull { it.code == code } ?: throw ProtocolException("unknown message class: '$code'")
    }
}

/** 업무구분코드. */
enum class BizType(val code: String) {
    POLL("PLL"),          // 링크 체크
    NAME_INQUIRY("NAM"),  // 예금주 성명조회
    TRANSFER("TRF");      // 지급이체 (1원 인증에도 사용)

    companion object {
        fun of(code: String): BizType =
            entries.firstOrNull { it.code == code } ?: throw ProtocolException("unknown biz type: '$code'")
    }
}

object ResponseCodes {
    const val OK = "0000"
    const val ACCOUNT_NOT_FOUND = "1001"
    const val SYSTEM_ERROR = "9999"
}

/**
 * 공통부(헤더). 모든 전문 앞에 붙는 33바이트.
 *
 * | 필드 | 길이 |
 * |---|---|
 * | 전문구분 | 4 |
 * | 업무구분 | 3 |
 * | 전문번호 | 8 (숫자, 0 채움) |
 * | 전문일시 | 14 (yyyyMMddHHmmss) |
 * | 응답코드 | 4 |
 */
data class Header(
    val messageClass: MessageClass,
    val bizType: BizType,
    val tranNo: Long,
    val tranDateTime: LocalDateTime,
    val responseCode: String = ResponseCodes.OK,
) {
    val isOk: Boolean get() = responseCode == ResponseCodes.OK

    /** 같은 전문번호로 응답 헤더를 만든다. 응답 매칭의 핵심은 전문번호를 그대로 돌려주는 것이다. */
    fun toResponse(responseCode: String = ResponseCodes.OK): Header =
        copy(messageClass = messageClass.toResponse(), responseCode = responseCode)

    companion object {
        const val LENGTH = 4 + 3 + 8 + 14 + 4
        const val TRAN_NO_LENGTH = 8
        const val MAX_TRAN_NO = 99_999_999L
    }
}
