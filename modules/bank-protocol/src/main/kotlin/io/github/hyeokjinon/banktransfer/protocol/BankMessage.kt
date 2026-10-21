package io.github.hyeokjinon.banktransfer.protocol

/**
 * 은행과 주고받는 전문. sealed 로 막아 두었기 때문에 `when` 에서 모든 종류를 빠짐없이 처리하도록
 * 컴파일러가 강제한다. 새 전문을 추가하면 직렬화 코드가 컴파일 에러로 알려 준다.
 */
sealed interface BankMessage {
    val header: Header
}

data class PollRequest(override val header: Header) : BankMessage

data class PollResponse(override val header: Header) : BankMessage

data class NameInquiryRequest(
    override val header: Header,
    val bankCode: String,
    val accountNo: String,
) : BankMessage

data class NameInquiryResponse(
    override val header: Header,
    val bankCode: String,
    val accountNo: String,
    val holderName: String,
) : BankMessage

data class TransferRequest(
    override val header: Header,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    /** 받는 사람 통장에 찍히는 적요. 1원 인증에서는 여기에 인증코드를 싣는다. */
    val memo: String,
    /** 이용기관 거래번호 20자 (v0.2). 세션 안에서 돌고 도는 전문번호와 달리 업무 키라서 결과조회에 쓴다. */
    val reference: String,
) : BankMessage

data class TransferResponse(
    override val header: Header,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    val memo: String,
    /** 이용기관 거래번호 20자 (v0.2). 세션 안에서 돌고 도는 전문번호와 달리 업무 키라서 결과조회에 쓴다. */
    val reference: String,
) : BankMessage

/** 결과조회 요청 (v0.2): 결과를 모르는 이체를 이용기관 거래번호로 묻는다. */
data class InquiryRequest(
    override val header: Header,
    val reference: String,
) : BankMessage

/**
 * 결과조회 응답 (v0.2). [result] 는 원거래 처리결과(0000 처리됨 · 2001 원거래 없음)이고,
 * 공통부 응답코드는 조회 전문 자체가 성공했는지를 뜻한다.
 */
data class InquiryResponse(
    override val header: Header,
    val reference: String,
    val result: String,
) : BankMessage

class ProtocolException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
