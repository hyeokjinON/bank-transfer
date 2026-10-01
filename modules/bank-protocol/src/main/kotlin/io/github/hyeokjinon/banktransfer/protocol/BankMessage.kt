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
) : BankMessage

data class TransferResponse(
    override val header: Header,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    val memo: String,
) : BankMessage

class ProtocolException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
