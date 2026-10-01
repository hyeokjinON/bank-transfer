package io.github.hyeokjinon.banktransfer.protocol

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * 전문 <-> 바이트 배열 변환. Netty 를 몰라도 테스트할 수 있도록 순수 함수로 분리했다.
 *
 * 개별부 레이아웃 (공통부 33바이트 뒤)
 * - 성명조회 요청: 은행코드 3 + 계좌번호 16
 * - 성명조회 응답: 요청 + 예금주명 20
 * - 이체 요청/응답: 은행코드 3 + 계좌번호 16 + 금액 13 + 적요 20
 */
object BankMessageSerializer {
    private val DATE_TIME: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

    fun serialize(message: BankMessage): ByteArray {
        val w = FixedLengthWriter()
        writeHeader(w, message.header)
        when (message) {
            is PollRequest, is PollResponse -> Unit
            is NameInquiryRequest -> w.text(message.bankCode, 3).text(message.accountNo, 16)
            is NameInquiryResponse -> w.text(message.bankCode, 3).text(message.accountNo, 16).text(message.holderName, 20)
            is TransferRequest -> w.transferBody(message.bankCode, message.accountNo, message.amount, message.memo)
            is TransferResponse -> w.transferBody(message.bankCode, message.accountNo, message.amount, message.memo)
        }
        return w.toByteArray()
    }

    fun deserialize(bytes: ByteArray): BankMessage {
        val r = FixedLengthReader(bytes)
        val header = readHeader(r)
        val request = header.messageClass.isRequest
        val message = when (header.bizType) {
            BizType.POLL -> if (request) PollRequest(header) else PollResponse(header)
            BizType.NAME_INQUIRY -> {
                val bankCode = r.text(3)
                val accountNo = r.text(16)
                if (request) NameInquiryRequest(header, bankCode, accountNo)
                else NameInquiryResponse(header, bankCode, accountNo, r.text(20))
            }
            BizType.TRANSFER -> {
                val bankCode = r.text(3)
                val accountNo = r.text(16)
                val amount = r.number(13)
                val memo = r.text(20)
                if (request) TransferRequest(header, bankCode, accountNo, amount, memo)
                else TransferResponse(header, bankCode, accountNo, amount, memo)
            }
        }
        if (r.remaining != 0) throw ProtocolException("unexpected ${r.remaining} trailing bytes for ${header.bizType}")
        return message
    }

    private fun writeHeader(w: FixedLengthWriter, h: Header) {
        w.text(h.messageClass.code, 4)
            .text(h.bizType.code, 3)
            .number(h.tranNo, Header.TRAN_NO_LENGTH)
            .text(h.tranDateTime.format(DATE_TIME), 14)
            .text(h.responseCode, 4)
    }

    private fun readHeader(r: FixedLengthReader): Header {
        val messageClass = MessageClass.of(r.text(4))
        val bizType = BizType.of(r.text(3))
        val tranNo = r.number(Header.TRAN_NO_LENGTH)
        val rawDateTime = r.text(14)
        val dateTime = runCatching { LocalDateTime.parse(rawDateTime, DATE_TIME) }
            .getOrElse { throw ProtocolException("bad date time: $rawDateTime", it) }
        return Header(messageClass, bizType, tranNo, dateTime, r.text(4))
    }

    private fun FixedLengthWriter.transferBody(bankCode: String, accountNo: String, amount: Long, memo: String) =
        text(bankCode, 3).text(accountNo, 16).number(amount, 13).text(memo, 20)
}
