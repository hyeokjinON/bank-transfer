package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.transfer.BankInquiryPort
import io.github.hyeokjinon.banktransfer.transfer.InquiryOutcome
import org.slf4j.LoggerFactory

/**
 * 결과조회 포트를 게이트웨이에 연결한다. 확실한 답(처리됨 0000 · 원거래 없음 2001)만 확정으로 돌려주고,
 * 그 밖(조회 전문 거절, 모르는 처리결과, 시간 초과, 연결 없음 …)은 모두 Unknown — 다음 주기에 다시 묻는다.
 * 예외를 던지지 않는다.
 */
class BankInquiryAdapter(private val client: BankGatewayClient) : BankInquiryPort {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun inquire(reference: String): InquiryOutcome =
        try {
            val response = client.inquire(reference).get()
            when {
                !response.header.isOk -> unknown(reference, "inquiry rejected: ${response.header.responseCode}")
                response.result == ResponseCodes.OK -> InquiryOutcome.Processed
                response.result == ResponseCodes.ORIGINAL_NOT_FOUND -> InquiryOutcome.NotProcessed(response.result)
                else -> unknown(reference, "unexpected result: ${response.result}")
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            unknown(reference, e.toString())
        } catch (e: Exception) {
            unknown(reference, (e.cause ?: e).toString())
        }

    private fun unknown(reference: String, reason: String): InquiryOutcome {
        log.warn("bank inquiry inconclusive: reference={} reason={}", reference, reason)
        return InquiryOutcome.Unknown
    }
}
