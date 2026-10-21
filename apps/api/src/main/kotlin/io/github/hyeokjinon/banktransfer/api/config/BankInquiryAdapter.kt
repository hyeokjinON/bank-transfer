package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
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
            classify(reference, client.inquire(reference).get())
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            unknown(reference, e.toString())
        } catch (e: Exception) {
            unknown(reference, (e.cause ?: e).toString())
        }

    /**
     * 응답이 우리가 물은 거래번호에 대한 것인지 먼저 본다. 다른 거래의 "처리 안 됨(2001)" 을 이 건의 FAILED 로 확정하면
     * 돈이 나간 건을 실패로 적게 되므로, 거래번호가 다르면 모름으로 두고 다음 주기에 다시 묻는다.
     */
    internal fun classify(reference: String, response: InquiryResponse): InquiryOutcome {
        val echoed = response.reference.trim()
        if (echoed != reference) {
            return unknown(reference, "reference mismatch: echoed=$echoed")
        }
        return when {
            !response.header.isOk -> unknown(reference, "inquiry rejected: ${response.header.responseCode}")
            response.result == ResponseCodes.OK -> InquiryOutcome.Processed
            response.result == ResponseCodes.ORIGINAL_NOT_FOUND -> InquiryOutcome.NotProcessed(response.result)
            else -> unknown(reference, "unexpected result: ${response.result}")
        }
    }

    private fun unknown(reference: String, reason: String): InquiryOutcome {
        log.warn("bank inquiry inconclusive: reference={} reason={}", reference, reason)
        return InquiryOutcome.Unknown
    }
}
