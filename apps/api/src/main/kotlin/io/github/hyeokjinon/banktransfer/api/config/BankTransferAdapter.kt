package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.transfer.BankTransferOutcome
import io.github.hyeokjinon.banktransfer.transfer.BankTransferPort
import java.util.concurrent.ExecutionException

/**
 * transfer 모듈의 BankTransferPort 를 Netty 게이트웨이에 연결한다. 예외를 "보냈나 / 모르나"로 분류하는 곳이 여기다.
 * - BankNotSentException (연결 없음): 은행에 가지 않았다 → NotSent (다시 보내도 안전)
 * - 그 밖의 실패 (응답 시간 초과, 보낸 뒤 연결 끊김, 쓰기 실패 …): 은행이 받았을 수 있다 → ResultUnknown (다시 보내지 않는다)
 * 응답 시간 초과는 게이트웨이(bank.gateway.response-timeout)가 건다.
 */
class BankTransferAdapter(private val client: BankGatewayClient) : BankTransferPort {

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): BankTransferOutcome =
        try {
            val response = client.transfer(bankCode, accountNo, amount, memo).get()
            if (response.header.isOk) BankTransferOutcome.Completed else BankTransferOutcome.Rejected(response.header.responseCode)
        } catch (e: ExecutionException) {
            // 예외를 던지지 않고 NotSent 를 돌려줘야 한다: 프로세서는 포트가 던진 예외를 전부 "결과 모름(UNKNOWN)"으로 취급한다
            if (e.cause is BankNotSentException) BankTransferOutcome.NotSent else BankTransferOutcome.ResultUnknown
        }
}
