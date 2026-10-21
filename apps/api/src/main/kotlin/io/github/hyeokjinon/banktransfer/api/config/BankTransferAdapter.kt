package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.transfer.BankTransferOutcome
import io.github.hyeokjinon.banktransfer.transfer.BankTransferPort
import org.slf4j.LoggerFactory
import java.util.concurrent.ExecutionException

/**
 * transfer 모듈의 BankTransferPort 를 Netty 게이트웨이에 연결한다. 예외를 "보냈나 / 모르나"로 분류하는 곳이 여기다.
 * - BankNotSentException (연결 없음): 은행에 가지 않았다 → NotSent (다시 보내도 안전)
 * - 그 밖의 실패 (응답 시간 초과, 보낸 뒤 연결 끊김, 쓰기 실패 …): 은행이 받았을 수 있다 → ResultUnknown (다시 보내지 않는다)
 * 응답 시간 초과는 게이트웨이(bank.gateway.response-timeout)가 건다.
 */
class BankTransferAdapter(private val client: BankGatewayClient) : BankTransferPort {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome =
        try {
            val response = client.transfer(bankCode, accountNo, amount, memo, reference).get()
            if (response.header.isOk) BankTransferOutcome.Completed else BankTransferOutcome.Rejected(response.header.responseCode)
        } catch (e: ExecutionException) {
            // 예외를 던지지 않고 NotSent 를 돌려줘야 한다: 프로세서는 포트가 던진 예외를 전부 "결과 모름(UNKNOWN)"으로 취급한다
            val cause = e.cause ?: e
            if (cause is BankNotSentException) {
                log.info("bank transfer not sent: account={}", mask(accountNo))
                BankTransferOutcome.NotSent
            } else {
                unknown(accountNo, cause)
            }
        } catch (e: InterruptedException) {
            // 기다리다 중단됐다. 보냈는지 알 수 없으니 UNKNOWN 으로 두고, 스레드의 중단 표시는 되살려 상위(컨슈머)가 알아채게 한다
            Thread.currentThread().interrupt()
            unknown(accountNo, e)
        } catch (e: Exception) {
            // 그 밖의 예상 못 한 예외도 "보냈을 수 있다"로 본다 (다시 보내는 쪽이 더 위험하다)
            unknown(accountNo, e)
        }

    private fun unknown(accountNo: String, cause: Throwable): BankTransferOutcome {
        log.warn("bank transfer result unknown: account={} cause={}", mask(accountNo), "${cause.javaClass.name}: ${cause.message}")
        return BankTransferOutcome.ResultUnknown
    }

    /** 뒤 4자리만 남긴다. transfer 모듈의 maskAccountNo 는 internal 이라 여기서 따로 둔다. */
    private fun mask(accountNo: String): String =
        if (accountNo.length <= 4) "*".repeat(accountNo.length) else "*".repeat(accountNo.length - 4) + accountNo.takeLast(4)
}
