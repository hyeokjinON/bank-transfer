package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import io.github.hyeokjinon.banktransfer.transfer.BankTransferOutcome
import io.github.hyeokjinon.banktransfer.transfer.BankTransferPort
import org.slf4j.LoggerFactory
import java.util.concurrent.ExecutionException

/**
 * transfer 모듈의 BankTransferPort 를 Netty 게이트웨이에 연결한다 — 블로킹 판 (컨슈머 스레드가 .get() 으로 기다린다).
 * 예외를 "보냈나 / 모르나"로 나누는 규칙은 [classifyBankFailure] 하나를 코루틴 판(SuspendBankTransferAdapter)과 같이 쓴다 (P6-1).
 * 응답 시간 초과는 게이트웨이(bank.gateway.response-timeout)가 건다.
 */
class BankTransferAdapter(private val client: BankGatewayClient) : BankTransferPort {

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome =
        try {
            outcomeOf(client.transfer(bankCode, accountNo, amount, memo, reference).get())
        } catch (e: ExecutionException) {
            // 예외를 던지지 않고 분류해 돌려줘야 한다: 프로세서는 포트가 던진 예외를 전부 "결과 모름(UNKNOWN)"으로 취급한다
            classifyBankFailure(e.cause ?: e, accountNo)
        } catch (e: InterruptedException) {
            // 기다리다 중단됐다. 보냈는지 알 수 없으니 UNKNOWN 으로 두고, 스레드의 중단 표시는 되살려 상위(컨슈머)가 알아채게 한다
            Thread.currentThread().interrupt()
            classifyBankFailure(e, accountNo)
        } catch (e: Exception) {
            // 그 밖의 예상 못 한 예외도 "보냈을 수 있다"로 본다 (다시 보내는 쪽이 더 위험하다)
            classifyBankFailure(e, accountNo)
        }
}

/** 은행 응답 → 결과. 0000 이면 완료, 아니면 거절 (응답코드 그대로). */
internal fun outcomeOf(response: TransferResponse): BankTransferOutcome =
    if (response.header.isOk) BankTransferOutcome.Completed else BankTransferOutcome.Rejected(response.header.responseCode)

/**
 * 은행 호출 실패를 "보냈나 / 모르나"로 나눈다 — 블로킹 · 코루틴 어댑터가 이 함수 하나를 쓴다 (P6-1). 규칙이 둘로 갈라지면 한쪽만 고쳐 이중 지급이 생길 수 있다.
 * - BankNotSentException (연결 없음 · 점검 시간): 은행에 가지 않았다 → NotSent (다시 보내도 안전)
 * - 그 밖 전부 (응답 시간 초과, 보낸 뒤 연결 끊김, 쓰기 실패, 기다리다 중단 …): 은행이 받았을 수 있다 → ResultUnknown (다시 보내지 않는다)
 * 코루틴 취소는 여기로 오지 않는다 — 코루틴 판이 분류하지 않고 다시 던진다 (설계 §4).
 */
internal fun classifyBankFailure(cause: Throwable, accountNo: String): BankTransferOutcome =
    if (cause is BankNotSentException) {
        log.info("bank transfer not sent: account={}", mask(accountNo))
        BankTransferOutcome.NotSent
    } else {
        log.warn("bank transfer result unknown: account={} cause={}", mask(accountNo), "${cause.javaClass.name}: ${cause.message}")
        BankTransferOutcome.ResultUnknown
    }

/** 로그는 지금처럼 BankTransferAdapter 이름으로 남긴다 (두 판이 같은 줄을 남긴다). */
private val log = LoggerFactory.getLogger(BankTransferAdapter::class.java)

/** 뒤 4자리만 남긴다. transfer 모듈의 maskAccountNo 는 internal 이라 여기서 따로 둔다. */
private fun mask(accountNo: String): String =
    if (accountNo.length <= 4) "*".repeat(accountNo.length) else "*".repeat(accountNo.length - 4) + accountNo.takeLast(4)
