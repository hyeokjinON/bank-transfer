package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.transfer.BankTransferOutcome
import io.github.hyeokjinon.banktransfer.transfer.SuspendBankTransferPort
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletionException

/**
 * SuspendBankTransferPort 를 은행 게이트웨이(BankGateway)에 연결한다 — 코루틴 판 (P6-1, ADR-0012). 응답은 await 로 기다린다: 스레드를 잡지 않는다.
 * 분류는 블로킹 판과 같은 [classifyBankFailure] 다. 다른 것은 취소 하나:
 * 기다리는 중 코루틴이 취소되면(앱 종료 등) 은행에 갔는지 모른다 — 분류하지 않고 취소를 다시 던진다.
 * NotSent 로 돌려주면 RECEIVED 로 되돌아가 다시 보내진다(이중 지급). 출금은 SENDING 으로 남고 정리 작업이 30초 뒤 UNKNOWN → 결과조회로 확정한다.
 * await 는 취소될 때 게이트웨이의 future 도 취소한다 — 대기 표 항목은 응답 시간 초과(3초) 뒤 지워진다 (BankGatewayClientTest 가 고정).
 */
class SuspendBankTransferAdapter(private val client: BankGateway) : SuspendBankTransferPort {

    override suspend fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome =
        try {
            outcomeOf(client.transfer(bankCode, accountNo, amount, memo, reference).await())
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // await 는 CompletionException 을 벗겨 던지지만, 한 겹 더 감싸여 와도 같은 규칙이 되게 벗긴다 (CLAUDE.md)
            classifyBankFailure(if (e is CompletionException) e.cause ?: e else e, accountNo)
        }
}
