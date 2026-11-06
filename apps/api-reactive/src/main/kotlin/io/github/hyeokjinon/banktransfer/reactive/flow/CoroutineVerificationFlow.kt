package io.github.hyeokjinon.banktransfer.reactive.flow

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationRequestRules
import io.github.hyeokjinon.banktransfer.account.infra.holderNameOf
import io.github.hyeokjinon.banktransfer.account.infra.maskAccount
import io.github.hyeokjinon.banktransfer.account.infra.requireOk
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.transfer.ReferenceGenerator
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory

/**
 * 1원인증 요청 — WebFlux 코루틴 판 (P6-2, ADR-0013). 판정은 [VerificationRequestRules] (네 판 공용), 여기는 기다리는 방법만 정한다.
 * - 은행 두 번(성명조회 · 1원 이체)은 await — 기다리는 동안 스레드를 잡지 않는다.
 * - await 뒤는 은행 응답을 완료한 게이트웨이 이벤트 루프에서 이어진다 (실험 E1). 거기서 JDBC 를 기다리면 그 루프의 모든 은행 응답이 멈추므로
 *   저장만 [io] 로 넘긴다. 해석 · 비교는 짧은 계산이라 루프에서 해도 된다.
 * - 클라이언트가 끊으면 WebFlux 가 코루틴을 취소한다 (실험 E2). 취소는 다시 던진다 — 오류 응답으로 바꾸지 않는다(보낼 곳이 없다).
 *   1원 이체를 기다리는 중 취소되면 1원이 나갔을 수 있어 경고를 남긴다. 사용자는 다시 요청한다 (설계 §4-3: 대가는 최악에 1원 두 번).
 */
class CoroutineVerificationFlow(
    private val client: BankGatewayClient,
    private val rules: VerificationRequestRules,
    private val repository: VerificationRepository,
    private val references: ReferenceGenerator,
    private val io: CoroutineDispatcher = Dispatchers.IO,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    suspend fun request(command: RequestVerification): AccountVerification {
        val found = holderNameOf(client.inquireName(command.bankCode, command.accountNo).await())
        val holderName = rules.checkHolder(found, command.holderName)
        val draft = rules.newDraft()
        try {
            requireOk(client.transfer(command.bankCode, command.accountNo, 1, draft.memo, references.next()).await())
        } catch (e: CancellationException) {
            log.warn(DISCONNECTED_DURING_TRANSFER, command.bankCode, maskAccount(command.accountNo))
            throw e
        }
        return withContext(io) { repository.save(rules.issue(draft, command, holderName)) }
    }

    companion object {
        /** 1원 이체 응답을 기다리는 중 취소 (클라이언트 끊김 등). 계좌는 마스킹 — Task 7 끊김 테스트가 이 형식을 본다. */
        const val DISCONNECTED_DURING_TRANSFER =
            "1원인증 요청 취소 (stage=transfer) — 1원 이체 응답을 기다리는 중이라 1원이 나갔을 수 있다. 사용자는 다시 요청한다: bank={} account={}"
    }
}
