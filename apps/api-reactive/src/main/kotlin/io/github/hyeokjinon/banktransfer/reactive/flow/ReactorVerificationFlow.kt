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
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers

/**
 * 1원인증 요청 — WebFlux Reactor 판 (P6-2, ADR-0013). 판정 · 해석 · 저장소는 코루틴 판과 같고, 기다리는 방법만 연산자로 쓴다.
 * 코루틴 판과의 대응:
 * - `client.xxx().await()`        ↔ `Mono.fromFuture { client.xxx() }` — 구독할 때 호출, 구독 취소 때 future 도 취소 (실험 E3)
 * - 다음 단계로 값 넘기기          ↔ `map` (짧은 계산) · `flatMap` (또 기다리는 것)
 * - `withContext(io) { save }`    ↔ `publishOn(boundedElastic())` 뒤 `map { save }` — 이 아래 연산자는 블로킹을 허용하는 스레드에서 돈다
 * - `catch (CancellationException)` ↔ `doOnCancel` — 클라이언트 끊김이 구독 취소로 온다 (실험 E2)
 * fromFuture 뒤 map 은 게이트웨이 이벤트 루프에서 돈다(실험 E1) — 그래서 저장 전에 publishOn 이 꼭 있어야 한다.
 */
class ReactorVerificationFlow(
    private val client: BankGatewayClient,
    private val rules: VerificationRequestRules,
    private val repository: VerificationRepository,
    private val references: ReferenceGenerator,
    private val scheduler: Scheduler = Schedulers.boundedElastic(),
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun request(command: RequestVerification): Mono<AccountVerification> =
        Mono.fromFuture { client.inquireName(command.bankCode, command.accountNo) }
            .map { res -> rules.checkHolder(holderNameOf(res), command.holderName) to rules.newDraft() }
            .flatMap { (holderName, draft) ->
                Mono.fromFuture { client.transfer(command.bankCode, command.accountNo, 1, draft.memo, references.next()) }
                    .map { res -> requireOk(res) }
                    .doOnCancel { log.warn(DISCONNECTED_DURING_TRANSFER, command.bankCode, maskAccount(command.accountNo)) }
                    .thenReturn(holderName to draft)
            }
            .publishOn(scheduler)
            .map { (holderName, draft) -> repository.save(rules.issue(draft, command, holderName)) }

    companion object {
        /** 코루틴 판(CoroutineVerificationFlow.DISCONNECTED_DURING_TRANSFER)과 같은 형식 — 끊김 테스트가 두 판에 같은 단언을 쓴다. */
        const val DISCONNECTED_DURING_TRANSFER = CoroutineVerificationFlow.DISCONNECTED_DURING_TRANSFER
    }
}
