package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.TranNoSource
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

/**
 * 지금 은행 연결(클라이언트)의 단일 진실 (P8-3b 결정 4 · 18). 엔드포인트 · 헬스 · 종료 훅 · 선출 콜백이 모두 여기를 거친다 — 상태가 어긋날 틈이 없다.
 * - 리더 임기마다 [BankGatewayClient] 를 새로 만든다 (close 가 이벤트 루프까지 닫아 재사용할 수 없다 — 임기 단위로 상태를 끊는다).
 * - 리더가 아니면 연결이 없다: 요청은 BankNotSentException → 503 BANK_NOT_SENT (호출자는 안전하게 재시도).
 * - [stepDown] = 정상 교대 1 ~ 3 단계: 새 요청 거절 → 처리 중 전문 비우기 → 끊기 (결정 11).
 */
class BankConnectionOwner(
    private val tranNoSource: TranNoSource,
    private val newClient: () -> BankGatewayClient,
) : BankGateway, AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)

    @Volatile private var client: BankGatewayClient? = null
    @Volatile private var steppingDown = false

    val isLeader: Boolean get() = client != null

    fun current(): BankGatewayClient? = client

    /** 임기 시작: 번호 구간 준비 → 새 클라이언트로 접속 (비동기). 종료 중이면 무시한다 (늦게 온 콜백). */
    @Synchronized
    fun startLeading() {
        if (steppingDown || client != null) return
        tranNoSource.prepare()
        client = newClient().start()
        log.info("bank connection term started — connecting")
    }

    /** 임기 끝: 즉시 끊는다. 응답을 기다리던 전문은 BankUnavailableException (결과 모름 → 결과조회, 결정 13). */
    @Synchronized
    fun stopLeading() {
        val c = client ?: return
        client = null
        c.close()
        log.info("bank connection term ended — closed")
    }

    /** 정상 교대 1 ~ 3: 새 요청 거절 → 응답 대기 전문이 0이 될 때까지(최대 [drainTimeout]) → 끊기. */
    fun stepDown(drainTimeout: Duration) {
        steppingDown = true
        val c = client
        if (c != null) {
            val deadline = System.nanoTime() + drainTimeout.toNanos()
            while (c.pendingCount > 0 && System.nanoTime() < deadline) Thread.sleep(50)
            if (c.pendingCount > 0) log.warn("step down: {} requests still pending after {} — closing anyway", c.pendingCount, drainTimeout)
        }
        stopLeading()
    }

    override fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> =
        withClient { it.inquireName(bankCode, accountNo) }

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> =
        withClient { it.transfer(bankCode, accountNo, amount, memo, reference) }

    override fun inquire(reference: String): CompletableFuture<InquiryResponse> =
        withClient { it.inquire(reference) }

    private fun <T> withClient(call: (BankGatewayClient) -> CompletableFuture<T>): CompletableFuture<T> {
        if (steppingDown) return CompletableFuture.failedFuture(BankNotSentException("gateway is stepping down"))
        val c = client ?: return CompletableFuture.failedFuture(BankNotSentException("not the bank connection leader"))
        return call(c)
    }

    override fun close() {
        steppingDown = true
        stopLeading()
    }
}
