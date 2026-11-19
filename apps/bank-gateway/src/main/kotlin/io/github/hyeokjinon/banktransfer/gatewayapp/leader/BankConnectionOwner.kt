package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.NotSentReason
import io.github.hyeokjinon.banktransfer.gateway.TranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.metrics.BankRequestType
import io.github.hyeokjinon.banktransfer.gatewayapp.metrics.GatewayMetrics
import io.github.hyeokjinon.banktransfer.gatewayapp.tracing.BankRequestTracing
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

/**
 * 지금 은행 연결(클라이언트)의 단일 진실 (P8-3b 결정 4 · 18). 엔드포인트 · 헬스 · 종료 훅 · 선출 콜백이 모두 여기를 거친다 — 상태가 어긋날 틈이 없다.
 * - 리더 임기마다 [BankGatewayClient] 를 새로 만든다 (close 가 이벤트 루프까지 닫아 재사용할 수 없다 — 임기 단위로 상태를 끊는다).
 * - 리더가 아니면 연결이 없다: 요청은 BankNotSentException → 503 BANK_NOT_SENT (호출자는 안전하게 재시도).
 * - [stepDown] = 정상 교대 1 ~ 3 단계: 새 요청 거절 → 처리 중 전문 비우기 → 끊기 (결정 11).
 *   보장: 거절 확인을 통과한 요청은 끊기 전에 끝난다 (정상 교대의 "결과 모름" 0). 거절 확인과 대기 표 등록 사이 틈을 처리 중 카운터([inFlight])로 닫는다 —
 *   요청은 카운터를 먼저 올린 뒤 거절 · 클라이언트를 확인하고 (전문은 send 가 돌아오기 전에 대기 표에 오른다), stepDown 은 카운터와 대기 표가 모두 0이 될 때까지 기다린다.
 */
class BankConnectionOwner(
    private val tranNoSource: TranNoSource,
    /** P9a 지표 — 은행 호출 · 안 보냄 사유 · 임기 수를 여기 한 곳에서 센다. */
    private val metrics: GatewayMetrics = GatewayMetrics.noop(),
    /** P9b 추적 — 은행 호출 구간 bank.request 를 여기 한 곳에서 연다. */
    private val tracing: BankRequestTracing = BankRequestTracing.NOOP,
    private val newClient: () -> BankGatewayClient,
) : BankGateway, AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)

    @Volatile private var client: BankGatewayClient? = null
    @Volatile private var steppingDown = false
    private val inFlight = AtomicInteger()   // 거절 확인 ~ 대기 표 등록(send 반환) 사이에 있는 요청 수

    val isLeader: Boolean get() = client != null

    fun current(): BankGatewayClient? = client

    /** 임기 시작: 번호 구간 준비 → 새 클라이언트로 접속 (비동기). 종료 중이면 무시한다 (늦게 온 콜백). */
    @Synchronized
    fun startLeading() {
        if (steppingDown || client != null) return
        tranNoSource.prepare()
        client = newClient().start()
        metrics.leaderTermStarted()
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
            while ((inFlight.get() > 0 || c.pendingCount > 0) && System.nanoTime() < deadline) Thread.sleep(50)
            if (inFlight.get() > 0 || c.pendingCount > 0) {
                log.warn("step down: {} in flight, {} pending after {} — closing anyway", inFlight.get(), c.pendingCount, drainTimeout)
            }
        }
        stopLeading()
    }

    override fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> =
        withClient(BankRequestType.NAME, reference = null) { it.inquireName(bankCode, accountNo) }

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> =
        withClient(BankRequestType.TRANSFER, reference) { it.transfer(bankCode, accountNo, amount, memo, reference) }

    override fun inquire(reference: String): CompletableFuture<InquiryResponse> =
        withClient(BankRequestType.INQUIRY, reference) { it.inquire(reference) }

    /**
     * 은행 호출은 모두 여기를 지난다 — 지표(P9a) · 추적 구간(P9b) · 호출 로그 한 줄(P9c)도 여기 한 곳. 돌려주는 future 는 원래 것 그대로다:
     * whenComplete 는 옆가지라 취소 · 완료 의미를 바꾸지 않는다 (GatewayEndpoints 의 "취소가 원본으로 번지지 않는다" 그대로).
     * 구간은 지금 구간(서버 요청)의 자식으로 열고, 완료 콜백(은행 Netty 이벤트 루프)에서 객체를 직접 들고 닫는다.
     * 추적 호출은 BankRequestTracing 안에서 던지지 않게 막혀 있다 (E10) — 은행 호출은 정확히 한 번, 결과 그대로.
     */
    private fun <T> withClient(type: BankRequestType, reference: String?, call: (BankGatewayClient) -> CompletableFuture<T>): CompletableFuture<T> {
        val startedAt = System.nanoTime()
        val span = tracing.start(type, reference)
        val result = dispatch(call)
        result.whenComplete { value, error ->
            // 구간을 먼저 닫는다 — 지표가 던져도 구간이 열린 채 남지 않게 (E10). 지표 실패는 은행 호출 결과에 번지지 않는다
            runCatching { tracing.end(span, value, error) }.onFailure { log.warn("bank.request 구간 닫기 실패 — {}", it.javaClass.name) }
            runCatching { metrics.recordBankCall(type, System.nanoTime() - startedAt, value, error) }.onFailure { log.warn("은행 호출 지표 기록 실패 — {}", it.javaClass.name) }
            // P9c: 결과 한 줄을 구간 문맥에서 — traceId 로 게이트웨이 줄을 찾고, 그 줄의 reference 로 모의 은행 통장 줄을 찾는다
            runCatching { tracing.inScope(span) { BankCallLog.write(type, reference, value, error, System.nanoTime() - startedAt) } }
                .onFailure { log.warn("은행 호출 로그 실패 — {}", it.javaClass.name) }
        }
        return result
    }

    private fun <T> dispatch(call: (BankGatewayClient) -> CompletableFuture<T>): CompletableFuture<T> {
        inFlight.incrementAndGet()   // 먼저 올린다 — stepDown 이 steppingDown 을 세운 뒤 이 값을 보면, 이 요청의 대기 표 등록까지 기다린다
        try {
            if (steppingDown) return CompletableFuture.failedFuture(BankNotSentException("gateway is stepping down", reason = NotSentReason.STEPPING_DOWN))
            val c = client ?: return CompletableFuture.failedFuture(BankNotSentException("not the bank connection leader", reason = NotSentReason.NOT_LEADER))
            return call(c)   // 전문은 send 가 돌아오기 전에 대기 표에 오른다
        } finally {
            inFlight.decrementAndGet()
        }
    }

    override fun close() {
        steppingDown = true
        stopLeading()
    }
}
