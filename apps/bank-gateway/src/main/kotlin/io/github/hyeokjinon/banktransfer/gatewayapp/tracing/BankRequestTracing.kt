package io.github.hyeokjinon.banktransfer.gatewayapp.tracing

import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gatewayapp.metrics.BankRequestType
import io.github.hyeokjinon.banktransfer.gatewayapp.metrics.GatewayMetrics
import io.github.hyeokjinon.banktransfer.protocol.BankMessage
import io.micrometer.tracing.Span
import io.micrometer.tracing.Tracer
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletionException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 은행 호출 구간 `bank.request` (P9b 스펙 §4-3, 결정 E8 · E9). 전문 규격에 추적 자리가 없어 모의 은행으로 넘기지 않는다 —
 * 게이트웨이의 은행 호출 한 점(BankConnectionOwner.withClient)에서 열고 future 가 끝날 때 닫는다.
 * - 부모 = 지금 구간(서버 요청). 닫는 쪽은 은행 Netty 이벤트 루프의 완료 콜백 — 구간 객체를 직접 들고 닫는다 (스레드 지역 문맥에 기대지 않음, 위험 C4).
 * - Observation 이 아니라 Tracer 로 연다: 관측이면 P9a 의 Timer(banktransfer.gateway.bank.request)와 같은 이름의 지표가 다른 태그로 하나 더 생긴다.
 * - 결과 분류는 지표와 같은 [GatewayMetrics.outcomeOf]. 속성에 계좌 · 금액 · 예금주를 넣지 않는다 — 거래번호(reference)는 결과조회 키라 넣는다.
 * - E10: 추적 호출이 던져도 은행 호출 · 그 future 의 결과는 그대로다. 열기가 실패하면 [Span.NOOP] 으로 추적 없이 계속하고, 경고는 한 번만 (이후 DEBUG).
 */
class BankRequestTracing(private val tracer: Tracer) {

    fun start(type: BankRequestType, reference: String?): Span =
        runCatching {
            val span = tracer.nextSpan().name(SPAN_NAME).tag(TYPE, type.tag)
            if (reference != null) span.tag(REFERENCE, reference)
            span.start()
        }.onFailure { warnOnce("start", it) }.getOrDefault(Span.NOOP)

    fun end(span: Span, value: Any?, error: Throwable?) {
        runCatching {
            span.tag(OUTCOME, GatewayMetrics.outcomeOf(value, error).tag)
            (value as? BankMessage)?.let { span.tag(TRAN_NO, it.header.tranNo.toString()) }
            when (val cause = unwrap(error)) {
                null -> {}
                is BankNotSentException -> span.tag(NOT_SENT_REASON, cause.reason.tag)
                else -> span.tag(ERROR_TYPE, cause.javaClass.simpleName)   // 메시지는 남기지 않는다 (E9)
            }
        }.onFailure { warnOnce("tag", it) }
        runCatching { span.end() }.onFailure { warnOnce("end", it) }   // 속성이 실패해도 구간은 닫는다
    }

    private fun unwrap(e: Throwable?): Throwable? = if (e is CompletionException && e.cause != null) e.cause else e

    private fun warnOnce(what: String, e: Throwable) {
        if (warned.compareAndSet(false, true)) log.warn("bank.request tracing $what failed — 추적 없이 계속한다 (이후 실패는 DEBUG)", e)
        else log.debug("bank.request tracing {} failed", what, e)
    }

    private val warned = AtomicBoolean(false)

    companion object {
        const val SPAN_NAME = "bank.request"
        const val TYPE = "bank.request.type"
        const val OUTCOME = "bank.outcome"
        const val TRAN_NO = "bank.tran_no"
        const val NOT_SENT_REASON = "bank.not_sent.reason"
        const val REFERENCE = "transfer.reference"
        const val ERROR_TYPE = "error.type"
        private val log = LoggerFactory.getLogger(BankRequestTracing::class.java)
        val NOOP = BankRequestTracing(Tracer.NOOP)
    }
}
