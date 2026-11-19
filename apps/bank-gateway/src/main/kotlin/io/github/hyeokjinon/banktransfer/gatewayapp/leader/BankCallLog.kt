package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gatewayapp.metrics.BankRequestType
import io.github.hyeokjinon.banktransfer.gatewayapp.metrics.GatewayMetrics
import io.github.hyeokjinon.banktransfer.protocol.BankMessage
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletionException

/**
 * 은행 호출 한 번 = 로그 한 줄 (P9c 스펙 §1 기준 2). bank.request 구간 문맥에서 찍혀 traceId 로 찾히고,
 * reference(이용기관 거래번호 — 모의 은행 통장 줄의 '거래번호=')로 모의 은행 줄과 잇는다.
 * 계좌 · 금액 · 예금주 · 적요는 싣지 않는다 (P9b E9 와 같은 목록). 결과 · 사유는 지표 · 구간과 같은 값.
 * 모양: bank call type=<transfer|inquiry|name> outcome=<ok|rejected|not_sent|unknown> reason=<사유|-> tranNo=<전문번호|-> reference=<거래번호|-> ms=<걸린 시간>
 */
internal object BankCallLog {
    private val log = LoggerFactory.getLogger(BankCallLog::class.java)

    fun write(type: BankRequestType, reference: String?, value: Any?, error: Throwable?, elapsedNanos: Long) {
        val cause = if (error is CompletionException && error.cause != null) error.cause else error
        log.info(
            "bank call type={} outcome={} reason={} tranNo={} reference={} ms={}",
            type.tag,
            GatewayMetrics.outcomeOf(value, error).tag,
            (cause as? BankNotSentException)?.reason?.tag ?: "-",
            (value as? BankMessage)?.header?.tranNo ?: "-",
            reference ?: "-",
            elapsedNanos / 1_000_000,
        )
    }
}
