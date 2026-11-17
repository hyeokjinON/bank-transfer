package io.github.hyeokjinon.banktransfer.gatewayapp.metrics

import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import io.github.hyeokjinon.banktransfer.gateway.NotSentReason
import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.CompletionException
import kotlin.test.Test
import kotlin.test.assertEquals

/** 게이트웨이 사건 지표 (P9a 스펙 §4-3): 결과 분류는 P8-1 실패 분류와 같은 기준, 태그 조합은 처음부터 0. */
class GatewayMetricsTest {
    private val registry = SimpleMeterRegistry()
    private val metrics = GatewayMetrics(registry)

    private fun response(code: String) = TransferResponse(
        Header(MessageClass.RESPONSE, BizType.TRANSFER, 1, LocalDateTime.of(2026, 11, 17, 10, 0), code),
        "090", "1002123456789", 1_000, "m", "T261117METRICS00001",
    )

    @Test
    fun `결과 분류 - 0000 은 ok, 다른 응답코드는 rejected, 안 보냄은 not_sent, 그 밖 예외는 unknown`() {
        assertEquals(BankCallOutcome.OK, GatewayMetrics.outcomeOf(response("0000"), null))
        assertEquals(BankCallOutcome.REJECTED, GatewayMetrics.outcomeOf(response("1001"), null))
        assertEquals(BankCallOutcome.NOT_SENT, GatewayMetrics.outcomeOf(null, BankNotSentException("x", reason = NotSentReason.DISCONNECTED)))
        assertEquals(BankCallOutcome.NOT_SENT, GatewayMetrics.outcomeOf(null, CompletionException(BankNotSentException("x"))))
        assertEquals(BankCallOutcome.UNKNOWN, GatewayMetrics.outcomeOf(null, BankTimeoutException(1, Duration.ofSeconds(3))))
        assertEquals(BankCallOutcome.UNKNOWN, GatewayMetrics.outcomeOf(null, BankUnavailableException("connection to bank lost")))
        assertEquals(BankCallOutcome.UNKNOWN, GatewayMetrics.outcomeOf(null, IllegalStateException("duplicate tranNo 1")))
    }

    @Test
    fun `모든 태그 조합이 처음부터 0 으로 있다 - increase 가 첫 증가를 놓치지 않게`() {
        assertEquals(12, registry.find(GatewayMetrics.BANK_REQUEST).timers().size)
        assertEquals(NotSentReason.entries.size, registry.find(GatewayMetrics.NOT_SENT).counters().size)
        assertEquals(3, registry.find(GatewayMetrics.TRAN_NO_RESERVE).counters().size)
        assertEquals(0.0, registry.get(GatewayMetrics.NOT_SENT).tag("reason", "not_leader").counter().count())
        assertEquals(0.0, registry.get(GatewayMetrics.LEADER_TERMS).counter().count())
    }

    @Test
    fun `안 보냄은 호출 시간과 사유를 함께 센다`() {
        metrics.recordBankCall(BankRequestType.TRANSFER, 2_000_000, null, BankNotSentException("x", reason = NotSentReason.MAINTENANCE))

        assertEquals(1, registry.get(GatewayMetrics.BANK_REQUEST).tag("type", "transfer").tag("outcome", "not_sent").timer().count())
        assertEquals(1.0, registry.get(GatewayMetrics.NOT_SENT).tag("reason", "maintenance").counter().count())
    }
}
