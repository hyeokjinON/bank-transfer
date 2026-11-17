package io.github.hyeokjinon.banktransfer.gatewayapp.metrics

import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.NotSentReason
import io.github.hyeokjinon.banktransfer.protocol.BankMessage
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.util.concurrent.CompletionException
import java.util.concurrent.TimeUnit

enum class BankRequestType(val tag: String) { TRANSFER("transfer"), INQUIRY("inquiry"), NAME("name") }

enum class BankCallOutcome(val tag: String) { OK("ok"), REJECTED("rejected"), NOT_SENT("not_sent"), UNKNOWN("unknown") }

enum class TranNoReserveResult(val tag: String) { OK("ok"), CONFLICT("conflict"), ERROR("error") }

/**
 * 게이트웨이 사건 지표 (P9a 스펙 §4-3). 은행 호출은 BankConnectionOwner.withClient 한 곳에서 센다 — 엔드포인트 세 개 · 두 서버 판이 모두 그곳을 지난다.
 * 라벨은 종류 · 결과 · 사유뿐 (계좌 · 거래번호 · 금액 없음, 결정 D7). 모든 태그 조합을 처음에 0 으로 만든다 (Prometheus increase() 가 첫 증가를 놓치지 않게).
 */
class GatewayMetrics(registry: MeterRegistry) {

    private val requests: Map<Pair<BankRequestType, BankCallOutcome>, Timer> =
        BankRequestType.entries.flatMap { t -> BankCallOutcome.entries.map { o -> t to o } }.associateWith { (t, o) ->
            Timer.builder(BANK_REQUEST).description("은행 호출 시간 (게이트웨이 → 은행 → 응답)")
                .tag("type", t.tag).tag("outcome", o.tag)
                .serviceLevelObjectives(*BANK_CALL_BUCKETS)
                .register(registry)
        }

    private val notSent: Map<NotSentReason, Counter> = NotSentReason.entries.associateWith {
        Counter.builder(NOT_SENT).description("은행에 보내지 않은 요청 (사유별)").tag("reason", it.tag).register(registry)
    }

    private val terms: Counter = Counter.builder(LEADER_TERMS).description("리더 임기 시작 = 은행 연결 주인이 된 횟수").register(registry)

    private val reserves: Map<TranNoReserveResult, Counter> = TranNoReserveResult.entries.associateWith {
        Counter.builder(TRAN_NO_RESERVE).description("전문번호 구간 예약 결과 (conflict = 409 로 다시 읽음)").tag("result", it.tag).register(registry)
    }

    fun recordBankCall(type: BankRequestType, elapsedNanos: Long, value: Any?, error: Throwable?) {
        val outcome = outcomeOf(value, error)
        requests.getValue(type to outcome).record(elapsedNanos, TimeUnit.NANOSECONDS)
        if (outcome == BankCallOutcome.NOT_SENT) notSent.getValue((unwrap(error) as BankNotSentException).reason).increment()
    }

    fun leaderTermStarted() = terms.increment()

    fun tranNoReserved(result: TranNoReserveResult) = reserves.getValue(result).increment()

    companion object {
        const val BANK_REQUEST = "banktransfer.gateway.bank.request"
        const val NOT_SENT = "banktransfer.gateway.not_sent"
        const val LEADER = "banktransfer.gateway.leader"
        const val BANK_CONNECTED = "banktransfer.gateway.bank.connected"
        const val LEADER_TERMS = "banktransfer.gateway.leader.terms"
        const val TRAN_NO_REMAINING = "banktransfer.gateway.tran_no.remaining"
        const val TRAN_NO_RESERVE = "banktransfer.gateway.tran_no.reserve"
        const val BANK_MAINTENANCE = "banktransfer.bank.maintenance"

        /** 은행 응답 시간 초과 3초 · 호출자 HTTP 5초 근처를 가르는 버킷만 (히스토그램 시계열을 줄인다 — 위험 B2). */
        private val BANK_CALL_BUCKETS: Array<Duration> =
            longArrayOf(10, 25, 50, 100, 250, 500, 1_000, 3_000, 5_000).map { Duration.ofMillis(it) }.toTypedArray()

        /**
         * 결과 분류 — P8-1 실패 분류와 같은 기준. 응답이 왔으면 0000 = ok, 그 밖 = rejected (은행의 정상 응답),
         * BankNotSentException = not_sent (은행에 안 갔다), 나머지 예외 = unknown (갔을 수 있다 — 시간 초과 · 보낸 뒤 끊김 · 쓰기 실패).
         */
        fun outcomeOf(value: Any?, error: Throwable?): BankCallOutcome = when {
            error != null -> if (unwrap(error) is BankNotSentException) BankCallOutcome.NOT_SENT else BankCallOutcome.UNKNOWN
            (value as? BankMessage)?.header?.isOk == true -> BankCallOutcome.OK
            value is BankMessage -> BankCallOutcome.REJECTED
            else -> BankCallOutcome.UNKNOWN
        }

        private fun unwrap(e: Throwable?): Throwable? = if (e is CompletionException && e.cause != null) e.cause else e

        /** 지표 없이 조립할 때 (테스트 · 기본값). */
        fun noop(): GatewayMetrics = GatewayMetrics(SimpleMeterRegistry())
    }
}
