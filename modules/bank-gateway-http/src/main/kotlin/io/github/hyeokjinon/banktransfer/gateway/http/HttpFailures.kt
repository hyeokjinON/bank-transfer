package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import java.net.ConnectException
import java.net.UnknownHostException
import java.net.http.HttpConnectTimeoutException
import java.time.Duration

/**
 * HTTP 구간의 실패를 은행 예외 셋으로 옮기는 곳 하나 (P8-1, 스펙 §4-3). 두 클라이언트 판(WebClient · JDK)이 같이 쓴다 —
 * 규칙이 판마다 갈라지면 한쪽에서 "모름" 을 "안 보냄" 으로 바꿔 이중 출금이 날 수 있다.
 *
 * 원칙: 은행에 확실히 가지 않았다고 증명할 수 있을 때만 [BankNotSentException].
 * - 게이트웨이가 503 + BANK_NOT_SENT 라고 말했다 (게이트웨이가 은행에 쓰기 전에 멈췄다)
 * - 게이트웨이에 연결하지 못했다 (거부 · 연결 시간 초과 · 이름 풀이 실패 · 연결 풀에서 연결을 얻지 못함) — 요청을 쓰기 전이다
 * 나머지(응답 시간 초과, 응답 전 끊김, 5xx, 읽을 수 없는 본문, 모르는 예외)는 요청이 갔을 수 있으므로 모름.
 */
object HttpFailures {

    fun fromTransport(error: Throwable): RuntimeException =
        if (causes(error).any(::isBeforeRequest)) {
            BankNotSentException("gateway not reached: ${rootMessage(error)}")
        } else {
            BankUnavailableException("gateway call failed, the request may have reached the bank: ${rootMessage(error)}", error)
        }

    fun fromStatus(status: Int, body: String): RuntimeException {
        val error = GatewayJson.errorOrNull(body)
        return when {
            status == 503 && error?.code == GatewayErrorCodes.BANK_NOT_SENT ->
                BankNotSentException("gateway: ${error.message}")
            status == 504 && error?.code == GatewayErrorCodes.BANK_TIMEOUT ->
                BankTimeoutException(error.tranNo ?: 0, Duration.ofMillis(error.timeoutMs ?: 0))
            else -> BankUnavailableException("gateway status=$status code=${error?.code}")
        }
    }

    fun <T> parse(status: Int, body: String, ok: (String) -> T): T {
        if (status != 200) throw fromStatus(status, body)
        return try {
            ok(body)
        } catch (e: Exception) {
            throw BankUnavailableException("unreadable gateway response", e)
        }
    }

    /**
     * 요청을 쓰기 전 단계의 실패인가. 셰이드된 Reactor Netty 예외(reactor.netty.internal.shaded...PoolAcquireTimeoutException)는
     * 패키지가 버전마다 바뀔 수 있어 클래스 단순 이름으로 본다. Netty ConnectTimeoutException 은 ConnectException 의 하위다.
     */
    private fun isBeforeRequest(t: Throwable): Boolean =
        t is ConnectException || t is HttpConnectTimeoutException || t is UnknownHostException ||
            t.javaClass.simpleName == "PoolAcquireTimeoutException"

    private fun causes(t: Throwable): Sequence<Throwable> = generateSequence(t) { it.cause?.takeIf { c -> c !== it } }

    private fun rootMessage(t: Throwable): String = causes(t).last().let { "${it.javaClass.simpleName}: ${it.message}" }
}
