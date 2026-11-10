package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import java.io.IOException
import java.net.ConnectException
import java.net.UnknownHostException
import java.net.http.HttpConnectTimeoutException
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.concurrent.CompletionException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

/** 스펙 §4-3 표를 한 줄씩. "안 보냄" 은 은행에 확실히 안 간 경우뿐이다. */
class HttpFailuresTest {

    private fun err(code: String, tranNo: Long? = null, timeoutMs: Long? = null) =
        GatewayJson.write(GatewayError(code, "m", tranNo, timeoutMs))

    @Test
    fun `503 + BANK_NOT_SENT 만 안 보냄`() {
        assertIs<BankNotSentException>(HttpFailures.fromStatus(503, err(GatewayErrorCodes.BANK_NOT_SENT)))
    }

    @Test
    fun `코드 없는 503 은 모름 - 앞단 프록시가 만든 503 일 수 있다`() {
        val e = HttpFailures.fromStatus(503, "<html>Service Unavailable</html>")
        assertIs<BankUnavailableException>(e)
        assert(e !is BankNotSentException)
    }

    @Test
    fun `504 + BANK_TIMEOUT 은 BankTimeoutException, 전문번호 · 시간을 옮긴다`() {
        val e = assertIs<BankTimeoutException>(HttpFailures.fromStatus(504, err(GatewayErrorCodes.BANK_TIMEOUT, tranNo = 42, timeoutMs = 3000)))
        assertEquals(42, e.tranNo)
        assertEquals(Duration.ofSeconds(3), e.timeout)
    }

    @Test
    fun `502 · 500 · 400 은 모름`() {
        for (status in listOf(502, 500, 400)) {
            val e = HttpFailures.fromStatus(status, err(GatewayErrorCodes.BANK_UNAVAILABLE))
            assertIs<BankUnavailableException>(e)
            assert(e !is BankNotSentException) { "status $status" }
        }
    }

    @Test
    fun `연결 단계 실패는 안 보냄 - 거부 · 연결 시간 초과 · 이름 풀이 · 연결 풀 대기`() {
        assertIs<BankNotSentException>(HttpFailures.fromTransport(ConnectException("refused")))
        assertIs<BankNotSentException>(HttpFailures.fromTransport(CompletionException(RuntimeException("wrap", ConnectException("refused")))))
        assertIs<BankNotSentException>(HttpFailures.fromTransport(HttpConnectTimeoutException("connect timed out")))
        assertIs<BankNotSentException>(HttpFailures.fromTransport(UnknownHostException("gw")))
        assertIs<BankNotSentException>(HttpFailures.fromTransport(PoolAcquireTimeoutException()))
    }

    @Test
    fun `응답 시간 초과 · 응답 전 끊김 · 그 밖은 모름`() {
        for (e in listOf(HttpTimeoutException("request timed out"), IOException("closed"), IllegalStateException("x"))) {
            val r = HttpFailures.fromTransport(e)
            assertIs<BankUnavailableException>(r)
            assert(r !is BankNotSentException) { e.toString() }
        }
    }

    @Test
    fun `200 이라도 본문을 읽지 못하면 모름`() {
        val e = runCatching { HttpFailures.parse(200, "{broken") { GatewayJson.read(it, TransferResult::class.java) } }.exceptionOrNull()
        assertIs<BankUnavailableException>(e)
    }

    /** Reactor Netty 의 셰이드된 예외와 이름만 같은 가짜 — 분류는 클래스 이름으로 본다 (셰이드 패키지에 기대지 않게). */
    private class PoolAcquireTimeoutException : RuntimeException("pool")
}
