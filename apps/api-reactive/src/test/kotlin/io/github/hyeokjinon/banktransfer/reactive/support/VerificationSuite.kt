package io.github.hyeokjinon.banktransfer.reactive.support

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.account.infra.contract.VerificationErrorContract
import io.github.hyeokjinon.banktransfer.reactive.web.ReactiveExceptionHandler
import io.github.hyeokjinon.banktransfer.reactive.web.USER_ID_HEADER
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.slf4j.LoggerFactory
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.web.method.annotation.ExceptionHandlerMethodResolver
import kotlin.test.Test
import java.net.Socket
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** WebFlux 두 판이 같이 지켜야 하는 것 (P6-2). 판마다 이 클래스를 상속한 테스트 클래스가 하나씩 있다 (CoroutineModeTest · ReactorModeTest). */
abstract class VerificationSuite : ReactiveIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    @Test
    fun `오류 계약 - HTTP 로 재현되는 경우 상태와 code 가 apps-api 와 같은 표를 따른다`() {
        for (case in VerificationErrorContract.httpCases) {
            val (status, body) = client.post().uri(VerificationErrorContract.PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .headers { h -> case.userId?.let { h.set(USER_ID_HEADER, it) } }
                .body(case.body)
                .exchange { _, res -> res.statusCode.value() to (res.bodyTo(mapType) ?: emptyMap()) }
            assertEquals(case.status, status, "${case.name}: $body")
            if (case.code == null) {
                assertEquals(VerificationErrorContract.SUCCESS_FIELDS, body.keys, "${case.name}: 성공 응답 키 (인증코드 없음)")
                assertEquals("SENT", body["status"])
            } else {
                assertEquals(case.code, body["code"], "${case.name}: $body")
            }
        }
    }

    @Test
    fun `오류 계약 - 예외에서 상태 · code · Retry-After 로`() {
        val handler = ReactiveExceptionHandler()
        // 직접 호출만으로는 실제 요청이 이 메서드로 오는지 모른다 — 프레임워크와 같은 해석기로 라우팅을 확인한다 (예상 못 한 예외가 Boot 기본 500 으로 새던 구멍)
        val resolver = ExceptionHandlerMethodResolver(ReactiveExceptionHandler::class.java)
        for (case in VerificationErrorContract.exceptionCases) {
            assertEquals("handle", resolver.resolveMethod(case.exception)?.name, case.exception.toString())
            val res = handler.handle(case.exception)
            assertEquals(case.status, res.statusCode.value(), case.exception.toString())
            assertEquals(case.code, res.body!!.properties?.get("code"), case.exception.toString())
            assertEquals(case.retryAfter, res.headers.getFirst(HttpHeaders.RETRY_AFTER), case.exception.toString())
            if (case.status == 500) assertEquals("internal error", res.body!!.detail)
        }
    }

    /** 이 판의 흐름 클래스 로거 이름 (끊김 경고를 거기서 찾는다). */
    protected abstract val flowLoggerName: String

    /** 요청을 보내고 [until] 이 참이 될 때까지 응답을 읽지 않고 기다렸다가 연결을 끊는다. */
    private fun postAndDisconnect(userId: String, accountNo: String, holderName: String, until: () -> Boolean) {
        val bytes = """{"bankCode":"090","accountNo":"$accountNo","holderName":"$holderName"}""".toByteArray()
        Socket("localhost", port).use { s ->
            s.getOutputStream().write(
                ("POST /api/v1/account-verifications HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n" +
                    "$USER_ID_HEADER: $userId\r\nContent-Length: ${bytes.size}\r\n\r\n").toByteArray() + bytes,
            )
            s.getOutputStream().flush()
            val deadline = System.currentTimeMillis() + 5_000
            while (!until()) { check(System.currentTimeMillis() < deadline) { "기다리던 상태가 오지 않았다" }; Thread.sleep(5) }
        }
    }

    /**
     * 취소를 오류로 바꾸거나(코루틴) 안쪽 Mono 의 doOnCancel 을 잃으면(Reactor) 경고 개수가 어긋나 실패하여 잡을 것으로 본다
     * (추정 — 변이로 확인하지 않음).
     */
    @Test
    fun `끊김 - 1원 이체 응답을 기다리는 중 끊으면 행이 없고, 마스킹한 경고가 남고, 대기 표가 비워진다`() {
        val logger = LoggerFactory.getLogger(flowLoggerName) as Logger
        val appender = ListAppender<ILoggingEvent>().also { it.start() }
        logger.addAppender(appender)
        try {
            val user = newUserId()
            val before = bank.depositsOf("1002123456789").size
            bank.responseDelay = Duration.ofMillis(300)

            // 모의 은행은 이체를 받자마자 입금을 기록하고 응답만 늦게 보낸다 → 입금이 늘었다 = 1원 이체 응답을 기다리는 중
            postAndDisconnect(user, "1002123456789", "홍길동") { bank.depositsOf("1002123456789").size > before }

            await.atMost(Duration.ofSeconds(5)) untilAsserted {
                val warns = appender.list.filter { it.level == Level.WARN }
                assertEquals(1, warns.size, warns.toString())
                val line = warns.single().formattedMessage
                assertTrue("stage=transfer" in line && "****6789" in line, line)
                assertTrue("1002123456789" !in line, "계좌 원문을 남기지 않는다: $line")
            }
            Thread.sleep(500)   // 은행 응답(300ms)이 지난 뒤에도
            assertEquals(0, rowsOf(user), "끊긴 요청은 저장하지 않는다 (1원은 나갔다 — 설계 §4-3 의 대가)")
            assertEquals(before + 1, bank.depositsOf("1002123456789").size)
            await.atMost(Duration.ofSeconds(5)) untilAsserted { assertEquals(0, (gateway as BankGatewayClient).pendingCount) }
        } finally {
            logger.detachAppender(appender)
        }
    }

    @Test
    fun `끊김 - 성명조회를 기다리는 중 끊으면 1원을 보내지 않고 행도 없다`() {
        val user = newUserId()
        val before = bank.depositsOf("1002123456789").size
        bank.responseDelay = Duration.ofMillis(300)
        val started = System.currentTimeMillis()

        postAndDisconnect(user, "1002123456789", "홍길동") { System.currentTimeMillis() - started > 100 }   // 성명조회 응답(300ms) 전

        Thread.sleep(1_000)   // 성명조회 응답 · (보냈다면) 1원 이체가 끝났을 시간
        assertEquals(before, bank.depositsOf("1002123456789").size, "1원을 보내지 않는다")
        assertEquals(0, rowsOf(user))
    }
}
