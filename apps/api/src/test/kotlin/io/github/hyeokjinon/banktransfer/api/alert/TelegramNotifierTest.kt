package io.github.hyeokjinon.banktransfer.api.alert

import ch.qos.logback.classic.Level
import com.sun.net.httpserver.HttpServer
import io.github.hyeokjinon.banktransfer.api.support.captureLogs
import org.junit.jupiter.api.assertDoesNotThrow
import tools.jackson.databind.json.JsonMapper
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** 가짜 텔레그램 서버(JDK HttpServer)로 경로 · 본문 · 실패 처리를 본다 (P5-1). 진짜 텔레그램에는 닿지 않는다. */
class TelegramNotifierTest {

    private data class Received(val path: String, val body: String)

    private val received = CopyOnWriteArrayList<Received>()

    @Volatile private var status = 200
    @Volatile private var delay = Duration.ZERO

    private val server: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext("/") { exchange ->
            received += Received(exchange.requestURI.path, exchange.requestBody.readAllBytes().toString(Charsets.UTF_8))
            Thread.sleep(delay.toMillis())
            val reply = """{"ok":${status == 200}}""".toByteArray()
            exchange.sendResponseHeaders(status, reply.size.toLong())
            exchange.responseBody.use { it.write(reply) }
        }
        executor = Executors.newCachedThreadPool()
        start()
    }

    @AfterTest
    fun stop() = server.stop(0)

    private fun notifier(readTimeout: Duration = Duration.ofSeconds(5)) =
        TelegramNotifier("http://127.0.0.1:${server.address.port}", TOKEN, "42", readTimeout = readTimeout)

    @Test
    fun `POST bot{token}_sendMessage 에 chat_id 와 일반 텍스트 - parse_mode 는 보내지 않는다`() {
        notifier().notify(Alert(AlertLevel.INFO, "일마감 대사 완료", listOf("출금 3건")))

        val request = received.single()
        assertEquals("/bot$TOKEN/sendMessage", request.path)
        val json = JsonMapper.shared().readTree(request.body)
        assertEquals("42", json.get("chat_id").asString())
        assertEquals("[INFO] 일마감 대사 완료\n출금 3건", json.get("text").asString())
        assertNull(json.get("parse_mode"))
    }

    @Test
    fun `보내는 본문도 계좌번호를 가리고 목록은 20건 + 외 n건`() {
        notifier().notify(Alert(AlertLevel.CRITICAL, "불일치", items = (1..25).map { "090-1002123456789 #$it" }))

        val text = JsonMapper.shared().readTree(received.single().body).get("text").asString()
        assertFalse("1002123456789" in text)
        assertTrue("090-1002****6789 #20" in text)
        assertFalse("#21" in text)
        assertTrue(text.endsWith("외 5건"))
    }

    @Test
    fun `보내는 본문은 4096자 이하로 자른다`() {
        notifier().notify(Alert(AlertLevel.INFO, "x".repeat(5_000)))

        val text = JsonMapper.shared().readTree(received.single().body).get("text").asString()
        assertEquals(AlertText.MAX_CHARS, text.length)
        assertTrue(text.endsWith("…"))
    }

    @Test
    fun `500 이어도 예외 없이 ERROR 로그만 - 로그에 토큰이 없다`() {
        status = 500

        val logs = captureLogs(TelegramNotifier::class.java) { assertDoesNotThrow { notifier().notify(Alert(AlertLevel.WARN, "경고")) } }

        val error = logs.single { it.level == Level.ERROR }
        assertTrue("HTTP 500" in error.formattedMessage)
        assertFalse(TOKEN in error.formattedMessage)
    }

    @Test
    fun `응답이 늦으면(시간 초과) 기다리지 않고 예외 없이 끝난다 - 로그에 토큰이 없다`() {
        delay = Duration.ofSeconds(2)
        val started = System.nanoTime()

        val logs = captureLogs(TelegramNotifier::class.java) {
            assertDoesNotThrow { notifier(readTimeout = Duration.ofMillis(200)).notify(Alert(AlertLevel.WARN, "느림")) }
        }

        assertTrue(Duration.ofNanos(System.nanoTime() - started) < Duration.ofMillis(1_500))
        val error = logs.single { it.level == Level.ERROR }
        assertFalse(TOKEN in error.formattedMessage)
        assertFalse(error.throwableProxy != null && TOKEN in error.throwableProxy.message.orEmpty())
    }

    @Test
    fun `연결이 안 돼도(아무도 듣지 않는 포트) 예외 없이 끝나고 ERROR 로그만 - 로그에 토큰이 없다`() {
        val closedPort = ServerSocket(0).use { it.localPort }

        val logs = captureLogs(TelegramNotifier::class.java) {
            assertDoesNotThrow { TelegramNotifier("http://127.0.0.1:$closedPort", TOKEN, "42").notify(Alert(AlertLevel.CRITICAL, "닿지 않음")) }
        }

        val error = logs.single { it.level == Level.ERROR }
        assertFalse(TOKEN in error.formattedMessage)
        assertFalse(error.throwableProxy != null && TOKEN in error.throwableProxy.message.orEmpty())
    }

    @Test
    fun `텔레그램을 켜도 로그에 먼저 남긴다 - 한 줄로`() {
        val logs = captureLogs("ALERT") { notifier().notify(Alert(AlertLevel.CRITICAL, "대사 미완료", listOf("이유: FILE_NOT_READY"))) }

        val log = logs.single()
        assertEquals(Level.ERROR, log.level)
        assertEquals("[CRITICAL] 대사 미완료 | 이유: FILE_NOT_READY", log.formattedMessage)
    }

    private companion object {
        /** 가짜 토큰 — 진짜 봇 토큰은 저장소 · 로그 · 테스트 출력 어디에도 두지 않는다. */
        const val TOKEN = "123456:TEST-ONLY-NOT-A-REAL-TOKEN"
    }
}
