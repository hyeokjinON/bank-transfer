package io.github.hyeokjinon.banktransfer.api.alert

import ch.qos.logback.classic.Level
import com.sun.net.httpserver.HttpServer
import io.github.hyeokjinon.banktransfer.api.support.captureLogs
import org.junit.jupiter.api.assertDoesNotThrow
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import java.net.InetSocketAddress
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull

/** 텔레그램은 토큰 · chat id 가 둘 다 있을 때만 (P5-1). 없으면 로그 — 봇이 없는 PC · CI 에서도 그대로 돈다. */
class AlertConfigurationTest {

    private val runner = ApplicationContextRunner().withUserConfiguration(AlertConfiguration::class.java)

    @Test
    fun `설정이 없으면 LogNotifier`() {
        runner.run { ctx -> assertIs<LogNotifier>(ctx.getBean(AlertNotifier::class.java)) }
    }

    @Test
    fun `토큰만 있거나 chat id 만 있으면 LogNotifier`() {
        runner.withPropertyValues("alert.telegram.bot-token=123:TEST").run { ctx -> assertIs<LogNotifier>(ctx.getBean(AlertNotifier::class.java)) }
        runner.withPropertyValues("alert.telegram.chat-id=42").run { ctx -> assertIs<LogNotifier>(ctx.getBean(AlertNotifier::class.java)) }
    }

    @Test
    fun `둘 다 있으면 TelegramNotifier - 빈 · 설정의 toString 에 토큰이 없다`() {
        runner.withPropertyValues("alert.telegram.bot-token=123:TEST-SECRET", "alert.telegram.chat-id=42").run { ctx ->
            val notifier = ctx.getBean(AlertNotifier::class.java)
            assertIs<TelegramNotifier>(notifier)
            assertFalse("TEST-SECRET" in notifier.toString())
            assertFalse("TEST-SECRET" in ctx.getBean(TelegramProperties::class.java).toString())
        }
    }

    @Test
    fun `토큰 · chat id 끝의 줄바꿈 · 공백은 다듬는다 - 기동이 깨지지 않고 요청 경로에도 섞이지 않는다`() {
        val paths = CopyOnWriteArrayList<String>()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { exchange ->
                paths += exchange.requestURI.rawPath
                exchange.requestBody.readAllBytes()
                exchange.sendResponseHeaders(200, -1)
                exchange.close()
            }
            start()
        }
        try {
            val props = TelegramProperties("123:TEST-SECRET\n", " 42 ", " http://127.0.0.1:${server.address.port}/\n")

            val notifier = assertDoesNotThrow { AlertConfiguration().alertNotifier(props) }

            assertIs<TelegramNotifier>(notifier)
            notifier.notify(Alert(AlertLevel.INFO, "다듬기"))
            assertEquals(listOf("/bot123:TEST-SECRET/sendMessage"), paths)
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `환경변수 끝에 줄바꿈이 붙어 들어와도 컨텍스트가 뜬다 - 텔레그램`() {
        runner.withPropertyValues("alert.telegram.bot-token=123:TEST-SECRET\n", "alert.telegram.chat-id=42\n").run { ctx ->
            assertNull(ctx.startupFailure)
            assertIs<TelegramNotifier>(ctx.getBean(AlertNotifier::class.java))
        }
    }

    @Test
    fun `다듬어도 주소가 안 되는 토큰이면 기동은 되고 LogNotifier 로 물러난다 - 예외 · 로그에 토큰이 없다`() {
        val props = TelegramProperties("123:TEST SECRET", "42")

        val notifier = assertDoesNotThrow { AlertConfiguration().alertNotifier(props) }
        assertIs<LogNotifier>(notifier)

        val logs = captureLogs(AlertConfiguration::class.java) { AlertConfiguration().alertNotifier(props) }
        val error = logs.single { it.level == Level.ERROR }
        assertFalse("TEST" in error.formattedMessage || "SECRET" in error.formattedMessage)
        assertFalse(error.throwableProxy != null)

        runner.withPropertyValues("alert.telegram.bot-token=123:TEST SECRET", "alert.telegram.chat-id=42").run { ctx ->
            assertNull(ctx.startupFailure)
            assertIs<LogNotifier>(ctx.getBean(AlertNotifier::class.java))
        }
    }
}
