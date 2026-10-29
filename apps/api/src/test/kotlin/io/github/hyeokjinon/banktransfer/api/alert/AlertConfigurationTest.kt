package io.github.hyeokjinon.banktransfer.api.alert

import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertIs

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
}
