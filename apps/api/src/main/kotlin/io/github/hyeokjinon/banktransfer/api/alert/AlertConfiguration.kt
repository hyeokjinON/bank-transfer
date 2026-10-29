package io.github.hyeokjinon.banktransfer.api.alert

import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * 텔레그램 알림 설정 (P5-1). 값은 환경변수(ALERT_TELEGRAM_BOT_TOKEN · ALERT_TELEGRAM_CHAT_ID)로만 준다 — 저장소 · 로그에 남기지 않는다 (SETUP 2-6).
 * toString 은 토큰을 가린다 (data class 기본 toString 은 모든 필드를 찍는다).
 */
@ConfigurationProperties("alert.telegram")
data class TelegramProperties(
    val botToken: String = "",
    val chatId: String = "",
    val baseUrl: String = "https://api.telegram.org",
) {
    val enabled: Boolean get() = botToken.isNotBlank() && chatId.isNotBlank()

    override fun toString(): String = "TelegramProperties(botToken=${if (botToken.isBlank()) "" else "****"}, chatId=$chatId, baseUrl=$baseUrl)"
}

/**
 * 알림 구현 하나를 고른다 (P5-1): 토큰 · chat id 가 둘 다 있으면 텔레그램(로그도 남긴다), 아니면 로그만.
 * (TelegramProperties 는 @ConfigurationPropertiesScan 도 등록하지만, 이 클래스만 떼어 쓰는 테스트를 위해 여기서도 켠다 — 같은 이름이라 한 번만 등록된다.)
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TelegramProperties::class)
class AlertConfiguration {
    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    fun alertNotifier(props: TelegramProperties): AlertNotifier =
        if (props.enabled) {
            log.info("alerts go to telegram chat {} (and the log)", props.chatId)
            TelegramNotifier(props.baseUrl, props.botToken, props.chatId)
        } else {
            log.info("alerts go to the log only (alert.telegram.bot-token / chat-id not set)")
            LogNotifier()
        }
}
