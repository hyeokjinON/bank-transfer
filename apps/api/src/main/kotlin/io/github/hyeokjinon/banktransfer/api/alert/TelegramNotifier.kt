package io.github.hyeokjinon.banktransfer.api.alert

import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.http.client.JdkClientHttpRequestFactory
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientResponseException
import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.time.Duration

/**
 * 텔레그램 Bot API 로 알림을 보낸다 (P5-1). `POST {baseUrl}/bot{token}/sendMessage`, 일반 텍스트(parse_mode 없음 — 이스케이프 실수로 메시지가 거절되지 않게).
 * 연결 3초 · 응답 5초. 실패(4xx · 5xx · 시간 초과 · 연결 거부)는 ERROR 로그만 남기고 삼킨다.
 * 토큰은 URL 경로에 있으므로 예외 메시지 · URL 을 로그에 남기지 않는다 — 예외 종류와 HTTP 상태만. 보내기 전에 [log] 에 먼저 남긴다.
 * 주소를 만들 수 없는 값이면 토큰 없는 [IllegalArgumentException] 으로 생성이 실패한다 (AlertConfiguration 이 로그 알림으로 물러난다).
 */
class TelegramNotifier(
    baseUrl: String,
    botToken: String,
    private val chatId: String,
    connectTimeout: Duration = Duration.ofSeconds(3),
    readTimeout: Duration = Duration.ofSeconds(5),
    private val log: AlertNotifier = LogNotifier(),
) : AlertNotifier {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val uri: URI = try {
        URI.create("${baseUrl.trimEnd('/')}/bot$botToken/sendMessage")
    } catch (_: IllegalArgumentException) {
        // URISyntaxException 류의 메시지에는 URL 전체(= 토큰)가 들어 있다 — 원인을 붙이지 않고 토큰 없는 예외로 바꾼다
        throw IllegalArgumentException("telegram url is not a valid URI (base url / bot token are not shown)")
    }
    private val http: RestClient = RestClient.builder()
        .requestFactory(
            JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(connectTimeout).build()).apply { setReadTimeout(readTimeout) },
        )
        .build()

    override fun notify(alert: Alert) {
        log.notify(alert)
        try {
            val body = JsonMapper.shared().writeValueAsString(mapOf("chat_id" to chatId, "text" to AlertText.render(alert)))
            http.post().uri(uri).contentType(MediaType.APPLICATION_JSON).body(body).retrieve().toBodilessEntity()
        } catch (e: RestClientResponseException) {
            logger.error("telegram alert '{}' was rejected: HTTP {}", alert.title, e.statusCode.value())
        } catch (e: Exception) {
            logger.error("telegram alert '{}' failed: {}", alert.title, e.javaClass.name)
        }
    }

    /** 토큰을 찍지 않는다. */
    override fun toString(): String = "TelegramNotifier(chatId=$chatId)"
}
