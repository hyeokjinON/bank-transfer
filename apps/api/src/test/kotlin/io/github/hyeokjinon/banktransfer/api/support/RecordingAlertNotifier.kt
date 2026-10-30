package io.github.hyeokjinon.banktransfer.api.support

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.LogNotifier
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import java.time.LocalDate
import java.util.concurrent.CopyOnWriteArrayList

/**
 * 통합 테스트의 알림 (P5-1): 받은 알림을 모으고 로그에도 남긴다. 공유 컨텍스트라 다른 테스트의 알림도 섞이므로 [about] 처럼 걸러 본다.
 * [onNotify] 를 주면 알림 안에서 부른다 — 대사 잡을 ⑥ 보고에서 붙잡아 두는 동시 실행 테스트용 (테스트가 끝나면 null 로).
 */
class RecordingAlertNotifier : AlertNotifier {
    private val log = LogNotifier()
    val received: MutableList<Alert> = CopyOnWriteArrayList()

    @Volatile
    var onNotify: ((Alert) -> Unit)? = null

    override fun notify(alert: Alert) {
        log.notify(alert)
        received += alert
        onNotify?.invoke(alert)
    }

    /** 제목에 이 날짜가 든 알림 (대사 알림은 제목에 대사일을 쓴다). */
    fun about(date: LocalDate): List<Alert> = received.filter { date.toString() in it.title }
}

/** 앱의 AlertNotifier(로그)보다 먼저 주입된다. */
@TestConfiguration(proxyBeanMethods = false)
class AlertTestConfiguration {

    @Bean
    @Primary
    fun recordingAlertNotifier() = RecordingAlertNotifier()
}
