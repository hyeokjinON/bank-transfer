package io.github.hyeokjinon.banktransfer.api.alert

import org.slf4j.LoggerFactory

/**
 * 텔레그램이 없을 때(개발 PC · CI)의 알림 = 로그 한 줄 (P5-1). 텔레그램을 켜도 먼저 여기에 남긴다 — 알림 채널이 죽어도 기록은 남게.
 * 로거 이름 `ALERT` 로 모아 grep 하기 쉽게, 줄바꿈은 ` | ` 로 바꿔 한 줄로 (로그 한 건이 여러 줄로 깨지지 않게).
 */
class LogNotifier : AlertNotifier {
    private val log = LoggerFactory.getLogger("ALERT")

    override fun notify(alert: Alert) {
        val text = AlertText.render(alert).replace("\n", " | ")
        when (alert.level) {
            AlertLevel.INFO -> log.info(text)
            AlertLevel.WARN -> log.warn(text)
            AlertLevel.CRITICAL -> log.error(text)
        }
    }
}
