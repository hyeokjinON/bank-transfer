package io.github.hyeokjinon.banktransfer.reactive.support

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory

/**
 * [loggerClass] 의 로거가 [block] 동안 남기는 로그를 모아 돌려준다 (logback ListAppender).
 * apps/api 의 같은 파일을 복사 (P6-2 — 테스트 지원은 앱마다 둔다)
 */
fun captureLogs(loggerClass: Class<*>, block: () -> Unit): List<ILoggingEvent> = captureLogs(loggerClass.name, block)

/** 이름으로 정한 로거(예: 알림 로거 `ALERT`)용. */
fun captureLogs(loggerName: String, block: () -> Unit): List<ILoggingEvent> {
    val logger = LoggerFactory.getLogger(loggerName) as Logger
    val appender = ListAppender<ILoggingEvent>().also { it.start() }
    logger.addAppender(appender)
    try {
        block()
    } finally {
        logger.detachAppender(appender)
    }
    return appender.list
}
