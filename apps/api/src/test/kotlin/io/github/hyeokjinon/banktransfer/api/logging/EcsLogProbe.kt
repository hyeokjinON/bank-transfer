package io.github.hyeokjinon.banktransfer.api.logging

import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.boot.Banner
import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType

/**
 * 별도 JVM 에서 Spring Boot 로깅만 켜고 줄 두 개를 찍는다 (StructuredLogFormatTest 가 부른다).
 * 같은 JVM 에서 하면 로그 설정(JVM 전역)이 바뀌어 공유 테스트 컨텍스트의 콘솔까지 JSON 이 된다.
 * MDC 키 traceId · spanId 는 Micrometer Tracing 이 구간 안에서 넣는 이름이다 (P9b — Boot 상관 패턴도 같은 키를 읽는다).
 */
class EcsLogProbe

const val PROBE_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
const val PROBE_SPAN_ID = "00f067aa0ba902b7"

fun main(args: Array<String>) {
    val ctx = SpringApplication(EcsLogProbe::class.java).apply {
        setWebApplicationType(WebApplicationType.NONE)
        setBannerMode(Banner.Mode.OFF)
    }.run(*args)
    val log = LoggerFactory.getLogger("io.github.hyeokjinon.banktransfer.api.logging.Probe")
    MDC.put("traceId", PROBE_TRACE_ID)
    MDC.put("spanId", PROBE_SPAN_ID)
    log.info("probe line in span")
    log.error("probe error line", IllegalStateException("probe failure"))
    MDC.clear()
    ctx.close()
}
