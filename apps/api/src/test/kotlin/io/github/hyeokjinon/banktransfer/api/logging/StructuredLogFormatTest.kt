package io.github.hyeokjinon.banktransfer.api.logging

import java.nio.file.Files
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * kind 의 구조화 로그(ECS) 계약 (P9c 스펙 §8, 결정 F4). Alloy(infra/k8s/monitoring/alloy/pipeline.alloy)가 이 키를 읽는다:
 * log.level → level, traceId → trace_id. Boot 를 올렸을 때 키가 바뀌면 이 테스트가 먼저 깨진다 — 그때 pipeline.alloy 도 같이 고친다.
 */
class StructuredLogFormatTest {

    @Test
    fun `ECS - 한 줄 JSON, log 객체의 level · logger, MDC traceId · spanId 는 맨 위 키, 서비스 이름은 spring application name`() {
        val info = runProbe("--logging.structured.format.console=ecs").single { "probe line in span" in it }

        assertTrue(info.startsWith("{") && info.endsWith("}"), info)
        assertTrue("\"log\":{\"level\":\"INFO\",\"logger\":\"io.github.hyeokjinon.banktransfer.api.logging.Probe\"}" in info, info)
        assertTrue("\"traceId\":\"$PROBE_TRACE_ID\"" in info, info)
        assertTrue("\"spanId\":\"$PROBE_SPAN_ID\"" in info, info)
        assertTrue("\"service\":{\"name\":\"bank-transfer\"" in info, info)
    }

    @Test
    fun `ECS - 예외는 error 객체 하나 (type · message · stack_trace) 라 여러 줄로 깨지지 않는다`() {
        val lines = runProbe("--logging.structured.format.console=ecs")
        val error = lines.single { "probe error line" in it }

        assertTrue(error.startsWith("{") && error.endsWith("}"), error)
        assertTrue("\"log\":{\"level\":\"ERROR\"" in error, error)
        assertTrue("\"error\":{\"type\":\"java.lang.IllegalStateException\",\"message\":\"probe failure\",\"stack_trace\":\"" in error, error)
        assertFalse(lines.any { it.trimStart().startsWith("at ") }, "스택이 따로 줄로 나왔다")
    }

    @Test
    fun `설정이 없으면 (로컬 · IntelliJ · Compose) 텍스트 그대로`() {
        val info = runProbe().single { "probe line in span" in it }

        assertFalse(info.trimStart().startsWith("{"), info)
        assertTrue(" INFO " in info, info)
    }

    /** 테스트 JVM 의 클래스패스로 EcsLogProbe 를 따로 띄운다. Windows 명령줄 길이 때문에 인자 파일로 (GatewayProcess 와 같은 방식). */
    private fun runProbe(vararg args: String): List<String> {
        val argFile = Files.createTempFile("ecs-probe-cp-", ".args")
        try {
            Files.writeString(argFile, "-cp \"" + System.getProperty("java.class.path").replace('\\', '/') + "\"\n")
            val java = ProcessHandle.current().info().command().orElse("java")
            val process = ProcessBuilder(listOf(java, "@$argFile", "-Xmx128m", "-Dstdout.encoding=UTF-8", PROBE_MAIN, *args))
                .redirectErrorStream(true)
                .start()
            val out = process.inputStream.readAllBytes().toString(Charsets.UTF_8)
            assertTrue(process.waitFor(60, TimeUnit.SECONDS), "probe JVM 이 60초 안에 끝나지 않았다")
            assertEquals(0, process.exitValue(), out)
            return out.lines().map { it.trimEnd('\r') }
        } finally {
            Files.deleteIfExists(argFile)
        }
    }

    private companion object {
        const val PROBE_MAIN = "io.github.hyeokjinon.banktransfer.api.logging.EcsLogProbeKt"
    }
}
