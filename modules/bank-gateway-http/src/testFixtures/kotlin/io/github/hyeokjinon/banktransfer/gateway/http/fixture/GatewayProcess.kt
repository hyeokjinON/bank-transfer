package io.github.hyeokjinon.banktransfer.gateway.http.fixture

import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * 게이트웨이 앱을 별도 JVM 으로 띄운다 (P8-1 흐름 테스트). 호출하는 앱(api)과 같은 JVM 에 올리면 그 앱의 자동 설정(JPA · Kafka)이
 * 게이트웨이 컨텍스트에도 걸리므로, Gradle 이 넘겨 준 게이트웨이 런타임 클래스패스(시스템 속성 [CLASSPATH_PROPERTY])로 java 를 실행한다.
 * 클래스패스는 인자 파일(@file)로 넘긴다 — Windows 명령행 길이 제한(32K)을 피한다.
 */
class GatewayProcess private constructor(private val process: Process, val port: Int, private val log: Path) : AutoCloseable {

    val baseUrl: String get() = "http://127.0.0.1:$port"

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build()

    /** /actuator/health/bankConnection 이 UP 인가 (게이트웨이가 아직 안 떴거나 꺼졌으면 false). */
    fun bankConnected(): Boolean = runCatching {
        val req = HttpRequest.newBuilder(URI.create("$baseUrl/actuator/health/bankConnection")).timeout(Duration.ofSeconds(2)).GET().build()
        http.send(req, HttpResponse.BodyHandlers.ofString()).body().contains("\"UP\"")
    }.getOrDefault(false)

    private fun awaitBankConnection(timeout: Duration) {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (!bankConnected()) {
            check(process.isAlive) { "gateway process exited — log: $log\n" + Files.readString(log).takeLast(4000) }
            check(System.nanoTime() < deadline) { "gateway not connected to bank within $timeout — log: $log" }
            Thread.sleep(100)
        }
    }

    /** 테스트 JVM 이 강제 종료돼도 자식 게이트웨이 JVM 이 남지 않게 한다. */
    private val shutdownHook = Thread { process.destroyForcibly() }.also { Runtime.getRuntime().addShutdownHook(it) }

    override fun close() {
        try { Runtime.getRuntime().removeShutdownHook(shutdownHook) } catch (_: IllegalStateException) { /* 종료 중 */ }
        process.destroy()
        if (!process.waitFor(10, TimeUnit.SECONDS)) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS)
    }

    companion object {
        const val CLASSPATH_PROPERTY = "bank-gateway.classpath"
        const val MAIN_CLASS = "io.github.hyeokjinon.banktransfer.gatewayapp.BankGatewayApplicationKt"

        /** [bankPort] 의 모의 은행에 붙는 게이트웨이를 빈 포트에 띄우고, 은행 연결이 UP 이 될 때까지 기다린다. */
        fun start(bankPort: Int, vararg args: String): GatewayProcess {
            val classpath = System.getProperty(CLASSPATH_PROPERTY)
                ?: error("system property $CLASSPATH_PROPERTY is missing — run through Gradle (build.gradle.kts bankGatewayRuntime)")
            val port = ServerSocket(0).use { it.localPort }
            val argFile = Files.createTempFile("bank-gateway-cp-", ".args")
            Files.writeString(argFile, "-cp \"" + classpath.replace('\\', '/') + "\"\n")
            val log = Files.createTempFile("bank-gateway-", ".log")
            val java = ProcessHandle.current().info().command().orElse("java")
            val command = listOf(
                java, "@$argFile", "-Xmx256m", "-Dstdout.encoding=UTF-8", MAIN_CLASS,
                "--server.port=$port", "--bank.gateway.host=127.0.0.1", "--bank.gateway.port=$bankPort",
                "--bank.maintenance.enabled=false", *args,
            )
            val process = try {
                ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start()
            } catch (e: Throwable) {
                runCatching { Files.deleteIfExists(argFile) }
                throw e
            }
            val gateway = GatewayProcess(process, port, log)
            try {
                gateway.awaitBankConnection(Duration.ofSeconds(60))
            } catch (e: Throwable) {
                gateway.close()   // 은행 연결을 못 기다린 경우에도 자식 JVM 을 남기지 않는다
                throw e
            } finally {
                // 인자 파일은 JVM 이 뜬 뒤(연결 확인 뒤)에야 지운다 — 시작 직후에 지우면 java 가 읽기 전일 수 있다. 로그는 오류 메시지가 가리키므로 남긴다
                runCatching { Files.deleteIfExists(argFile) }
            }
            return gateway
        }
    }
}
