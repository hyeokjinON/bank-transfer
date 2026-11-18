package io.github.hyeokjinon.banktransfer.api.support

import com.google.protobuf.ByteString
import com.sun.net.httpserver.HttpServer
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest
import java.net.InetSocketAddress
import java.util.concurrent.CopyOnWriteArrayList

/**
 * 별도 JVM 게이트웨이가 OTLP HTTP 로 보내는 구간을 받는다 (P9b — 진짜 Tempo 대신). 본문 = protobuf ExportTraceServiceRequest.
 * 게이트웨이에 `--management.opentelemetry.tracing.export.otlp.endpoint=<endpoint>` 를 주면 운영과 같은 보내기 경로(지우개 포함)를 지난다.
 */
class OtlpReceiver : AutoCloseable {
    data class ReceivedSpan(
        val service: String,
        val traceId: String,
        val spanId: String,
        val parentSpanId: String,
        val name: String,
        val attributes: Map<String, String>,
    )

    private val errors = CopyOnWriteArrayList<Exception>()

    private val received = CopyOnWriteArrayList<ReceivedSpan>()

    private val server: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext("/v1/traces") { ex ->
            val request = try {
                ex.requestBody.use { ExportTraceServiceRequest.parseFrom(it) }
            } catch (e: Exception) {
                errors += e   // 조용히 구간이 빠지지 않게: 400 으로 답하고 기록 — 테스트가 errors() 로 본다
                ex.sendResponseHeaders(400, -1)
                ex.close()
                return@createContext
            }
            request.resourceSpansList.forEach { rs ->
                val service = rs.resource.attributesList.firstOrNull { it.key == "service.name" }?.value?.stringValue.orEmpty()
                rs.scopeSpansList.flatMap { it.spansList }.forEach { s ->
                    received += ReceivedSpan(
                        service, s.traceId.hex(), s.spanId.hex(), s.parentSpanId.hex(), s.name,
                        s.attributesList.associate { it.key to it.value.stringValue },
                    )
                }
            }
            ex.responseHeaders.add("Content-Type", "application/x-protobuf")
            ex.sendResponseHeaders(200, -1)   // 빈 ExportTraceServiceResponse = 전부 받음
            ex.close()
        }
        start()
    }

    val endpoint: String get() = "http://127.0.0.1:${server.address.port}/v1/traces"

    /** 받은 구간. protobuf 를 읽지 못한 요청이 있었으면 구간이 조용히 빠지지 않고 여기서 던진다. */
    fun spans(): List<ReceivedSpan> {
        errors.firstOrNull()?.let { throw AssertionError("OTLP 본문을 읽지 못했다 (${errors.size}건)", it) }
        return received.toList()
    }

    override fun close() = server.stop(0)

    private fun ByteString.hex(): String = toByteArray().joinToString("") { "%02x".format(it) }
}
