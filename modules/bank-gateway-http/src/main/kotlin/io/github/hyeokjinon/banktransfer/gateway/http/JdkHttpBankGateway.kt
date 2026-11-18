package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationRegistry
import io.micrometer.observation.transport.Kind
import io.micrometer.observation.transport.RequestReplySenderContext
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CompletableFuture

/**
 * JDK java.net.http.HttpClient 판 — 비교용 (P8-1, 스펙 §5-2).
 * sendAsync 가 CompletableFuture 를 그대로 돌려준다. 응답은 셀렉터 스레드가 읽고, future 완료 · 뒤처리는 기본 실행기
 * (늘어나는 캐시 스레드 풀 — 이것 자체가 비교 대상이라 바꾸지 않는다)에서 돈다.
 * 연결 풀 개수 상한이 없고 조절할 수 없다. POST 는 연결이 끊겨도 다시 보내지 않는다 (jdk.httpclient.enableAllMethodRetry 기본 false).
 * HTTP/2 를 먼저 시도하는 기본값 대신 HTTP/1.1 로 고정한다 — WebClient 판과 같은 조건으로 비교한다.
 * P9b: JDK HttpClient 는 자동 계측이 없다 — 보내는 쪽 관측([OBSERVATION])을 직접 연다. 시작할 때 추적 처리기가 setter 로 traceparent 를 요청 builder 에 넣으므로
 * build 는 시작 뒤에 한다. 닫는 쪽은 완료 콜백(HttpClient 스레드) — 관측 객체를 직접 들고 닫는다.
 */
class JdkHttpBankGateway(
    private val settings: HttpGatewaySettings,
    private val observations: ObservationRegistry = ObservationRegistry.NOOP,
) : HttpBankGateway {

    private val http: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(settings.connectTimeout)
        .build()

    override fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> =
        post(GatewayPaths.NAME_INQUIRIES, NameInquiryBody(bankCode, accountNo)) { GatewayJson.read(it, NameInquiryResult::class.java).toResponse() }

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> =
        post(GatewayPaths.TRANSFERS, TransferBody(bankCode, accountNo, amount, memo, reference)) { GatewayJson.read(it, TransferResult::class.java).toResponse() }

    override fun inquire(reference: String): CompletableFuture<InquiryResponse> =
        post(GatewayPaths.INQUIRIES, InquiryBody(reference)) { GatewayJson.read(it, InquiryResult::class.java).toResponse() }

    private fun <T> post(path: String, body: Any, parse: (String) -> T): CompletableFuture<T> {
        val builder = HttpRequest.newBuilder(URI.create(settings.baseUrl + path))
            .timeout(settings.responseTimeout)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(GatewayJson.write(body)))
        val context = RequestReplySenderContext<HttpRequest.Builder, HttpResponse<String>>({ carrier, key, value -> carrier?.setHeader(key, value) }, Kind.CLIENT)   // CLIENT 를 꼭 준다 — 기본은 구간이 INTERNAL 로 나간다
        context.setCarrier(builder)
        context.remoteServiceName = REMOTE_SERVICE
        val observation = Observation.createNotStarted(OBSERVATION, { context }, observations)
            .contextualName("http post")
            .lowCardinalityKeyValue("uri", path)
            .lowCardinalityKeyValue("client", "jdk")
            .start()
        val sent = try {
            http.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofString())
        } catch (e: RuntimeException) {
            observation.error(e)
            observation.stop()
            throw e
        }
        return sent
            .whenComplete { response, error ->
                if (error != null) observation.error(error) else context.setResponse(response)
                observation.stop()
            }
            .handle { response, error ->
                if (error != null) throw HttpFailures.fromTransport(error)
                HttpFailures.parse(response.statusCode(), response.body(), parse)
            }
    }

    override fun close() {
        http.close()   // Java 21: HttpClient 는 AutoCloseable
    }

    companion object {
        /** JDK 판 클라이언트 관측 이름 (P9b). 관측이 지표도 만든다 — 이름 규칙(banktransfer.)을 따르고, WebClient 의 http.client.requests 와 태그 모양을 섞지 않는다. */
        const val OBSERVATION = "banktransfer.gateway.http.client"
        private const val REMOTE_SERVICE = "bank-gateway"
    }
}
