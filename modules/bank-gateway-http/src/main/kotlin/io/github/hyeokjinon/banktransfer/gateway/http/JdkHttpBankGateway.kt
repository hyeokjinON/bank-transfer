package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
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
 */
class JdkHttpBankGateway(private val settings: HttpGatewaySettings) : HttpBankGateway {

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
        val request = HttpRequest.newBuilder(URI.create(settings.baseUrl + path))
            .timeout(settings.responseTimeout)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(GatewayJson.write(body)))
            .build()
        return http.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .handle { response, error ->
                if (error != null) throw HttpFailures.fromTransport(error)
                HttpFailures.parse(response.statusCode(), response.body(), parse)
            }
    }

    override fun close() {
        http.close()   // Java 21: HttpClient 는 AutoCloseable
    }
}
