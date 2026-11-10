package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import io.netty.channel.ChannelOption
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import java.time.Duration
import java.util.concurrent.CompletableFuture

/**
 * WebClient(Reactor Netty) 판 — 기본 (P8-1, 스펙 §5-2).
 * 응답은 Reactor Netty 이벤트 루프가 받고 future 를 완료한다. 그 뒤의 DB 작업은 호출자가 이 스레드에서 돌리면 안 된다
 * (api 의 BankGatewayAdapter 가 thenApplyAsync 로 넘기는 이유가 직결 판과 같다).
 *
 * 자동 재시도는 끈다(disableRetry): Reactor Netty 는 재사용하던 연결이 요청 중 끊기면 한 번 다시 보낼 수 있는데,
 * 요청이 정말 안 갔는지 우리가 판단할 수 없다 — 이체가 두 번 갈 여지를 없앤다. 재시도는 Kafka 재시도 토픽 · 정리 작업만 한다.
 */
class WebClientBankGateway(private val settings: HttpGatewaySettings) : HttpBankGateway {

    private val provider: ConnectionProvider = ConnectionProvider.builder("bank-gateway")
        .maxConnections(settings.maxConnections)
        .pendingAcquireTimeout(settings.pendingAcquireTimeout)
        // 서버(servlet 판)의 유휴 keep-alive 종료보다 짧게 — 닫히는 중인 연결을 재사용해 생기는 쓸데없는 UNKNOWN 을 피한다.
        .maxIdleTime(Duration.ofSeconds(15))
        .evictInBackground(Duration.ofSeconds(30))
        .build()

    private val webClient: WebClient = WebClient.builder()
        .baseUrl(settings.baseUrl)
        .clientConnector(
            ReactorClientHttpConnector(
                HttpClient.create(provider)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, settings.connectTimeout.toMillis().toInt())
                    .responseTimeout(settings.responseTimeout)
                    .disableRetry(true),
            ),
        )
        .build()

    override fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> =
        post(GatewayPaths.NAME_INQUIRIES, NameInquiryBody(bankCode, accountNo)) { GatewayJson.read(it, NameInquiryResult::class.java).toResponse() }

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> =
        post(GatewayPaths.TRANSFERS, TransferBody(bankCode, accountNo, amount, memo, reference)) { GatewayJson.read(it, TransferResult::class.java).toResponse() }

    override fun inquire(reference: String): CompletableFuture<InquiryResponse> =
        post(GatewayPaths.INQUIRIES, InquiryBody(reference)) { GatewayJson.read(it, InquiryResult::class.java).toResponse() }

    /** 본문은 문자열로 주고받는다 — JSON 은 GatewayJson 한 곳 (두 클라이언트 판이 같게). 상태 코드는 예외로 바꾸지 않고 직접 본다. */
    private fun <T> post(path: String, body: Any, parse: (String) -> T): CompletableFuture<T> =
        webClient.post().uri(path)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(GatewayJson.write(body))
            .exchangeToMono { res -> res.bodyToMono(String::class.java).defaultIfEmpty("").map { res.statusCode().value() to it } }
            .toFuture()
            .handle { reply, error ->
                if (error != null) throw HttpFailures.fromTransport(error)
                // 오류가 없으면 reply 는 항상 있다 (defaultIfEmpty) — 그래도 null 이면 읽을 수 없는 응답 = 모름
                val r = reply ?: throw HttpFailures.fromTransport(IllegalStateException("empty gateway reply"))
                HttpFailures.parse(r.first, r.second, parse)
            }

    override fun close() {
        provider.dispose()
    }
}
