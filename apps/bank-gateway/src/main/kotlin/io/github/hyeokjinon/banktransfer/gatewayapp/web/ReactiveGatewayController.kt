package io.github.hyeokjinon.banktransfer.gatewayapp.web

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayPaths
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

/**
 * WebFlux 판 (기본, P8-1 스펙 §5-1). Reactor Netty 이벤트 루프가 요청을 받고, 은행 Netty 이벤트 루프가 future 를 완료하면
 * 이벤트 루프가 응답을 쓴다 — 요청당 스레드가 없다.
 * Mono.fromFuture(f, suppressCancel = true): 호출자가 끊겨 구독이 취소돼도 future 를 취소하지 않는다 (은행 송신은 되돌릴 수 없다).
 */
@RestController
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
class ReactiveGatewayController(private val bank: BankGateway) {

    @PostMapping(GatewayPaths.NAME_INQUIRIES)
    fun nameInquiry(@RequestBody json: String): Mono<ResponseEntity<String>> = Mono.fromFuture(GatewayEndpoints.nameInquiry(bank, json), true)

    @PostMapping(GatewayPaths.TRANSFERS)
    fun transfer(@RequestBody json: String): Mono<ResponseEntity<String>> = Mono.fromFuture(GatewayEndpoints.transfer(bank, json), true)

    @PostMapping(GatewayPaths.INQUIRIES)
    fun inquiry(@RequestBody json: String): Mono<ResponseEntity<String>> = Mono.fromFuture(GatewayEndpoints.inquiry(bank, json), true)
}
