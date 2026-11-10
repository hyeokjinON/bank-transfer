package io.github.hyeokjinon.banktransfer.gatewayapp.web

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayPaths
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import java.util.concurrent.CompletableFuture

/**
 * MVC 비동기 판 — 비교용 (P8-1 스펙 §5-1). Tomcat 스레드가 요청을 받고 future 를 돌려준 뒤 바로 반납된다.
 * 은행 응답으로 future 가 완료되면 AsyncContext 재디스패치로 Tomcat 스레드가 다시 잡혀 응답을 쓴다 (WebFlux 판과의 차이 — 한 요청에 Tomcat 스레드 두 번).
 * 돌려주는 future 는 은행 future 의 의존 future 라, 비동기 시간 초과 · 끊김으로 Spring 이 이것을 취소해도 은행 송신은 취소되지 않는다.
 */
@RestController
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
class ServletGatewayController(private val bank: BankGateway) {

    @PostMapping(GatewayPaths.NAME_INQUIRIES)
    fun nameInquiry(@RequestBody json: String): CompletableFuture<ResponseEntity<String>> = GatewayEndpoints.nameInquiry(bank, json)

    @PostMapping(GatewayPaths.TRANSFERS)
    fun transfer(@RequestBody json: String): CompletableFuture<ResponseEntity<String>> = GatewayEndpoints.transfer(bank, json)

    @PostMapping(GatewayPaths.INQUIRIES)
    fun inquiry(@RequestBody json: String): CompletableFuture<ResponseEntity<String>> = GatewayEndpoints.inquiry(bank, json)
}
