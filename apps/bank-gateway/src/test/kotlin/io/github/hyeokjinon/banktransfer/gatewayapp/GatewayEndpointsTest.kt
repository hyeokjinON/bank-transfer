package io.github.hyeokjinon.banktransfer.gatewayapp

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayErrorCodes
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayJson
import io.github.hyeokjinon.banktransfer.gatewayapp.web.GatewayEndpoints
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals

/** send 가 future 를 돌려주지 않고 동기로 던지는 경우도 같은 오류 변환을 거친다 (500 INTERNAL 행도 여기서 확인). */
class GatewayEndpointsTest {
    private class Throwing(private val e: RuntimeException) : BankGateway {
        override fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> = throw e
        override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> = throw e
        override fun inquire(reference: String): CompletableFuture<InquiryResponse> = throw e
    }

    private val body = """{"bankCode":"090","accountNo":"1002123456789","amount":1000,"memo":"m","reference":"R1"}"""

    @Test
    fun `동기로 던진 예상 밖 예외는 500 INTERNAL`() {
        val res = GatewayEndpoints.transfer(Throwing(IllegalStateException("boom")), body).get(2, TimeUnit.SECONDS)
        assertEquals(500, res.statusCode.value())
        assertEquals(GatewayErrorCodes.INTERNAL, GatewayJson.errorOrNull(res.body!!)!!.code)
    }

    @Test
    fun `동기로 던진 BankNotSentException 은 503 BANK_NOT_SENT`() {
        val res = GatewayEndpoints.transfer(Throwing(BankNotSentException("closed")), body).get(2, TimeUnit.SECONDS)
        assertEquals(503, res.statusCode.value())
        assertEquals(GatewayErrorCodes.BANK_NOT_SENT, GatewayJson.errorOrNull(res.body!!)!!.code)
    }
}
