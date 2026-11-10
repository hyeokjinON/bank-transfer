package io.github.hyeokjinon.banktransfer.gateway.http

class JdkHttpBankGatewayTest : HttpBankGatewayClientContract() {
    override fun newGateway(settings: HttpGatewaySettings): HttpBankGateway = JdkHttpBankGateway(settings)
}
