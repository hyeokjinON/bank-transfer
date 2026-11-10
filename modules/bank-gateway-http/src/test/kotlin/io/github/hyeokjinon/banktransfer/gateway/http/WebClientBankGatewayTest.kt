package io.github.hyeokjinon.banktransfer.gateway.http

class WebClientBankGatewayTest : HttpBankGatewayClientContract() {
    override fun newGateway(settings: HttpGatewaySettings): HttpBankGateway = WebClientBankGateway(settings)
}
