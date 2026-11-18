package io.github.hyeokjinon.banktransfer.gateway.http

import io.micrometer.observation.ObservationRegistry

class JdkHttpBankGatewayTest : HttpBankGatewayClientContract() {
    override fun newGateway(settings: HttpGatewaySettings, observations: ObservationRegistry): HttpBankGateway = JdkHttpBankGateway(settings, observations)
}
