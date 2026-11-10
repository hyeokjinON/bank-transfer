package io.github.hyeokjinon.banktransfer.gatewayapp

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class BankGatewayApplication

fun main(args: Array<String>) {
    runApplication<BankGatewayApplication>(*args)
}
