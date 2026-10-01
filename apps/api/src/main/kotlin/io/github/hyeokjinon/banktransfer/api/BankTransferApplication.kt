package io.github.hyeokjinon.banktransfer.api

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class BankTransferApplication

fun main(args: Array<String>) {
    runApplication<BankTransferApplication>(*args)
}
