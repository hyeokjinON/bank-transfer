package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.account.infra.ACCOUNT_INFRA_PACKAGE
import org.springframework.boot.autoconfigure.AutoConfigurationPackage
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
// 1원인증 엔티티 · 저장소는 modules/account-infra (앱 패키지 밖)에 있다 — JPA 엔티티 · 저장소 스캔에 그 패키지를 더한다 (P6-2).
// 명시한 @AutoConfigurationPackage 가 @SpringBootApplication 의 기본(앱 패키지)을 덮어쓰므로 앱 패키지도 같이 적는다
@AutoConfigurationPackage(basePackages = [ACCOUNT_INFRA_PACKAGE], basePackageClasses = [BankTransferApplication::class])
class BankTransferApplication

fun main(args: Array<String>) {
    runApplication<BankTransferApplication>(*args)
}
