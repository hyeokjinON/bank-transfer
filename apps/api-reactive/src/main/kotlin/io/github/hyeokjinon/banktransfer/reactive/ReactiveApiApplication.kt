package io.github.hyeokjinon.banktransfer.reactive

import io.github.hyeokjinon.banktransfer.account.infra.ACCOUNT_INFRA_PACKAGE
import org.springframework.boot.autoconfigure.AutoConfigurationPackage
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

/**
 * 1원인증 요청만 하는 WebFlux 앱 (P6-2, ADR-0013). 같은 요청을 apps/api(MVC)와 같은 도구로 비교하려고 둔다 — 운영 기본은 apps/api 다.
 * Spring 은 한 앱에 MVC 와 WebFlux 가 같이 있으면 MVC 를 고르므로 앱을 나눴다.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
// 1원인증 엔티티 · 저장소는 modules/account-infra (앱 패키지 밖)에 있다 — JPA 엔티티 · 저장소 스캔에 그 패키지를 더한다 (apps/api 와 같다).
// 명시한 @AutoConfigurationPackage 가 @SpringBootApplication 의 기본(앱 패키지)을 덮어쓰므로 앱 패키지도 같이 적는다
@AutoConfigurationPackage(basePackageClasses = [ReactiveApiApplication::class], basePackages = [ACCOUNT_INFRA_PACKAGE])
class ReactiveApiApplication

fun main(args: Array<String>) {
    runApplication<ReactiveApiApplication>(*args)
}
