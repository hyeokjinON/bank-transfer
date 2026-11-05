package io.github.hyeokjinon.banktransfer.reactive

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

/**
 * 1원인증 요청만 하는 WebFlux 앱 (P6-2, ADR-0013). 같은 요청을 apps/api(MVC)와 같은 도구로 비교하려고 둔다 — 운영 기본은 apps/api 다.
 * Spring 은 한 앱에 MVC 와 WebFlux 가 같이 있으면 MVC 를 고르므로 앱을 나눴다.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
class ReactiveApiApplication

fun main(args: Array<String>) {
    runApplication<ReactiveApiApplication>(*args)
}
