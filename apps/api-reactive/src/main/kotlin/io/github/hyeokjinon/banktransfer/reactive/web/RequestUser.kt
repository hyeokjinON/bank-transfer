package io.github.hyeokjinon.banktransfer.reactive.web

import org.springframework.context.annotation.Configuration
import org.springframework.core.MethodParameter
import org.springframework.web.reactive.BindingContext
import org.springframework.web.reactive.config.WebFluxConfigurer
import org.springframework.web.reactive.result.method.HandlerMethodArgumentResolver
import org.springframework.web.reactive.result.method.annotation.ArgumentResolverConfigurer
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono

const val USER_ID_HEADER = "X-User-Id"

/** 요청한 사용자 — apps/api 의 RequestUser 와 같은 규칙(영숫자 · 하이픈 1~36자). 웹 계층은 앱마다 따로 두고 계약 테스트로 맞춘다 (P6-2). */
data class RequestUser(val id: String)

class UserIdRequiredException : RuntimeException("$USER_ID_HEADER header is required (1-36 letters, digits or hyphens)")

/**
 * WebFlux 판 인자 해석기. 컨트롤러는 [RequestUser] 를 **첫 인자**로 받는다 — WebFlux 는 인자들을 차례로 구독해 묶으므로(zip) 먼저 실패한 헤더 오류가
 * 본문 검증보다 먼저 응답이 된다 (apps/api 와 같은 순서, 계약 테스트 "헤더 없음 + 본문 틀림" 이 지킨다).
 */
class RequestUserResolver : HandlerMethodArgumentResolver {

    override fun supportsParameter(parameter: MethodParameter): Boolean = parameter.parameterType == RequestUser::class.java

    override fun resolveArgument(parameter: MethodParameter, bindingContext: BindingContext, exchange: ServerWebExchange): Mono<Any> {
        val value = exchange.request.headers.getFirst(USER_ID_HEADER)
        return if (value == null || !USER_ID.matches(value)) Mono.error(UserIdRequiredException()) else Mono.just(RequestUser(value))
    }

    private companion object {
        val USER_ID = Regex("[A-Za-z0-9-]{1,36}")
    }
}

@Configuration(proxyBeanMethods = false)
class WebConfiguration : WebFluxConfigurer {
    override fun configureArgumentResolvers(configurer: ArgumentResolverConfigurer) {
        configurer.addCustomResolver(RequestUserResolver())
    }
}
