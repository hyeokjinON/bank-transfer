package io.github.hyeokjinon.banktransfer.api.config

import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import tools.jackson.databind.cfg.CoercionAction
import tools.jackson.databind.cfg.CoercionInputShape
import tools.jackson.databind.type.LogicalType

@Configuration(proxyBeanMethods = false)
class JacksonConfiguration {

    /**
     * 금액 같은 정수 필드는 JSON 정수만 받는다.
     * Jackson 기본값은 50000.9 를 50000 으로 자르고 "70000" 을 70000 으로 바꿔 받는데,
     * 돈을 다루는 API 에서 조용히 값이 바뀌는 것보다 400 으로 거절하는 편이 안전하다.
     * (accept-float-as-int=false 프로퍼티는 소수만 막고 문자열은 막지 못해서 coercion 설정 한 곳으로 모았다.)
     */
    @Bean
    fun strictIntegerCoercion(): JsonMapperBuilderCustomizer = JsonMapperBuilderCustomizer { builder ->
        builder.withCoercionConfig(LogicalType.Integer) { config ->
            config.setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
            config.setCoercion(CoercionInputShape.String, CoercionAction.Fail)
        }
    }
}
