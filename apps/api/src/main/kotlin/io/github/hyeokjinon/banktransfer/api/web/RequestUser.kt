package io.github.hyeokjinon.banktransfer.api.web

import org.springframework.context.annotation.Configuration
import org.springframework.core.MethodParameter
import org.springframework.web.bind.support.WebDataBinderFactory
import org.springframework.web.context.request.NativeWebRequest
import org.springframework.web.method.support.HandlerMethodArgumentResolver
import org.springframework.web.method.support.ModelAndViewContainer
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer

const val USER_ID_HEADER = "X-User-Id"

/**
 * 요청한 사용자 (P4-1). 지금은 X-User-Id 헤더 값이고, 인증을 붙이면 토큰의 주체로 바뀐다 — 값의 출처는 [RequestUserResolver] 한 곳이다.
 * 컨트롤러는 이 타입을 **첫 인자**로 받는다: 인자는 선언 순서로 해석되므로 헤더 오류가 본문 검증보다 먼저 난다.
 */
data class RequestUser(val id: String)

/** X-User-Id 가 없거나 형식(영숫자 · 하이픈 1~36자)이 아니다 → 400 USER_ID_REQUIRED. */
class UserIdRequiredException : RuntimeException("$USER_ID_HEADER header is required (1-36 letters, digits or hyphens)")

/**
 * 컨트롤러 인자 [RequestUser] 를 채운다. 헤더를 읽고 검사하는 곳은 여기뿐이다.
 * 값은 믿는다(인증은 범위 밖, ADR-0008). DB 는 ascii_bin 이라 대소문자가 다르면 다른 사용자다.
 */
class RequestUserResolver : HandlerMethodArgumentResolver {

    override fun supportsParameter(parameter: MethodParameter): Boolean = parameter.parameterType == RequestUser::class.java

    override fun resolveArgument(
        parameter: MethodParameter,
        mavContainer: ModelAndViewContainer?,
        webRequest: NativeWebRequest,
        binderFactory: WebDataBinderFactory?,
    ): RequestUser {
        val value = webRequest.getHeader(USER_ID_HEADER)
        if (value == null || !USER_ID.matches(value)) throw UserIdRequiredException()
        return RequestUser(value)
    }

    private companion object {
        val USER_ID = Regex("[A-Za-z0-9-]{1,36}")
    }
}

@Configuration(proxyBeanMethods = false)
class WebConfiguration : WebMvcConfigurer {
    override fun addArgumentResolvers(resolvers: MutableList<HandlerMethodArgumentResolver>) {
        resolvers += RequestUserResolver()
    }
}
