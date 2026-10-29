package io.github.hyeokjinon.banktransfer.api.support

import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.web.client.RestClient

/**
 * 통합 테스트가 HTTP 응답에서 보는 것: 상태 · JSON 본문(없으면 빈 맵) · Retry-After.
 * RestClient 는 4xx · 5xx 에서 예외를 던지므로 exchange 로 응답을 그대로 받는다.
 */
data class ApiResponse(val status: Int, val body: Map<String, Any?>, val retryAfter: String?)

private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

fun send(request: RestClient.RequestHeadersSpec<*>): ApiResponse =
    request.exchange { _, r -> ApiResponse(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), r.headers.getFirst(HttpHeaders.RETRY_AFTER)) }
