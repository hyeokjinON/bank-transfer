package io.github.hyeokjinon.banktransfer.gateway.http

import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule

/**
 * 게이트웨이 계약 JSON 매퍼 하나 (P8-1). 서버(두 판)와 클라이언트(두 판)가 모두 이것으로 문자열을 쓰고 읽는다 —
 * 프레임워크 코덱 설정 차이가 판 비교에 섞이지 않게. Jackson 3 은 java.time 을 기본으로 ISO 문자열로 다룬다.
 */
object GatewayJson {
    val mapper: JsonMapper = JsonMapper.builder().addModule(KotlinModule.Builder().build()).build()

    fun write(value: Any): String = mapper.writeValueAsString(value)

    fun <T> read(json: String, type: Class<T>): T = mapper.readValue(json, type)

    /** 오류 본문이면 읽고, 아니면(빈 본문 · HTML · 깨진 JSON) null. */
    fun errorOrNull(json: String): GatewayError? = runCatching { read(json, GatewayError::class.java) }.getOrNull()
}
