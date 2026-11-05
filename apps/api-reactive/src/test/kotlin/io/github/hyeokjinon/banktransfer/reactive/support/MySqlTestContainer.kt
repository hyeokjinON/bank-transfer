package io.github.hyeokjinon.banktransfer.reactive.support

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.mysql.MySQLContainer

/**
 * 통합 테스트용 MySQL. infra/docker-compose.yml 과 같은 버전 · 문자셋 · 시간대로 띄운다.
 * apps/api 의 같은 파일을 복사 (P6-2 — 테스트 지원은 앱마다 둔다)
 * @ServiceConnection 이 spring.datasource.* 를 이 컨테이너로 바꿔 준다.
 * URL 파라미터는 컨테이너 주소에 다시 붙여야 한다 (application.yml 의 url 은 덮어써진다).
 */
@TestConfiguration(proxyBeanMethods = false)
class MySqlTestContainer {

    @Bean
    @ServiceConnection
    fun mysql(): MySQLContainer = MySQLContainer("mysql:8.4")
        .withDatabaseName("bank_transfer")
        .withCommand("--character-set-server=utf8mb4", "--collation-server=utf8mb4_0900_ai_ci", "--default-time-zone=+09:00")
        .withUrlParam("connectionTimeZone", "Asia/Seoul")
        .withUrlParam("forceConnectionTimeZoneToSession", "true")
}
