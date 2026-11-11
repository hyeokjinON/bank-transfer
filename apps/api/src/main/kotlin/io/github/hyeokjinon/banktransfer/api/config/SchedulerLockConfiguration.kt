package io.github.hyeokjinon.banktransfer.api.config

import net.javacrumbs.shedlock.core.LockProvider
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import javax.sql.DataSource

/**
 * 하루 배치 락 (P8-2, ADR-0016). api 가 여러 대가 되면 @Scheduled 가 대수만큼 돈다 — 일마감 대사 · 보존 배치만 한 대에서 돌게 한다.
 * 락은 1차 방어다: 같은 날짜 잡은 Spring Batch 잡 인스턴스가, 대사 이슈는 UNIQUE (recon_date, type, ref_key) 가 두 번째로 막는다.
 * 발행기 · 정리 작업에는 걸지 않는다 (걸면 한 대만 일한다 — 이미 SKIP LOCKED · 조건부 UPDATE 로 행 단위 안전, 스펙 §2).
 *
 * - usingDbTime: 락 시각을 DB 시각(UTC_TIMESTAMP)으로 쓰고 비교한다 → 서버끼리 시계가 달라도 같다.
 * - locked_by = info.instance (Compose 의 INFO_INSTANCE = api-1 · api-2, 없으면 local) — 누가 돌았는지 shedlock 표 · 로그로 맞춰 본다.
 * - 방식은 PROXY_METHOD(기본): @SchedulerLock 메서드를 가진 빈을 CGLIB 프록시로 감싼다 → 스케줄러 클래스와 run() 은 open 이어야 한다.
 */
@Configuration(proxyBeanMethods = false)
@EnableSchedulerLock(defaultLockAtMostFor = "PT30M")
class SchedulerLockConfiguration {

    @Bean
    fun lockProvider(dataSource: DataSource, @Value("\${info.instance:local}") instance: String): LockProvider =
        JdbcTemplateLockProvider(
            JdbcTemplateLockProvider.Configuration.builder()
                .withJdbcTemplate(JdbcTemplate(dataSource))
                .usingDbTime()
                .withLockedByValue(instance)
                .build(),
        )
}
