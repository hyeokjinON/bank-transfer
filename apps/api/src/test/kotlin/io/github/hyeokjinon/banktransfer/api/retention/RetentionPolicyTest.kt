package io.github.hyeokjinon.banktransfer.api.retention

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.scheduling.support.CronExpression
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/** 기준 시각 · 받는 날짜 · 설정 하한 (P5-2, 설계 §4 · §5 · §6). 날짜로만 정해지는 순수 함수라 재시작해도 기준이 같다. */
class RetentionPolicyTest {

    private val seoul = ZoneId.of("Asia/Seoul")
    private val today = LocalDate.of(2026, 11, 2)

    private fun kst(date: LocalDate, hour: Int, minute: Int = 0): Instant = date.atTime(hour, minute).atZone(seoul).toInstant()

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(DataRetentionProperties::class)
    class PropertiesOnly

    private val runner = ApplicationContextRunner().withUserConfiguration(PropertiesOnly::class.java)

    @Test
    fun `기준 시각 = 그 날짜 00시(KST) − 보관 기간`() {
        assertEquals(Instant.parse("2026-10-25T15:00:00Z"), RetentionPolicy.cutoff(today, Duration.ofDays(7)))
        assertEquals(Instant.parse("2026-10-02T15:00:00Z"), RetentionPolicy.cutoff(today, Duration.ofDays(30)))
    }

    @Test
    fun `내일 이후는 거부, 오늘과 지난 날은 받는다 - 내일을 받으면 기준이 앞당겨져 정책보다 일찍 지운다`() {
        val noon = kst(today, 12)

        RetentionPolicy.requireNotFuture(today, noon)
        RetentionPolicy.requireNotFuture(today.minusDays(400), noon)

        val tomorrow = assertFailsWith<RetentionDateInFutureException> { RetentionPolicy.requireNotFuture(today.plusDays(1), noon) }
        assertEquals("retention date 2026-11-03 is in the future (today is 2026-11-02, KST)", tomorrow.message)
    }

    @Test
    fun `오늘은 KST 로 정한다 - KST 00시 30분(UTC 전날 15시 30분)이면 KST 오늘을 받고 KST 내일은 거부`() {
        val justAfterMidnight = kst(today, 0, 30)
        assertEquals(Instant.parse("2026-11-01T15:30:00Z"), justAfterMidnight)

        RetentionPolicy.requireNotFuture(today, justAfterMidnight)   // UTC 날짜(11-01)로 보면 내일이라 거부하게 된다
        assertFailsWith<RetentionDateInFutureException> { RetentionPolicy.requireNotFuture(today.plusDays(1), justAfterMidnight) }
    }

    @Test
    fun `설정 하한 - 보관 기간은 1일 이상, 묶음은 1 ~ 10000`() {
        DataRetentionProperties(outboxRetention = Duration.ofDays(1), verificationRetention = Duration.ofDays(1))
        DataRetentionProperties(batchSize = 1)
        DataRetentionProperties(batchSize = 10_000)

        assertFailsWith<IllegalArgumentException> { DataRetentionProperties(outboxRetention = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { DataRetentionProperties(outboxRetention = Duration.ofHours(23)) }
        assertFailsWith<IllegalArgumentException> { DataRetentionProperties(verificationRetention = Duration.ofHours(23)) }
        assertFailsWith<IllegalArgumentException> { DataRetentionProperties(batchSize = 0) }
        assertFailsWith<IllegalArgumentException> { DataRetentionProperties(batchSize = 10_001) }
    }

    @Test
    fun `잘못된 설정이면 기동에 실패한다 - 0d 같은 실수로 방금 발행한 행을 지우지 않게`() {
        for (bad in listOf(
            "data-retention.outbox-retention=0d",
            "data-retention.verification-retention=12h",
            "data-retention.batch-size=0",
            "data-retention.batch-size=10001",
        )) {
            runner.withPropertyValues(bad).run { ctx -> assertNotNull(ctx.startupFailure, bad) }
        }
        runner.run { ctx -> assertNull(ctx.startupFailure) }
    }

    @Test
    fun `application yml 의 값이 바인딩된다 - 7일 · 30일 · 1000 · 켜짐 · 매일 06시 30분 KST`() {
        val yml = Binder(ConfigurationPropertySources.from(YamlPropertySourceLoader().load("application", ClassPathResource("application.yml"))))

        val props = yml.bind("data-retention", DataRetentionProperties::class.java).get()

        assertEquals(Duration.ofDays(7), props.outboxRetention)
        assertEquals(Duration.ofDays(30), props.verificationRetention)
        assertEquals(1000, props.batchSize)
        assertEquals(DataRetentionProperties.Schedule(enabled = true, cron = "0 30 6 * * *", zone = "Asia/Seoul"), props.schedule)
        val next = CronExpression.parse(props.schedule.cron).next(kst(today, 0).atZone(seoul))
        assertEquals(LocalTime.of(6, 30), next!!.toLocalTime())
    }
}
