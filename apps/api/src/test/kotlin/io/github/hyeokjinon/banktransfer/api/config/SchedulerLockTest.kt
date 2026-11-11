package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyReconScheduler
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconRun
import io.github.hyeokjinon.banktransfer.api.retention.DataRetentionScheduler
import io.github.hyeokjinon.banktransfer.api.retention.RetentionRun
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.concurrently
import net.javacrumbs.shedlock.core.LockProvider
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.junit.jupiter.api.BeforeEach
import org.springframework.aop.support.AopUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.assertj.AssertableApplicationContext
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DelegatingDataSource
import java.time.Clock
import java.util.Collections
import java.util.function.Supplier
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 하루 배치 락 (P8-2, 스펙 §6 · §7-1). api 두 대를 ApplicationContextRunner 두 개(instance api-1 · api-2)로 흉내 낸다 —
 * 락 표는 공유 컨텍스트의 MySQL(Testcontainers, Flyway V6) 하나다. 실행기는 함수로 바꿔 끼워 누가 돌았는지만 센다.
 * 시간이 흐른 것은 lock_until 을 DB 시각(UTC) 1초 전으로 당겨 만든다 — ShedLock 이 usingDbTime 으로 UTC_TIMESTAMP(3) 와 비교한다.
 */
class SchedulerLockTest : ApiIntegrationTest() {

    @Autowired private lateinit var dataSource: DataSource
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var appLockProvider: LockProvider

    /** 실행기가 불린 서버 이름 (순서대로). 두 컨텍스트가 같은 목록에 쓴다. */
    private val ran = Collections.synchronizedList(mutableListOf<String>())

    /**
     * 서버 한 대. DataSource 는 감싸서 넘긴다 — Hikari 는 AutoCloseable 이라 그대로 등록하면 이 작은 컨텍스트가 닫힐 때
     * 공유 컨텍스트의 커넥션 풀까지 닫는다 (Spring 은 AutoCloseable 빈의 close() 를 추론해 부른다).
     */
    private fun server(instance: String): ApplicationContextRunner = ApplicationContextRunner()
        .withUserConfiguration(SchedulerLockConfiguration::class.java)
        .withPropertyValues("info.instance=$instance")
        .withBean(DataSource::class.java, Supplier<DataSource> { DelegatingDataSource(dataSource) })
        .withBean(DailyReconScheduler::class.java, Supplier {
            DailyReconScheduler(
                { date, _ -> ran += instance; Thread.sleep(500); ReconRun.AlreadyCompleted(date) },
                Clock.systemUTC(), "0 0 1-6 * * *", "Asia/Seoul", AlertNotifier { },
            )
        })
        .withBean(DataRetentionScheduler::class.java, Supplier {
            DataRetentionScheduler({ date -> ran += instance; RetentionRun.AlreadyCompleted(date) }, Clock.systemUTC(), "Asia/Seoul", AlertNotifier { })
        })

    private fun twoServers(block: (AssertableApplicationContext, AssertableApplicationContext) -> Unit) =
        server("api-1").run { a ->
            server("api-2").run { b ->
                assertNull(a.startupFailure)
                assertNull(b.startupFailure)
                block(a, b)
            }
        }

    private fun lockedBy(name: String): String? =
        jdbc.queryForList("SELECT locked_by FROM shedlock WHERE name = ?", String::class.java, name).firstOrNull()

    private fun expire(name: String) =
        jdbc.update("UPDATE shedlock SET lock_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(3)) WHERE name = ?", name)

    @BeforeEach
    fun clearLocks() {
        jdbc.update("DELETE FROM shedlock WHERE name IN ('daily-recon', 'data-retention')")
    }

    @Test
    fun `① 두 서버가 같은 회차를 동시에 돌려도 실행기는 한 번 - locked_by 는 이긴 쪽`() {
        twoServers { a, b ->
            val schedulers = listOf(a, b).map { it.getBean(DailyReconScheduler::class.java) }

            concurrently(2) { i -> schedulers[i].run() }

            assertEquals(1, ran.size, "ran=$ran")
            assertEquals(ran.single(), lockedBy("daily-recon"))
            assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM shedlock WHERE name = 'daily-recon' AND locked_at > TIMESTAMPADD(MINUTE, -5, UTC_TIMESTAMP(3))", Int::class.java))
        }
    }

    @Test
    fun `② 끝나도 lockAtLeastFor(1분) 동안은 다음 호출을 건너뛰고, 지난 뒤에는 돈다`() {
        twoServers { a, b ->
            a.getBean(DailyReconScheduler::class.java).run()
            b.getBean(DailyReconScheduler::class.java).run()   // 시계가 조금 다른 서버가 같은 회차를 다시 돌리지 않게 1분은 잡고 있다
            assertEquals(listOf("api-1"), ran.toList())

            expire("daily-recon")
            b.getBean(DailyReconScheduler::class.java).run()

            assertEquals(listOf("api-1", "api-2"), ran.toList())
            assertEquals("api-2", lockedBy("daily-recon"))
        }
    }

    @Test
    fun `③ 살아 있는 락은 기다리고, 죽은 서버의 락은 lock_until 이 지나면 가져간다`() {
        jdbc.update(
            "INSERT INTO shedlock (name, lock_until, locked_at, locked_by) " +
                "VALUES ('data-retention', TIMESTAMPADD(MINUTE, 29, UTC_TIMESTAMP(3)), UTC_TIMESTAMP(3), 'busy-api')",
        )
        twoServers { a, _ ->
            a.getBean(DataRetentionScheduler::class.java).run()
            assertEquals(emptyList<String>(), ran.toList())

            expire("data-retention")   // busy-api 가 돌다 죽었다: lock_until(= 시작 + lockAtMostFor)이 지났다
            a.getBean(DataRetentionScheduler::class.java).run()

            assertEquals(listOf("api-1"), ran.toList())
            assertEquals("api-1", lockedBy("data-retention"))
        }
    }

    @Test
    fun `④ 실제 스케줄러 두 개에 락 이름 · 시간이 붙고 프록시로 감싸진다, 앱의 락 저장소는 JDBC`() {
        val recon = DailyReconScheduler::class.java.getMethod("run").getAnnotation(SchedulerLock::class.java)
        assertEquals(Triple("daily-recon", "PT50M", "PT1M"), Triple(recon.name, recon.lockAtMostFor, recon.lockAtLeastFor))
        val retention = DataRetentionScheduler::class.java.getMethod("run").getAnnotation(SchedulerLock::class.java)
        assertEquals(Triple("data-retention", "PT30M", "PT1M"), Triple(retention.name, retention.lockAtMostFor, retention.lockAtLeastFor))

        server("api-1").run { ctx ->
            assertNull(ctx.startupFailure)
            assertTrue(AopUtils.isAopProxy(ctx.getBean(DailyReconScheduler::class.java)))
            assertTrue(AopUtils.isAopProxy(ctx.getBean(DataRetentionScheduler::class.java)))
        }
        assertIs<JdbcTemplateLockProvider>(appLockProvider)
    }
}
