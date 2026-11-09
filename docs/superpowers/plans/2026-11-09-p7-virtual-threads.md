# P7 가상 스레드 적용 비교 — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `apps/api` 를 Boot 설정 하나(`spring.threads.virtual.enabled`)로 가상 스레드에서 띄울 수 있게 하고, 진행 조건 실험(V1 · V2 · V3)과 같은 측정 도구로 웹 1원인증 · 블로킹 컨슈머를 끈 판과 비교한다.

**Architecture:** 업무 코드는 바꾸지 않는다. `application.yml` 에 기본값(끔)과 `/actuator/info` 노출을 더하고, 실험 테스트가 "무엇이 가상 스레드가 되나 · pinning 이 있나 · 스레드 지표가 무엇을 세나" 를 고정한다. 측정 도구 두 개(`load/verification/run.sh` · `load/consumer/run.sh`)에 `VT=1` · `RAW_SUB` 를 더하고, 공통 부분은 `load/lib/vt.sh`, P7 표는 새 `load/p7/summarize.sh` 가 만든다.

**Tech Stack:** Kotlin 2.3.21, Java 21 (toolchain), Spring Boot 4.1.1, Spring Kafka, Testcontainers(MySQL · Kafka), JDK Flight Recorder(`jdk.jfr.consumer.RecordingStream`, `jcmd`, `jfr`), Git Bash, k6 2.3.0(Docker)

**Spec:** `docs/superpowers/specs/2026-11-09-p7-virtual-threads-design.md`

## Global Constraints
- 새 의존성 없음. JFR 은 JDK 내장(`jdk.jfr`)만 쓴다.
- 업무 코드(`src/main`) 변경은 `apps/api/src/main/resources/application.yml` 뿐이다 (기본값 · info). 기본값은 `spring.threads.virtual.enabled: false` — 켤지는 ADR-0014 (설계 §6-5).
- 가상 스레드 `VT=1` 은 웹은 MVC 판(`blocking` · `async`)에만, 컨슈머는 `blocking` 에만 허용한다 (그 밖은 인자 오류 exit 2).
- 결과 이름: 웹 `<판>[-vt]-d<지연>-r<도착률>-<회차>`, 컨슈머 `blocking[-vt]-d<지연>-h<hikari>-<회차>`. JSON 에 `"virtual_threads": true|false`, `"pinned_events": <수>|null`(VT 를 끈 회차는 JFR 을 붙이지 않아 null).
- 원자료 폴더: `docs/perf/raw/<RAW_SUB>`, `RAW_SUB` 기본은 웹 `p6-2` · 컨슈머 `p6-1` (기존 그대로). P7 측정은 `RAW_SUB=p7`. 허용 형식 `^p[0-9]+(-[0-9]+)?$`.
- P6 의 `load/verification/summarize.sh` · `load/consumer/summarize.sh` 는 고치지 않는다 (P6 결과 문서를 그대로 다시 만들 수 있게).
- 측정 데이터는 웹 `p62-u001` ~ `u100`, 컨슈머 `load-p61-u001` ~ `u100` 만 만들고 지운다. 개발 DB(3306)는 그 밖에는 읽기만. DB 비밀번호는 컨테이너 환경변수로만 (`MYSQL_PWD="$MYSQL_PASSWORD"` 를 컨테이너 안 셸에서).
- 개발용 컨슈머 그룹(`transfer-processor`) 리셋은 사용자 OK 가 있어야 한다.
- Postman 앱이 다시 쓴 `postman/` 아래 파일은 커밋하지 않는다.
- 주석 · 문서는 한국어, 기존 문체(확인 / 추정 구분). 커밋 메시지 끝: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- 테스트 수: 지금 518 → 이 계획 뒤 524 (Task 1 +2, Task 2 +4).

## 파일 구조
| 파일 | 할 일 |
|---|---|
| `apps/api/src/main/resources/application.yml` | 수정: `spring.threads.virtual.enabled: false`, `info.threads.virtual` |
| `apps/api/src/test/kotlin/.../api/config/VirtualThreadDefaultTest.kt` | 신규: 기본값(끔) 고정 |
| `apps/api/src/test/kotlin/.../api/experiment/VirtualThreadExperimentTest.kt` | 신규: V1 · V2 · V2 대조 (가상 스레드 컨텍스트) |
| `apps/api/src/test/kotlin/.../api/experiment/VirtualThreadMetricsExperimentTest.kt` | 신규: V3 (Spring 없음) |
| `load/lib/vt.sh` | 신규: `VT` 검사 · 인자 · JFR 덤프 · pinning 집계 |
| `load/verification/run.sh` · `load/verification/verification.js` | 수정: `VT` · `RAW_SUB` |
| `load/consumer/run.sh` | 수정: `VT` · `RAW_SUB` |
| `load/p7/summarize.sh` | 신규: P7 표 두 개 |
| `docs/perf/p7-virtual-threads.md`, `docs/perf/raw/p7/*.json` | 신규 (측정 후) |
| `docs/adr/0014-virtual-threads.md` | 신규 (측정 후) |
| `OPERATION.md` §13, `ARCHITECTURE.md`, `README.md`, `SETUP.md`, `HANDOVER.md`, `CHANGELOG.md` | 수정 (측정 후) |

(`.../api` = `io/github/hyeokjinon/banktransfer/api`)

---

### Task 1: 기본값(끔) · info 노출

**Files:**
- Modify: `apps/api/src/main/resources/application.yml` (`spring:` 블록, `info:` 블록)
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/config/VirtualThreadDefaultTest.kt`

**Interfaces:**
- Produces: `/actuator/info` JSON 에 `"threads":{"virtual":"false"}` (값은 `spring.threads.virtual.enabled` 를 따른다). Task 3 의 `run.sh` 가 `"virtual":"?(true|false)"?[,}]` 로 확인한다.

- [ ] **Step 1: 실패하는 테스트를 쓴다**

```kotlin
package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.tomcat.autoconfigure.TomcatVirtualThreadsWebServerFactoryCustomizer
import org.springframework.context.ApplicationContext
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * P7 (설계 §5 · §7): 가상 스레드는 기본으로 꺼져 있다 — 켤지는 ADR-0014. 측정 사전 확인이 읽는 `/actuator/info` 의 `threads.virtual` 도 고정한다.
 * 공유 컨텍스트(ApiIntegrationTest 기본 설정)를 그대로 쓴다.
 */
class VirtualThreadDefaultTest : ApiIntegrationTest() {

    @Autowired private lateinit var context: ApplicationContext
    @Autowired @Qualifier("applicationTaskExecutor") private lateinit var executor: Executor

    @Test
    fun `기본은 가상 스레드를 쓰지 않는다 - 앱 실행기는 플랫폼 스레드, Tomcat 가상 스레드 설정 없음`() {
        assertFalse(CompletableFuture.supplyAsync({ Thread.currentThread().isVirtual }, executor).get(5, TimeUnit.SECONDS))
        assertTrue(context.getBeansOfType(TomcatVirtualThreadsWebServerFactoryCustomizer::class.java).isEmpty())
    }

    @Test
    fun `actuator info 에 threads virtual false 가 보인다 - 측정 사전 확인이 읽는다`() {
        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!
        assertTrue(Regex("\"threads\":\\{\"virtual\":\"?false\"?[,}]").containsMatchIn(info), info)
    }
}
```

`TomcatVirtualThreadsWebServerFactoryCustomizer` 의 패키지는 Boot 4.1.1 `spring-boot-tomcat` jar 에서 확인했다 (`org/springframework/boot/tomcat/autoconfigure/`). 테스트 컴파일 클래스패스에서 보이지 않으면 `Class.forName("org.springframework.boot.tomcat.autoconfigure.TomcatVirtualThreadsWebServerFactoryCustomizer")` 로 바꿔 `getBeanNamesForType(그 클래스).isEmpty()` 로 확인한다.

- [ ] **Step 2: 실패를 확인한다**

Run: `./gradlew :apps:api:test --tests "*VirtualThreadDefaultTest"`
Expected: 첫 테스트는 통과(기본이 이미 꺼짐), 두 번째는 FAIL — info 에 `threads` 가 없다.

- [ ] **Step 3: 설정을 더한다**

`application.yml` 의 `spring:` 블록, `task:` 블록 바로 뒤(같은 들여쓰기 2칸)에:
```yaml
  threads:
    virtual:
      enabled: false   # 가상 스레드 (P7): 켜면 Tomcat 요청 · 앱 실행기 · @Scheduled · Kafka 리스너가 가상 스레드. 기본은 끔 — 켤지는 ADR-0014, 측정은 VT=1 (OPERATION §13)
```
`info:` 블록 맨 끝(`datasource:` 아래, 같은 들여쓰기 2칸)에:
```yaml
  threads:
    virtual: ${spring.threads.virtual.enabled}   # 지금 가상 스레드 여부 (P7, load/verification · load/consumer run.sh 가 측정 전에 확인)
```

- [ ] **Step 4: 통과를 확인한다**

Run: `./gradlew :apps:api:test --tests "*VirtualThreadDefaultTest"`
Expected: PASS 2개.

- [ ] **Step 5: 커밋**

```bash
git add apps/api/src/main/resources/application.yml apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/config/VirtualThreadDefaultTest.kt
git commit -m "feat(p7): 가상 스레드 설정 기본값(끔) · /actuator/info 노출, 기본값 고정 테스트

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: 진행 조건 실험 V1 · V2 · V3

**Files:**
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/experiment/VirtualThreadExperimentTest.kt`
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/experiment/VirtualThreadMetricsExperimentTest.kt`

**Interfaces:**
- Consumes: Task 1 의 `info.threads.virtual`. 기존 `ApiIntegrationTest` (`client`, `newUserId()`, `fundedUser()`, `verifiedBy()`, `bank`), `send(...)`(support/ApiResponse.kt), `USER_ID_HEADER`, `AccountVerificationService.request(RequestVerification): CompletableFuture<AccountVerification>`, `OutboxRelay.publishPending()`, `TransferStore.findById(UUID)`, `TransferStatus.COMPLETED`.
- Produces: 실험 결과(콘솔 `[P7 실험 V…]` 줄)를 Task 5 의 ADR-0014 · 결과 문서가 옮긴다.

이 Task 는 **진행 조건**이다. V2 의 실제 경로에서 pinning 이 1건 이상 나오면 테스트를 그대로 두고 실패시키지 말고 멈춰서 컨트롤러에 보고한다 (스택 맨 위 8줄 포함). 컨트롤러가 설계 §4 "다르면" 에 따라 단언을 "실제로 이렇다" 로 바꾸는 판정을 내린다.

- [ ] **Step 1: V1 · V2 테스트를 쓴다**

```kotlin
package io.github.hyeokjinon.banktransfer.api.experiment

import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.send
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import jakarta.servlet.Filter
import jakarta.servlet.http.HttpServletRequest
import jdk.jfr.consumer.RecordedEvent
import jdk.jfr.consumer.RecordingStream
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.tomcat.autoconfigure.TomcatVirtualThreadsWebServerFactoryCustomizer
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.test.context.TestPropertySource
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * P7 진행 조건 실험 V1 · V2 (설계 §4). `spring.threads.virtual.enabled=true` 컨텍스트 — 설정이 달라 새 컨텍스트(컨테이너도 새로)가 뜬다.
 * V1: Tomcat 요청 · 앱 실행기 · 예약 작업 · Kafka 리스너가 가상 스레드인지. 예약 작업 · 리스너는 테스트용 탐침이다 — 앱 것과 같은 Boot 스케줄러 ·
 *     컨테이너 팩토리를 쓴다 (앱에 직접 만든 팩토리가 없다). Tomcat 실행기는 웹 판과 상관없이 같아 기본 판(async) 요청으로 본다.
 * V2: JFR jdk.VirtualThreadPinned 를 기준 0ms 로 켜고 실제 경로를 돌린다. 블로킹 판은 컨트롤러가 하는 일(service.request(...).join())을
 *     가상 스레드에서 그대로 부른다 (Tomcat 요청 스레드 = 가상 스레드인 것은 V1 이 본다). 대조 테스트는 검출이 실제로 동작하는지 본다.
 * 결과는 ADR-0014 · 결과 문서에 옮긴다.
 */
@TestPropertySource(properties = ["spring.threads.virtual.enabled=true"])
@Import(VirtualThreadExperimentTest.Probes::class)
class VirtualThreadExperimentTest : ApiIntegrationTest() {

    /** 곳마다 처음 본 스레드. 이 클래스만 쓰는 컨텍스트라 다른 테스트와 섞이지 않는다. */
    object ThreadProbe {
        val seen = ConcurrentHashMap<String, Thread>()
        fun record(where: String) {
            seen.putIfAbsent(where, Thread.currentThread())
        }
    }

    class ScheduledProbe {
        @Scheduled(fixedDelay = 100)
        fun tick() = ThreadProbe.record("scheduled")
    }

    class ListenerProbe {
        @KafkaListener(topics = [PROBE_TOPIC], groupId = "p7-vt-probe")
        fun on(@Suppress("UNUSED_PARAMETER") value: String) = ThreadProbe.record("kafka")
    }

    @TestConfiguration(proxyBeanMethods = false)
    class Probes {
        @Bean
        fun vtProbeFilter(): Filter = Filter { req, res, chain ->
            if ((req as HttpServletRequest).requestURI == "/api/v1/account-verifications") ThreadProbe.record("tomcat")
            chain.doFilter(req, res)
        }

        @Bean
        fun vtProbeScheduled() = ScheduledProbe()

        @Bean
        fun vtProbeListener() = ListenerProbe()
    }

    companion object {
        const val PROBE_TOPIC = "p7.vt.probe"
        private const val ACCOUNT = "1002123456789"   // 모의 은행 기본 계좌 (예금주 홍길동)
    }

    @Autowired private lateinit var context: ApplicationContext
    @Autowired @Qualifier("applicationTaskExecutor") private lateinit var executor: Executor
    @Autowired private lateinit var kafka: KafkaTemplate<String, String>
    @Autowired private lateinit var service: AccountVerificationService
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    @Test
    fun `V1 설정 하나로 Tomcat 요청 · 앱 실행기 · 예약 작업 · Kafka 리스너가 가상 스레드에서 돈다`() {
        assertFalse(context.getBeansOfType(TomcatVirtualThreadsWebServerFactoryCustomizer::class.java).isEmpty())
        assertEquals(201, requestVerification(newUserId()).status)
        val executorVirtual = CompletableFuture.supplyAsync({ Thread.currentThread().isVirtual }, executor).get(5, TimeUnit.SECONDS)
        await.atMost(Duration.ofSeconds(30)).ignoreExceptions() untilAsserted {
            kafka.send(PROBE_TOPIC, "ping").get(5, TimeUnit.SECONDS)   // 리스너가 붙기 전에 보낸 것은 earliest 로 다시 읽는다
            assertNotNull(ThreadProbe.seen["kafka"])
        }
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertNotNull(ThreadProbe.seen["scheduled"]) }
        val seen = listOf("tomcat", "scheduled", "kafka").associateWith { ThreadProbe.seen[it]!! }
        println("[P7 실험 V1] 앱 실행기 virtual=$executorVirtual, " + seen.entries.joinToString { (k, t) -> "$k virtual=${t.isVirtual} ($t)" })
        assertTrue(executorVirtual, "applicationTaskExecutor 가 플랫폼 스레드다")
        seen.forEach { (where, t) -> assertTrue(t.isVirtual, "$where 가 플랫폼 스레드다: $t") }
    }

    @Test
    fun `V2 1원인증 두 판과 블로킹 출금 한 건을 끝까지 돌려도 pinning 이 없다 (기준 0ms)`() {
        val pinned = recordPinned {
            assertEquals(201, requestVerification(newUserId()).status)   // async 판: Tomcat 요청 · 앱 실행기 뒤처리
            inVirtualThread {   // blocking 판이 하는 일 그대로 (BlockingVerificationRequestController)
                service.request(RequestVerification(newUserId(), "090", ACCOUNT, "홍길동")).join()
            }
            val user = fundedUser()
            val account = verifiedBy(user, ACCOUNT)
            val id = postTransfer(user, account)
            await.atMost(Duration.ofSeconds(20)) untilAsserted {
                inVirtualThread { relay.publishPending() }   // 운영에서는 예약 작업(가상 스레드)이 부른다
                assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
            }
        }
        println("[P7 실험 V2] pinning ${pinned.size}건" + pinned.joinToString("") { "\n" + describe(it) })
        assertEquals(0, pinned.size, pinned.joinToString("\n") { describe(it) })
    }

    @Test
    fun `V2 대조 - synchronized 안에서 sleep 하면 pinning 이 잡힌다 (검출이 동작하는지)`() {
        val lock = Any()
        val pinned = recordPinned { inVirtualThread { synchronized(lock) { Thread.sleep(30) } } }
        println("[P7 실험 V2 대조] pinning ${pinned.size}건")
        assertTrue(pinned.isNotEmpty(), "synchronized 안 sleep 이 잡히지 않았다 — 녹화 설정을 확인한다")
        assertTrue(
            pinned.any { e -> e.stackTrace?.frames?.any { it.method.type.name.contains("VirtualThreadExperimentTest") } == true },
            pinned.joinToString("\n") { describe(it) },
        )
    }

    private fun requestVerification(user: String) = send(
        client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .body(mapOf("bankCode" to "090", "accountNo" to ACCOUNT, "holderName" to "홍길동")),
    )

    private fun postTransfer(user: String, account: String): UUID {
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 10_000, "memo" to "P7"))
            .retrieve().body(mapType)!!
        return UUID.fromString(body["id"] as String)
    }

    /** [block] 을 도는 동안의 jdk.VirtualThreadPinned (기준 0ms, 스택 포함). stop() 은 그때까지의 이벤트를 모두 처리한 뒤 돌아온다 (JDK 20+). */
    private fun recordPinned(block: () -> Unit): List<RecordedEvent> {
        val events = CopyOnWriteArrayList<RecordedEvent>()
        RecordingStream().use { rs ->
            rs.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO).withStackTrace()
            rs.onEvent("jdk.VirtualThreadPinned") { events += it }
            rs.startAsync()
            block()
            rs.stop()
        }
        return events
    }

    private fun <T> inVirtualThread(block: () -> T): T {
        var result: Result<T>? = null
        Thread.ofVirtual().start { result = runCatching(block) }.join()
        return result!!.getOrThrow()
    }

    private fun describe(e: RecordedEvent): String =
        "${e.duration.toMillis()}ms\n" + (e.stackTrace?.frames?.take(8)?.joinToString("\n") { "    ${it.method.type.name}.${it.method.name}:${it.lineNumber}" } ?: "    (스택 없음)")
}
```

- [ ] **Step 2: V3 테스트를 쓴다**

```kotlin
package io.github.hyeokjinon.banktransfer.api.experiment

import java.lang.management.ManagementFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * P7 진행 조건 실험 V3 (설계 §4). Micrometer `jvm.threads.live` 는 `ThreadMXBean.threadCount` 를 읽는다 (JvmThreadMetrics).
 * 가상 스레드 200개가 기다리는 동안 이 값이 200 만큼 늘지 않으면, 측정의 "스레드 최대" 는 플랫폼 스레드만 센 값이다 — 결과 문서 열 이름에 반영.
 * 캐리어(ForkJoinPool)는 코어 수까지 늘 수 있어 차이 100 미만이면 "세지 않음" 으로 본다.
 */
class VirtualThreadMetricsExperimentTest {

    @Test
    fun `V3 jvm threads live (ThreadMXBean threadCount) 는 가상 스레드를 세지 않는다`() {
        val mx = ManagementFactory.getThreadMXBean()
        val before = mx.threadCount
        val release = CountDownLatch(1)
        val started = CountDownLatch(200)
        val threads = (1..200).map { Thread.ofVirtual().start { started.countDown(); release.await() } }
        try {
            assertTrue(started.await(10, TimeUnit.SECONDS))
            val during = mx.threadCount
            println("[P7 실험 V3] 플랫폼 스레드 $before → $during (가상 스레드 200개가 기다리는 중)")
            assertTrue(during - before < 100, "가상 스레드를 센 것으로 보인다: $before → $during")
        } finally {
            release.countDown()
            threads.forEach { it.join() }
        }
    }
}
```

- [ ] **Step 3: 돌려서 결과를 본다**

Run: `./gradlew :apps:api:test --tests "*VirtualThreadExperimentTest" --tests "*VirtualThreadMetricsExperimentTest" -i 2>&1 | grep -E "P7 실험|PASSED|FAILED|Tests? "`
Expected: 4개 PASS, 콘솔에 `[P7 실험 V1] 앱 실행기 virtual=true, tomcat virtual=true …`, `[P7 실험 V2] pinning 0건`, `[P7 실험 V2 대조] pinning 1건` 이상, `[P7 실험 V3] 플랫폼 스레드 a → b` (b − a < 100).
- V1 이 실패하면 어느 곳이 플랫폼 스레드인지 그대로 보고한다 (고치지 않는다 — 설계 §4 "다르면").
- V2(실제 경로)가 실패하면 위 Task 머리말대로 멈추고 보고한다.

- [ ] **Step 4: 변이 검사 (커밋하지 않고 되돌림)**

`VirtualThreadExperimentTest` 의 `@TestPropertySource(...)` 줄을 잠깐 지우고 V1 만 돌린다.
Run: `./gradlew :apps:api:test --tests "*VirtualThreadExperimentTest*V1*"`
Expected: FAIL (`TomcatVirtualThreadsWebServerFactoryCustomizer` 없음 또는 "플랫폼 스레드다"). 확인 뒤 `git checkout -- <파일>` 이 아니라 지운 줄을 되살린다 (파일이 아직 커밋 전이다). 보고서에 변이 결과를 적는다.

- [ ] **Step 5: 전체 테스트**

Run: `./gradlew :apps:api:test`
Expected: 전부 PASS (기존 + Task 1 · 2).

- [ ] **Step 6: 커밋**

```bash
git add apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/experiment/VirtualThreadExperimentTest.kt apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/experiment/VirtualThreadMetricsExperimentTest.kt
git commit -m "test(p7): 진행 조건 실험 V1 · V2 · V3 — 무엇이 가상 스레드가 되나, pinning(JFR 0ms, 대조 포함), 스레드 지표

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: 측정 도구 — `VT` · `RAW_SUB` · P7 표

**Files:**
- Create: `load/lib/vt.sh`
- Modify: `load/verification/run.sh`, `load/verification/verification.js:93`
- Modify: `load/consumer/run.sh`
- Create: `load/p7/summarize.sh`

**Interfaces:**
- Consumes: Task 1 의 info `"threads":{"virtual":"true|false"}`.
- Produces: `VT=1 RAW_SUB=p7 bash load/verification/run.sh <blocking|async> …`, `VT=1 RAW_SUB=p7 bash load/consumer/run.sh blocking …`, JSON 키 `virtual_threads` · `pinned_events`, `bash load/p7/summarize.sh` (환경변수 `RAW_DIR` 로 시험 폴더). Task 4 가 그대로 쓴다.

- [ ] **Step 1: `load/lib/vt.sh` 를 만든다**

```bash
# shellcheck shell=bash
# P7 가상 스레드 회차 공통 (설계 §6-3) — load/verification/run.sh · load/consumer/run.sh 가 source 한다.
# VT=1 이면 앱에 --spring.threads.virtual.enabled=true, JVM 에 JFR 녹화(default 설정 = jdk.VirtualThreadPinned 20ms 기준).
# Git Bash 의 kill 은 Windows 프로세스를 바로 끝내(종료 훅 없음) 녹화가 남지 않는다 → 앱을 내리기 전에 vt_pinned 로 덤프 · 집계한다.

vt_validate() {   # $1 = 이 판에 VT=1 을 허용하면 1
  case "${VT:-0}" in
    0) ;;
    1) [ "$1" = 1 ] || { echo "VT=1 은 웹 MVC 판(blocking · async) · 컨슈머 blocking 에만 쓴다 (설계 §6-3)" >&2; exit 2; } ;;
    *) echo "VT 는 0 또는 1: ${VT}" >&2; exit 2 ;;
  esac
}
vt_bool() { if [ "${VT:-0}" = 1 ]; then echo true; else echo false; fi; }
vt_tag() { if [ "${VT:-0}" = 1 ]; then echo "-vt"; fi; }
raw_sub_validate() {   # $1 = RAW_SUB 값
  [[ "$1" =~ ^p[0-9]+(-[0-9]+)?$ ]] || { echo "RAW_SUB 은 p7 · p6-2 처럼: $1" >&2; exit 2; }
}
# 배열 VT_JVM_ARGS · VT_APP_ARGS 를 채운다 (VT=0 이면 빈 배열). $1 = JFR 파일 (저장소 루트 기준 상대 경로)
vt_args() {
  VT_JVM_ARGS=(); VT_APP_ARGS=()
  [ "${VT:-0}" = 1 ] || return 0
  VT_JVM_ARGS=("-XX:StartFlightRecording=name=p7,settings=default,filename=$1")
  VT_APP_ARGS=(--spring.threads.virtual.enabled=true)
}
# 포트 $1 의 앱 JFR 을 $2 로 덤프하고 jdk.VirtualThreadPinned 수를 표준출력에. 0 이 아니면 스택(맨 위 8줄)을 $3 에. 실패하면 return 1
vt_pinned() {
  local port=$1 file=$2 stacks=$3 bin wpid n
  bin=$(dirname "$(command -v java)")
  wpid=$(netstat -ano | grep -E "TCP +[^ ]+:$port +[^ ]+ +LISTENING" | awk '{ print $NF }' | head -1)
  [ -n "$wpid" ] || { echo "vt_pinned: 포트 $port 의 PID 를 찾지 못했다" >&2; return 1; }
  "$bin/jcmd" "$wpid" JFR.dump name=p7 filename="$(pwd -W)/$file" >/dev/null || return 1
  n=$("$bin/jfr" summary "$file" | tr -d '\r' | awk '$1 == "jdk.VirtualThreadPinned" { print $2 }')
  n=${n:-0}
  [[ "$n" =~ ^[0-9]+$ ]] || { echo "vt_pinned: 이벤트 수를 읽지 못했다: $n" >&2; return 1; }
  if [ "$n" != 0 ]; then "$bin/jfr" print --events jdk.VirtualThreadPinned --stack-depth 8 "$file" > "$stacks"; fi
  echo "$n"
}
```

- [ ] **Step 2: `verification.js` 의 요약 경로가 `OUT_SUB` 를 따르게**

`load/verification/verification.js` 93번째 줄:
```js
    [`/out/p6-2/${RUN}.k6.json`]: JSON.stringify(result, null, 2),
```
→
```js
    [`/out/${__ENV.OUT_SUB || 'p6-2'}/${RUN}.k6.json`]: JSON.stringify(result, null, 2),   // OUT_SUB = run.sh 의 RAW_SUB (P7)
```

- [ ] **Step 3: `load/verification/run.sh` 를 고친다**

(1) 머리 주석의 "사용" 줄을 바꾼다:
```bash
#   [VT=1] [RAW_SUB=p7] bash load/verification/run.sh <blocking|async|coroutine|reactor> <지연ms 0~2999> <도착률 1~5000> <회차> [톰캣 최대 스레드=200]
#   VT=1: 가상 스레드 + JFR pinning 집계 (MVC 판만, P7 설계 §6-3). RAW_SUB: 원자료 폴더 docs/perf/raw/<RAW_SUB> (기본 p6-2)
```
(2) `[ "$APP" = api ] || [ "$TOMCAT" = 200 ] || …` 줄 바로 뒤에:
```bash
. load/lib/vt.sh
vt_validate "$([ "$APP" = api ] && echo 1 || echo 0)"
RAW_SUB=${RAW_SUB:-p6-2}; raw_sub_validate "$RAW_SUB"
```
(3) 이름 · 원자료 경로:
```bash
NAME="$MODE-d$DELAY-r$RATE"
```
→
```bash
NAME="$MODE$(vt_tag)-d$DELAY-r$RATE"
```
그리고 `RAW=docs/perf/raw/p6-2` → `RAW="docs/perf/raw/$RAW_SUB"`.
(4) `BANK_FILES="$WORK/bank-files"` 줄 뒤에:
```bash
vt_args "$WORK/app.jfr"   # VT=1 이면 VT_JVM_ARGS · VT_APP_ARGS (lib/vt.sh)
```
(5) 앱 기동 줄:
```bash
java -Xms1g -Xmx1g -Dstdout.encoding=UTF-8 -jar "$JAR" "${ARGS[@]}" > "$WORK/app.log" 2>&1 &
```
→
```bash
java -Xms1g -Xmx1g "${VT_JVM_ARGS[@]}" -Dstdout.encoding=UTF-8 -jar "$JAR" "${ARGS[@]}" "${VT_APP_ARGS[@]}" > "$WORK/app.log" 2>&1 &
```
(6) 톰캣 info 확인 줄(`if [ "$APP" = api ]; then grep -qE "\"max\"…`) 바로 뒤에:
```bash
if [ "$APP" = api ]; then grep -qE "\"virtual\":\"?$(vt_bool)\"?[,}]" <<<"$INFO" || die "앱의 가상 스레드가 $(vt_bool) 가 아니다: $INFO"; fi
```
(7) k6 실행의 `-e` 들에 `-e OUT_SUB="$RAW_SUB"` 를 더한다.
(8) `WARNS=$(grep -cE …)` 줄 바로 뒤, `stop_pid "$APP_PID"` 앞에:
```bash
PINNED=null
if [ "${VT:-0}" = 1 ]; then PINNED=$(vt_pinned "$PORT" "$WORK/app.jfr" "$WORK/pinned.txt") || die "JFR 덤프 · pinning 집계 실패 — $WORK"; fi
```
(9) JSON 첫 줄 `"run": …, "round": "$ROUND",` 뒤에 `"virtual_threads": $(vt_bool), "pinned_events": $PINNED,` 를 더한다 (같은 줄 끝).
(10) 마지막 echo 를 `… · 스레드 최대 $THREADS_MAX · pinned $PINNED · 시스템 CPU 평균 …` 로.

- [ ] **Step 4: `load/consumer/run.sh` 를 고친다**

(1) 머리 주석 "사용" 첫 줄:
```bash
#   [VT=1] [RAW_SUB=p7] bash load/consumer/run.sh <blocking|coroutine> <지연ms 0~2999> <회차> [상한=64] [hikari=20]
#   VT=1: 처리 단계 API 를 가상 스레드 + JFR pinning 집계로 (blocking 만, P7 설계 §6-3). RAW_SUB: 원자료 폴더 (기본 p6-1)
```
(2) `case "$MODE" in blocking|coroutine) …` 줄 바로 뒤에:
```bash
. load/lib/vt.sh
vt_validate "$([ "$MODE" = blocking ] && echo 1 || echo 0)"
RAW_SUB=${RAW_SUB:-p6-1}; raw_sub_validate "$RAW_SUB"
```
(3) 이름: `if [ "$MODE" = blocking ]; then NAME="blocking-d$DELAY-h$HIKARI-$ROUND"; …` → `NAME="blocking$(vt_tag)-d$DELAY-h$HIKARI-$ROUND"` (blocking 분기만). `RAW=docs/perf/raw/p6-1` → `RAW="docs/perf/raw/$RAW_SUB"`.
(4) `BANK_FILES=…` 줄 뒤에 `vt_args "$WORK/app.jfr"`.
(5) `API_PID=; BANK_PID=; SAMPLERS=()` 줄을 `API_PID=; BANK_PID=; SAMPLERS=(); API_JVM_ARGS=()` 로, `start_api` 의 java 줄을:
```bash
  java -Xms1g -Xmx1g "${API_JVM_ARGS[@]}" -Dstdout.encoding=UTF-8 -jar "$JAR" "${COMMON[@]}" "$@" > "$log" 2>&1 &
```
(6) "4. 처리만" 의 `start_api "$WORK/api-process.log" …` 바로 앞에 `API_JVM_ARGS=("${VT_JVM_ARGS[@]}")   # 발행 단계는 끈 채, 처리 단계만 VT · JFR`, 그 호출 인자 끝에 `"${VT_APP_ARGS[@]}"` 를 더한다.
(7) Hikari info 확인 줄 뒤에:
```bash
grep -qE "\"virtual\":\"?$(vt_bool)\"?[,}]" <<<"$INFO" || die "앱의 가상 스레드가 $(vt_bool) 가 아니다: $INFO"
```
(8) 처리 단계의 `WARNS=$(grep -cE …)` 줄 뒤, `stop_api` 앞에:
```bash
PINNED=null
if [ "${VT:-0}" = 1 ]; then PINNED=$(vt_pinned "$API_PORT" "$WORK/app.jfr" "$WORK/pinned.txt") || die "JFR 덤프 · pinning 집계 실패 — $WORK"; fi
```
(9) JSON 첫 줄 끝에 `"virtual_threads": $(vt_bool), "pinned_events": $PINNED,`.

- [ ] **Step 5: `load/p7/summarize.sh` 를 만든다**

```bash
#!/usr/bin/env bash
# P7 측정 결과를 표(마크다운)로 → docs/perf/p7-virtual-threads.md (설계 §6).
# 사용: bash load/p7/summarize.sh   (docs/perf/raw/p7/*.json — .invalid · .k6.json 은 이름 규칙으로 빠진다. RAW_DIR 은 시험용)
# 회차가 숫자인 것만 표에 넣는다 (drift 는 아래 한 줄, smoke 는 빠짐). 지표마다 따로 중앙값, pinned 는 회차 중 최댓값 (VT 를 끈 판은 '-').
set -euo pipefail
cd "$(dirname "$0")/../.."
RAW=${RAW_DIR:-docs/perf/raw/p7}

num() { grep -o "\"$2\": *[-0-9.eE+]*" "$1" | head -1 | sed 's/.*: *//'; }   # null → 빈 값
median() { awk '/^[-+]?[0-9]/' | sort -g | awk '{ v[NR] = $1 } END { if (NR == 0) print "-"; else print v[int((NR + 1) / 2)] }'; }
maxv() { awk '/^[-+]?[0-9]/' | sort -g | awk 'END { if (NR == 0) print "-"; else print $1 }'; }
fmt() { if [[ "$1" =~ ^[-+]?[0-9] ]]; then printf "%.${2}f" "$1"; else printf -- "-"; fi; }
maxdev() { awk '/^[-+]?[0-9]/' | awk -v m="$1" '{ d = (m == 0) ? 0 : ($1 - m) / m * 100; if (d < 0) d = -d; if (d > x) x = d } END { printf "%.0f", x }'; }
files() { ls "$RAW/$1-"[0-9]*.json 2>/dev/null || true; }   # $1 = 회차 앞까지의 이름
med() { local k=$1; shift; for f in "$@"; do num "$f" "$k"; done | median; }
mx() { local k=$1; shift; for f in "$@"; do num "$f" "$k"; done | maxv; }

echo; echo "### 웹 1원인증 — 은행 지연 500ms (요청 하나 ≈ 1,000ms + α)"; echo
echo "| 판 · 도착률 | 회차 | 201 처리량 (건/s) | p50 (ms) | p95 (ms) | p99 (ms) | 플랫폼 스레드 최대 | 시스템 CPU 평균 (%) | 시간 초과 | dropped | pinned (최대) |"
echo "|---|---|---|---|---|---|---|---|---|---|---|"
for r in 400 800; do
  for v in blocking blocking-vt async async-vt coroutine; do
    mapfile -t fs < <(files "$v-d500-r$r")
    [ ${#fs[@]} -gt 0 ] || continue
    echo "| $v · $r | ${#fs[@]} | $(fmt "$(med rps_201 "${fs[@]}")" 1) | $(fmt "$(med p50_201 "${fs[@]}")" 1) | $(fmt "$(med p95_201 "${fs[@]}")" 1) | $(fmt "$(med p99_201 "${fs[@]}")" 1) | $(fmt "$(med threads_max "${fs[@]}")" 0) | $(fmt "$(med sys_cpu_avg "${fs[@]}")" 1) | $(fmt "$(med timeouts "${fs[@]}")" 0) | $(fmt "$(med dropped_iterations "${fs[@]}")" 0) | $(fmt "$(mx pinned_events "${fs[@]}")" 0) |"
  done
done
d="$RAW/blocking-vt-d500-r400-drift.json"
if [ -f "$d" ]; then echo; echo "- drift (마지막에 blocking-vt · 400 한 번 더): 201 $(fmt "$(num "$d" rps_201)" 1)건/s · p99 $(fmt "$(num "$d" p99_201)" 1)ms · pinned $(fmt "$(num "$d" pinned_events)" 0)"; fi

echo; echo "### 출금 컨슈머 — 은행 지연 100ms · Hikari 20"; echo
echo "| 판 | 회차 | N | 처리량 (건/s) | p50 (ms) | p95 (ms) | 시스템 CPU 평균 (%) | 은행 최대 동시 | pinned (최대) | 편차 |"
echo "|---|---|---|---|---|---|---|---|---|---|"
for v in blocking blocking-vt; do
  mapfile -t fs < <(files "$v-d100-h20")
  [ ${#fs[@]} -gt 0 ] || continue
  tps=$(med tps "${fs[@]}"); p95=$(med p95_ms "${fs[@]}")
  d1=$(for f in "${fs[@]}"; do num "$f" tps; done | maxdev "$tps")
  d2=$(for f in "${fs[@]}"; do num "$f" p95_ms; done | maxdev "$p95")
  echo "| $v | ${#fs[@]} | $(fmt "$(med n "${fs[@]}")" 0) | $(fmt "$tps" 1) | $(fmt "$(med p50_ms "${fs[@]}")" 1) | $(fmt "$p95" 1) | $(fmt "$(med sys_cpu_avg "${fs[@]}")" 1) | $(fmt "$(med bank_peak "${fs[@]}")" 0) | $(fmt "$(mx pinned_events "${fs[@]}")" 0) | $(( d1 > d2 ? d1 : d2 ))% |"
done
```

- [ ] **Step 6: 문법 · 인자 검사**

Run (Git Bash, 저장소 루트):
```bash
bash -n load/lib/vt.sh && bash -n load/verification/run.sh && bash -n load/consumer/run.sh && bash -n load/p7/summarize.sh && echo SYNTAX-OK
VT=1 bash load/verification/run.sh coroutine 500 400 smoke; echo "exit=$?"
VT=2 bash load/verification/run.sh blocking 500 400 smoke; echo "exit=$?"
RAW_SUB=../x bash load/verification/run.sh blocking 500 400 smoke; echo "exit=$?"
VT=1 bash load/consumer/run.sh coroutine 100 smoke; echo "exit=$?"
```
Expected: `SYNTAX-OK`, 그리고 네 번 모두 `exit=2` 와 해당 오류 문구. 인자 검사는 사전 확인 · 정리보다 앞이라 아무것도 건드리지 않는다 (확인: `load/verification/run.sh` 의 판 · 지연 검사 블록, `load/consumer/run.sh` 의 `case "$MODE"` 블록 바로 뒤에 넣었는지).

- [ ] **Step 7: 표 스크립트 시험 (임시 폴더)**

```bash
T=$(mktemp -d)
for r in 1 2 3; do
  printf '{ "run": "x", "virtual_threads": false, "pinned_events": null, "rps_201": 53.%s, "p50_201": 8399, "p95_201": 9800, "p99_201": 9900, "threads_max": 226, "sys_cpu_avg": 24, "timeouts": 4999, "dropped_iterations": 6401 }\n' "$r" > "$T/blocking-d500-r400-$r.json"
  printf '{ "run": "x", "virtual_threads": true, "pinned_events": %s, "rps_201": 400.0, "p50_201": 1019, "p95_201": 1026, "p99_201": 1036, "threads_max": 60, "sys_cpu_avg": 37, "timeouts": 0, "dropped_iterations": 150 }\n' "$(( r - 1 ))" > "$T/blocking-vt-d500-r400-$r.json"
  printf '{ "run": "x", "virtual_threads": true, "pinned_events": 0, "n": 1500, "tps": 2%s.0, "p50_ms": 117, "p95_ms": 127, "sys_cpu_avg": 20, "bank_peak": 3 }\n' "$r" > "$T/blocking-vt-d100-h20-$r.json"
done
printf '{ "rps_201": 399.9, "p99_201": 1040, "pinned_events": 0 }\n' > "$T/blocking-vt-d500-r400-drift.json"
printf '{ "rps_201": 1 }\n' > "$T/blocking-vt-d500-r400-smoke.json"
RAW_DIR="$T" bash load/p7/summarize.sh; rm -rf "$T"
```
Expected: 웹 표에 `blocking · 400 | 3 | 53.2 | …| - |`(pinned 열 `-`), `blocking-vt · 400 | 3 | 400.0 | … | 2 |` 두 줄, drift 한 줄(`399.9건/s`), smoke 는 없음. 컨슈머 표에 `blocking-vt | 3 | 1500 | 22.0 | … | 3 | 0 | 5% |` 한 줄 (tps 21 · 22 · 23 → 중앙값 22, 편차 5%).

- [ ] **Step 8: 커밋**

```bash
git add load/lib/vt.sh load/verification/run.sh load/verification/verification.js load/consumer/run.sh load/p7/summarize.sh
git commit -m "feat(p7): 측정 도구에 VT=1 (가상 스레드 + JFR pinning 집계) · RAW_SUB, P7 표 스크립트

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: 측정 (컨트롤러가 직접 — 사용자 PC 의 개발 인프라를 쓴다)

**Files:**
- Create: `docs/perf/raw/p7/*.json` (회차마다 하나)

이 Task 는 하위 에이전트에 맡기지 않는다. 개발 DB · Kafka 를 쓰고 한 시간 가까이 걸린다.

- [ ] **Step 1: 사전 준비**
  - `./gradlew build` 통과(테스트 524개)를 확인한다.
  - `./gradlew :apps:api:bootJar :apps:api-reactive:bootJar :apps:mock-bank:installDist`
  - 인프라: `docker compose -f infra/docker-compose.yml --env-file infra/.env up -d mysql kafka` (이미 떠 있으면 그대로).
  - 개발용 API(8080) · IntelliJ 실행 · 측정 포트(18080 · 18081 · 9190)가 비어 있는지 확인한다. 컨슈머 측정은 8080 이 꺼져 있어야 한다 — 켜져 있으면 사용자에게 끄도록 부탁한다.
  - `java -version` 이 21, `jcmd` · `jfr` 가 `java` 와 같은 폴더에 있는지 확인한다.

- [ ] **Step 2: 시험 회차 (결과는 남기지 않는다)**
```bash
RAW_SUB=p7 VT=1 bash load/verification/run.sh blocking 500 100 smoke
RAW_SUB=p7 VT=1 bash load/consumer/run.sh blocking 100 smoke
```
Expected: 두 회차 모두 끝까지 가고 JSON 에 `"virtual_threads": true, "pinned_events": <수>` 가 있다. 확인 뒤 `rm docs/perf/raw/p7/*smoke*`. pinning 이 0 이 아니면 `build/load-p6*/…-smoke/pinned.txt` 를 보고 기록한 뒤 본 측정을 그대로 진행한다 (설계 §6-5 기준 1 판정 재료).

- [ ] **Step 3: 웹 본 측정 (약 35분)** — 회차를 판 사이에 번갈아:
```bash
export RAW_SUB=p7
for round in 1 2 3; do
  bash load/verification/run.sh blocking 500 400 $round
  VT=1 bash load/verification/run.sh blocking 500 400 $round
  bash load/verification/run.sh async 500 400 $round
  VT=1 bash load/verification/run.sh async 500 400 $round
  bash load/verification/run.sh coroutine 500 400 $round
  VT=1 bash load/verification/run.sh blocking 500 800 $round
  bash load/verification/run.sh async 500 800 $round
  VT=1 bash load/verification/run.sh async 500 800 $round
  bash load/verification/run.sh coroutine 500 800 $round
done
VT=1 bash load/verification/run.sh blocking 500 400 drift
bash load/verification/run.sh cleanup
```
`.json.invalid` 가 생기면 원인을 기록하고 그 칸만 같은 회차 이름으로 다시 돈다.

- [ ] **Step 4: 컨슈머 본 측정 (약 15분)**
```bash
export RAW_SUB=p7
for round in 1 2 3; do
  bash load/consumer/run.sh blocking 100 $round
  VT=1 bash load/consumer/run.sh blocking 100 $round
done
bash load/consumer/run.sh cleanup
```
`cleanup` 이 개발용 그룹 `transfer-processor` 의 밀린 메시지를 안내하면, **사용자 OK 를 받은 뒤에만** 개발용 API 를 끈 채 `bash load/consumer/run.sh reset-dev-group` 을 친다.

- [ ] **Step 5: 손 확인 (설계 §7, 사용자와 함께 — 머지 전)**
  - 사용자가 개발용 API 를 `--spring.threads.virtual.enabled=true` 로 띄운다 (IntelliJ 실행 구성의 프로그램 인자 또는 `java -jar apps/api/build/libs/api-0.1.0-SNAPSHOT.jar --spring.threads.virtual.enabled=true`), 개발용 모의 은행(9090)과 함께.
  - `curl -s localhost:8080/actuator/info` 에 `"threads":{"virtual":"true"}`.
  - 1원인증 → 201 → 모의 은행 콘솔의 코드 → `/confirm` → `VERIFIED` → 그 계좌로 출금 → `COMPLETED`. Git Bash 의 `curl -d` 에는 한글 대신 JSON 유니코드 이스케이프를 쓴다 (HANDOVER).
  - 결과를 Task 5 의 PR 본문 "손 확인" 에 옮긴다.

- [ ] **Step 6: 표를 만들고 커밋**
```bash
bash load/p7/summarize.sh
git add docs/perf/raw/p7
git commit -m "perf(p7): 가상 스레드 측정 원자료 — 웹 28회 · 컨슈머 6회

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: 결과 문서 · ADR-0014 · 운영 문서

**Files:**
- Create: `docs/perf/p7-virtual-threads.md`, `docs/adr/0014-virtual-threads.md`
- Modify: `OPERATION.md` (§13 추가), `ARCHITECTURE.md`, `README.md`, `SETUP.md`, `HANDOVER.md`, `CHANGELOG.md`, `docs/superpowers/specs/2026-11-09-p7-virtual-threads-design.md` (상태 줄)

**Interfaces:**
- Consumes: Task 2 콘솔 결과(V1 · V2 · V3), Task 4 의 `bash load/p7/summarize.sh` 출력 · 원자료 · `build/load-p6*/<이름>/pinned.txt`(있으면).

- [ ] **Step 1: 결과 문서 `docs/perf/p7-virtual-threads.md`** — P6-2 결과 문서(`docs/perf/p6-2-web-reactive.md`)와 같은 짜임:
  1. 한 줄 결론 (웹 · 컨슈머 · pinning, 확인 / 추정)
  2. 무엇을 왜 재는가 (설계 §1 가설 1 · 2, 범위를 줄인 이유)
  3. 그림 — 플랫폼 스레드 vs 가상 스레드가 은행을 기다리는 모습 (mermaid `sequenceDiagram` 두 개: Tomcat 스레드 200개가 `join()` 에서 막힘 / 가상 스레드가 `join()` 에서 캐리어를 내려놓음), 컨슈머는 "파티션 3 = 동시 3" 그림 하나
  4. 조건 (P6-2 §4 표를 따르고 JFR · `RAW_SUB` · 측정 시각 · 코드 커밋을 더함)
  5. 결과 (`load/p7/summarize.sh` 출력 그대로 + drift)
  6. 진행 조건 실험 결과 (V1 · V2 · V3)
  7. 예상과 비교 (설계 §6-6 표 대 측정)
  8. 해석 — 블로킹 + VT 가 코루틴만큼 받는 이유(리틀의 법칙: 동시에 기다리는 수 = 처리량 × 지연), 컨슈머가 그대로인 이유, 플랫폼 스레드 최대가 줄어든 이유
  9. 언제 무엇을 쓰나 (블로킹 레거시 · 코루틴 · WebFlux · 가상 스레드)
  10. 한계 (설계 §8)
  11. 원자료 (`docs/perf/raw/p7/`, 회차 이름 규칙)

- [ ] **Step 2: ADR-0014 `docs/adr/0014-virtual-threads.md`** (상태: 제안) — 맥락(P6 비교 · 공고), 결정(설계 §6-5 기준으로 판정한 결과와 근거 숫자), 무엇이 가상 스레드가 되나(V1), pinning(V2 · 측정), 결과(풀이 없어진다 · 동시 호출 상한은 범위 밖 · Java 25 다시 볼 조건), 대안(코루틴 · WebFlux · 실행기 크기 조정). 기존 ADR 머리 형식(`docs/adr/0013-…`)을 따른다. 기본값을 실제로 바꾸는 일은 이 PR 에서 하지 않는다 (설계 §6-5).

- [ ] **Step 3: OPERATION §13 "가상 스레드 측정 (P7)"** — 13-1 켜고 끄기(`--spring.threads.virtual.enabled=true`, `/actuator/info` 의 `threads.virtual`), 13-2 측정 한 회(`VT` · `RAW_SUB` · JFR · `pinned.txt` 위치), 13-3 측정 순서(Task 4 의 반복문 그대로), 13-4 끝난 뒤(cleanup · 개발용 그룹 안내).

- [ ] **Step 4: 나머지 문서**
  - `ARCHITECTURE.md`: §3 1원인증 흐름 아래 "판" 문단(177행 부근)과 §6 P3-2 처리 흐름의 컨슈머 문단(231행 부근)에 한 줄씩 — "`spring.threads.virtual.enabled=true` 면 같은 코드가 가상 스레드에서 돈다 (P7, ADR-0014)". 264행 "스케줄러 스레드 4개" 에 "가상 스레드를 켜면 풀 크기 대신 작업마다 가상 스레드" 를 덧붙인다. §8 설계 결정 색인에 ADR-0014.
  - `README.md`: 로드맵 P7 체크 + `→ [ADR-0014](…) · [측정 결과](docs/perf/p7-virtual-threads.md)`, 테스트 수 524.
  - `SETUP.md`: 테스트 수 524.
  - `HANDOVER.md`: §1 진행 상황에 P7, §3 다음 할 일 5번을 "P8" 로 바꾸고, "### P7 범위 밖으로 남긴 것" (설계 §1 범위 밖 목록 + 측정에서 새로 본 것). 중복된 "### P6-2 범위 밖으로 남긴 것" 두 절을 하나로 합친다.
  - `CHANGELOG.md`: P7 항목 (날짜, PR 링크는 PR 을 연 뒤 채운다).
  - 설계 문서 상태 줄: `상태: 구현 완료 (2026-11-xx) · 결정 기록: ADR-0014`.

- [ ] **Step 5: 확인 · 커밋**

Run: `./gradlew build` → 통과, 테스트 524. 문서의 숫자가 `bash load/p7/summarize.sh` 출력 · 원자료와 같은지 다시 본다.
```bash
git add docs OPERATION.md ARCHITECTURE.md README.md SETUP.md HANDOVER.md CHANGELOG.md
git commit -m "docs(p7): 가상 스레드 측정 결과 · ADR-0014(제안) · OPERATION §13 · 로드맵

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
