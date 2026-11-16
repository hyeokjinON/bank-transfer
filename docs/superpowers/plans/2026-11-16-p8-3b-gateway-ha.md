# P8-3b 게이트웨이 이중화 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 게이트웨이 2대 중 리더 1대만 은행에 붙고(K8s Lease 선출), 전문번호는 선출과 독립된 ConfigMap 구간 예약으로 기관 · 일자별 유일하게 만든다. 모의 은행에 실무 제약(세션 하나 · 번호 유일)을 옵션으로 재현해 이전 구성(H0)과 실험으로 비교한다.

**Architecture:** `apps/bank-gateway` 에 `BankConnectionOwner`(지금 은행 연결의 단일 진실, `BankGateway` 구현)를 두고, 선출이 켜지면 fabric8 `LeaderElector` 콜백이 Owner 의 임기를 열고 닫는다. 트래픽은 리더가 자기 Pod 에 붙이는 라벨 `bank-transfer/gateway-role=leader` 로 Service selector 가 고른다(둘 다 Ready). 전문번호는 `modules/bank-gateway` 의 `TranNoSource` 포트, K8s 에서는 ConfigMap `resourceVersion` CAS 로 1,000개씩 예약한다.

**Tech Stack:** Kotlin · Spring Boot 4.1.1 · Netty 4.2 · fabric8 kubernetes-client 8.0.0 (JDK HttpClient) · kind · kustomize · Bash · k6

**Spec:** `docs/superpowers/specs/2026-11-16-p8-3b-gateway-ha-design.md` (§16 계획 단계 변경 포함)

## Global Constraints

- fabric8 버전 고정: `io.fabric8:kubernetes-client:8.0.0` (2026-09-24 출시, 2주 이상 지난 안정판) — `kubernetes-httpclient-vertx-5` 는 exclude, `io.fabric8:kubernetes-httpclient-jdk:8.0.0` 를 runtimeOnly. 테스트는 `io.fabric8:kubernetes-server-mock:8.0.0`. fabric8 은 `apps/bank-gateway` 에만 (통신 모듈 `modules/bank-gateway` 는 K8s 를 모른다 — 결정 17).
- K8s 클라이언트는 빨리 실패: 요청 재시도 0 · 연결 2s · 요청 5s (재시도는 선출기 재시도 주기가 맡는다 — 자기 차단 시각을 흐리지 않게, 스펙 §16).
- `bank.gateway.leader-election.enabled` 기본 **false** — IntelliJ · Compose · 기존 테스트(623개) 동작 불변 (결정 7).
- Lease 15s · 갱신 기한 10s · 재시도 2s · 비우기 5s · 구간 1,000개 · 미리 예약 100개 전 · 예약 재시도 5회 (결정 6 · 8 · 11).
- 이름: Lease `bank-gateway-leader`, ConfigMap `bank-gateway-tran-no`(키 `date` · `reservedTo`), 라벨 `bank-transfer/gateway-role=leader`, ServiceAccount `bank-gateway`.
- 응답코드 `3001` = 전문번호 중복(처리 안 함) → 게이트웨이는 `BankNotSentException` → `503 BANK_NOT_SENT` (결정 15). 규격 버전은 **v0.4** (v0.3 은 대사 파일로 이미 쓰였다 — 스펙 §16).
- 날짜는 KST(`Asia/Seoul`) `yyyyMMdd`. 테스트 JVM 은 `-Duser.timezone=UTC` 라 시간대에 기대지 않는다.
- 모의 은행 옵션은 기본 꺼짐, K8s 매니페스트에서만 켠다: `--single-session` · `--session-idle-timeout=60s` · `--unique-tran-no`.
- 로그 문구 고정 (실험 도구가 grep 한다): 모의 은행 `session opened sessions=N max=M` · `session closed sessions=N max=M` · `session rejected sessions=N max=M` · `session evicted idle` · `duplicate tran no`; 게이트웨이 `leader election: became leader` · `leader election: lost leadership` · `leader election: stepping down`.
- 주석 · 테스트 이름 · 문서는 한국어 (기존 스타일). 테스트는 kotlin.test + 백틱 한국어 이름.
- 커밋: 파일을 이름으로 stage (`git add -A` 금지). `.run/2. API (8080).run.xml` 은 절대 stage 하지 않는다(` M` 유지). `postman/` 의 Postman 재작성 파일 커밋 금지. `git stash` 금지.
- 커밋 메시지 끝: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- 새 yaml · sh · awk 는 LF (`.gitattributes` 가 처리 — 확인만).
- 전 직장 코드 · 민감정보(IP · 계정 · 키) 를 저장소에 넣지 않는다. `infra/.env` 출력 · 커밋 금지.

---

## 확인한 사실 (계획 근거)

| 항목 | 사실 | 근거 |
|---|---|---|
| fabric8 8.0.0 Jackson | **Jackson 3 (`tools.jackson`) 3.2.3** + annotations 2.22. 스펙 §13 의 "Jackson 2 공존" 걱정은 해당 없음 — 대신 Boot 4.1.1 이 관리하는 Jackson 3.1.5 로 내려 맞춰지는 버전 차가 위험 (미확인 → Task 1) | 조사 (Maven Central POM) |
| fabric8 HTTP | 기본 `kubernetes-httpclient-vertx-5` (Netty 끌어옴). `kubernetes-httpclient-jdk` 는 Netty 없음 | 조사 |
| fabric8 선출 API | `LeaderElectionConfigBuilder`(withName · withLock · withLeaseDuration · withRenewDeadline · withRetryPeriod · withLeaderCallbacks · withReleaseOnCancel), `LeaderCallbacks(onStart, onStop, onNewLeader)`, `client.leaderElector().withConfig(cfg).build().start()` → `CompletableFuture`. 검증: leaseDuration > renewDeadline > retryPeriod × 1.2 이상. 선출기는 한 번 쓰고 버린다. cancel + releaseOnCancel 이면 holder 비움 + onStopLeading 호출 | 조사 |
| ConfigMap CAS | GET → resourceVersion 유지한 채 수정 → `resource(cm).update()` → 낡았으면 409. `replace()` 는 쓰지 않는다. `create()` 는 있으면 409 (미확인 → Task 1) | 조사 |
| 가짜 API 서버 | `kubernetes-server-mock` CRUD 모드는 PUT 의 resourceVersion 을 검사(409). Lease · JSON merge patch 지원은 미확인 → Task 1. Vert.x · Netty 를 테스트 클래스패스에 끌어온다 | 조사 |
| 의존성 관리 | 루트 `build.gradle.kts` 가 모든 코드 프로젝트에 `io.spring.dependency-management` 1.1.7 + Boot BOM. Jackson 덮어쓰기가 필요하면 프로젝트별 `dependencyManagement { imports { mavenBom(SpringBootPlugin.BOM_COORDINATES) { bomProperty("jackson-bom.version", "3.2.3") } } }` | 코드 확인 |
| 전문번호 지금 | `BankGatewayClient` 의 `AtomicLong`(인스턴스마다 1부터). 폴링(`WRITER_IDLE`)도 `newHeader` → 번호 발급이 예외면 `exceptionCaught` 가 회선을 끊는다 | `BankGatewayClient.kt:97 · 235 · 252` |
| 게이트웨이 앱 조립 | `BankGatewayClient` 빈(`destroyMethod="close"`)을 컨트롤러가 `BankGateway` 로 주입. 테스트 3개 파일이 `getBean(BankGatewayClient::class.java)` | `GatewayConfiguration.kt` · `GatewayContractTest` · `GatewayHttpContractTest` · `ToxiproxyGatewayTest` |
| `BankGatewayClient(` 다른 사용처 | `apps/api` direct 모드 · api-reactive · 여러 테스트 → 새 생성자 인자는 맨 뒤 + 기본값 | grep |
| 규격 버전 | `docs/protocol.md` 변경 이력에 v0.3 (2026-10-29 대사 파일) 이 이미 있다 → 이번은 **v0.4** | 코드 확인 |
| 모의 은행 실행 인자 | 이미지 CMD `["9090","--bank-files=/data/bank-files"]` — 매니페스트 `args` 를 주면 CMD 를 덮으므로 두 값을 다시 적는다 | P8-3a |
| 스모크 | [0] 은 `readyReplicas == replicas` (2대 · 둘 다 Ready 면 그대로 통과), [6] 은 `k exec deploy/bank-gateway` (아무 Pod — 리더로 바꾼다), [8] 은 `bank-gateway:8082` 막힘 확인 (리더 엔드포인트가 있어야 28) | `infra/k8s/smoke.sh` |
| 실험 도구 | `run.sh` 사전 확인 `ready == replicas`, `GW_NODE` = 첫 게이트웨이 Pod 노드, 결과 폴더 `docs/perf/raw/p8-3a` 고정, k6 `OUT_SUBDIR` 환경변수 | `load/k8s/run.sh` |
| 결과 모름 건수 | `transfer.inquiry_attempts > 0` = 결과조회를 한 번 이상 보낸 건 (안 보냄은 시도로 세지 않는다) | `V1__init.sql:29` |
| Boot 종료 phase | 웹 서버 graceful 종료 = `SmartLifecycle.DEFAULT_PHASE - 1024`. 높은 phase 가 먼저 멈춘다 → 우리 훅은 `DEFAULT_PHASE` | Spring 규칙 (Boot 4 클래스 위치는 Task 5 에서 확인) |

## 파일 구조

| 파일 | 책임 | Task |
|---|---|---|
| `apps/bank-gateway/build.gradle.kts` | fabric8 의존성 | 1 |
| `apps/bank-gateway/src/test/.../gatewayapp/k8s/MockK8s.kt` | 가짜 API 서버 (테스트가 켜고 끈다) | 1 |
| `apps/bank-gateway/src/test/.../gatewayapp/k8s/Fabric8ContractTest.kt` | fabric8 가정 고정 (CAS 409 · create 409 · Lease 선출 · merge patch) | 1 |
| `modules/bank-protocol/.../protocol/Header.kt` | `DUPLICATE_TRAN_NO = "3001"` | 2 |
| `apps/mock-bank/.../MockBankOptions.kt` · `MockBankServer.kt` | 옵션 3개 · 세션 로그 | 2 |
| `apps/mock-bank/src/test/.../MockBankConstraintsTest.kt` | 옵션 3개 테스트 | 2 |
| `docs/protocol.md` | v0.4 | 2 |
| `modules/bank-gateway/.../gateway/TranNoSource.kt` | 포트 + 메모리 구현 | 3 |
| `modules/bank-gateway/.../gateway/BankGatewayClient.kt` | 번호를 포트에서, `3001` → 안 보냄, 폴링 보호, 동기 예외 → future | 3 |
| `apps/bank-gateway/.../gatewayapp/leader/ConfigMapTranNoSource.kt` | 구간 예약 | 4 |
| `apps/bank-gateway/.../gatewayapp/leader/BankConnectionOwner.kt` | 임기 · 교대 순서 1 ~ 3 | 5 |
| `apps/bank-gateway/.../gatewayapp/leader/AlwaysLeader.kt` | 선출 꺼짐의 종료 훅 + `LEADERSHIP_PHASE` | 5 |
| `apps/bank-gateway/.../gatewayapp/config/GatewayConfiguration.kt` | 조립 · `LeaderElectionProperties` | 5 · 6 |
| `apps/bank-gateway/.../gatewayapp/health/BankConnectionHealthIndicator.kt` | standby UP · 리더 끊김 DOWN | 5 |
| `apps/bank-gateway/.../gatewayapp/leader/GatewayLeaderElection.kt` | fabric8 선출 · 교대 순서 4 · 5 | 6 |
| `apps/bank-gateway/.../gatewayapp/leader/LeaderLabel.kt` | 자기 Pod 라벨 맞추기 | 6 |
| `infra/k8s/base/{bank-gateway,rbac-gateway,pdb,mock-bank,kustomization}.yaml` · `overlays/experiment-gateway-single/` | 매니페스트 | 7 |
| `infra/k8s/smoke.sh` · `lib.sh` | [6] 리더 · [9] 이중화 · `gw_leader` | 7 |
| `load/k8s/run.sh` · `timeline.sh` · `timeline.awk` · `apicut-netpol.yaml` | 실험 H0 ~ H5 도구 | 8 |
| `docs/perf/p8-3b-gateway-ha.md` · `docs/perf/raw/p8-3b/` | 측정 | 9 |
| `docs/adr/0018-gateway-ha-leader-election.md` · OPERATION §16 · README · ARCHITECTURE · HANDOVER · CHANGELOG · `infra/k8s/README.md` | 문서 | 10 |

패키지 약어: `.../gatewayapp/` = `apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/`, 테스트는 `src/test/kotlin/...` 같은 경로. `.../gateway/` = `modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/`. `.../mockbank/` = `apps/mock-bank/src/main/kotlin/io/github/hyeokjinon/banktransfer/mockbank/`.

---

### Task 1: fabric8 의존성 + 가정 확인 (결정 관문)

**Files:**
- Modify: `apps/bank-gateway/build.gradle.kts`
- Create: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/k8s/MockK8s.kt`
- Create: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/k8s/Fabric8ContractTest.kt`

**Interfaces:**
- Produces: `MockK8s` (테스트 공용) — `val client: KubernetesClient`, `val url: String`, `fun createPod(namespace: String, name: String)`, `fun stopApiServer()`, `close()`. Task 4 · 6 이 쓴다.
- Produces (결정): fabric8 이 이 저장소에서 동작한다는 확인. 실패하면 아래 "관문" 대로.

이 Task 는 Maven Central 에서 fabric8 jar 를 받는다 (사용자가 "A. Lease + fabric8" 로 승인한 범위).

- [ ] **Step 1: 의존성 추가**

`apps/bank-gateway/build.gradle.kts` 의 `dependencies { … }` 에서 `implementation("tools.jackson.module:jackson-module-kotlin")` 다음 줄에:

```kotlin
    // P8-3b 리더 선출 · 전문번호 구간 예약 (K8s Lease · ConfigMap). 8.0.0 = Jackson 3 (tools.jackson) — Boot 4 와 같은 계열.
    // 기본 HTTP 구현(vertx-5)은 Netty 를 하나 더 끌어와 빼고, Netty 없는 JDK HttpClient 구현을 쓴다
    implementation("io.fabric8:kubernetes-client:8.0.0") { exclude(group = "io.fabric8", module = "kubernetes-httpclient-vertx-5") }
    runtimeOnly("io.fabric8:kubernetes-httpclient-jdk:8.0.0")
```

`testImplementation("eu.rekawek.toxiproxy:toxiproxy-java:2.1.7")` 다음 줄에:

```kotlin
    testImplementation("io.fabric8:kubernetes-server-mock:8.0.0")   // 가짜 API 서버 (CRUD 모드) — Lease · ConfigMap · Pod 라벨
```

- [ ] **Step 2: 운영 클래스패스에 Vert.x 가 없는지, Jackson 버전 확인**

Run: `./gradlew :apps:bank-gateway:dependencies --configuration runtimeClasspath | grep -iE 'vertx|tools.jackson.core:jackson-databind|io.fabric8:kubernetes-client '`
Expected: `vertx` 줄 없음. `jackson-databind:3.2.3 -> 3.1.5` 처럼 Boot BOM 버전으로 맞춰진 줄 (화살표 표시) — 이 차이가 Step 5 에서 깨지는지 본다. 출력 그대로 보고서에 적는다.

- [ ] **Step 3: 가짜 API 서버 도우미 작성**

`MockK8s.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.k8s

import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.client.Config
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.server.mock.KubernetesCrudDispatcher
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer
import io.fabric8.mockwebserver.Context
import io.fabric8.mockwebserver.MockWebServer

/**
 * fabric8 가짜 API 서버 (CRUD 모드, http). 테스트가 직접 켜고 끈다 — [stopApiServer] 가 "API 서버 단절" 이다 (실험 H5 의 단위 대체).
 * CRUD 모드는 PUT 의 resourceVersion 을 검사해 낡았으면 409 를 준다 (진짜 API 서버와 같다 — Fabric8ContractTest 가 고정).
 * 클라이언트는 앱과 같은 "빨리 실패" 설정 (재시도 없음 · 연결 1초) — fabric8 기본 재시도(지수 백오프)면 단절 테스트가 수십 초 걸린다.
 */
class MockK8s : AutoCloseable {
    private val server = KubernetesMockServer(Context(), MockWebServer(), HashMap(), KubernetesCrudDispatcher(), false)

    init {
        server.init()
    }

    private val serverClient: KubernetesClient = server.createClient()
    val url: String get() = serverClient.configuration.masterUrl
    val client: KubernetesClient = KubernetesClientBuilder().withConfig(
        ConfigBuilder(Config.empty()).withMasterUrl(url).withRequestRetryBackoffLimit(0).withConnectionTimeout(1_000).withRequestTimeout(3_000).build(),
    ).build()

    fun createPod(namespace: String, name: String) {
        client.pods().inNamespace(namespace).resource(
            PodBuilder().withNewMetadata().withName(name).addToLabels("app", "bank-gateway").endMetadata().build(),
        ).create()
    }

    fun stopApiServer() = server.destroy()

    override fun close() {
        client.close()
        serverClient.close()
        runCatching { server.destroy() }   // stopApiServer 로 이미 내렸을 수 있다
    }
}
```

생성자 · 패키지 · `ConfigBuilder` 메서드 이름이 8.0.0 에서 다르면(컴파일 오류) `javap -cp <kubernetes-server-mock jar> io.fabric8.kubernetes.client.server.mock.KubernetesMockServer` 로 CRUD 디스패처를 받는 생성자를 찾아 맞추고, 바꾼 내용을 보고서에 적는다. `init()` 이 없으면 `start()` 같은 시작 메서드를 쓴다.

- [ ] **Step 4: 가정 고정 테스트 작성**

`Fabric8ContractTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.k8s

import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.fabric8.kubernetes.client.KubernetesClientException
import io.fabric8.kubernetes.client.dsl.base.PatchContext
import io.fabric8.kubernetes.client.dsl.base.PatchType
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/**
 * P8-3b 가 기대는 fabric8 동작을 고정한다 (버전을 올리면 여기서 먼저 깨진다).
 * ConfigMap CAS(구간 예약) · create 충돌 · Lease 선출 · Pod 라벨 merge patch.
 */
class Fabric8ContractTest {
    private val k8s = MockK8s()
    private val ns = "test"

    @AfterTest
    fun tearDown() = k8s.close()

    private fun configMap(reservedTo: String) =
        ConfigMapBuilder().withNewMetadata().withName("bank-gateway-tran-no").endMetadata()
            .addToData("date", "20261116").addToData("reservedTo", reservedTo).build()

    @Test
    fun `ConfigMap 을 낡은 resourceVersion 으로 update 하면 409`() {
        val maps = k8s.client.configMaps().inNamespace(ns)
        maps.resource(configMap("1000")).create()
        val first = maps.withName("bank-gateway-tran-no").get()
        val second = maps.withName("bank-gateway-tran-no").get()

        first.data = mapOf("date" to "20261116", "reservedTo" to "2000")
        maps.resource(first).update()

        second.data = mapOf("date" to "20261116", "reservedTo" to "2000")
        val error = assertFailsWith<KubernetesClientException> { maps.resource(second).update() }
        assertEquals(409, error.code)
        assertEquals("2000", maps.withName("bank-gateway-tran-no").get().data["reservedTo"])
    }

    @Test
    fun `이미 있는 ConfigMap 을 create 하면 409`() {
        val maps = k8s.client.configMaps().inNamespace(ns)
        maps.resource(configMap("1000")).create()
        val error = assertFailsWith<KubernetesClientException> { maps.resource(configMap("1")).create() }
        assertEquals(409, error.code)
    }

    @Test
    fun `Lease 선출 - 둘 중 하나가 리더, holder = 그 identity`() {
        val leader = AtomicReference<String>()
        val futures = listOf("gw-a", "gw-b").map { id ->
            val config = LeaderElectionConfigBuilder()
                .withName("contract")
                .withLock(LeaseLock(ns, "bank-gateway-leader", id))
                .withLeaseDuration(Duration.ofSeconds(3))
                .withRenewDeadline(Duration.ofSeconds(2))
                .withRetryPeriod(Duration.ofMillis(500))
                .withReleaseOnCancel(true)
                .withLeaderCallbacks(LeaderCallbacks({ leader.compareAndSet(null, id) }, {}, {}))
                .build()
            k8s.client.leaderElector().withConfig(config).build().start()
        }
        try {
            await.atMost(Duration.ofSeconds(10)) untilAsserted {
                assertEquals(leader.get(), k8s.client.leases().inNamespace(ns).withName("bank-gateway-leader").get()?.spec?.holderIdentity)
            }
        } finally {
            futures.forEach { it.cancel(true) }
        }
    }

    @Test
    fun `Pod 라벨 JSON merge patch - 붙이고 null 로 뗀다`() {
        k8s.createPod(ns, "gw-a")
        val pod = k8s.client.pods().inNamespace(ns).withName("gw-a")
        pod.patch(PatchContext.of(PatchType.JSON_MERGE), """{"metadata":{"labels":{"bank-transfer/gateway-role":"leader"}}}""")
        assertEquals("leader", pod.get().metadata.labels["bank-transfer/gateway-role"])
        pod.patch(PatchContext.of(PatchType.JSON_MERGE), """{"metadata":{"labels":{"bank-transfer/gateway-role":null}}}""")
        assertNull(pod.get().metadata.labels["bank-transfer/gateway-role"])
        assertEquals("bank-gateway", pod.get().metadata.labels["app"])
    }
}
```

- [ ] **Step 5: 실행**

Run: `./gradlew :apps:bank-gateway:test --tests '*Fabric8ContractTest*'`
Expected: 4개 PASS.

Run: `./gradlew :apps:bank-gateway:test`
Expected: 기존 테스트 전부 PASS (Netty 4.2 · Jackson 3 이 가짜 서버의 Vert.x 와 함께 돈다).

- [ ] **Step 6: 관문 — 실패하면**

| 증상 | 조치 |
|---|---|
| `NoSuchMethodError` · `NoClassDefFoundError` 가 `tools.jackson.*` 에서 | `apps/bank-gateway/build.gradle.kts` 끝에 아래를 넣고 Step 5 다시. 그래도 실패면 **BLOCKED** 로 보고 (설계로 돌아간다 — 대안 B: Lease 직접 구현) |
| CRUD 모드가 Lease 를 모름 (404 · holder null 영구) | **BLOCKED** 로 보고. 출력 그대로 |
| merge patch 의 `null` 이 라벨을 지우지 않음 | 테스트는 그대로 두고 **DONE_WITH_CONCERNS** 로 보고 (가짜 서버 한계일 수 있다 — kind 에서 Task 7 이 확인) |
| 기존 테스트가 Netty 충돌로 실패 | 출력과 함께 **BLOCKED** |

```kotlin
// fabric8 8.0.0 이 컴파일된 Jackson 3.2.x 로 맞춘다 (Boot 4.1.1 BOM 은 3.1.5) — Task 1 에서 3.1.5 로는 깨지는 것을 확인함
dependencyManagement {
    imports {
        mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES) {
            bomProperty("jackson-bom.version", "3.2.3")
        }
    }
}
```

- [ ] **Step 7: 커밋**

```bash
git add apps/bank-gateway/build.gradle.kts apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/k8s/MockK8s.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/k8s/Fabric8ContractTest.kt
git commit -m "build(p8-3b): fabric8 8.0.0 (JDK HttpClient) + 가정 고정 테스트 — CAS 409 · create 409 · Lease 선출 · 라벨 merge patch

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: 규격 v0.4 응답코드 `3001` + 모의 은행 옵션 3개

**Files:**
- Modify: `modules/bank-protocol/src/main/kotlin/io/github/hyeokjinon/banktransfer/protocol/Header.kt:37-42`
- Modify: `apps/mock-bank/src/main/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankOptions.kt`
- Modify: `apps/mock-bank/src/main/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankServer.kt`
- Modify: `apps/mock-bank/src/test/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankOptionsTest.kt`
- Create: `apps/mock-bank/src/test/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankConstraintsTest.kt`
- Modify: `docs/protocol.md`

**Interfaces:**
- Produces: `ResponseCodes.DUPLICATE_TRAN_NO = "3001"`.
- Produces: `MockBankServer(..., singleSession: Boolean = false, sessionIdleTimeout: Duration? = null, uniqueTranNo: Boolean = false)` (기존 인자 뒤), `fun peakSessions(): Int`, `val sessionsOpened: Int`.
- Produces: `MockBankOptions(..., singleSession: Boolean = false, sessionIdleTimeout: Duration? = null, uniqueTranNo: Boolean = false)`.

- [ ] **Step 1: 응답코드 추가**

`Header.kt` 의 `object ResponseCodes` 에서 `ORIGINAL_NOT_FOUND` 다음 줄에:

```kotlin
    const val DUPLICATE_TRAN_NO = "3001"    // v0.4: 같은 기관 · 같은 날(KST) 이미 쓴 전문번호 — 은행이 처리하지 않았다
```

- [ ] **Step 2: 옵션 파싱 실패 테스트 작성**

`MockBankOptionsTest.kt` 끝(클래스 안)에 추가:

```kotlin
    @Test
    fun `실무 제약 옵션 (P8-3b) - 기본은 모두 꺼짐`() {
        val defaults = MockBankOptions.parse(emptyArray())
        assertEquals(false, defaults.singleSession)
        assertEquals(null, defaults.sessionIdleTimeout)
        assertEquals(false, defaults.uniqueTranNo)

        val on = MockBankOptions.parse(arrayOf("9090", "--bank-files=/data/bank-files", "--single-session", "--session-idle-timeout=60s", "--unique-tran-no"))
        assertEquals(true, on.singleSession)
        assertEquals(Duration.ofSeconds(60), on.sessionIdleTimeout)
        assertEquals(true, on.uniqueTranNo)
        assertEquals(Path.of("/data/bank-files"), on.bankFiles)
    }

    @Test
    fun `세션 유휴 시간은 1 ~ 3600초만 받는다`() {
        assertEquals(Duration.ofSeconds(1), MockBankOptions.parse(arrayOf("--session-idle-timeout=1s")).sessionIdleTimeout)
        assertEquals(Duration.ofSeconds(3600), MockBankOptions.parse(arrayOf("--session-idle-timeout=3600s")).sessionIdleTimeout)
        for (bad in listOf("0s", "3601s", "60", "60ms", "1m", "s")) {
            assertFailsWith<IllegalArgumentException>(bad) { MockBankOptions.parse(arrayOf("--session-idle-timeout=$bad")) }
        }
    }
```

Run: `./gradlew :apps:mock-bank:test --tests '*MockBankOptionsTest*'`
Expected: 컴파일 실패 (`singleSession` 없음).

- [ ] **Step 3: 옵션 구현**

`MockBankOptions.kt` 전체를 아래로 바꾼다:

```kotlin
package io.github.hyeokjinon.banktransfer.mockbank

import java.nio.file.Path
import java.time.Duration
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

/**
 * 모의 은행 실행 인자 (P5-1, P6-1, P8-3b):
 * `[포트] [--bank-files=디렉터리] [--close-day=yyyyMMdd] [--response-delay=<n>ms] [--single-session] [--session-idle-timeout=<n>s] [--unique-tran-no]`.
 * `--close-day` 가 있으면 서버를 띄우지 않고 그 날짜의 대사 파일만 만들고 끝난다 (손 확인 · 재생성용).
 * 디렉터리 기본값 `./bank-files` 는 작업 디렉터리 = 저장소 루트 기준이다 (Gradle run 이 루트에서 돈다 — API 의 daily-recon.file-dir 과 같은 곳).
 * `--response-delay` 는 모든 응답을 그만큼 늦춘다 (P6-1 측정). 0 ~ 2999ms — 게이트웨이 응답 시간 초과(3초) 이상이면 모든 출금이 결과 모름이 된다.
 * 실무 제약 재현 (P8-3b, 기본 꺼짐 — K8s 매니페스트에서만 켠다):
 * - `--single-session`: 기관당 세션 하나. 이미 있으면 새 연결을 받자마자 끊는다.
 * - `--session-idle-timeout=<n>s`: 그 시간 동안 받은 전문(폴링 포함)이 없으면 세션을 끊는다 (FIN 없이 사라진 상대 정리). 1 ~ 3600초.
 * - `--unique-tran-no`: 같은 KST 날짜에 이미 쓴 전문번호면 처리하지 않고 응답코드 3001.
 */
data class MockBankOptions(
    val port: Int = 9090,
    val bankFiles: Path = Path.of("./bank-files"),
    val closeDay: LocalDate? = null,
    val responseDelay: Duration = Duration.ZERO,
    val singleSession: Boolean = false,
    val sessionIdleTimeout: Duration? = null,
    val uniqueTranNo: Boolean = false,
) {
    companion object {
        private const val USAGE = "usage: [port] [--bank-files=dir] [--close-day=yyyyMMdd] [--response-delay=<0~2999>ms] " +
            "[--single-session] [--session-idle-timeout=<1~3600>s] [--unique-tran-no]"
        private const val MAX_DELAY_MS = 2999L
        private const val MAX_IDLE_S = 3600L
        private val DELAY = Regex("""(\d{1,4})ms""")
        private val IDLE = Regex("""(\d{1,4})s""")

        fun parse(args: Array<String>): MockBankOptions =
            args.fold(MockBankOptions()) { options, arg ->
                when {
                    arg.startsWith("--bank-files=") -> options.copy(bankFiles = Path.of(arg.substringAfter('=')))
                    arg.startsWith("--close-day=") -> options.copy(closeDay = closeDayOf(arg.substringAfter('=')))
                    arg.startsWith("--response-delay=") -> options.copy(responseDelay = responseDelayOf(arg.substringAfter('=')))
                    arg == "--single-session" -> options.copy(singleSession = true)
                    arg.startsWith("--session-idle-timeout=") -> options.copy(sessionIdleTimeout = idleTimeoutOf(arg.substringAfter('=')))
                    arg == "--unique-tran-no" -> options.copy(uniqueTranNo = true)
                    arg.startsWith("--") -> throw IllegalArgumentException("unknown option '$arg' — $USAGE")
                    else -> options.copy(port = arg.toIntOrNull() ?: throw IllegalArgumentException("port must be a number: '$arg' — $USAGE"))
                }
            }

        private fun closeDayOf(text: String): LocalDate =
            try {
                LocalDate.parse(text, DateTimeFormatter.BASIC_ISO_DATE)
            } catch (e: DateTimeParseException) {
                throw IllegalArgumentException("close-day must be yyyyMMdd: '$text'", e)
            }

        private fun responseDelayOf(text: String): Duration {
            val ms = DELAY.matchEntire(text)?.groupValues?.get(1)?.toLong()
                ?: throw IllegalArgumentException("response-delay must be <n>ms (0 ~ $MAX_DELAY_MS): '$text'")
            require(ms <= MAX_DELAY_MS) { "response-delay must be below the gateway timeout (3s): ${ms}ms" }
            return Duration.ofMillis(ms)
        }

        private fun idleTimeoutOf(text: String): Duration {
            val s = IDLE.matchEntire(text)?.groupValues?.get(1)?.toLong()
                ?: throw IllegalArgumentException("session-idle-timeout must be <n>s (1 ~ $MAX_IDLE_S): '$text'")
            require(s in 1..MAX_IDLE_S) { "session-idle-timeout must be 1 ~ $MAX_IDLE_S s: ${s}s" }
            return Duration.ofSeconds(s)
        }
    }
}
```

Run: `./gradlew :apps:mock-bank:test --tests '*MockBankOptionsTest*'`
Expected: PASS (기존 4개 + 새 2개).

- [ ] **Step 4: 서버 제약 실패 테스트 작성**

`MockBankConstraintsTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.mockbank

import io.github.hyeokjinon.banktransfer.protocol.BankMessage
import io.github.hyeokjinon.banktransfer.protocol.BankMessageSerializer
import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryRequest
import io.github.hyeokjinon.banktransfer.protocol.PollRequest
import io.github.hyeokjinon.banktransfer.protocol.PollResponse
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.protocol.TransferRequest
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.io.DataInputStream
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** 실무 제약 재현 옵션 (P8-3b 스펙 §7): 세션 하나 · 유휴 세션 정리 · 전문번호 일자별 유일. 기본은 모두 꺼짐. */
class MockBankConstraintsTest {

    /** 소켓으로 직접 붙는 최소 클라이언트 (모의 은행 모듈은 게이트웨이 모듈을 모른다 — 의존 방향). */
    private class RawClient(port: Int) : AutoCloseable {
        private val socket = Socket("127.0.0.1", port).apply { soTimeout = 3_000 }
        private val input = DataInputStream(socket.getInputStream())

        fun send(message: BankMessage) {
            val body = BankMessageSerializer.serialize(message)
            socket.getOutputStream().apply {
                write("%04d".format(body.size).toByteArray(Charsets.US_ASCII))
                write(body)
                flush()
            }
        }

        fun receive(): BankMessage {
            val length = String(ByteArray(4).also { input.readFully(it) }, Charsets.US_ASCII).toInt()
            return BankMessageSerializer.deserialize(ByteArray(length).also { input.readFully(it) })
        }

        fun poll(tranNo: Long): BankMessage {
            send(PollRequest(Header(MessageClass.ADMIN_REQUEST, BizType.POLL, tranNo, LocalDateTime.now())))
            return receive()
        }

        /** 서버가 연결을 끊었는지 (soTimeout 안에 EOF · RST 면 true, 시간이 지나도 열려 있으면 false). */
        fun closedByServer(): Boolean =
            try {
                input.read() == -1
            } catch (e: SocketTimeoutException) {
                false
            } catch (e: SocketException) {
                true
            }

        override fun close() = socket.close()
    }

    private class MutableClock(var now: Instant) : Clock() {
        override fun getZone(): ZoneId = ZoneId.of("UTC")
        override fun withZone(zone: ZoneId): Clock = this
        override fun instant(): Instant = now
    }

    private fun header(tranNo: Long, bizType: BizType = BizType.TRANSFER) =
        Header(MessageClass.REQUEST, bizType, tranNo, LocalDateTime.of(2026, 11, 16, 10, 0))

    private fun transfer(tranNo: Long, reference: String) =
        TransferRequest(header(tranNo), "090", "1002123456789", 1_000, "m", reference)

    @Test
    fun `--single-session - 세션이 있으면 새 연결을 받자마자 끊고, 원래 세션은 그대로`() {
        MockBankServer(singleSession = true).start().use { bank ->
            RawClient(bank.boundPort).use { first ->
                assertIs<PollResponse>(first.poll(1))   // 첫 세션이 확실히 받아들여진 뒤
                RawClient(bank.boundPort).use { second -> assertTrue(second.closedByServer()) }
                assertIs<PollResponse>(first.poll(2))
            }
            assertEquals(1, bank.peakSessions())
            assertEquals(1, bank.sessionsOpened)
            // 첫 세션이 닫히면 다시 받는다 (서버가 닫힘을 알아채는 데 잠깐 걸린다)
            val deadline = System.currentTimeMillis() + 3_000
            while (true) {
                val ok = RawClient(bank.boundPort).use { runCatching { it.poll(3) }.isSuccess }
                if (ok) break
                check(System.currentTimeMillis() < deadline) { "세션이 닫힌 뒤에도 새 연결을 받지 않는다" }
                Thread.sleep(50)
            }
        }
    }

    @Test
    fun `--session-idle-timeout - 받은 전문이 없으면 끊고, 폴링이 오면 유지`() {
        MockBankServer(sessionIdleTimeout = Duration.ofMillis(400)).start().use { bank ->
            RawClient(bank.boundPort).use { busy ->
                repeat(6) { i ->
                    assertIs<PollResponse>(busy.poll(i + 1L))
                    Thread.sleep(150)
                }
            }
            RawClient(bank.boundPort).use { idle -> assertTrue(idle.closedByServer()) }
        }
    }

    @Test
    fun `--unique-tran-no - 같은 KST 날짜에 같은 전문번호는 처리하지 않고 3001, 날짜가 바뀌면 다시 받는다`() {
        val clock = MutableClock(Instant.parse("2026-11-16T01:00:00Z"))   // KST 10:00
        MockBankServer(uniqueTranNo = true, clock = clock).use { bank ->
            assertEquals(ResponseCodes.OK, (bank.respond(transfer(7, "T261116UNIQUE000001")) as TransferResponse).header.responseCode)

            val dup = bank.respond(transfer(7, "T261116UNIQUE000002")) as TransferResponse
            assertEquals(ResponseCodes.DUPLICATE_TRAN_NO, dup.header.responseCode)
            assertEquals(1, bank.depositsOf("1002123456789").size)   // 두 번째는 입금하지 않았다
            assertEquals(
                ResponseCodes.DUPLICATE_TRAN_NO,
                bank.respond(NameInquiryRequest(header(7, BizType.NAME_INQUIRY), "090", "1002123456789"))!!.header.responseCode,
            )

            clock.now = Instant.parse("2026-11-16T15:00:00Z")   // KST 다음 날 00:00
            assertEquals(ResponseCodes.OK, (bank.respond(transfer(7, "T261117UNIQUE000003")) as TransferResponse).header.responseCode)
        }
    }

    @Test
    fun `옵션을 끄면 지금과 같다 - 같은 번호도 처리, 세션 여러 개`() {
        MockBankServer().start().use { bank ->
            assertEquals(ResponseCodes.OK, (bank.respond(transfer(7, "T261116DEFAULT00001")) as TransferResponse).header.responseCode)
            assertEquals(ResponseCodes.OK, (bank.respond(transfer(7, "T261116DEFAULT00002")) as TransferResponse).header.responseCode)
            RawClient(bank.boundPort).use { a ->
                RawClient(bank.boundPort).use { b ->
                    assertIs<PollResponse>(a.poll(1))
                    assertIs<PollResponse>(b.poll(1))
                }
            }
            assertEquals(2, bank.peakSessions())
        }
    }
}
```

Run: `./gradlew :apps:mock-bank:test --tests '*MockBankConstraintsTest*'`
Expected: 컴파일 실패 (`singleSession` · `peakSessions` 없음).

- [ ] **Step 5: 서버 구현**

`MockBankServer.kt` 를 고친다.

(a) import 추가 (알파벳 위치에 맞춰):

```kotlin
import io.netty.handler.timeout.IdleState
import io.netty.handler.timeout.IdleStateEvent
import io.netty.handler.timeout.IdleStateHandler
```

(b) 클래스 KDoc 목록 끝(`bankFiles (P5-1)` 항목 다음)에:

```kotlin
 * - 실무 제약 (P8-3b, 기본 꺼짐): singleSession(세션 하나 — 둘째 연결은 받자마자 끊는다), sessionIdleTimeout(받은 전문이 없으면 끊는다),
 *   uniqueTranNo(같은 KST 날짜에 쓴 전문번호면 처리하지 않고 3001). 연결을 받고 닫을 때마다 `sessions=<지금> max=<최대>` 를 남긴다 — 실험 도구가 읽는다
```

(c) 생성자 마지막 인자 `private val clock: Clock = Clock.systemUTC(),` 다음에:

```kotlin
    private val singleSession: Boolean = false,
    private val sessionIdleTimeout: Duration? = null,
    private val uniqueTranNo: Boolean = false,
```

(d) `private val pollCounter = AtomicInteger()` 다음에:

```kotlin
    /** 지금 붙어 있는 세션 수 · 최댓값 · 받아들인 총수 (P8-3b — "은행에 붙는 것은 언제나 하나" 의 은행 쪽 증거). */
    private val sessions = AtomicInteger()
    private val peakSessionCount = AtomicInteger()
    private val openedSessions = AtomicInteger()

    /** KST 날짜별로 이미 쓴 전문번호 (uniqueTranNo). 오늘 것만 남긴다. */
    private val seenTranNos = ConcurrentHashMap<LocalDate, MutableSet<Long>>()
```

(e) `childHandler` 의 `initChannel` 을 바꾼다:

```kotlin
        .childHandler(object : ChannelInitializer<SocketChannel>() {
            override fun initChannel(ch: SocketChannel) {
                if (!admit(ch)) return
                clients.add(ch)
                sessionIdleTimeout?.let { ch.pipeline().addLast("idle", IdleStateHandler(it.toMillis(), 0, 0, TimeUnit.MILLISECONDS)) }
                ch.pipeline().addBankCodec().addLast(BankHandler())
            }
        })
```

(f) `val pollCount: Int get() = pollCounter.get()` 다음에:

```kotlin
    /** 받아들인 세션 총수 (끊긴 것 포함). */
    val sessionsOpened: Int get() = openedSessions.get()

    /** 동시에 붙어 있던 세션 수의 최댓값. */
    fun peakSessions(): Int = peakSessionCount.get()
```

(g) `scheduleDailyClose()` 위에 세션 수락 함수:

```kotlin
    /** 세션을 받을지 정한다. singleSession 이면 둘째부터 받자마자 끊는다 (실무: 기관당 세션 수 제한). */
    private fun admit(ch: SocketChannel): Boolean {
        val now = sessions.incrementAndGet()
        if (singleSession && now > 1) {
            log.info("session rejected sessions={} max={}", sessions.decrementAndGet(), peakSessionCount.get())
            ch.close()
            return false
        }
        openedSessions.incrementAndGet()
        log.info("session opened sessions={} max={}", now, peakSessionCount.accumulateAndGet(now) { a, b -> maxOf(a, b) })
        ch.closeFuture().addListener { log.info("session closed sessions={} max={}", sessions.decrementAndGet(), peakSessionCount.get()) }
        return true
    }
```

(h) `respond` 를 블록 본문으로 바꾸고 맨 앞에서 중복을 거른다. 기존 `when (message) { … }` 내용은 그대로 `return when (message) { … }` 로 옮긴다:

```kotlin
    internal fun respond(message: BankMessage): BankMessage? {
        if (uniqueTranNo && message.header.messageClass.isRequest && !firstUseToday(message.header.tranNo)) {
            log.info("duplicate tran no {} biz={} — not processed ({})", message.header.tranNo, message.header.bizType, ResponseCodes.DUPLICATE_TRAN_NO)
            return duplicateReply(message)
        }
        return when (message) {
            // (기존 분기 그대로)
        }
    }

    /** 오늘(KST) 처음 쓰는 번호인가. 지난 날짜는 지운다 (메모리를 하루치로). */
    private fun firstUseToday(tranNo: Long): Boolean {
        val today = LocalDate.ofInstant(clock.instant(), KST)
        seenTranNos.keys.removeIf { it != today }
        return seenTranNos.computeIfAbsent(today) { ConcurrentHashMap.newKeySet() }.add(tranNo)
    }

    /** 중복 번호 응답: 처리하지 않고 공통부 응답코드 3001 (결과조회 처리결과 칸도 3001 — 처리 여부를 답한 게 아니다). */
    private fun duplicateReply(message: BankMessage): BankMessage? {
        val dup = ResponseCodes.DUPLICATE_TRAN_NO
        return when (message) {
            is PollRequest -> PollResponse(message.header.toResponse(dup))
            is NameInquiryRequest -> NameInquiryResponse(message.header.toResponse(dup), message.bankCode, message.accountNo, "")
            is TransferRequest -> message.reply(dup)
            is InquiryRequest -> InquiryResponse(message.header.toResponse(dup), message.reference, dup)
            is PollResponse, is NameInquiryResponse, is TransferResponse, is InquiryResponse -> null
        }
    }
```

(i) `BankHandler` 에 유휴 처리 (`exceptionCaught` 위):

```kotlin
        override fun userEventTriggered(ctx: ChannelHandlerContext, evt: Any) {
            if (evt is IdleStateEvent && evt.state() == IdleState.READER_IDLE) {
                // 상대가 FIN 없이 사라진 세션(노드째 정지)을 정리한다 — 실무 은행의 세션 정리 시간 흉내 (P8-3b H4)
                log.info("session evicted idle after {}ms without messages", sessionIdleTimeout?.toMillis())
                ctx.close()
            } else {
                super.userEventTriggered(ctx, evt)
            }
        }
```

(j) `main` 의 서버 생성 · 시작 문구:

```kotlin
    val server = MockBankServer(
        options.port,
        responseDelay = options.responseDelay,
        bankFiles = options.bankFiles,
        singleSession = options.singleSession,
        sessionIdleTimeout = options.sessionIdleTimeout,
        uniqueTranNo = options.uniqueTranNo,
    ).start()
```

```kotlin
    println(
        "Mock bank FEP started on port ${options.port}. Accounts: ${MockBankServer.DEFAULT_ACCOUNTS}. " +
            "Bank files: ${options.bankFiles.toAbsolutePath()}. Response delay: ${options.responseDelay.toMillis()}ms. " +
            "Single session: ${options.singleSession}, session idle timeout: ${options.sessionIdleTimeout?.seconds?.let { "${it}s" } ?: "off"}, " +
            "unique tran no: ${options.uniqueTranNo}",
    )
```

- [ ] **Step 6: 테스트 통과 확인**

Run: `./gradlew :apps:mock-bank:test`
Expected: 전부 PASS.

- [ ] **Step 7: 규격 문서 v0.4**

`docs/protocol.md`:
- §3 표 3행(전문번호) 값 칸을 바꾼다: `송신 측이 매기는 일련번호. **기관 · 일자(KST)별로 유일** (v0.4) — 같은 날 이미 쓴 번호는 처리하지 않고 응답코드 \`3001\`. 응답은 요청의 전문번호를 그대로 돌려준다`
- §3 표 5행(응답코드) 값 칸에 `\`9999\` 시스템오류` 다음으로 `, \`3001\` 전문번호 중복 — 처리하지 않음 (v0.4)` 를 넣는다.
- §4 아래 "이용기관 거래번호" 문단의 `전문번호는 세션 안에서 요청 · 응답을 짝짓는 용도라 재사용되므로 업무 키가 아니다` → `전문번호는 요청 · 응답 짝짓기 용도이고 하루 안에서만 유일하므로(v0.4) 업무 키가 아니다`.
- §6 세션 규칙 끝에 두 줄:
  - `- 기관당 세션은 하나일 수 있다 (은행마다 다름). 둘째 연결은 바로 끊긴다 — 송신 측은 연결 주인을 하나로 유지해야 한다 (v0.4, 모의 은행 \`--single-session\`).`
  - `- 은행은 일정 시간 전문이 없는 세션을 정리한다 (모의 은행 \`--session-idle-timeout\`, K8s 60초 = 폴링 주기 2배).`
- 변경 이력 표에 행 추가: `| v0.4 | 2026-11-16 | 전문번호 기관 · 일자(KST)별 유일, 응답코드 \`3001\` 전문번호 중복(처리 안 함), 세션 하나 · 유휴 정리 규칙 ([ADR-0018](adr/0018-gateway-ha-leader-election.md)) |`

- [ ] **Step 8: 커밋**

```bash
git add modules/bank-protocol/src/main/kotlin/io/github/hyeokjinon/banktransfer/protocol/Header.kt apps/mock-bank/src/main/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankOptions.kt apps/mock-bank/src/main/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankServer.kt apps/mock-bank/src/test/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankOptionsTest.kt apps/mock-bank/src/test/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankConstraintsTest.kt docs/protocol.md
git commit -m "feat(p8-3b): 규격 v0.4 전문번호 중복 3001 + 모의 은행 실무 제약 옵션 (세션 하나 · 유휴 정리 · 번호 유일, 기본 꺼짐)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `TranNoSource` 포트 + 클라이언트 (`3001` → 안 보냄, 폴링 보호)

**Files:**
- Create: `modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/TranNoSource.kt`
- Modify: `modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayClient.kt`
- Modify: `modules/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayClientTest.kt`

**Interfaces:**
- Consumes: `ResponseCodes.DUPLICATE_TRAN_NO`, `MockBankServer(uniqueTranNo = true)`, `MockBankServer.sessionsOpened` (Task 2).
- Produces:
  ```kotlin
  fun interface TranNoSource { fun next(): Long; fun prepare() {} }
  class InMemoryTranNoSource : TranNoSource
  class BankGatewayClient(config, clock = …, sendGate = SendGate.OPEN, tranNoSource: TranNoSource = InMemoryTranNoSource())
  ```
  `next()` 가 못 주면 `BankNotSentException` 을 던진다 (클라이언트가 future 로 바꾼다).

- [ ] **Step 1: 포트 작성**

`TranNoSource.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gateway

import io.github.hyeokjinon.banktransfer.protocol.Header
import java.util.concurrent.atomic.AtomicLong

/**
 * 전문번호 발급 (P8-3b, 규격 v0.4: 기관 · 일자별 유일). 유일성의 범위는 구현이 정한다 —
 * [InMemoryTranNoSource] 는 이 인스턴스 안, K8s 의 ConfigMap 구간 예약(apps/bank-gateway)은 게이트웨이 여러 대 사이.
 * 통신 모듈은 이 포트만 안다 (ADR-0001 포트/어댑터 — K8s 를 모른다).
 */
fun interface TranNoSource {
    /** 다음 번호. 줄 수 없으면 [BankNotSentException] — 아직 은행에 보내지 않았으므로 다시 보내도 안전하다. 막히지 않아야 한다 (이벤트 루프에서 부른다). */
    fun next(): Long

    /** 리더 임기 시작 때 한 번 (구간을 미리 예약). 실패해도 던지지 않는다 — next() 가 다시 시도한다. */
    fun prepare() {}
}

/** 메모리 카운터 (P8-3a 까지의 동작). 선출이 꺼진 환경(IntelliJ · Compose · 테스트)의 기본값. 다 쓰면 1부터. */
class InMemoryTranNoSource : TranNoSource {
    private val sequence = AtomicLong(0)

    override fun next(): Long = sequence.updateAndGet { if (it >= Header.MAX_TRAN_NO) 1 else it + 1 }
}
```

- [ ] **Step 2: 실패 테스트 작성**

`BankGatewayClientTest.kt` 의 `client(...)` 도우미에 인자를 하나 더한다:

```kotlin
    private fun client(
        port: Int,
        timeout: Duration = Duration.ofSeconds(2),
        poll: Duration = Duration.ofSeconds(30),
        tranNo: TranNoSource = InMemoryTranNoSource(),
    ) =
        BankGatewayClient(
            BankGatewayConfig(
                host = "127.0.0.1",
                port = port,
                responseTimeout = timeout,
                pollInterval = poll,
                reconnectMinDelay = Duration.ofMillis(100),
                reconnectMaxDelay = Duration.ofMillis(500),
            ),
            tranNoSource = tranNo,
        ).closeLater()
```

클래스 끝에 테스트 4개 추가:

```kotlin
    @Test
    fun `전문번호는 TranNoSource 에서 받는다`() {
        val bank = MockBankServer().start().closeLater()
        val numbers = java.util.concurrent.atomic.AtomicLong(500)
        val client = client(bank.boundPort, tranNo = { numbers.getAndIncrement() }).start()
        waitUntil { client.isConnected }

        assertEquals(500, client.inquireName("090", "1002123456789").get(3, TimeUnit.SECONDS).header.tranNo)
        assertEquals(501, client.inquireName("090", "1002123456789").get(3, TimeUnit.SECONDS).header.tranNo)
    }

    @Test
    fun `은행이 전문번호 중복(3001)으로 거절하면 BankNotSentException - 처리 안 했으니 다시 보내도 된다`() {
        val bank = MockBankServer(uniqueTranNo = true).start().closeLater()
        val first = client(bank.boundPort).start()
        val second = client(bank.boundPort).start()   // 메모리 카운터 — 둘 다 1부터 (이중화 전 문제)
        waitUntil { first.isConnected && second.isConnected }

        assertEquals(ResponseCodes.OK, first.transfer("090", "1002123456789", 1_000, "m", "T261116DUPCLIENT001").get(3, TimeUnit.SECONDS).header.responseCode)
        val error = second.transfer("090", "1002123456789", 1_000, "m", "T261116DUPCLIENT002").awaitFailure()

        assertIs<BankNotSentException>(error)
        assertEquals(1, bank.depositsOf("1002123456789").size)
    }

    @Test
    fun `번호를 못 받으면 보내지 않고 future 가 BankNotSentException (동기로 던지지 않는다)`() {
        val bank = MockBankServer().start().closeLater()
        val client = client(bank.boundPort, tranNo = { throw BankNotSentException("no tran no block") }).start()
        waitUntil { client.isConnected }

        val future = client.transfer("090", "1002123456789", 1_000, "m", "T261116NOTRANNO0001")

        assertIs<BankNotSentException>(future.awaitFailure())
        assertEquals(0, bank.depositsOf("1002123456789").size)
    }

    @Test
    fun `폴링 번호를 못 받아도 회선을 끊지 않는다 - 다음 폴링에서 다시`() {
        val bank = MockBankServer().start().closeLater()
        val calls = java.util.concurrent.atomic.AtomicInteger()
        val flaky = TranNoSource { if (calls.incrementAndGet() == 1) throw BankNotSentException("not yet") else calls.get().toLong() }
        val client = client(bank.boundPort, poll = Duration.ofMillis(200), tranNo = flaky).start()
        waitUntil { client.isConnected }

        waitUntil { bank.pollCount >= 2 }

        assertTrue(client.isConnected)
        assertEquals(1, bank.sessionsOpened)   // 끊고 다시 붙지 않았다
    }
```

Run: `./gradlew :modules:bank-gateway:test --tests '*BankGatewayClientTest*'`
Expected: 컴파일 실패 (`tranNoSource` 인자 없음).

- [ ] **Step 3: 클라이언트 구현**

`BankGatewayClient.kt`:

(a) import 에 `import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes` 추가. `import java.util.concurrent.atomic.AtomicLong` 삭제.

(b) 생성자에 마지막 인자 추가 (`sendGate` 다음):

```kotlin
    /** 전문번호 발급 (P8-3b). 기본은 이 인스턴스 안의 메모리 카운터 — K8s 에서는 게이트웨이 앱이 ConfigMap 구간 예약을 넣는다. */
    private val tranNoSource: TranNoSource = InMemoryTranNoSource(),
```

(c) `private val tranNoSequence = AtomicLong(0)` 줄 삭제.

(d) 업무 메서드 세 개를 `request { … }` 로 감싼다:

```kotlin
    override fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> =
        request { NameInquiryRequest(newHeader(BizType.NAME_INQUIRY), bankCode, accountNo) }.thenApply { it.expect<NameInquiryResponse>() }

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> =
        request { TransferRequest(newHeader(BizType.TRANSFER), bankCode, accountNo, amount, memo, reference) }.thenApply { it.expect<TransferResponse>() }

    /** 결과조회 (v0.2). 결과를 모르는 이체를 이용기관 거래번호로 묻는다 — 이체를 다시 보내지 않는다. */
    override fun inquire(reference: String): CompletableFuture<InquiryResponse> =
        request { InquiryRequest(newHeader(BizType.INQUIRY), reference) }.thenApply { it.expect<InquiryResponse>() }

    /**
     * 보낼 수 없으면 번호를 쓰기 전에 끝낸다 (구간 예약 번호를 헛되이 쓰지 않게).
     * 번호 발급 실패(BankNotSentException)도 동기로 던지지 않고 future 에 담는다 — 호출자는 실패를 한 경로로만 받는다.
     */
    private fun request(build: () -> BankMessage): CompletableFuture<BankMessage> {
        notSendableReason()?.let { return CompletableFuture.failedFuture(BankNotSentException(it)) }
        val message = try {
            build()
        } catch (e: BankNotSentException) {
            return CompletableFuture.failedFuture(e)
        }
        return send(message)
    }

    private fun notSendableReason(): String? =
        sendGate.closedReason() ?: if (channel?.isActive == true) null else "bank channel is not connected"
```

(e) `send()` 의 KDoc 끝에 `` 응답코드 `3001`(전문번호 중복, v0.4)은 은행이 처리하지 않았다는 뜻이라 BankNotSentException 이다 (P8-3b 결정 15). `` 를 한 줄 더하고, `handle` 의 `null -> response` 를 바꾼다:

```kotlin
                when (val cause = error?.unwrap()) {
                    null -> if (response.header.responseCode == ResponseCodes.DUPLICATE_TRAN_NO) {
                        throw BankNotSentException("bank rejected duplicate tranNo $tranNo (${ResponseCodes.DUPLICATE_TRAN_NO}) — not processed")
                    } else {
                        response
                    }
                    is TimeoutException -> throw BankTimeoutException(tranNo, config.responseTimeout)
                    else -> throw cause
                }
```

(f) 번호 함수를 포트로:

```kotlin
    /** 8자리 전문번호 (규격 v0.4: 기관 · 일자별 유일). 발급 규칙은 [TranNoSource] 가 정한다. */
    private fun nextTranNo(): Long = tranNoSource.next()
```

(g) `userEventTriggered` 의 폴링 줄을 보호한다:

```kotlin
                IdleState.WRITER_IDLE -> {
                    // 번호를 못 받으면(구간 예약 전 · API 서버 단절) 이번 폴링만 건너뛴다 — 예외가 exceptionCaught 로 가면 회선을 끊는다
                    val header = try {
                        newHeader(BizType.POLL, MessageClass.ADMIN_REQUEST)
                    } catch (e: BankNotSentException) {
                        log.warn("poll skipped: {}", e.message)
                        return
                    }
                    ctx.writeAndFlush(PollRequest(header))
                }
```

(`return` 은 `userEventTriggered` 에서 빠져나간다 — `when` 이 함수의 마지막 문장이라 그대로 쓸 수 있다. 컴파일러가 `when` 분기 타입을 문제 삼으면 `return@userEventTriggered` 대신 `Unit` 을 돌려주도록 `header?.let { ctx.writeAndFlush(PollRequest(it)) }` 형태로 바꾼다.)

- [ ] **Step 4: 테스트 통과 · 다른 모듈 회귀**

Run: `./gradlew :modules:bank-gateway:test`
Expected: 전부 PASS.

Run: `./gradlew :apps:api:compileTestKotlin :apps:bank-gateway:compileTestKotlin`
Expected: 성공 (`BankGatewayClient(` 의 다른 사용처는 새 인자 기본값으로 그대로).

- [ ] **Step 5: 커밋**

```bash
git add modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/TranNoSource.kt modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayClient.kt modules/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayClientTest.kt
git commit -m "feat(p8-3b): 전문번호 TranNoSource 포트 · 3001 → 안 보냄 · 번호 발급 실패는 future 로 · 폴링 번호 실패에도 회선 유지

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: ConfigMap 전문번호 구간 예약

**Files:**
- Create: `apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/ConfigMapTranNoSource.kt`
- Create: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/ConfigMapTranNoSourceTest.kt`

**Interfaces:**
- Consumes: `TranNoSource`, `BankNotSentException`, `Header.MAX_TRAN_NO` (Task 3), `MockK8s` (Task 1).
- Produces:
  ```kotlin
  class ConfigMapTranNoSource(k8s: KubernetesClient, namespace: String, name: String, clock: Clock,
      blockSize: Long = 1000, prefetchRemaining: Long = 100, maxAttempts: Int = 5) : TranNoSource, AutoCloseable
  internal fun reserve(date: String): LongRange
  ```

- [ ] **Step 1: 실패 테스트 작성**

`ConfigMapTranNoSourceTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gatewayapp.k8s.MockK8s
import io.github.hyeokjinon.banktransfer.protocol.Header
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** 전문번호 구간 예약 (P8-3b 스펙 §5): 게이트웨이 여러 대가 잠깐 겹쳐도 구간은 겹치지 않는다 (resourceVersion CAS). */
class ConfigMapTranNoSourceTest {
    private val k8s = MockK8s()
    private val ns = "test"
    private val name = "bank-gateway-tran-no"
    private val clock = MutableClock(Instant.parse("2026-11-16T01:00:00Z"))   // KST 2026-11-16 10:00
    private val sources = mutableListOf<ConfigMapTranNoSource>()

    @AfterTest
    fun tearDown() {
        sources.forEach { it.close() }
        k8s.close()
    }

    private fun source(blockSize: Long = 1000, prefetch: Long = 100, attempts: Int = 5) =
        ConfigMapTranNoSource(k8s.client, ns, name, clock, blockSize, prefetch, attempts).also { sources += it }

    private fun data() = k8s.client.configMaps().inNamespace(ns).withName(name).get().data

    @Test
    fun `처음 예약자가 ConfigMap 을 만들고 1부터 쓴다 - 날짜는 KST`() {
        val source = source()
        source.prepare()

        assertEquals(1, source.next())
        assertEquals(2, source.next())
        assertEquals(mapOf("date" to "20261116", "reservedTo" to "1000"), data())
    }

    @Test
    fun `두 예약자가 동시에 예약해도 구간이 겹치지 않는다`() {
        val a = source(attempts = 100)
        val b = source(attempts = 100)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val ranges = listOf(a, b).map { s -> pool.submit(Callable { (1..25).map { s.reserve("20261116") } }) }.flatMap { it.get() }
            val numbers = ranges.flatMap { it.toList() }
            assertEquals(50 * 1000, numbers.size)
            assertEquals(numbers.size, numbers.toSet().size)            // 겹침 없음
            assertEquals((1L..50_000L).toList(), numbers.sorted())       // 빈틈도 없음 (예약은 연속)
            assertEquals("50000", data()["reservedTo"])
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `다른 게이트웨이가 쓰던 날짜면 그 뒤부터 예약한다`() {
        k8s.client.configMaps().inNamespace(ns).resource(
            ConfigMapBuilder().withNewMetadata().withName(name).endMetadata().addToData("date", "20261116").addToData("reservedTo", "7000").build(),
        ).create()
        val source = source()
        source.prepare()

        assertEquals(7001, source.next())
        assertEquals("8000", data()["reservedTo"])
    }

    @Test
    fun `구간 끝 가까이에서 다음 구간을 미리 예약하고 이어서 쓴다`() {
        val source = source(blockSize = 10, prefetch = 3)
        source.prepare()

        val firstEight = (1..8).map { source.next() }
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertEquals("20", data()["reservedTo"]) }
        val nextThree = (1..3).map { source.next() }

        assertEquals((1L..8L).toList(), firstEight)
        assertEquals(listOf(9L, 10L, 11L), nextThree)
    }

    @Test
    fun `날짜가 바뀌면 보내지 않고 새 날짜 구간을 예약해 1부터`() {
        val source = source(blockSize = 10)
        source.prepare()
        assertEquals(1, source.next())

        clock.now = Instant.parse("2026-11-16T15:00:00Z")   // KST 2026-11-17 00:00 (점검 시간 안 — 송신은 원래 막혀 있다)
        assertFailsWith<BankNotSentException> { source.next() }

        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertEquals(1, runCatching { source.next() }.getOrNull()) }
        assertEquals(mapOf("date" to "20261117", "reservedTo" to "10"), data())
    }

    @Test
    fun `구간이 바닥나고 다음 예약이 안 되면 보내지 않는다 - API 서버 단절`() {
        val source = source(blockSize = 2, prefetch = 0)
        source.prepare()
        k8s.stopApiServer()

        assertEquals(1, source.next())
        assertEquals(2, source.next())
        assertFailsWith<BankNotSentException> { source.next() }
    }

    @Test
    fun `임기 시작 예약이 실패해도 던지지 않는다 - next 가 보내지 않음으로 답한다`() {
        val source = source()
        k8s.stopApiServer()

        source.prepare()

        assertFailsWith<BankNotSentException> { source.next() }
    }

    @Test
    fun `하루 한도(99,999,999)를 넘는 구간은 예약하지 않는다`() {
        k8s.client.configMaps().inNamespace(ns).resource(
            ConfigMapBuilder().withNewMetadata().withName(name).endMetadata()
                .addToData("date", "20261116").addToData("reservedTo", "${Header.MAX_TRAN_NO - 5}").build(),
        ).create()

        assertFailsWith<BankNotSentException> { source(blockSize = 10).reserve("20261116") }
        assertTrue(data()["reservedTo"] == "${Header.MAX_TRAN_NO - 5}")
    }

    private class MutableClock(var now: Instant) : Clock() {
        override fun getZone(): ZoneId = ZoneId.of("UTC")
        override fun withZone(zone: ZoneId): Clock = this
        override fun instant(): Instant = now
    }
}
```

Run: `./gradlew :apps:bank-gateway:test --tests '*ConfigMapTranNoSourceTest*'`
Expected: 컴파일 실패 (`ConfigMapTranNoSource` 없음).

- [ ] **Step 2: 구현**

`ConfigMapTranNoSource.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientException
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.TranNoSource
import io.github.hyeokjinon.banktransfer.protocol.Header
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 전문번호 구간 예약 (P8-3b 스펙 §5, 결정 8 · 9). ConfigMap `date` · `reservedTo` 를 resourceVersion 조건부 update 로 늘려
 * [blockSize] 개씩 가져온다 (hi/lo). 번호 유일성이 선출과 독립 — 게이트웨이 두 대가 잠깐 둘 다 리더라고 믿어도 구간은 겹치지 않는다.
 *
 * - [next] 는 막히지 않는다 (Netty 이벤트 루프 · WebFlux 스레드에서 부른다). 구간이 없으면 백그라운드 예약을 걸고 BankNotSentException.
 * - 구간에 [prefetchRemaining] 개 미만이 남으면 다음 구간을 미리 예약한다. 쓰지 못한 번호는 건너뛴다 (유일성만 요구 — 추정).
 * - 날짜(KST)가 바뀌면 새 날짜 구간을 1부터 (점검 시간 23:50 ~ 00:10 안이라 송신은 원래 막혀 있다 — 결정 10).
 */
class ConfigMapTranNoSource(
    private val k8s: KubernetesClient,
    private val namespace: String,
    private val name: String,
    private val clock: Clock,
    private val blockSize: Long = 1000,
    private val prefetchRemaining: Long = 100,
    private val maxAttempts: Int = 5,
) : TranNoSource, AutoCloseable {

    private class Block(val date: String, var next: Long, val last: Long)

    private val log = LoggerFactory.getLogger(javaClass)
    private val reserver = Executors.newSingleThreadExecutor { r -> Thread(r, "tran-no-reserve").apply { isDaemon = true } }
    private val reserving = AtomicBoolean(false)
    private var current: Block? = null    // this 로 보호
    private var upcoming: Block? = null   // this 로 보호

    override fun prepare() {
        val today = today()
        try {
            val range = reserve(today)
            synchronized(this) {
                current = Block(today, range.first, range.last)
                upcoming = null
            }
            log.info("tran no block reserved {} {}..{} (term start)", today, range.first, range.last)
        } catch (e: Exception) {
            log.warn("tran no block reservation failed at term start — sending stays blocked until it succeeds: {}", e.message)
        }
    }

    @Synchronized
    override fun next(): Long {
        val today = today()
        var block = current?.takeIf { it.date == today && it.next <= it.last }
        if (block == null) {
            block = upcoming?.takeIf { it.date == today }
            current = block
            upcoming = null
            if (block == null) {
                reserveAsync(today)
                throw BankNotSentException("no tran no block reserved for $today yet")
            }
        }
        val number = block.next++
        if (block.last - number < prefetchRemaining && upcoming == null) reserveAsync(today)
        return number
    }

    private fun reserveAsync(date: String) {
        if (!reserving.compareAndSet(false, true)) return
        try {
            reserver.execute {
                try {
                    val range = reserve(date)
                    synchronized(this) { upcoming = Block(date, range.first, range.last) }
                    log.info("tran no block reserved {} {}..{}", date, range.first, range.last)
                } catch (e: Exception) {
                    log.warn("tran no block reservation failed: {}", e.message)
                } finally {
                    reserving.set(false)
                }
            }
        } catch (e: RejectedExecutionException) {
            reserving.set(false)   // 닫는 중
        }
    }

    /** 구간 하나를 예약한다 (막힘 — API 서버 왕복). 다른 예약자와 겹치면 409 → 다시 읽고 반복. */
    internal fun reserve(date: String): LongRange {
        val configMaps = k8s.configMaps().inNamespace(namespace)
        repeat(maxAttempts) {
            val existing = configMaps.withName(name).get()
            if (existing == null) {
                val created = ConfigMapBuilder().withNewMetadata().withName(name).endMetadata()
                    .addToData(DATE, date).addToData(RESERVED_TO, "$blockSize").build()
                try {
                    configMaps.resource(created).create()
                    return 1L..blockSize
                } catch (e: KubernetesClientException) {
                    if (e.code != CONFLICT) throw e
                    return@repeat   // 다른 예약자가 먼저 만들었다 — 다시 읽는다
                }
            }
            val reservedTo = if (existing.data?.get(DATE) == date) existing.data.getValue(RESERVED_TO).toLong() else 0L
            val start = reservedTo + 1
            val last = start + blockSize - 1
            if (last > Header.MAX_TRAN_NO) throw BankNotSentException("tran no exhausted for $date (reservedTo=$reservedTo)")
            existing.data = mapOf(DATE to date, RESERVED_TO to "$last")   // resourceVersion 은 읽은 값 그대로 — 그 사이 바뀌었으면 409
            try {
                configMaps.resource(existing).update()
                return start..last
            } catch (e: KubernetesClientException) {
                if (e.code != CONFLICT) throw e
            }
        }
        throw BankNotSentException("tran no reservation conflicted $maxAttempts times")
    }

    private fun today(): String = LocalDate.ofInstant(clock.instant(), KST).format(DateTimeFormatter.BASIC_ISO_DATE)

    override fun close() {
        reserver.shutdownNow()
    }

    companion object {
        private val KST: ZoneId = ZoneId.of("Asia/Seoul")
        private const val DATE = "date"
        private const val RESERVED_TO = "reservedTo"
        private const val CONFLICT = 409
    }
}
```

- [ ] **Step 3: 테스트 통과 확인**

Run: `./gradlew :apps:bank-gateway:test --tests '*ConfigMapTranNoSourceTest*'`
Expected: 8개 PASS. "API 서버 단절" 두 테스트는 `MockK8s` 의 빨리 실패 설정(재시도 0)으로 1 ~ 2초 안에 끝나야 한다 — 10초를 넘으면 재시도 설정이 먹지 않은 것이니 보고서에 적는다.

- [ ] **Step 4: 커밋**

```bash
git add apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/ConfigMapTranNoSource.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/ConfigMapTranNoSourceTest.kt
git commit -m "feat(p8-3b): ConfigMap 전문번호 구간 예약 — resourceVersion CAS · 미리 예약 · KST 날짜 전환 · 실패 시 송신 안 함

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: `BankConnectionOwner` · 헬스 · 종료 훅 (선출 꺼짐 경로)

**Files:**
- Create: `apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/BankConnectionOwner.kt`
- Create: `apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/AlwaysLeader.kt`
- Modify: `apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/config/GatewayConfiguration.kt`
- Modify: `apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/health/BankConnectionHealthIndicator.kt`
- Modify: `apps/bank-gateway/src/main/resources/application.yml`
- Modify: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GatewayApps.kt`
- Modify: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/{GatewayContractTest,GatewayHttpContractTest,ToxiproxyGatewayTest}.kt` (`getBean(BankGatewayClient::class.java)` → `bankClient()`)
- Create: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/BankConnectionOwnerTest.kt`
- Create: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GracefulStepDownTest.kt`

**Interfaces:**
- Consumes: `TranNoSource` · `InMemoryTranNoSource` · `BankGatewayClient(…, tranNoSource)` (Task 3).
- Produces:
  ```kotlin
  class BankConnectionOwner(tranNoSource: TranNoSource, newClient: () -> BankGatewayClient) : BankGateway, AutoCloseable {
      val isLeader: Boolean; fun current(): BankGatewayClient?
      fun startLeading(); fun stopLeading(); fun stepDown(drainTimeout: Duration); override fun close()
  }
  const val LEADERSHIP_PHASE: Int   // = SmartLifecycle.DEFAULT_PHASE
  class AlwaysLeader(owner: BankConnectionOwner, drainTimeout: Duration) : SmartLifecycle
  @ConfigurationProperties("bank.gateway.leader-election") data class LeaderElectionProperties(...)   // 필드는 Step 4
  fun ConfigurableApplicationContext.bankClient(): BankGatewayClient   // 테스트 도우미
  ```
  Task 6 이 `GatewayConfiguration` 의 `tranNoSource` · `gatewayLeadership` 빈을 선출 켜짐 분기로 바꾼다.

- [ ] **Step 1: Owner 실패 테스트 작성**

`BankConnectionOwnerTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.InMemoryTranNoSource
import io.github.hyeokjinon.banktransfer.gateway.TranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.health.BankConnectionHealthIndicator
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.boot.health.contributor.Status
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** 지금 은행 연결의 단일 진실 (P8-3b 결정 4 · 18): 임기마다 새 클라이언트, 물러나면 즉시 close, 리더가 아니면 "안 보냄". */
class BankConnectionOwnerTest {
    private val bank = MockBankServer(singleSession = true).start()
    private val prepared = AtomicInteger()
    private val tranNo = object : TranNoSource {
        val delegate = InMemoryTranNoSource()
        override fun next() = delegate.next()
        override fun prepare() { prepared.incrementAndGet() }
    }
    private val owner = BankConnectionOwner(tranNo) {
        BankGatewayClient(
            BankGatewayConfig("127.0.0.1", bank.boundPort, reconnectMinDelay = Duration.ofMillis(100), reconnectMaxDelay = Duration.ofMillis(300)),
            tranNoSource = tranNo,
        )
    }

    @AfterTest
    fun tearDown() {
        owner.close()
        bank.close()
    }

    private fun failureOf(block: () -> java.util.concurrent.CompletableFuture<*>): Throwable =
        runCatching { block().get(3, TimeUnit.SECONDS) }.exceptionOrNull().let { (it as? ExecutionException)?.cause ?: it!! }

    @Test
    fun `리더가 아니면 연결 없음 - 요청은 BankNotSentException, 헬스는 UP(standby)`() {
        assertFalse(owner.isLeader)
        assertNull(owner.current())
        assertIs<BankNotSentException>(failureOf { owner.inquireName("090", "1002123456789") })

        val health = BankConnectionHealthIndicator(owner).health()
        assertEquals(Status.UP, health.status)
        assertEquals("standby", health.details["role"])
        assertEquals(0, bank.sessionsOpened)
    }

    @Test
    fun `임기를 시작하면 번호 구간을 준비하고 접속, 헬스는 연결을 따라간다`() {
        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(owner.current()!!.isConnected) }

        assertEquals(1, prepared.get())
        assertEquals("홍길동", owner.inquireName("090", "1002123456789").get(3, TimeUnit.SECONDS).holderName)
        val up = BankConnectionHealthIndicator(owner).health()
        assertEquals(Status.UP, up.status)
        assertEquals("leader", up.details["role"])

        bank.pause()
        await.atMost(Duration.ofSeconds(5)) untilAsserted {
            val down = BankConnectionHealthIndicator(owner).health()
            assertEquals(Status.DOWN, down.status)
            assertEquals("leader", down.details["role"])
        }
    }

    @Test
    fun `물러나면 즉시 연결을 닫고, 다음 임기는 새 클라이언트`() {
        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(owner.current()!!.isConnected) }
        val first = owner.current()

        owner.stopLeading()
        assertNull(owner.current())
        assertFalse(first!!.isConnected)

        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(owner.current()!!.isConnected) }
        assertTrue(owner.current() !== first)
        assertEquals(2, prepared.get())
        assertEquals(1, bank.peakSessions())   // 은행 쪽에서도 동시에 둘이 아니었다
    }

    @Test
    fun `stepDown - 새 요청 거절, 처리 중 전문은 비운 뒤 끊고, 다시 리더가 되지 않는다`() {
        bank.responseDelay = Duration.ofMillis(800)
        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(owner.current()!!.isConnected) }
        val inflight = owner.transfer("090", "1002123456789", 1_000, "m", "T261116STEPDOWN0001")

        val stepping = Thread { owner.stepDown(Duration.ofSeconds(5)) }.apply { start() }
        await.atMost(Duration.ofSeconds(2)) untilAsserted {
            assertIs<BankNotSentException>(failureOf { owner.inquireName("090", "1002123456789") })
        }
        stepping.join(5_000)

        assertEquals(ResponseCodes.OK, inflight.get(1, TimeUnit.SECONDS).header.responseCode)
        assertNull(owner.current())
        owner.startLeading()
        assertNull(owner.current())   // 종료 중에 늦게 온 "리더 됨" 콜백은 무시
    }
}
```

`Status` · `Health` 패키지는 지금 코드(`org.springframework.boot.health.contributor`)와 같다.

Run: `./gradlew :apps:bank-gateway:test --tests '*BankConnectionOwnerTest*'`
Expected: 컴파일 실패.

- [ ] **Step 2: Owner 구현**

`BankConnectionOwner.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.TranNoSource
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

/**
 * 지금 은행 연결(클라이언트)의 단일 진실 (P8-3b 결정 4 · 18). 엔드포인트 · 헬스 · 종료 훅 · 선출 콜백이 모두 여기를 거친다 — 상태가 어긋날 틈이 없다.
 * - 리더 임기마다 [BankGatewayClient] 를 새로 만든다 (close 가 이벤트 루프까지 닫아 재사용할 수 없다 — 임기 단위로 상태를 끊는다).
 * - 리더가 아니면 연결이 없다: 요청은 BankNotSentException → 503 BANK_NOT_SENT (호출자는 안전하게 재시도).
 * - [stepDown] = 정상 교대 1 ~ 3 단계: 새 요청 거절 → 처리 중 전문 비우기 → 끊기 (결정 11).
 */
class BankConnectionOwner(
    private val tranNoSource: TranNoSource,
    private val newClient: () -> BankGatewayClient,
) : BankGateway, AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)

    @Volatile private var client: BankGatewayClient? = null
    @Volatile private var steppingDown = false

    val isLeader: Boolean get() = client != null

    fun current(): BankGatewayClient? = client

    /** 임기 시작: 번호 구간 준비 → 새 클라이언트로 접속 (비동기). 종료 중이면 무시한다 (늦게 온 콜백). */
    @Synchronized
    fun startLeading() {
        if (steppingDown || client != null) return
        tranNoSource.prepare()
        client = newClient().start()
        log.info("bank connection term started — connecting")
    }

    /** 임기 끝: 즉시 끊는다. 응답을 기다리던 전문은 BankUnavailableException (결과 모름 → 결과조회, 결정 13). */
    @Synchronized
    fun stopLeading() {
        val c = client ?: return
        client = null
        c.close()
        log.info("bank connection term ended — closed")
    }

    /** 정상 교대 1 ~ 3: 새 요청 거절 → 응답 대기 전문이 0이 될 때까지(최대 [drainTimeout]) → 끊기. */
    fun stepDown(drainTimeout: Duration) {
        steppingDown = true
        val c = client
        if (c != null) {
            val deadline = System.nanoTime() + drainTimeout.toNanos()
            while (c.pendingCount > 0 && System.nanoTime() < deadline) Thread.sleep(50)
            if (c.pendingCount > 0) log.warn("step down: {} requests still pending after {} — closing anyway", c.pendingCount, drainTimeout)
        }
        stopLeading()
    }

    override fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> =
        withClient { it.inquireName(bankCode, accountNo) }

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> =
        withClient { it.transfer(bankCode, accountNo, amount, memo, reference) }

    override fun inquire(reference: String): CompletableFuture<InquiryResponse> =
        withClient { it.inquire(reference) }

    private fun <T> withClient(call: (BankGatewayClient) -> CompletableFuture<T>): CompletableFuture<T> {
        if (steppingDown) return CompletableFuture.failedFuture(BankNotSentException("gateway is stepping down"))
        val c = client ?: return CompletableFuture.failedFuture(BankNotSentException("not the bank connection leader"))
        return call(c)
    }

    override fun close() {
        steppingDown = true
        stopLeading()
    }
}
```

- [ ] **Step 3: 헬스 구현**

`BankConnectionHealthIndicator.kt` 전체:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.health

import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator

/**
 * 은행 회선 상태 — /actuator/health/bankConnection (P8-1). K8s readiness 그룹에 들어간다 (P8-3a).
 * P8-3b: 대기자(리더 아님)는 UP(`role: standby`) — 둘 다 Ready 여야 롤링(`maxUnavailable 0`) · `rollout status` · PDB 가 돈다 (스펙 §16).
 * 트래픽은 readiness 가 아니라 리더 라벨 + Service selector 가 고른다. 리더인데 회선이 끊기면 DOWN (P8-3a B1 그대로 — 트래픽에서 빠진다).
 */
class BankConnectionHealthIndicator(private val owner: BankConnectionOwner) : HealthIndicator {
    override fun health(): Health {
        val client = owner.current() ?: return Health.up().withDetail("role", "standby").build()
        return if (client.isConnected) {
            Health.up().withDetail("role", "leader").withDetail("pending", client.pendingCount).build()
        } else {
            Health.down().withDetail("role", "leader").withDetail("reason", "bank channel not connected").build()
        }
    }
}
```

Run: `./gradlew :apps:bank-gateway:test --tests '*BankConnectionOwnerTest*'`
Expected: 4개 PASS.

- [ ] **Step 4: 종료 훅 · 설정 · 조립**

`AlwaysLeader.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import org.springframework.context.SmartLifecycle
import java.time.Duration

/**
 * 종료 훅의 phase. 높은 phase 가 먼저 멈춘다 — 웹 서버 graceful 종료(DEFAULT_PHASE - 1024)보다 먼저 물러나,
 * 웹 서버가 아직 응답을 돌려줄 수 있을 때 처리 중 전문을 비운다 (P8-3b 결정 12).
 */
const val LEADERSHIP_PHASE: Int = SmartLifecycle.DEFAULT_PHASE

/** 선출 꺼짐 (IntelliJ · Compose · 테스트 기본): 기동 때 이미 리더(조립에서 startLeading). 종료 때만 거절 → 비우기 → 끊기. */
class AlwaysLeader(private val owner: BankConnectionOwner, private val drainTimeout: Duration) : SmartLifecycle {
    @Volatile private var running = false

    override fun start() {
        running = true
    }

    override fun stop() {
        owner.stepDown(drainTimeout)
        running = false
    }

    override fun isRunning(): Boolean = running

    override fun getPhase(): Int = LEADERSHIP_PHASE
}
```

`GatewayConfiguration.kt` 전체:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.gateway.InMemoryTranNoSource
import io.github.hyeokjinon.banktransfer.gateway.TranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.health.BankConnectionHealthIndicator
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.AlwaysLeader
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock
import java.time.Duration
import java.time.LocalTime
import java.time.ZoneId

@ConfigurationProperties("bank.gateway")
data class GatewayBankProperties(
    val host: String = "localhost",
    val port: Int = 9090,
    val responseTimeout: Duration = Duration.ofSeconds(3),
    val pollInterval: Duration = Duration.ofSeconds(30),
)

/** api 와 같은 규칙 (KST 벽시계 "HH:mm", 시작 포함 · 끝 제외, 따옴표 필수). */
@ConfigurationProperties("bank.maintenance")
data class GatewayMaintenanceProperties(
    val enabled: Boolean = true,
    val start: String = "23:50",
    val end: String = "00:10",
) {
    fun toWindow() = BankMaintenanceWindow(LocalTime.parse(start), LocalTime.parse(end), ZoneId.of("Asia/Seoul"), enabled)
}

/**
 * 게이트웨이 이중화 (P8-3b). 꺼져 있으면(기본) 기동 즉시 리더 — 지금까지와 같다.
 * 켜면 K8s Lease 로 리더 1대만 은행에 붙고, 전문번호는 ConfigMap 구간 예약 (스펙 §3 결정 3 · 6 · 8).
 */
@ConfigurationProperties("bank.gateway.leader-election")
data class LeaderElectionProperties(
    val enabled: Boolean = false,
    /** Pod 이름 (Lease holder · 라벨 대상). application.yml 이 POD_NAME 환경변수에서 읽는다. */
    val identity: String = "",
    /** POD_NAMESPACE. */
    val namespace: String = "",
    /** 비우면 Pod 안 자동 설정(ServiceAccount 토큰). 테스트가 가짜 API 서버 주소를 넣는다. */
    val apiServerUrl: String = "",
    val leaseName: String = "bank-gateway-leader",
    val tranNoConfigMap: String = "bank-gateway-tran-no",
    val leaseDuration: Duration = Duration.ofSeconds(15),
    val renewDeadline: Duration = Duration.ofSeconds(10),
    val retryPeriod: Duration = Duration.ofSeconds(2),
    /** 정상 교대 때 처리 중 전문을 기다리는 최대 시간 (응답 시간 초과 3초 + 여유). */
    val drainTimeout: Duration = Duration.ofSeconds(5),
    val tranNoBlockSize: Long = 1000,
)

/** 조립은 여기서만. 은행 연결은 이 앱에 하나뿐이고(스펙 P8-1 §2 결정 7), 그 주인은 [BankConnectionOwner] 다 (P8-3b). */
@Configuration(proxyBeanMethods = false)
class GatewayConfiguration {

    @Bean
    fun clock(): Clock = Clock.system(ZoneId.of("Asia/Seoul"))   // 저장소 시간대 정책은 KST (docs/database.md)

    /** 전문번호 발급. Task 6 이 선출 켜짐이면 ConfigMap 구간 예약으로 바꾼다. */
    @Bean
    fun tranNoSource(): TranNoSource = InMemoryTranNoSource()

    /**
     * 은행 연결의 주인. 컨트롤러는 이 빈을 BankGateway 로 받는다 (리더가 아니면 503 BANK_NOT_SENT).
     * 점검 중에는 전문을 보내지 않는다 (P5-1) — 503 BANK_NOT_SENT. 선출이 꺼져 있으면 여기서 바로 리더가 된다 (기동 중 readiness 가 standby 로 잠깐 UP 이 되지 않게).
     */
    @Bean(destroyMethod = "close")
    fun bankConnectionOwner(
        props: GatewayBankProperties,
        maintenance: GatewayMaintenanceProperties,
        election: LeaderElectionProperties,
        clock: Clock,
        tranNoSource: TranNoSource,
    ): BankConnectionOwner {
        val window = maintenance.toWindow()
        val config = BankGatewayConfig(host = props.host, port = props.port, responseTimeout = props.responseTimeout, pollInterval = props.pollInterval)
        return BankConnectionOwner(tranNoSource) {
            BankGatewayClient(
                config,
                clock,
                sendGate = { if (window.isActive(clock.instant())) "bank maintenance window" else null },
                tranNoSource = tranNoSource,
            )
        }.apply { if (!election.enabled) startLeading() }
    }

    /** 종료 훅 (웹 서버보다 먼저). Task 6 이 선출 켜짐이면 GatewayLeaderElection 으로 바꾼다. */
    @Bean
    fun gatewayLeadership(owner: BankConnectionOwner, election: LeaderElectionProperties): SmartLifecycle =
        AlwaysLeader(owner, election.drainTimeout)

    @Bean
    fun bankConnectionHealthIndicator(owner: BankConnectionOwner) = BankConnectionHealthIndicator(owner)
}
```

`application.yml` 의 `bank.gateway` 블록(`poll-interval: 30s` 다음)에:

```yaml
    leader-election:           # 게이트웨이 이중화 (P8-3b). 기본 꺼짐 = 기동 즉시 리더 (IntelliJ · Compose · 테스트). K8s 매니페스트가 켠다
      enabled: false
      identity: ${POD_NAME:}         # Downward API (Lease holder · 리더 라벨 대상)
      namespace: ${POD_NAMESPACE:}
      lease-duration: 15s            # 갱신 기한(10s)에 옛 리더가 스스로 끊고, 새 리더는 만료(15s) 뒤 — 시계 차 5초 미만 전제 (스펙 §13)
      renew-deadline: 10s
      retry-period: 2s
      drain-timeout: 5s              # 정상 교대 때 처리 중 전문을 기다리는 최대 시간 (응답 시간 초과 3s + 여유)
      tran-no-block-size: 1000       # 전문번호 구간 예약 단위 (ConfigMap bank-gateway-tran-no)
```

- [ ] **Step 5: 테스트 도우미 · 기존 테스트 이관**

`GatewayApps.kt` 의 `fun ConfigurableApplicationContext.port()` 다음에:

```kotlin
/** 지금 리더의 은행 클라이언트 (P8-3b — 클라이언트는 빈이 아니라 Owner 가 임기마다 만든다). */
fun ConfigurableApplicationContext.bankClient(): BankGatewayClient =
    getBean(BankConnectionOwner::class.java).current() ?: error("this gateway is not the bank connection leader")
```

(import: `io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient`, `io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner`)

세 파일의 `.getBean(BankGatewayClient::class.java)` 를 `.bankClient()` 로 바꾼다:

Run: `sed -i 's/\.getBean(BankGatewayClient::class\.java)/.bankClient()/g' apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GatewayContractTest.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GatewayHttpContractTest.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/ToxiproxyGatewayTest.kt`
그 뒤 `grep -rn 'getBean(BankGatewayClient' apps/bank-gateway/src/test` 가 비었는지 확인. 같은 패키지라 `bankClient` import 는 필요 없다.

- [ ] **Step 6: 종료 순서 테스트 작성**

`GracefulStepDownTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp

import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.LEADERSHIP_PHASE
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.boot.WebApplicationType
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** 종료 훅 (P8-3b 결정 11 · 12): SIGTERM(컨텍스트 종료) → 새 요청 거절 → 처리 중 전문 비우기 → 끊기. 웹 서버보다 먼저. */
class GracefulStepDownTest {

    @Test
    fun `종료 때 처리 중 전문은 응답을 받은 뒤 끊고, 새 요청은 안 보냄`() {
        MockBankServer(responseDelay = Duration.ofMillis(800)).start().use { bank ->
            val ctx = GatewayApps.start(WebApplicationType.REACTIVE, bank.boundPort, "--bank.gateway.response-timeout=3s")
            await.atMost(Duration.ofSeconds(10)) untilAsserted { assertTrue(ctx.bankClient().isConnected) }
            val owner = ctx.getBean(BankConnectionOwner::class.java)
            val inflight = owner.transfer("090", "1002123456789", 1_000, "m", "T261116GRACEFUL0001")

            val closing = Thread { ctx.close() }.apply { start() }
            await.atMost(Duration.ofSeconds(2)) untilAsserted {
                val error = runCatching { owner.inquireName("090", "1002123456789").get(1, TimeUnit.SECONDS) }.exceptionOrNull()
                assertIs<BankNotSentException>((error as? ExecutionException)?.cause)
            }
            closing.join(10_000)

            assertEquals(ResponseCodes.OK, inflight.get(1, TimeUnit.SECONDS).header.responseCode)
            assertEquals(1, bank.depositsOf("1002123456789").size)
        }
    }

    @Test
    fun `종료 훅은 웹 서버 graceful 종료보다 먼저 멈춘다 (phase 가 더 높다)`() {
        assertTrue(LEADERSHIP_PHASE > org.springframework.boot.web.server.context.WebServerGracefulShutdownLifecycle.SMART_LIFECYCLE_PHASE)
    }
}
```

`WebServerGracefulShutdownLifecycle` 의 Boot 4.1.1 패키지는 jar 에서 확인한다:
Run: `find ~/.gradle/caches -name 'spring-boot-web-server-4.1.1.jar' -o -name 'spring-boot-4.1.1.jar' | head -2 | xargs -I{} sh -c 'unzip -l {} | grep GracefulShutdownLifecycle'`
다른 패키지면 import 를 맞춘다. 상수가 없으면 `SmartLifecycle.DEFAULT_PHASE - 1024` 와 비교하고 주석에 근거(소스 위치)를 적는다.

- [ ] **Step 7: 전체 테스트**

Run: `./gradlew :apps:bank-gateway:test`
Expected: 전부 PASS (기존 계약 · 프로브 · 헬스 테스트 + 새 6개). `BankConnectionHealthTest` · `ProbeGroupsTest` 는 선출 꺼짐 = 리더라 그대로 통과해야 한다.

- [ ] **Step 8: 커밋**

```bash
git add apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/BankConnectionOwner.kt apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/AlwaysLeader.kt apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/config/GatewayConfiguration.kt apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/health/BankConnectionHealthIndicator.kt apps/bank-gateway/src/main/resources/application.yml apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GatewayApps.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GatewayContractTest.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GatewayHttpContractTest.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/ToxiproxyGatewayTest.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/BankConnectionOwnerTest.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GracefulStepDownTest.kt
git commit -m "feat(p8-3b): BankConnectionOwner — 임기마다 새 연결 · 대기자는 UP(standby) · 종료 훅(거절 → 비우기 → 끊기, 웹 서버보다 먼저)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Lease 리더 선출 · 리더 라벨 · 구간 예약 조립

**Files:**
- Create: `apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/LeaderLabel.kt`
- Create: `apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/GatewayLeaderElection.kt`
- Modify: `apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/config/GatewayConfiguration.kt` (`tranNoSource` · `gatewayLeadership` · `kubernetesClient` 빈)
- Create: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/LeaderLabelTest.kt`
- Create: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/GatewayLeaderElectionTest.kt`
- Create: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GatewayFailoverTest.kt`

**Interfaces:**
- Consumes: `BankConnectionOwner` · `LeaderElectionProperties` · `LEADERSHIP_PHASE` · `AlwaysLeader` (Task 5), `ConfigMapTranNoSource` (Task 4), `MockK8s` (Task 1), `MockBankServer(singleSession, uniqueTranNo)` · `peakSessions()` (Task 2).
- Produces:
  ```kotlin
  class LeaderLabel(k8s: KubernetesClient, namespace: String, podName: String) { fun ensure(leader: Boolean); companion object { const val KEY = "bank-transfer/gateway-role"; const val VALUE = "leader" } }
  class GatewayLeaderElection(k8s: KubernetesClient, props: LeaderElectionProperties, owner: BankConnectionOwner, label: LeaderLabel) : SmartLifecycle
  ```
  로그 문구: `leader election: became leader ({id})`, `leader election: lost leadership ({id}) — bank connection closed`, `leader election: stepping down (drain {ms}ms)` — Task 8 이 grep 한다.

- [ ] **Step 1: 라벨 실패 테스트**

`LeaderLabelTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.github.hyeokjinon.banktransfer.gatewayapp.k8s.MockK8s
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** 리더 라벨 (P8-3b 결정 5): Service selector 가 이 라벨로 리더 Pod 만 고른다. */
class LeaderLabelTest {
    private val k8s = MockK8s()

    @AfterTest
    fun tearDown() = k8s.close()

    private fun role() = k8s.client.pods().inNamespace("test").withName("gw-a").get().metadata.labels[LeaderLabel.KEY]

    @Test
    fun `리더면 붙이고 아니면 뗀다 - 다른 라벨은 그대로`() {
        k8s.createPod("test", "gw-a")
        val label = LeaderLabel(k8s.client, "test", "gw-a")

        label.ensure(true)
        assertEquals(LeaderLabel.VALUE, role())
        label.ensure(false)
        assertNull(role())
        assertEquals("bank-gateway", k8s.client.pods().inNamespace("test").withName("gw-a").get().metadata.labels["app"])
    }

    @Test
    fun `처음엔 상태를 모르니 대기자도 한 번 뗀다 - 컨테이너 재시작으로 남은 라벨 정리`() {
        k8s.createPod("test", "gw-a")
        LeaderLabel(k8s.client, "test", "gw-a").ensure(true)   // 앞 프로세스가 붙이고 죽었다

        LeaderLabel(k8s.client, "test", "gw-a").ensure(false)  // 같은 Pod 에서 다시 뜬 프로세스

        assertNull(role())
    }

    @Test
    fun `API 서버가 없으면 던지지 않는다 - 다음 주기에 다시`() {
        k8s.createPod("test", "gw-a")
        val label = LeaderLabel(k8s.client, "test", "gw-a")
        k8s.stopApiServer()

        label.ensure(true)   // 예외 없이 끝나야 한다
    }
}
```

Run: `./gradlew :apps:bank-gateway:test --tests '*LeaderLabelTest*'`
Expected: 컴파일 실패.

- [ ] **Step 2: 라벨 구현**

`LeaderLabel.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.base.PatchContext
import io.fabric8.kubernetes.client.dsl.base.PatchType
import org.slf4j.LoggerFactory

/**
 * 자기 Pod 의 리더 라벨 (P8-3b 결정 5 · 20). Service `bank-gateway` 의 selector 에 이 라벨이 있어 트래픽은 리더에게만 간다.
 * [ensure] 는 "원하는 상태" 로 맞춘다 — 실패하면 다음 주기(GatewayLeaderElection, retry 2s)에 다시. 처음엔 상태를 모르므로 무조건 한 번 맞춘다
 * (컨테이너만 재시작되면 Pod 라벨은 남는다).
 * API 서버와 끊긴 옛 리더는 라벨을 못 뗀다 → 잠깐 라벨 Pod 가 둘. 그쪽으로 간 요청은 연결이 없어 503 BANK_NOT_SENT 로 재시도된다 (스펙 §6-2).
 */
class LeaderLabel(private val k8s: KubernetesClient, private val namespace: String, private val podName: String) {
    private val log = LoggerFactory.getLogger(javaClass)
    private var labeled: Boolean? = null   // null = 모름

    @Synchronized
    fun ensure(leader: Boolean) {
        if (labeled == leader) return
        val value = if (leader) "\"$VALUE\"" else "null"   // JSON merge patch: null 은 키 삭제
        try {
            k8s.pods().inNamespace(namespace).withName(podName)
                .patch(PatchContext.of(PatchType.JSON_MERGE), """{"metadata":{"labels":{"$KEY":$value}}}""")
            labeled = leader
            log.info("leader label {} on {}", if (leader) "set" else "removed", podName)
        } catch (e: Exception) {
            log.warn("leader label update failed (retry next period): {}", e.message)
        }
    }

    companion object {
        const val KEY = "bank-transfer/gateway-role"
        const val VALUE = "leader"
    }
}
```

Run: `./gradlew :apps:bank-gateway:test --tests '*LeaderLabelTest*'`
Expected: 3개 PASS.

- [ ] **Step 3: 선출 실패 테스트**

`GatewayLeaderElectionTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.InMemoryTranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.config.LeaderElectionProperties
import io.github.hyeokjinon.banktransfer.gatewayapp.k8s.MockK8s
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import java.time.Duration
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * Lease 리더 선출 (P8-3b 결정 3 · 6 · 11): 둘 중 하나만 은행에 붙는다, 정상 교대는 반납으로 바로 넘어간다,
 * API 서버와 끊기면 갱신 기한 안에 스스로 끊는다 (실험 H5 의 단위 대체 — kind CNI 가 egress 를 집행하지 않을 때의 근거).
 * 시간은 줄여서: Lease 3s · 갱신 기한 2s · 재시도 500ms.
 */
class GatewayLeaderElectionTest {
    private val k8s = MockK8s()
    private val bank = MockBankServer(singleSession = true).start()
    private val ns = "test"
    private val started = mutableListOf<GatewayLeaderElection>()

    @AfterTest
    fun tearDown() {
        started.forEach { runCatching { it.stop() } }
        bank.close()
        k8s.close()
    }

    private fun props(id: String) = LeaderElectionProperties(
        enabled = true, identity = id, namespace = ns,
        leaseDuration = Duration.ofSeconds(3), renewDeadline = Duration.ofSeconds(2), retryPeriod = Duration.ofMillis(500),
        drainTimeout = Duration.ofSeconds(1),
    )

    private fun gateway(id: String): Pair<GatewayLeaderElection, BankConnectionOwner> {
        k8s.createPod(ns, id)
        val tranNo = InMemoryTranNoSource()
        val owner = BankConnectionOwner(tranNo) {
            BankGatewayClient(
                BankGatewayConfig("127.0.0.1", bank.boundPort, reconnectMinDelay = Duration.ofMillis(100), reconnectMaxDelay = Duration.ofMillis(300)),
                tranNoSource = tranNo,
            )
        }
        val election = GatewayLeaderElection(k8s.client, props(id), owner, LeaderLabel(k8s.client, ns, id))
        election.start()
        started += election
        return election to owner
    }

    private fun holder() = k8s.client.leases().inNamespace(ns).withName("bank-gateway-leader").get()?.spec?.holderIdentity
    private fun role(pod: String) = k8s.client.pods().inNamespace(ns).withName(pod).get().metadata.labels[LeaderLabel.KEY]

    @Test
    fun `둘 중 하나만 리더 - 그 Pod 에만 라벨, 은행 세션은 하나`() {
        val (_, a) = gateway("gw-a")
        val (_, b) = gateway("gw-b")

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            assertEquals(1, listOf(a, b).count { it.isLeader })
            val leader = if (a.isLeader) "gw-a" else "gw-b"
            assertEquals(leader, holder())
            assertEquals(LeaderLabel.VALUE, role(leader))
            assertEquals(null, role(if (leader == "gw-a") "gw-b" else "gw-a"))
            assertTrue((if (a.isLeader) a else b).current()!!.isConnected)
        }
        assertEquals(1, bank.peakSessions())
    }

    @Test
    fun `리더가 정상 종료하면 반납 - 대기자가 만료를 기다리지 않고 이어받는다`() {
        val (electionA, a) = gateway("gw-a")
        await.atMost(Duration.ofSeconds(10)) untilAsserted { assertTrue(a.isLeader) }
        val (_, b) = gateway("gw-b")

        electionA.stop()   // 거절 → 비우기 → 끊기 → 라벨 떼기 → 반납
        started -= electionA

        assertFalse(a.isLeader)
        assertEquals(null, role("gw-a"))
        assertNotEquals("gw-a", holder())
        // 반납했으니 Lease 만료(3s)가 아니라 재시도 주기(500ms) 수준에 넘어간다
        await.atMost(Duration.ofMillis(2_500)) untilAsserted {
            assertTrue(b.isLeader)
            assertEquals("gw-b", holder())
            assertEquals(LeaderLabel.VALUE, role("gw-b"))
            assertTrue(b.current()!!.isConnected)
        }
        assertEquals(1, bank.peakSessions())
    }

    @Test
    fun `API 서버와 끊기면 리더는 갱신 기한 안에 은행 연결을 끊는다 (자기 차단)`() {
        val (_, a) = gateway("gw-a")
        await.atMost(Duration.ofSeconds(10)) untilAsserted { assertTrue(a.isLeader && a.current()!!.isConnected) }

        val cutAt = System.nanoTime()
        k8s.stopApiServer()

        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertFalse(a.isLeader) }
        val fencedMs = (System.nanoTime() - cutAt) / 1_000_000
        assertTrue(fencedMs <= 2_000 + 1_500, "갱신 기한(2s) + 재시도 여유 안에 끊어야 한다: ${fencedMs}ms")
    }
}
```

Run: `./gradlew :apps:bank-gateway:test --tests '*GatewayLeaderElectionTest*'`
Expected: 컴파일 실패.

- [ ] **Step 4: 선출 구현**

`GatewayLeaderElection.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock
import io.github.hyeokjinon.banktransfer.gatewayapp.config.LeaderElectionProperties
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * K8s Lease 리더 선출 (P8-3b 결정 3 · 6 · 11 · 12). 리더만 은행에 붙는다.
 * - 콜백 · 재후보 · 라벨 맞추기는 한 스레드(gateway-leader)에서 차례로 — 시작 · 끝이 뒤섞이지 않는다.
 * - 갱신 기한(10s) 안에 갱신하지 못하면 fabric8 이 임기를 끝낸다 → 즉시 연결을 끊는다 (자기 차단). 새 리더는 만료(15s) 뒤 — 겹침 없음 (시계 차 < 5s 전제).
 * - 선출기는 한 번 쓰고 버린다: 임기를 잃으면 새 선출기로 다시 후보가 된다.
 * - 종료(SIGTERM) = 정상 교대: 거절 → 비우기 → 끊기 → 라벨 떼기 → Lease 반납. 대기자는 만료를 기다리지 않고 재시도 주기(2s) 안에 이어받는다.
 */
class GatewayLeaderElection(
    private val k8s: KubernetesClient,
    private val props: LeaderElectionProperties,
    private val owner: BankConnectionOwner,
    private val label: LeaderLabel,
) : SmartLifecycle {

    private val log = LoggerFactory.getLogger(javaClass)
    private val worker = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "gateway-leader").apply { isDaemon = true } }

    @Volatile private var running = false
    @Volatile private var elector: CompletableFuture<*>? = null

    override fun start() {
        require(props.identity.isNotBlank()) { "bank.gateway.leader-election.identity (POD_NAME) is empty" }
        require(props.namespace.isNotBlank()) { "bank.gateway.leader-election.namespace (POD_NAMESPACE) is empty" }
        running = true
        worker.execute { runElector() }
        // 라벨을 "지금 역할" 에 맞춘다 — 붙이기 · 떼기 실패, API 단절 동안 못 뗀 라벨, 컨테이너 재시작으로 남은 라벨을 주기마다 고친다
        worker.scheduleWithFixedDelay({ if (running) label.ensure(owner.isLeader) }, 0, props.retryPeriod.toMillis(), TimeUnit.MILLISECONDS)
    }

    private fun runElector() {
        if (!running) return
        val config = LeaderElectionConfigBuilder()
            .withName("bank-gateway")
            .withLock(LeaseLock(props.namespace, props.leaseName, props.identity))
            .withLeaseDuration(props.leaseDuration)
            .withRenewDeadline(props.renewDeadline)
            .withRetryPeriod(props.retryPeriod)
            .withReleaseOnCancel(true)
            .withLeaderCallbacks(
                LeaderCallbacks(
                    { submit { onStartLeading() } },
                    { submit { onStopLeading() } },
                    { holder -> log.info("leader election: current leader is {}", holder) },
                ),
            )
            .build()
        val future = k8s.leaderElector().withConfig(config).build().start()
        elector = future
        future.whenComplete { _, error ->
            if (error != null && error !is CancellationException) log.warn("leader election: elector ended: {}", error.message)
            if (running) runCatching { worker.schedule({ runElector() }, props.retryPeriod.toMillis(), TimeUnit.MILLISECONDS) }
        }
    }

    private fun submit(task: () -> Unit) {
        runCatching { worker.execute(task) }   // 종료 뒤 늦게 온 콜백은 버린다 (owner 는 이미 물러났다)
    }

    private fun onStartLeading() {
        if (!running) return
        log.info("leader election: became leader ({})", props.identity)
        owner.startLeading()   // 구간 예약 → 접속. 접속 전에는 readiness DOWN 이라 라벨이 붙어도 트래픽이 오지 않는다
        label.ensure(true)
    }

    private fun onStopLeading() {
        owner.stopLeading()
        log.warn("leader election: lost leadership ({}) — bank connection closed", props.identity)
        if (running) label.ensure(false)
    }

    override fun stop() {
        log.info("leader election: stepping down (drain {}ms)", props.drainTimeout.toMillis())
        running = false
        owner.stepDown(props.drainTimeout)   // 1 · 2 · 3: 거절 → 비우기 → 끊기
        label.ensure(false)                  // 4: Service 대상에서 빠진다
        elector?.cancel(true)                // 5: releaseOnCancel — holder 비움
        awaitReleased()
        worker.shutdownNow()
    }

    /** 반납이 API 서버에 닿을 때까지 최대 2초 — 프로세스가 먼저 끝나면 대기자가 만료(15s)를 기다린다. */
    private fun awaitReleased() {
        val deadline = System.nanoTime() + 2_000_000_000L
        while (System.nanoTime() < deadline) {
            val holder = runCatching { k8s.leases().inNamespace(props.namespace).withName(props.leaseName).get()?.spec?.holderIdentity }
                .getOrElse { return }   // API 서버가 없으면 기다려도 소용없다
            if (holder != props.identity) return
            Thread.sleep(100)
        }
        log.warn("leader election: lease release not observed within 2s — standby will wait for expiry")
    }

    override fun isRunning(): Boolean = running

    override fun getPhase(): Int = LEADERSHIP_PHASE
}
```

Run: `./gradlew :apps:bank-gateway:test --tests '*GatewayLeaderElectionTest*'`
Expected: 3개 PASS. "정상 종료 반납" 이 2.5초를 넘기면 fabric8 의 cancel 이 반납을 비동기로 하는지 보고서에 적고 `awaitReleased` 가 기다린 시간을 로그로 확인한다 (테스트 시간을 늘리지 않는다 — 반납이 안 되는 것이 결함이다).

- [ ] **Step 5: 조립 (선출 켜짐 분기)**

`GatewayConfiguration.kt`:

(a) import 추가:

```kotlin
import io.fabric8.kubernetes.client.Config
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.ConfigMapTranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.GatewayLeaderElection
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.LeaderLabel
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
```

(b) `tranNoSource` · `gatewayLeadership` 빈을 바꾸고 `kubernetesClient` 빈을 더한다:

```kotlin
    /**
     * K8s API 클라이언트 — 선출이 켜졌을 때만 (Pod 안 ServiceAccount 토큰으로 자동 설정, 테스트는 가짜 API 서버 주소).
     * 빨리 실패: 재시도 없음 · 연결 2초 · 요청 5초. fabric8 기본 재시도(지수 백오프)는 API 서버 단절 때 갱신 실패를 늦게 알려
     * "갱신 기한(10s) 안에 자기 차단" 을 흐린다 — 재시도는 선출기의 재시도 주기(2s)가 맡는다.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "bank.gateway.leader-election", name = ["enabled"], havingValue = "true")
    fun kubernetesClient(election: LeaderElectionProperties): KubernetesClient {
        val base = if (election.apiServerUrl.isBlank()) {
            Config.autoConfigure(null)
        } else {
            ConfigBuilder(Config.empty()).withMasterUrl(election.apiServerUrl).withNamespace(election.namespace).build()
        }
        val config = ConfigBuilder(base).withRequestRetryBackoffLimit(0).withConnectionTimeout(2_000).withRequestTimeout(5_000).build()
        return KubernetesClientBuilder().withConfig(config).build()
    }

    /** 전문번호 발급: 선출 켜짐 = ConfigMap 구간 예약(게이트웨이 여러 대 사이 유일), 꺼짐 = 메모리 카운터 (지금과 같다). */
    @Bean
    fun tranNoSource(election: LeaderElectionProperties, k8s: ObjectProvider<KubernetesClient>, clock: Clock): TranNoSource =
        if (election.enabled) {
            ConfigMapTranNoSource(k8s.getObject(), election.namespace, election.tranNoConfigMap, clock, election.tranNoBlockSize)
        } else {
            InMemoryTranNoSource()
        }

    /** 종료 훅 + (켜짐이면) 선출. 둘 다 웹 서버 graceful 종료보다 먼저 멈춘다. */
    @Bean
    fun gatewayLeadership(owner: BankConnectionOwner, election: LeaderElectionProperties, k8s: ObjectProvider<KubernetesClient>): SmartLifecycle =
        if (election.enabled) {
            val client = k8s.getObject()
            GatewayLeaderElection(client, election, owner, LeaderLabel(client, election.namespace, election.identity))
        } else {
            AlwaysLeader(owner, election.drainTimeout)
        }
```

`ConfigMapTranNoSource` 는 `AutoCloseable` 이라 Spring 의 추론 destroy(`close`)가 닫는다.

- [ ] **Step 6: 두 앱 교대 통합 테스트**

`GatewayFailoverTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp

import io.github.hyeokjinon.banktransfer.gatewayapp.k8s.MockK8s
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.LeaderLabel
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.boot.WebApplicationType
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.web.client.RestClient
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 게이트웨이 앱 두 대 + 가짜 API 서버 + 세션 하나 · 번호 유일 모의 은행 (P8-3b 성공 기준 1 · 2 · 3 의 앱 단위 확인).
 * 리더만 은행에 붙고, 둘 다 readiness 200, 리더가 정상 종료하면 대기자가 이어받아 번호가 겹치지 않게 보낸다.
 */
class GatewayFailoverTest {
    private val ns = "test"

    private fun start(k8s: MockK8s, bankPort: Int, id: String): ConfigurableApplicationContext {
        k8s.createPod(ns, id)
        return GatewayApps.start(
            WebApplicationType.REACTIVE, bankPort,
            "--bank.gateway.response-timeout=3s",
            "--bank.gateway.leader-election.enabled=true",
            "--bank.gateway.leader-election.api-server-url=${k8s.url}",
            "--bank.gateway.leader-election.namespace=$ns",
            "--bank.gateway.leader-election.identity=$id",
            "--bank.gateway.leader-election.lease-duration=3s",
            "--bank.gateway.leader-election.renew-deadline=2s",
            "--bank.gateway.leader-election.retry-period=500ms",
            "--bank.gateway.leader-election.drain-timeout=1s",
            "--bank.gateway.leader-election.tran-no-block-size=10",
        )
    }

    private fun ConfigurableApplicationContext.owner() = getBean(BankConnectionOwner::class.java)
    private fun ConfigurableApplicationContext.readiness(): Int =
        RestClient.create("http://127.0.0.1:${port()}").get().uri("/actuator/health/readiness").exchange { _, res -> res.statusCode.value() }

    @Test
    fun `리더만 은행에 붙고 둘 다 Ready, 리더가 내려가면 대기자가 이어받아 번호가 겹치지 않는다`() {
        MockK8s().use { k8s ->
            MockBankServer(singleSession = true, uniqueTranNo = true).start().use { bank ->
                val a = start(k8s, bank.boundPort, "gw-a")
                val b = start(k8s, bank.boundPort, "gw-b")
                try {
                    await.atMost(Duration.ofSeconds(15)) untilAsserted {
                        assertEquals(1, listOf(a, b).count { it.owner().isLeader })
                        assertTrue(listOf(a, b).first { it.owner().isLeader }.bankClient().isConnected)
                        assertEquals(200, a.readiness())
                        assertEquals(200, b.readiness())
                    }
                    val (leader, standby) = if (a.owner().isLeader) a to b else b to a
                    repeat(12) { i ->   // 구간(10개)을 넘겨 미리 예약까지 거친다
                        assertEquals(ResponseCodes.OK, leader.owner().transfer("090", "1002123456789", 1_000, "m", "T261116FAILOVER%04d".format(i)).get(3, TimeUnit.SECONDS).header.responseCode)
                    }

                    leader.close()   // 정상 교대

                    await.atMost(Duration.ofSeconds(5)) untilAsserted {
                        assertTrue(standby.owner().isLeader)
                        assertTrue(standby.bankClient().isConnected)
                    }
                    repeat(12) { i ->
                        assertEquals(ResponseCodes.OK, standby.owner().transfer("090", "1002123456789", 1_000, "m", "T261116TAKEOVER%04d".format(i)).get(3, TimeUnit.SECONDS).header.responseCode)
                    }
                    assertEquals(24, bank.depositsOf("1002123456789").size)   // 3001(번호 중복) 없이 전부 처리
                    assertEquals(1, bank.peakSessions())
                    val standbyPod = if (standby === a) "gw-a" else "gw-b"
                    assertEquals(LeaderLabel.VALUE, k8s.client.pods().inNamespace(ns).withName(standbyPod).get().metadata.labels[LeaderLabel.KEY])
                } finally {
                    listOf(a, b).forEach { runCatching { it.close() } }
                }
            }
        }
    }
}
```

Run: `./gradlew :apps:bank-gateway:test`
Expected: 전부 PASS.

- [ ] **Step 7: 전체 회귀**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL. 테스트 수 = 623 + 이번 Task 들의 새 테스트 (보고서에 숫자).

- [ ] **Step 8: 커밋**

```bash
git add apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/LeaderLabel.kt apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/GatewayLeaderElection.kt apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/config/GatewayConfiguration.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/LeaderLabelTest.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/leader/GatewayLeaderElectionTest.kt apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GatewayFailoverTest.kt
git commit -m "feat(p8-3b): Lease 리더 선출 — 리더 라벨 · 자기 차단 · 정상 교대(거절 → 비우기 → 끊기 → 라벨 → 반납) · 구간 예약 조립

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: 매니페스트 · 스모크 [6] · [9]

**Files:**
- Modify: `infra/k8s/base/bank-gateway.yaml`
- Create: `infra/k8s/base/rbac-gateway.yaml`
- Modify: `infra/k8s/base/pdb.yaml`
- Modify: `infra/k8s/base/mock-bank.yaml`
- Modify: `infra/k8s/base/kustomization.yaml`
- Create: `infra/k8s/overlays/experiment-gateway-single/kustomization.yaml`
- Modify: `infra/k8s/lib.sh`
- Modify: `infra/k8s/smoke.sh`

**Interfaces:**
- Consumes: 환경변수 `BANK_GATEWAY_LEADERELECTION_ENABLED` · `POD_NAME` · `POD_NAMESPACE` (Task 5 application.yml), 로그 `sessions=N` (Task 2), Lease · 라벨 이름 (Global Constraints).
- Produces: `lib.sh` 의 `gw_leader` (리더 라벨 Pod 이름, 없으면 빈 줄) — Task 8 이 쓴다.

- [ ] **Step 1: 게이트웨이 매니페스트**

`infra/k8s/base/bank-gateway.yaml` 전체:

```yaml
# 은행 게이트웨이 2대 (P8-3b). active-standby: Lease 리더 1대만 은행에 붙고, 리더가 자기 Pod 에 붙이는 라벨로 Service 가 리더만 고른다.
# 둘 다 Ready (대기자 bankConnection = UP standby) — 롤링(maxUnavailable 0) · rollout status · PDB 가 정상 동작한다 (스펙 §16)
apiVersion: v1
kind: Service
metadata:
  name: bank-gateway
  labels: {app: bank-gateway}
spec:
  selector: {app: bank-gateway, bank-transfer/gateway-role: leader}
  ports:
    - {name: http, port: 8082}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bank-gateway
spec:
  replicas: 2
  strategy:
    type: RollingUpdate
    rollingUpdate: {maxUnavailable: 0, maxSurge: 1}   # 새 Pod 가 Ready(대기자) 된 뒤에 옛 Pod 를 내린다 — 옛 리더는 SIGTERM 에 반납, 대기자가 이어받는다
  selector:
    matchLabels: {app: bank-gateway}
  template:
    metadata:
      labels: {app: bank-gateway}
    spec:
      serviceAccountName: bank-gateway   # Lease · ConfigMap · 자기 Pod 라벨 (rbac-gateway.yaml)
      enableServiceLinks: false   # bank-gateway Service 가 BANK_GATEWAY_PORT=tcp://… 를 넣으면 bank.gateway.port 바인딩이 깨진다
      terminationGracePeriodSeconds: 45   # 물러나기(비우기 최대 5s) + graceful 단계 20s + 여유. 넘으면 SIGKILL
      nodeSelector: {bank-transfer/role: app}
      topologySpreadConstraints:   # 두 대를 다른 워커에 — 노드 하나 장애에 대기자가 산다
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels: {app: bank-gateway}
          # 롤링 중 옛 ReplicaSet Pod 까지 세면 새 Pod 가 한 워커로 몰린다 — 같은 리비전끼리만 센다 (P8-3a 와 같다)
          matchLabelKeys: [pod-template-hash]
      containers:
        - name: bank-gateway
          image: bank-transfer/bank-gateway:dev
          imagePullPolicy: IfNotPresent
          env:
            - {name: BANK_GATEWAY_HOST, value: mock-bank}
            - {name: BANK_GATEWAY_PORT, value: "9090"}
            - {name: BANK_GATEWAY_LEADERELECTION_ENABLED, value: "true"}
            - name: POD_NAME
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
            - name: POD_NAMESPACE
              valueFrom: {fieldRef: {fieldPath: metadata.namespace}}
            - name: BANK_MAINTENANCE_ENABLED
              valueFrom: {configMapKeyRef: {name: app-config, key: BANK_MAINTENANCE_ENABLED}}
            - name: BANK_MAINTENANCE_START
              valueFrom: {configMapKeyRef: {name: app-config, key: BANK_MAINTENANCE_START}}
            - name: BANK_MAINTENANCE_END
              valueFrom: {configMapKeyRef: {name: app-config, key: BANK_MAINTENANCE_END}}
          ports:
            - {name: http, containerPort: 8082}
          # preStop 없음 (결정 12): 호출자(api)는 "안 보냄"(503 · 연결 거부)을 안전하게 재시도한다 — SIGTERM 에 바로 물러나는 편이 교대 공백이 짧다
          startupProbe:   # 기동(최대 120초) 동안은 liveness 가 죽이지 못하게
            httpGet: {path: /actuator/health/liveness, port: http}
            periodSeconds: 5
            timeoutSeconds: 3
            failureThreshold: 24
          readinessProbe:   # 대기자 UP(standby), 리더는 은행 회선을 따라간다
            httpGet: {path: /actuator/health/readiness, port: http}
            periodSeconds: 5
            timeoutSeconds: 3
            failureThreshold: 2
          livenessProbe:   # 실패하면 재시작 — 프로세스가 멈췄을 때만. 외부 의존성은 넣지 않는다
            httpGet: {path: /actuator/health/liveness, port: http}
            periodSeconds: 10
            timeoutSeconds: 3
            failureThreshold: 3
          resources:
            requests: {memory: 512Mi, cpu: 100m}
            limits: {memory: 512Mi}
```

- [ ] **Step 2: RBAC · PDB · 모의 은행 · kustomization**

`infra/k8s/base/rbac-gateway.yaml`:

```yaml
# 게이트웨이 선출 권한 (P8-3b 결정 20). 최소 권한: Lease · ConfigMap 은 이름 한정.
# Pod get · patch 는 namespace 범위 — 자기 Pod 에 리더 라벨을 붙이고 떼는데, Pod 이름을 미리 알 수 없어 이름 한정이 안 된다 (스펙 §16 의 대가)
# create 는 resourceNames 로 제한할 수 없어(이름이 요청 본문에 있다) 따로 둔다 — 처음 예약자 · 처음 선출이 만든다
apiVersion: v1
kind: ServiceAccount
metadata:
  name: bank-gateway
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: bank-gateway
rules:
  - apiGroups: [coordination.k8s.io]
    resources: [leases]
    resourceNames: [bank-gateway-leader]
    verbs: [get, update, patch]
  - apiGroups: [coordination.k8s.io]
    resources: [leases]
    verbs: [create]
  - apiGroups: [""]
    resources: [configmaps]
    resourceNames: [bank-gateway-tran-no]
    verbs: [get, update]
  - apiGroups: [""]
    resources: [configmaps]
    verbs: [create]
  - apiGroups: [""]
    resources: [pods]
    verbs: [get, patch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: bank-gateway
subjects:
  - {kind: ServiceAccount, name: bank-gateway}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: bank-gateway
```

`infra/k8s/base/pdb.yaml` 첫 줄 주석을 `# api · 게이트웨이 (2대짜리만). drain 이 두 대를 한 번에 내리지 못하게. 1대짜리(모의 은행 · 인프라)에 걸면 drain 이 끝나지 않는다` 로 바꾸고 끝에 추가:

```yaml
---
# 게이트웨이 2대 (P8-3b 결정 19): drain 이 리더 · 대기자를 한 번에 내리지 못하게
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: bank-gateway
spec:
  minAvailable: 1
  selector:
    matchLabels: {app: bank-gateway}
```

`infra/k8s/base/mock-bank.yaml` 의 컨테이너 `imagePullPolicy: IfNotPresent` 다음에:

```yaml
          # 실무 제약 재현 (P8-3b): 세션 하나 · 유휴 세션 60초(폴링 30초의 2배) 정리 · 전문번호 일자별 유일.
          # args 가 이미지 CMD 를 덮으므로 포트 · 기록 디렉터리를 다시 적는다
          args: ["9090", "--bank-files=/data/bank-files", "--single-session", "--session-idle-timeout=60s", "--unique-tran-no"]
```

`infra/k8s/base/kustomization.yaml` 의 `resources` 에서 `- bank-gateway.yaml` 다음 줄에 `  - rbac-gateway.yaml`. ConfigMap `bank-gateway-tran-no` 는 매니페스트에 넣지 않는다 (다시 apply 하면 `reservedTo` 가 되돌아가 번호가 겹친다 — 처음 예약자가 만든다). 이 이유를 kustomization 주석 한 줄로 남긴다.

- [ ] **Step 3: H0 대조 overlay**

`infra/k8s/overlays/experiment-gateway-single/kustomization.yaml`:

```yaml
# H0 대조 (P8-3b 스펙 §9): 이전 구성 — 게이트웨이 1대 · Recreate · 선출 끔 (메모리 전문번호). 모의 은행 제약은 켠 채로 둔다.
# 끝나면 apply_overlay local <태그> 로 되돌린다. kubectl apply 는 지운 리소스를 치우지 않으니 PDB 는 손으로: kubectl -n bank-transfer delete pdb bank-gateway
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../local
patches:
  - target: {kind: Deployment, name: bank-gateway}
    patch: |-
      apiVersion: apps/v1
      kind: Deployment
      metadata: {name: bank-gateway}
      spec:
        replicas: 1
        strategy: {type: Recreate, rollingUpdate: null}
        template:
          spec:
            containers:
              - name: bank-gateway
                env:
                  - {name: BANK_GATEWAY_LEADERELECTION_ENABLED, value: "false"}
  - target: {kind: Service, name: bank-gateway}
    patch: |-
      - op: remove
        path: /spec/selector/bank-transfer~1gateway-role
  - patch: |-
      apiVersion: policy/v1
      kind: PodDisruptionBudget
      metadata: {name: bank-gateway}
      $patch: delete
```

Run: `kubectl kustomize infra/k8s/overlays/experiment-gateway-single | grep -nE 'replicas: 1|type: Recreate|LEADERELECTION_ENABLED|gateway-role|name: bank-gateway$' `
Expected: 게이트웨이 `replicas: 1`, `type: Recreate`, `value: "false"`, Service selector 에 `gateway-role` 없음, PDB `bank-gateway` 없음. 출력을 보고서에.

Run: `kubectl kustomize infra/k8s/overlays/local > /dev/null && kubectl kustomize infra/k8s/overlays/ci > /dev/null && echo ok`
Expected: `ok`.

- [ ] **Step 4: lib.sh · smoke.sh**

`infra/k8s/lib.sh` 의 `deployed_tag()` 줄 다음에:

```bash
# 리더 라벨이 붙은 게이트웨이 Pod 이름 (P8-3b). 없으면 빈 줄 — 선출을 끈 H0 대조 등
gw_leader() { k get pods -l app=bank-gateway,bank-transfer/gateway-role=leader -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true; }
```

`infra/k8s/smoke.sh`:
- 머리 주석의 `6) 게이트웨이 readiness UP` 을 `6) 리더 게이트웨이 readiness UP` 으로, `8) NetworkPolicy …` 줄 다음에 `#   9) 게이트웨이 이중화: Ready 2/2, 리더 라벨 Pod 1개 = Lease holder = Service 엔드포인트, 모의 은행 세션 1 (P8-3b)` 추가.
- 모든 `"[n/8]"` 를 `"[n/9]"` 로 (`sed -i 's#/8\]#/9]#' infra/k8s/smoke.sh`).
- [6] 의 첫 줄을 바꾼다:

```bash
GW_LEADER=$(gw_leader)
[ -n "$GW_LEADER" ] || fail "리더 라벨(bank-transfer/gateway-role=leader) 이 붙은 게이트웨이 Pod 가 없다"
k exec "$GW_LEADER" -- wget -qO- http://127.0.0.1:8082/actuator/health/readiness | grep -q '"status":"UP"' || fail "리더 게이트웨이 readiness 가 UP 이 아니다"
```

  같은 단계 `ok` 문구의 `게이트웨이 readiness UP` → `리더 게이트웨이($GW_LEADER) readiness UP`.
- `echo "k8s smoke 통과"` 바로 앞에 [9]:

```bash
echo "[9/9] 게이트웨이 이중화"
# 리더가 정해지고 라벨이 붙기까지 잠깐 걸릴 수 있다 (기동 직후) — 최대 30초
for _ in $(seq 15); do
  LEADERS=$(k get pods -l app=bank-gateway,bank-transfer/gateway-role=leader -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
  HOLDER=$(k get lease bank-gateway-leader -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || true)
  [ "$(grep -c . <<<"$LEADERS" || true)" = 1 ] && [ "$LEADERS" = "$HOLDER" ] && break
  sleep 2
done
r=$(k get deploy bank-gateway -o jsonpath='{.status.readyReplicas}')
[ "${r:-0}" = 2 ] || fail "게이트웨이 Ready ${r:-0}/2 (대기자도 Ready 여야 한다 — bankConnection role=standby)"
[ "$(grep -c . <<<"$LEADERS" || true)" = 1 ] || fail "리더 라벨 Pod 가 정확히 1개가 아니다: '$(tr '\n' ' ' <<<"$LEADERS")'"
[ "$LEADERS" = "$HOLDER" ] || fail "리더 라벨 Pod($LEADERS) ≠ Lease holder($HOLDER)"
EP=$(k get endpointslices -l kubernetes.io/service-name=bank-gateway -o jsonpath='{range .items[*].endpoints[*]}{.targetRef.name}{" "}{.conditions.ready}{"\n"}{end}' | awk '$2 == "true" {print $1}')
[ "$EP" = "$HOLDER" ] || fail "Service bank-gateway 엔드포인트($(tr '\n' ' ' <<<"$EP")) ≠ 리더($HOLDER)"
SESS=$(k logs deploy/mock-bank --tail=-1 | grep -o 'sessions=[0-9]*' | tail -1 | cut -d= -f2)
[ "$SESS" = 1 ] || fail "모의 은행의 지금 세션 수가 1이 아니다: '${SESS:-없음}' (kubectl -n bank-transfer logs deploy/mock-bank | grep sessions=)"
ok "게이트웨이 Ready 2/2, 리더 $HOLDER = Lease holder = Service 엔드포인트, 모의 은행 세션 1"
```

- [ ] **Step 5: 셸 문법 확인**

Run: `bash -n infra/k8s/smoke.sh && bash -n infra/k8s/lib.sh && echo ok`
Expected: `ok`.

- [ ] **Step 6: 커밋**

```bash
git add infra/k8s/base/bank-gateway.yaml infra/k8s/base/rbac-gateway.yaml infra/k8s/base/pdb.yaml infra/k8s/base/mock-bank.yaml infra/k8s/base/kustomization.yaml infra/k8s/overlays/experiment-gateway-single/kustomization.yaml infra/k8s/lib.sh infra/k8s/smoke.sh
git commit -m "feat(p8-3b): 게이트웨이 2대 매니페스트 — 리더 라벨 selector · RBAC · PDB · 분산 · 모의 은행 제약, 스모크 [6] 리더 · [9] 이중화

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 7 (컨트롤러): kind 에서 확인**

컨트롤러가 직접 돌린다 (구현자 아님). 클러스터가 없으면 `bash infra/k8s/up.sh`, 있으면 이미지 재빌드 · 적용 경로(up.sh 가 처리). 그 뒤:

Run: `bash infra/k8s/smoke.sh`
Expected: `[0/9]` ~ `[9/9]` 전부 OK, `k8s smoke 통과`. 실패하면 원인을 고쳐 Task 7 의 수정 커밋으로 남긴다. 특히 확인: 리더 Pod 로그에 `leader election: became leader`, 대기자 `/actuator/health/bankConnection` 이 `role: standby`, `kubectl -n bank-transfer get configmap bank-gateway-tran-no -o yaml` 의 `reservedTo` 가 1000 의 배수.

---

### Task 8: 실험 도구 확장 (H0 ~ H5)

**Files:**
- Modify: `load/k8s/run.sh`
- Modify: `load/k8s/timeline.sh`
- Modify: `load/k8s/timeline.awk`
- Create: `load/k8s/apicut-netpol.yaml`

**Interfaces:**
- Consumes: `gw_leader` (Task 7), 로그 문구 (Task 2 · 6), `transfer.inquiry_attempts`.
- Produces: `bash load/k8s/run.sh <동작> <회차>` 의 새 동작 `gwroll` · `gwkill` · `gwnode` · `gwcut`, 환경변수 `PHASE`(기본 `p8-3a`), 결과 JSON 새 키 `gw_leader_before` · `bank_sessions_max` · `bank_sessions_rejected` · `bank_sessions_evicted` · `bank_dup_tran_no` · `unknown_results` · `gw_lost_s` · `gw_acquired_s` · `gw_leaderless_s` · `leader_changes`.

| 실험 | 명령 |
|---|---|
| H0 | (overlay 전환 뒤) `PHASE=p8-3b bash load/k8s/run.sh drain 1` |
| H1 | `PHASE=p8-3b bash load/k8s/run.sh drain 1` (기본 대상 = 리더 노드) |
| H2 | `PHASE=p8-3b bash load/k8s/run.sh gwroll 1` |
| H3 | `PHASE=p8-3b bash load/k8s/run.sh gwkill 1` |
| H4 | `PHASE=p8-3b bash load/k8s/run.sh gwnode 1` |
| H5 | `PHASE=p8-3b bash load/k8s/run.sh gwcut 1` |

- [ ] **Step 1: H5 정책 파일**

`load/k8s/apicut-netpol.yaml`:

```yaml
# H5 (P8-3b 스펙 §9): 실험 라벨이 붙은 게이트웨이 Pod 의 나가는 연결을 모의 은행 · DNS 로만 허용 → API 서버(Lease 갱신)만 끊긴다.
# 리더 라벨로 고르지 않는다 — 새 리더에게도 걸려 연쇄 차단이 된다. run.sh gwcut 이 옛 리더에게만 bank-transfer/experiment=apicut 을 붙인다
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: experiment-apicut
spec:
  podSelector:
    matchLabels: {bank-transfer/experiment: apicut}
  policyTypes: [Egress]
  egress:
    - to:
        - podSelector: {matchLabels: {app: mock-bank}}
      ports:
        - {port: 9090, protocol: TCP}
    - to:
        - namespaceSelector: {}
          podSelector: {matchLabels: {k8s-app: kube-dns}}
      ports:
        - {port: 53, protocol: UDP}
        - {port: 53, protocol: TCP}
```

- [ ] **Step 2: 표본기에 라벨 · Lease**

`load/k8s/timeline.sh`:
- 머리 주석 줄 형식을 `# 줄: P <epoch_ms> <app> <pod> <node> <Ready> <재시작 수> <게이트웨이 역할>   ·   N <epoch_ms> <node> <Ready>   ·   L <epoch_ms> <Lease holder>   (빈 값은 -)` 로.
- `k get pods` 의 jsonpath 끝 `{.status.containerStatuses[0].restartCount}` 뒤에 `{"\t"}{.metadata.labels.bank-transfer/gateway-role}` 를 넣고, awk 의 printf 를 `"P %s %s %s %s %s %s %s\n", …, ($5 == "" ? 0 : $5), ($6 == "" ? "-" : $6)` 로.
- 노드 줄 다음에:

```bash
  h=$(k get lease bank-gateway-leader -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || true)
  echo "L $t ${h:--}" >> "$OUT"
```

- [ ] **Step 3: 시간 지표에 리더 없는 시간 · 교대 수**

`load/k8s/timeline.awk`:
- `$1 == "P"` 블록의 `if ($3 == "bank-gateway") {` 안에 `if ($6 == "True" && $8 == "leader") gwLead[t] = 1` 추가.
- 새 규칙 (`$1 == "N"` 규칙 다음):

```awk
$1 == "L" && ($2 + 0) >= ta {
  if (lastHolder != "" && $3 != lastHolder) changes++
  lastHolder = $3
}
```

- `allnull()` 에 `print "gw_leaderless_s=null"; print "leader_changes=null"` 추가.
- END 의 루프 `if (!(t in gwReady) && i < n) gwNot += ts[i + 1] - t` 다음 줄에 `if (!(t in gwLead) && i < n) gwNoLead += ts[i + 1] - t`, 앞에 `gwNoLead = 0` 초기화.
- 출력 끝에:

```awk
  print "gw_leaderless_s=" sprintf("%.1f", gwNoLead / 1000)   # Ready 이면서 리더 라벨인 게이트웨이가 없던 시간 (선출 끈 H0 은 전 구간 — gw_not_ready_s 를 본다)
  print "leader_changes=" (changes + 0)
```

- [ ] **Step 4: run.sh — 동작 · 대상 · 결과 폴더**

`load/k8s/run.sh`:

(a) 사용법 · 동작 목록:

```bash
ACTION=${1:?동작: steady | rolling | podkill | tagtrap | bank | drain | node | gwroll | gwkill | gwnode | gwcut | cleanup}
```

```bash
case "$ACTION" in steady|rolling|podkill|tagtrap|bank|drain|node|gwroll|gwkill|gwnode|gwcut) ;; *) die "모르는 동작: $ACTION" ;; esac
```

(b) 결과 폴더를 단계별로 (P8-3a 결과는 그대로 `p8-3a`):

```bash
PHASE=${PHASE:-p8-3a}   # 결과 폴더 — P8-3b 실험은 PHASE=p8-3b
[[ "$PHASE" =~ ^p8-3[ab]$ ]] || die "PHASE 는 p8-3a | p8-3b: $PHASE"
RUN="$ACTION-$ROUND"
OUT_DIR=docs/perf/raw/$PHASE; OUT="$OUT_DIR/$RUN.json"; WORK="build/load-$PHASE/$RUN"
```

(기존 `OUT_DIR=docs/perf/raw/p8-3a; …; WORK="build/load-p83a/$RUN"` 줄을 이것으로 바꾼다. P8-3a 기본의 `WORK` 경로가 `build/load-p8-3a` 로 바뀌는 것은 빌드 산출물이라 무해.) k6 실행의 `-e OUT_SUBDIR=p8-3a` → `-e OUT_SUBDIR="$PHASE"`, k6 컨테이너 이름 `K6_NAME="p83a-k6-$RUN"` → `K6_NAME="k6-$PHASE-$RUN"`.

(c) 기본 부하 시간: `[ "$ACTION" = rolling ] && DEF_DURATION=150s` 다음 줄에 `[ "$ACTION" = gwroll ] && DEF_DURATION=150s; [ "$ACTION" = gwcut ] && DEF_DURATION=90s`. 변수 줄에 `CUT_S=${CUT_S:-40}` 를 더하고 양의 정수 검사 목록에 `CUT_S` 를 넣는다.

(d) 대상 — `GW_NODE=…` 줄을 바꾼다:

```bash
GW_LEADER=$(gw_leader)   # 리더 라벨 Pod (P8-3b). 선출을 끈 H0 은 비어 있다 → 첫 게이트웨이
if [ -n "$GW_LEADER" ]; then
  GW_NODE=$(printf '%s\n' "$LIVE_GW" | awk -v p="$GW_LEADER" '$1 == p {print $2; exit}')
else
  GW_LEADER=$(printf '%s\n' "$LIVE_GW" | awk 'NF {print $1; exit}')
  GW_NODE=$(printf '%s\n' "$LIVE_GW" | awk 'NF {print $2; exit}')
fi
[ -n "$GW_NODE" ] || die "살아 있는 bank-gateway Pod 가 없다"
case "$ACTION" in
  drain|gwnode) NODE=${NODE:-$GW_NODE} ;;   # 기본: 리더(1대짜리면 그 게이트웨이)가 있는 워커
  node)  NODE=${NODE:-$(printf '%s\n' "$LIVE_API" | awk 'NF {print $2}' | { grep -vx "$GW_NODE" || true; } | head -1)} ;;   # 기본: 게이트웨이가 없는 api 워커 — api 복구만 본다
  *)     NODE=- ;;
esac
```

(e) `gwroll` 이미지 준비 — `if [ "$ACTION" = rolling ]; then … fi` 블록 다음에:

```bash
if [ "$ACTION" = gwroll ]; then   # 게이트웨이 같은 이미지 새 태그 (부하 중 kind load 하지 않게 미리)
  CUR=$(deployed_tag); NEW="${CUR%-r*}-r$(date +%s)"
  d image inspect "bank-transfer/bank-gateway:$CUR" >/dev/null 2>&1 || die "PC 에 bank-transfer/bank-gateway:$CUR 가 없다 — up.sh 로 만든 태그여야 한다"
  d tag "bank-transfer/bank-gateway:$CUR" "bank-transfer/bank-gateway:$NEW"
  kindc load docker-image "bank-transfer/bank-gateway:$NEW" --name "$CLUSTER_NAME" >/dev/null
fi
```

(f) 게이트웨이 로그도 처음부터 따라간다 — api `FOLLOW` 줄 다음에:

```bash
MSYS_NO_PATHCONV=1 kubectl --context "$CTX" -n "$NAMESPACE" logs -f -l app=bank-gateway --prefix --timestamps --tail=0 --max-log-requests=10 > "$WORK/gateway-follow.log" 2>&1 &
GW_FOLLOW=$!
```

  `kill "$SAMPLER" "$FOLLOW"` 줄을 `kill "$SAMPLER" "$FOLLOW" "$GW_FOLLOW"` 로.

(g) 동작 `case` 에 추가 (`node)` 분기 다음):

```bash
    gwroll)  k set image deploy/bank-gateway "bank-gateway=bank-transfer/bank-gateway:$NEW" >/dev/null
             k rollout status deploy/bank-gateway --timeout=300s > "$WORK/rollout.txt" 2>&1 || echo "[$RUN] 주의: 게이트웨이 롤링이 300초 안에 안 끝났다" >&2 ;;
    gwkill)  VICTIM=$GW_LEADER
             k delete pod "$VICTIM" --grace-period=0 --force > "$WORK/delete.txt" 2>&1 ;;
    gwnode)  RESTORE=node; VICTIM=$GW_LEADER; d stop "$NODE" >/dev/null ;;
    gwcut)   RESTORE=gwcut; VICTIM=$GW_LEADER
             k label pod "$VICTIM" bank-transfer/experiment=apicut --overwrite >/dev/null
             k apply -f load/k8s/apicut-netpol.yaml >/dev/null
             sleep "$CUT_S"
             k delete -f load/k8s/apicut-netpol.yaml --ignore-not-found >/dev/null
             k label pod "$VICTIM" bank-transfer/experiment- >/dev/null 2>&1 || true; RESTORE= ;;
```

(h) 되돌리기 — `stop_bg` 의 `case "$RESTORE" in` 에 분기 추가:

```bash
    gwcut) k delete -f load/k8s/apicut-netpol.yaml --ignore-not-found >/dev/null 2>&1 || true
           [ -n "${VICTIM:-}" ] && k label pod "$VICTIM" bank-transfer/experiment- >/dev/null 2>&1 || true ;;
```

  부하 뒤 노드 되돌리기 `case "$ACTION" in … node)` 를 `node|gwnode)` 로.

(i) 지표 — `# --- 6. 로그 · 시간 지표 ---` 의 게이트웨이 로그 줄 다음에:

```bash
k logs deploy/mock-bank --timestamps --tail=-1 --since-time="$T0" > "$WORK/mock-bank.log" 2>&1 || true
BANK_SESS_MAX=$({ grep -ao 'sessions=[0-9]*' "$WORK/mock-bank.log" || true; } | cut -d= -f2 | sort -n | tail -1)
BANK_REJECTED=$({ grep -a 'session rejected' "$WORK/mock-bank.log" || true; } | wc -l | tr -d ' ')
BANK_EVICTED=$({ grep -a 'session evicted idle' "$WORK/mock-bank.log" || true; } | wc -l | tr -d ' ')
BANK_DUP=$({ grep -a 'duplicate tran no' "$WORK/mock-bank.log" || true; } | wc -l | tr -d ' ')
UNKNOWN=$(sql <<<"SELECT COUNT(*) FROM transfer WHERE $USERS AND inquiry_attempts > 0" | tr -d '\r')   # 결과 모름 → 결과조회를 거친 건
# 게이트웨이 로그의 선출 사건 (동작 시각 뒤 처음) — 삭제된 Pod 로그는 follow 파일에 있다
log_ms() {   # $1 = 패턴: 동작 시각 이후 처음 나온 줄의 epoch ms (없으면 빈 값)
  cat "$WORK/gateway.log" "$WORK/gateway-follow.log" 2>/dev/null | grep -a "$1" | awk '{print $2}' \
    | while read -r ts; do date -d "$ts" +%s%3N 2>/dev/null; done | awk -v ta="$TA_MS" '$1 >= ta' | sort -n | head -1
}
since_ta() { if [ -n "${1:-}" ] && [ "$TA_MS" != 0 ]; then awk -v a="$1" -v b="$TA_MS" 'BEGIN { printf "%.1f", (a - b) / 1000 }'; else echo null; fi; }
GW_LOST_S=$(since_ta "$(log_ms 'leader election: lost leadership')")
GW_ACQ_S=$(since_ta "$(log_ms 'leader election: became leader')")
```

(j) 결과 JSON — 마지막 줄 `"node_notready_s":…,"replacement_ready_s":…}` 를 아래 두 줄로 바꾼다:

```bash
 "node_notready_s":${M[node_notready_s]:-null},"pod_notready_s":${M[pod_notready_s]:-null},"replacement_ready_s":${M[replacement_ready_s]:-null},
 "gw_leader_before":$(str "$GW_LEADER"),"bank_sessions_max":$(num "$BANK_SESS_MAX"),"bank_sessions_rejected":$(num "$BANK_REJECTED"),"bank_sessions_evicted":$(num "$BANK_EVICTED"),"bank_dup_tran_no":$(num "$BANK_DUP"),"unknown_results":$(num "$UNKNOWN"),"gw_lost_s":$GW_LOST_S,"gw_acquired_s":$GW_ACQ_S,"gw_leaderless_s":${M[gw_leaderless_s]:-null},"leader_changes":${M[leader_changes]:-null}}
```

판정(`FAILS`)은 바꾸지 않는다 — 돈 정확성(접수 = 출금 행 = 은행 기록, 이중 0)만 판정하고, 세션 거부 · 번호 중복 · 결과 모름은 결과로 남겨 문서가 해석한다 (스펙 §9 "판정은 P8-3a 와 같다 + 결과 모름 건수").

- [ ] **Step 5: 문법 · 정적 확인**

Run: `bash -n load/k8s/run.sh && bash -n load/k8s/timeline.sh && awk -f load/k8s/timeline.awk -v ta=0 -v node=- /dev/null && echo ok`
Expected: `ok` 와 함께 null 지표 줄들.

Run (표본 한 줄 손 확인): `printf 'P 1000 bank-gateway gw-a w1 True 0 leader\nP 2000 bank-gateway gw-a w1 True 0 -\nP 3000 bank-gateway gw-b w2 True 0 leader\nL 1000 gw-a\nL 3000 gw-b\n' | awk -v ta=1000 -v node=- -f load/k8s/timeline.awk | grep -E 'leaderless|leader_changes'`
Expected: `gw_leaderless_s=1.0`, `leader_changes=1`.

- [ ] **Step 6: 커밋**

```bash
git add load/k8s/run.sh load/k8s/timeline.sh load/k8s/timeline.awk load/k8s/apicut-netpol.yaml
git commit -m "feat(p8-3b): 실험 도구 — 리더 대상 drain · gwroll · gwkill · gwnode · gwcut, 세션 · 번호 중복 · 결과 모름 · 리더 공백 지표

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 7 (컨트롤러): 정상 부하 한 번**

Run: `PHASE=p8-3b bash load/k8s/run.sh steady 1`
Expected: `통과`, `bank_sessions_max` 1, `bank_sessions_rejected` 0, `bank_dup_tran_no` 0, `unknown_results` 0, `leader_changes` 0. 실패면 Task 8 수정 커밋.

---

### Task 9 (컨트롤러): 실험 H0 ~ H5 · 측정 문서

구현자 없이 컨트롤러가 직접 돌린다 (부하 · 클러스터 조작 · 해석). 각 실험 전 `bash infra/k8s/smoke.sh` 는 돌리지 않는다(시드가 섞인다) — `run.sh` 의 사전 확인이 대신한다. 실험 사이 2분 이상 쉬어 재접속 백오프 · 표본을 가라앉힌다.

**Files:**
- Create: `docs/perf/p8-3b-gateway-ha.md`
- Create: `docs/perf/raw/p8-3b/*.json` · `*.timeline.txt` · `*.rebalance.txt` · `*.k6.json` (run.sh 가 만든다)

- [ ] **Step 1: H0 대조 (이전 구성)**

```bash
bash -c '. infra/k8s/lib.sh; k rollout restart deploy/mock-bank; k rollout status deploy/mock-bank --timeout=180s'   # 번호 기록을 비운다 (메모리 카운터가 1부터 다시 쓰는 것만 보이게)
bash -c '. infra/k8s/lib.sh; apply_overlay experiment-gateway-single "$(deployed_tag)"; k delete pdb bank-gateway --ignore-not-found; k rollout status deploy/bank-gateway --timeout=300s'
PHASE=p8-3b bash load/k8s/run.sh drain 1
```

`docs/perf/raw/p8-3b/drain-1.json` 을 `h0-drain-1.json` 으로 이름 바꾼다 (H1 과 구분 — `.timeline.txt` · `.rebalance.txt` · `.k6.json` 도 같이). 기대: `bank_sessions_rejected` > 0 (새 게이트웨이가 옛 세션 때문에 거부), `bank_dup_tran_no` > 0 (메모리 카운터가 1부터), 돈 정확성은 통과 (3001 → 안 보냄 → 재시도).

되돌리기:

```bash
bash -c '. infra/k8s/lib.sh; apply_overlay local "$(deployed_tag)"; wait_ready'
bash infra/k8s/smoke.sh
```

- [ ] **Step 2: H1 리더 노드 drain ×2**

```bash
PHASE=p8-3b bash load/k8s/run.sh drain 1
PHASE=p8-3b bash load/k8s/run.sh drain 2
```

기대: `bank_sessions_rejected` 0, `bank_dup_tran_no` 0, `unknown_results` 0, `leader_changes` ≥ 1, `gw_acquired_s` 수 초. 2회차가 대기자 노드를 빼게 되면(리더가 이미 옮겨 갔다) 그대로 둔다 — 기본 대상이 "지금 리더 노드" 라 매번 리더를 뺀다.

- [ ] **Step 3: H2 게이트웨이 롤링 ×2**

```bash
PHASE=p8-3b bash load/k8s/run.sh gwroll 1
PHASE=p8-3b bash load/k8s/run.sh gwroll 2
```

기대: H1 과 같음. 롤링 한 번에 리더 교대가 1 ~ 2회 (`leader_changes`) — 새 Pod 가 대기자로 붙은 뒤 옛 리더가 내려가면 1회, 남은 옛 대기자가 리더가 되면 그 Pod 가 내려갈 때 1회 더.

- [ ] **Step 4: H3 리더 강제 삭제 ×2**

```bash
PHASE=p8-3b bash load/k8s/run.sh gwkill 1
PHASE=p8-3b bash load/k8s/run.sh gwkill 2
```

본다: `gw_acquired_s` 가 수 초(= `--force` 도 SIGTERM 을 보내 정상 교대, P8-3a R2 결과와 일치)인지 15초 이상(만료 대기)인지. 게이트웨이 follow 로그에 옛 리더의 `stepping down` 이 있으면 정상 교대.

- [ ] **Step 5: H4 리더 노드 정지 ×1**

```bash
PHASE=p8-3b bash load/k8s/run.sh gwnode 1
```

기대: `gw_acquired_s` ≈ 15초대(Lease 만료), 그 뒤 새 리더 접속은 `bank_sessions_evicted` 1(옛 세션 유휴 정리, 약 60초) 전까지 `bank_sessions_rejected` 증가, `unknown_results` 는 결과조회로 전부 `COMPLETED` (판정 통과). 노드가 돌아온 뒤 `bash infra/k8s/smoke.sh`.

- [ ] **Step 6: H5 API 서버 단절 ×1**

```bash
PHASE=p8-3b bash load/k8s/run.sh gwcut 1
```

본다: `gw_lost_s` ≤ 10초대(자기 차단), `gw_acquired_s` ≥ 15초대(만료 뒤 획득), `bank_sessions_max` 1. **`gw_lost_s` 가 null 이면 kindnet 이 egress 를 집행하지 않은 것** — 결과 JSON 은 남기고, 문서에 "kind CNI egress 미집행 → 단위 테스트 `GatewayLeaderElectionTest`(API 서버 단절 → 갱신 기한 안 자기 차단)로 대신" 이라고 적는다 (스펙 §9).

- [ ] **Step 7: 측정 문서**

`docs/perf/p8-3b-gateway-ha.md` — P8-3a 문서(`docs/perf/p8-3a-k8s.md`)와 같은 구성:
1. 목적 · 성공 기준 7개 (스펙 §1) 각각 통과 여부 표.
2. 환경 (kind 노드 · 이미지 태그 · 모의 은행 옵션 · Lease 15/10/2s · 구간 1,000).
3. 결과 표: 실험 × 회차 행, 열 = 접수 · 결과 모름 · 세션 최대 · 세션 거부 · 유휴 정리 · 번호 중복 · 리더 공백(`gw_leaderless_s`) · 자기 차단(`gw_lost_s`) · 획득(`gw_acquired_s`) · 교대 수 · 판정. 숫자는 `docs/perf/raw/p8-3b/*.json` 에서 그대로.
4. H0 ↔ H1 비교 (같은 drain 에서 거부 · 중복이 0 이 되는 것) — 이 단계의 핵심 숫자.
5. 해석 · 놀란 점 · 예상과 다른 점 (추정과 측정을 구분해 "측정" · "추정" 표기).
6. 한계: kind 한 호스트(시계 차 없음), 모의 은행 제약은 흉내, 처리량은 안 쟀다(성공 기준 7).

- [ ] **Step 8: 커밋**

```bash
git add docs/perf/p8-3b-gateway-ha.md docs/perf/raw/p8-3b
git commit -m "docs(p8-3b): 실험 H0 ~ H5 측정 — 이전 구성 대조 · 정상 교대 · 강제 삭제 · 노드 정지 · API 서버 단절

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

(`git add docs/perf/raw/p8-3b` 는 디렉터리 하나 — 그 안에는 run.sh 가 만든 결과만 있다. 추가 전 `git status --short docs/perf/raw/p8-3b` 로 목록을 본다.)

---

### Task 10: 문서

**Files:**
- Create: `docs/adr/0018-gateway-ha-leader-election.md`
- Modify: `OPERATION.md` (§16 추가), `README.md`, `ARCHITECTURE.md`, `HANDOVER.md`, `CHANGELOG.md`, `infra/k8s/README.md`, `apps/bank-gateway` 관련 문단 (있으면)

**Interfaces:**
- Consumes: Task 9 의 숫자 (문서에 그대로 인용 — 새로 만들지 않는다).

- [ ] **Step 1: ADR-0018 (제안)**

`docs/adr/0018-gateway-ha-leader-election.md` — 기존 ADR(`0017-kubernetes-kind.md`) 형식을 따른다. 상태 `제안 (2026-11-16)` (머지 뒤 정리 PR 에서 `채택`). 내용:
- 맥락: P8-3a D1 의 4 ~ 7초 동시 연결, 단일 장애점, 실무 제약(세션 수 · 번호 유일 — 추정).
- 결정: active-standby · K8s Lease(fabric8 8.0.0, JDK HttpClient) · 리더 라벨 + 둘 다 Ready · 전문번호 ConfigMap 구간 예약(선출과 독립) · 종료 훅 교대 순서 · 규격 v0.4 `3001`.
- 버린 대안 (스펙 §3 표의 "버린 대안" 열 + 리더만 Ready).
- 결과: Task 9 숫자 (H0 ↔ H1 거부 · 중복, 교대 공백, H4 공백).
- 위험 · 다음: 시계 차 전제, API 서버 장기 장애 = 송신 중단, Pod 권한 namespace 범위, active-active · outlier detection · 백오프 상한 (스펙 §14).

- [ ] **Step 2: OPERATION §16**

`OPERATION.md` 끝에 `## 16. 게이트웨이 이중화 (P8-3b)`:
- 지금 리더 보기: `kubectl -n bank-transfer get lease bank-gateway-leader -o jsonpath='{.spec.holderIdentity}'`, `kubectl -n bank-transfer get pods -l app=bank-gateway -L bank-transfer/gateway-role`.
- 전문번호 예약 보기: `kubectl -n bank-transfer get configmap bank-gateway-tran-no -o yaml` — **손으로 고치지 않는다** (줄이면 번호가 겹친다).
- 대기자 헬스: `/actuator/health/bankConnection` 의 `role`.
- 장애 대응 표: 리더 라벨 Pod 둘(API 단절 직후 — 기다리면 정리), 라벨 Pod 없음(선출 · RBAC 확인), `3001` 증가(번호 저장소 확인), Lease 가 15초 넘게 안 넘어감(옛 리더 노드 상태).
- 실험 재현: Task 8 표 (H0 ~ H5 명령).

- [ ] **Step 3: 나머지**

- `README.md`: 로드맵 P8-3b 줄 (아직 `[ ]` — 정리 PR 에서 `[x]`), 구성 요약에 "게이트웨이 2대 active-standby".
- `ARCHITECTURE.md`: 게이트웨이 부분에 리더 선출 · 리더 라벨 · 구간 예약 그림 한 줄과 "(제안)" 표시 (정리 PR 에서 뗀다).
- `HANDOVER.md`: P8-3b 진행 상태 · 다음 단계 · 확인 명령.
- `CHANGELOG.md`: `## P8-3b 게이트웨이 이중화` (Added · Changed · 측정 요약). PR 번호는 PR 생성 뒤 채운다.
- `infra/k8s/README.md`: 파일 목록에 `rbac-gateway.yaml` · `experiment-gateway-single`, 스모크 [9].
- `docs/protocol.md` 는 Task 2 에서 끝남 — 링크만 확인.

- [ ] **Step 4: 문서 링크 확인 · 커밋**

Run: `grep -n '0018' README.md ARCHITECTURE.md OPERATION.md docs/protocol.md docs/adr/0018-gateway-ha-leader-election.md | head`
Expected: ADR 링크가 실제 파일을 가리킨다.

```bash
git add docs/adr/0018-gateway-ha-leader-election.md OPERATION.md README.md ARCHITECTURE.md HANDOVER.md CHANGELOG.md infra/k8s/README.md
git commit -m "docs(p8-3b): ADR-0018 (제안) · OPERATION §16 · README · ARCHITECTURE · HANDOVER · CHANGELOG · infra/k8s README

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## 스펙 대비 점검 (작성자 자체 검토)

| 스펙 | 구현 Task |
|---|---|
| §1 기준 1 (세션 1) | Task 2 `--single-session` · 로그, Task 6 테스트 `peakSessions == 1`, Task 7 스모크 [9], Task 9 `bank_sessions_max` |
| §1 기준 2 (번호 중복 0 + 경쟁 테스트) | Task 4 경쟁 테스트, Task 6 `GatewayFailoverTest` (3001 없이 24건), Task 9 `bank_dup_tran_no` |
| §1 기준 3 · 4 (교대 결과 모름 · 돈 정확) | Task 5 stepDown 테스트, Task 9 H1 · H2 · H3 · H4 + 기존 판정 |
| §1 기준 5 (H0) | Task 7 overlay, Task 9 Step 1 |
| §1 기준 6 (기존 동작) | 기본 꺼짐 (Task 5 조립 · application.yml), Task 6 Step 7 전체 빌드 |
| §3 결정 1 ~ 22 | 결정 3 · 6 → Task 6, 4 · 18 → Task 5, 5 · 20 → Task 6 · 7, 7 → Task 5, 8 · 9 · 10 → Task 4, 11 · 12 → Task 5 · 6, 13 → 기존 (변경 없음), 14 · 15 → Task 2 · 3, 16 → Task 2 · 7, 17 → Task 3, 19 → Task 7, 21 → 변경 없음 (H5 정책은 실험용 파일), 22 → Task 7 |
| §5 구간 예약 | Task 4 |
| §6 교대 | Task 5 (1 ~ 3) · Task 6 (4 · 5, 획득 → 접속 → 라벨) |
| §7 규격 · 모의 은행 | Task 2 (버전은 v0.4 — §16) |
| §8 매니페스트 | Task 7 |
| §9 실험 | Task 8 · 9 |
| §10 테스트 | Task 2 ~ 6 |
| §11 CI · 스모크 | Task 7 (`k8s-smoke` 잡 변경 없음 — 시간은 PR CI 에서 확인) |
| §13 위험 Jackson | Task 1 관문 |
| §15 문서 | Task 10 (+ Task 2 protocol, Task 9 perf) |
