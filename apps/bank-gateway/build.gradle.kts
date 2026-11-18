plugins {
    kotlin("plugin.spring")
    id("org.springframework.boot")
}

// modules:bank-gateway 와 프로젝트 이름이 같아 Gradle 이 같은 모듈로 착각한다(순환 의존) — group 을 달리한다
group = "io.github.hyeokjinon.apps"

// 은행 FEP 연결을 혼자 가진 게이트웨이 앱 (P8-1, ADR-0015). DB · Kafka 없음.
// 서버 판은 spring.main.web-application-type 으로 고른다: reactive(기본, WebFlux) | servlet(MVC 비동기, Task 5 에서 스타터 추가)
dependencies {
    implementation(project(":modules:bank-gateway"))
    implementation(project(":modules:bank-gateway-http"))   // 계약 DTO · 오류 코드 · JSON

    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")   // servlet 판 (MVC 비동기, P8-1 비교). 둘 다 있으면 web-application-type 이 고른다
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")
    // P8-3b 리더 선출 · 전문번호 구간 예약 (K8s Lease · ConfigMap). 8.0.0 = Jackson 3 (tools.jackson) — Boot 4 와 같은 계열.
    // 기본 HTTP 구현(vertx-5)은 Netty 를 하나 더 끌어와 빼고, Netty 없는 JDK HttpClient 구현을 쓴다
    implementation("io.fabric8:kubernetes-client:8.0.0") { exclude(group = "io.fabric8", module = "kubernetes-httpclient-vertx-5") }
    runtimeOnly("io.fabric8:kubernetes-httpclient-jdk:8.0.0")
    // P9a 지표 노출 /actuator/prometheus (버전은 Boot BOM)
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    // P9b 분산 추적 (api 와 같은 스타터 — OTLP 지표 레지스트리는 application.yml 에서 끈다)
    implementation("org.springframework.boot:spring-boot-starter-opentelemetry")
    implementation(project(":modules:tracing"))

    testImplementation(project(":apps:mock-bank"))
    testImplementation(testFixtures(project(":modules:bank-gateway-http")))   // TcpCutProxy
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.awaitility:awaitility-kotlin")
    testImplementation("org.testcontainers:testcontainers-toxiproxy")   // 버전은 Boot BOM (Testcontainers 2.0.5)
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("eu.rekawek.toxiproxy:toxiproxy-java:2.1.7")    // Toxiproxy 제어 클라이언트 (BOM 밖 — 버전 고정)
    testImplementation("io.fabric8:kubernetes-server-mock:8.0.0")   // 가짜 API 서버 (CRUD 모드) — Lease · ConfigMap · Pod 라벨
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing")   // InMemorySpanExporter (P9b)
    testImplementation("io.micrometer:micrometer-tracing-test")        // SimpleTracer — bank.request 단위 테스트 (P9b)
}

tasks.withType<Test> {
    jvmArgs("-Duser.timezone=UTC")   // 점검 시간 판단이 JVM 기본 시간대에 기대지 않는지 (apps/api 와 같다)
}

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    jvmArgs("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
    workingDir = rootProject.projectDir
}
