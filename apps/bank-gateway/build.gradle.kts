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
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")

    testImplementation(project(":apps:mock-bank"))
    testImplementation(testFixtures(project(":modules:bank-gateway-http")))   // TcpCutProxy
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.awaitility:awaitility-kotlin")
}

tasks.withType<Test> {
    jvmArgs("-Duser.timezone=UTC")   // 점검 시간 판단이 JVM 기본 시간대에 기대지 않는지 (apps/api 와 같다)
}

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    jvmArgs("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
    workingDir = rootProject.projectDir
}
