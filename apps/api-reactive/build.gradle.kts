plugins {
    kotlin("plugin.spring")
    id("org.springframework.boot")
}

// 1원인증 요청만 하는 WebFlux 앱 (P6-2, ADR-0013). Kafka · 스케줄러 · 배치 · Flyway 는 넣지 않는다 — 테이블은 apps/api 의 Flyway 가 만든다.
// JPA(블로킹 JDBC)는 IO 디스패처 · boundedElastic 에서만 부른다 (R2DBC 는 범위 밖).
dependencies {
    implementation(project(":modules:account"))
    implementation(project(":modules:bank-gateway"))
    implementation(project(":modules:transfer"))   // RandomReferenceGenerator — 1원인증 거래번호(V 접두)를 apps/api 와 같은 생성기로

    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")
    // 버전은 Boot BOM (kotlinx-coroutines 1.10.2). core: await · withContext. reactor: WebFlux 가 suspend 핸들러를 Mono 로 감싸 부를 때 쓴다
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    runtimeOnly("com.mysql:mysql-connector-j")

    testImplementation(project(":apps:mock-bank"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-mysql")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.awaitility:awaitility-kotlin")
    // 테스트에서만 apps/api 의 마이그레이션으로 스키마를 만든다 (앱 본체는 스키마를 바꾸지 않는다)
    testImplementation("org.springframework.boot:spring-boot-starter-flyway")
    testImplementation("org.flywaydb:flyway-mysql")
}

tasks.withType<Test> {
    jvmArgs("-Duser.timezone=UTC")   // apps/api 와 같다: KST 변환이 JVM 기본 시간대에 기대지 않는지 서버(UTC) 조건에서 본다
    // 스키마 소유는 apps/api — 그 마이그레이션 폴더를 그대로 쓴다 (복사하면 갈라진다)
    systemProperty(
        "spring.flyway.locations",
        "filesystem:" + rootProject.file("apps/api/src/main/resources/db/migration").absolutePath.replace('\\', '/'),
    )
}

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    jvmArgs("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
    workingDir = rootProject.projectDir
}
