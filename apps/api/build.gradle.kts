plugins {
    kotlin("plugin.spring")
    kotlin("plugin.jpa")          // @Entity 에 인자 없는 생성자를 만들어 준다
    id("org.springframework.boot")
}

// Hibernate 가 엔티티를 프록시로 감쌀 수 있게 열어 둔다 (Kotlin 클래스는 기본이 final)
allOpen {
    annotation("jakarta.persistence.Entity")
}

dependencies {
    implementation(project(":modules:account"))
    implementation(project(":modules:transfer"))
    implementation(project(":modules:bank-gateway"))

    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.flywaydb:flyway-mysql")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")
    runtimeOnly("com.mysql:mysql-connector-j")

    testImplementation(project(":apps:mock-bank"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-mysql")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
}

// 테스트 JVM 시간대를 UTC 로 고정한다. 기본값(개발자 PC 의 KST)에 기대어 KST 변환 테스트가 우연히 통과하지 않게,
// 앱이 JVM 기본 시간대와 무관하게 Asia/Seoul 로 저장하는지를 서버(UTC)와 같은 조건에서 검증한다.
tasks.withType<Test> {
    jvmArgs("-Duser.timezone=UTC")
}

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    jvmArgs("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
}
