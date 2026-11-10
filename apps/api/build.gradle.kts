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
    implementation(project(":modules:bank-gateway-http"))   // mode=http: 게이트웨이 앱을 부르는 클라이언트 (P8-1)
    implementation(project(":modules:account-infra"))   // 1원인증 JPA 저장소 · 은행 응답 해석 (P6-2 에서 apps/api-reactive 와 같이 쓰려고 옮김)

    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.flywaydb:flyway-mysql")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    // 일마감 대사 (P5-1). Boot 4 는 배치 자동 설정을 나눴다: starter-batch 만이면 메모리 잡 저장소(ResourcelessJobRepository)라
    // 재시작 · 중복 거부가 프로세스를 넘지 못한다. JDBC 잡 저장소(BATCH_* 표)는 이 스타터가 켠다. 버전은 Boot BOM (Spring Batch 6.0.5)
    implementation("org.springframework.boot:spring-boot-starter-batch-jdbc")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")
    // Redis 분산락 (P4-2, redis 전략일 때만 클라이언트를 만든다). 스타터 없이 빈 하나만 필요하다. Netty 는 Boot BOM 버전(4.2.x)으로 맞춰진다
    implementation("org.redisson:redisson:4.8.0")
    // 컨슈머 코루틴 (P6-1, ADR-0012). 버전은 Boot BOM (kotlinx-coroutines 1.10.2). WebFlux 는 넣지 않는다 —
    // core: CompletionStage.await() · Semaphore · withContext. reactor: Spring 이 suspend 리스너를 Mono 로 감싸 부를 때 쓴다 (CoroutinesUtils)
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    runtimeOnly("com.mysql:mysql-connector-j")

    testImplementation(project(":apps:mock-bank"))
    testImplementation(testFixtures(project(":modules:bank-gateway-http")))   // TcpCutProxy · GatewayProcess (P8-1)
    testImplementation(testFixtures(project(":modules:account-infra")))   // 오류 계약 기대표
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-mysql")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.awaitility:awaitility-kotlin")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers")   // GenericContainer (Redis 전용 모듈이 없다)
}

// 테스트 JVM 시간대를 UTC 로 고정한다. 기본값(개발자 PC 의 KST)에 기대어 KST 변환 테스트가 우연히 통과하지 않게,
// 앱이 JVM 기본 시간대와 무관하게 Asia/Seoul 로 저장하는지를 서버(UTC)와 같은 조건에서 검증한다.
// http 모드 흐름 테스트가 게이트웨이 앱을 별도 JVM 으로 띄운다 (P8-1). 같은 JVM 에 올리면 이 앱의 JPA · Kafka 자동 설정이
// 게이트웨이 컨텍스트에도 걸린다 — 클래스패스를 따로 받아 java -cp 로 실행한다.
val bankGatewayRuntime: Configuration by configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
    }
}
dependencies { bankGatewayRuntime(project(":apps:bank-gateway")) }

tasks.withType<Test> {
    inputs.files(bankGatewayRuntime)
    jvmArgumentProviders.add(CommandLineArgumentProvider { listOf("-Dbank-gateway.classpath=" + bankGatewayRuntime.asPath) })
    jvmArgs("-Duser.timezone=UTC")
}

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    jvmArgs("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
    // daily-recon.file-dir 기본값 ./bank-files 를 모의 은행과 같은 곳(저장소 루트)으로 (P5-1)
    workingDir = rootProject.projectDir
}
