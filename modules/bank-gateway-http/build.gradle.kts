plugins {
    `java-library`
    `java-test-fixtures`   // 끊는 프록시 · 게이트웨이 프로세스 — 게이트웨이 앱 · api · api-reactive 테스트가 같이 쓴다
}

// 게이트웨이 앱(apps/bank-gateway)과 그 호출자(apps/api · apps/api-reactive)가 같이 쓰는 HTTP 계약 · 클라이언트 (P8-1).
// Spring Boot 에 의존하지 않는다 — WebClient(spring-webflux + Reactor Netty)와 Jackson 만. 버전은 Boot BOM.
dependencies {
    api(project(":modules:bank-gateway"))   // BankGateway · 예외 3종 · 전문 타입
    implementation("org.springframework:spring-webflux")
    implementation("org.springframework:spring-context")   // ReactorClientHttpConnector 의 상위 타입(SmartLifecycle) — spring-webflux 가 끌어오지 않는다
    implementation("io.projectreactor.netty:reactor-netty-http")
    implementation("tools.jackson.core:jackson-databind")
    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    testImplementation("org.awaitility:awaitility-kotlin")
}
