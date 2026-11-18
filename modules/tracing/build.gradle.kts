plugins {
    `java-library`
}

// api · bank-gateway 가 같이 쓰는 추적 내보내기 도구 (P9b). Spring 에 의존하지 않는다 — OTel SDK · Micrometer Tracing API 만. 버전은 Boot BOM
dependencies {
    api("io.opentelemetry:opentelemetry-sdk-trace")
    api("io.micrometer:micrometer-tracing")

    testImplementation("io.opentelemetry:opentelemetry-sdk-testing")
    testImplementation("io.micrometer:micrometer-tracing-test")
}
