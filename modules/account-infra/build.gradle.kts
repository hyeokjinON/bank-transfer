plugins {
    `java-library`
    `java-test-fixtures`          // 오류 계약 기대표를 두 앱의 테스트가 같이 읽는다 (P6-2)
    kotlin("plugin.spring")
    kotlin("plugin.jpa")          // @Entity 에 인자 없는 생성자
}

// Hibernate 가 엔티티를 프록시로 감쌀 수 있게 연다 (apps/api 에서 옮겨 온 설정)
allOpen {
    annotation("jakarta.persistence.Entity")
}

// account 도메인 포트(VerificationRepository · 은행 응답)의 바깥 구현. 도메인 모듈(account)은 여전히 Spring · Netty 의존이 없다 (ADR-0001).
dependencies {
    api(project(":modules:account"))
    api(project(":modules:bank-gateway"))   // bank-protocol 의 응답 타입 · 게이트웨이 예외 (bank-gateway 가 api 로 내보낸다)
    api("org.springframework.boot:spring-boot-starter-data-jpa")   // 공개 시그니처에 JpaRepository · TransactionTemplate 이 있다

    testFixturesApi(project(":modules:account"))
    testFixturesApi(project(":modules:bank-gateway"))
}
