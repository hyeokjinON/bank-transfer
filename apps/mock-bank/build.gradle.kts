plugins {
    `java-library`   // 게이트웨이·API 테스트가 테스트용 은행으로 가져다 쓴다
    application
}

dependencies {
    api(project(":modules:bank-protocol"))
    runtimeOnly("ch.qos.logback:logback-classic")
}

application {
    mainClass = "io.github.hyeokjinon.banktransfer.mockbank.MockBankServerKt"
}
