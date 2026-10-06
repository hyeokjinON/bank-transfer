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

// Windows 콘솔·IntelliJ 실행 창에서 한글 로그가 깨지지 않도록 출력 인코딩을 UTF-8 로 고정
tasks.named<JavaExec>("run") {
    jvmArgs("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
}
