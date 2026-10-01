plugins { `java-library` }

dependencies {
    // 다른 모듈이 코덱을 파이프라인에 붙일 수 있도록 Netty 를 api 로 노출한다
    api("io.netty:netty-handler")
}
