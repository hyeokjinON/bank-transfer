rootProject.name = "bank-transfer"

// 라이브러리 모듈: 다른 모듈이 가져다 쓰는 코드
include(":modules:bank-protocol")   // 고정길이 전문 + Netty 프레이밍 코덱
include(":modules:bank-gateway")    // 은행 FEP 접속 클라이언트 (폴링, 재접속, 응답 매칭)
include(":modules:account")         // 1원 계좌인증 도메인 (Spring·Netty 의존 없음)

// 실행 모듈
include(":apps:api")                // Spring Boot REST API
include(":apps:mock-bank")          // 모의 은행 FEP 서버 (Netty)
