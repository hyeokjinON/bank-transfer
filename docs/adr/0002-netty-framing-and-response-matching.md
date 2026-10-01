# ADR-0002 프레이밍은 LengthFieldBasedFrameDecoder 상속, 응답 매칭은 전문번호 → CompletableFuture

- 상태: 채택 (2026-10-01)

## 맥락
- TCP 는 메시지 경계가 없다. 한 번 읽을 때 전문이 반만 오거나 두 개가 붙어 올 수 있다.
- 예전 코드는 `ByteToMessageDecoder` 를 직접 상속해 길이 필드를 인스턴스 변수로 들고 있었다.
  길이 파싱에 한 번 실패하면 이후 스트림이 계속 어긋났고, 최대 길이 검사가 없었다.
- 응답은 요청 스레드가 DB 이력 테이블을 1초 간격으로 다시 조회해서 기다렸다. 그동안 요청 스레드가 묶였다.

## 결정
1. **프레이밍**: Netty 의 `LengthFieldBasedFrameDecoder` 를 상속하고 `getUnadjustedFrameLength` 만 바꿔
   ASCII 숫자 길이("0052")를 해석한다. 누적·부분 수신·최대 길이 검사는 검증된 부모 구현을 그대로 쓴다.
   길이가 숫자가 아니면 `CorruptedFrameException` → 연결 종료.
2. **응답 매칭**: 요청마다 전문번호를 키로 `CompletableFuture` 를 맵에 등록하고, 응답 핸들러가 같은 번호의 future 를 완료한다.
   `orTimeout` 으로 시간 초과를 걸고, 끝나면(성공·실패 모두) 맵에서 지운다.
3. **링크 체크**: `IdleStateHandler(reader=90s, writer=30s)`. 쓰기 유휴 → 폴링 전송, 읽기 유휴 → 연결 종료 → 재접속.

## 결과
- 요청 스레드가 은행 응답을 기다리지 않는다. Spring MVC 가 `CompletableFuture` 를 받아 서블릿 비동기로 처리한다.
- 동시 요청 50건이 한 TCP 연결 위에서 섞여 와도 각자 자기 응답을 받는다 (테스트로 확인).
- 한계: 응답이 요청을 보낸 인스턴스로 돌아온다는 전제다. 수신 회선이 따로 있거나 인스턴스가 여러 대면
  응답 라우팅(Kafka 파티션 키, Redis Pub/Sub 등)이 필요하다. 예전 시스템이 DB 폴링을 쓴 이유도 여기에 있었다.
