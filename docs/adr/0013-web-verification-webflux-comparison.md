# ADR-0013: 웹 1원인증 경로의 WebFlux 비교 — 네 판 · 별도 앱 · 끊기면 취소

- 상태: 제안 (2026-11-05)
- 관련: [설계](../superpowers/specs/2026-11-05-p6-2-web-reactive-design.md) · [계획](../superpowers/plans/2026-11-05-p6-2-web-reactive.md) · [ADR-0012](0012-consumer-coroutines.md) (컨슈머 코루틴) · [측정 결과](../perf/p6-2-web-reactive.md)

## 맥락
- 웹 요청 중 은행을 기다리는 곳은 1원인증 요청 하나다 (성명조회 → 1원 이체, 은행 두 번). 출금 접수는 DB 트랜잭션 하나로 202 를 돌려주고 은행을 기다리지 않는다 — WebFlux 로 옮겨도 JDBC 를 기다리는 것은 같다.
- 지금의 1원인증은 이미 CompletableFuture 를 돌려주는 서블릿 비동기다. 기다리는 동안 Tomcat 스레드를 놓아 준다.
- P6-1 은 "빠른 것은 코루틴이 아니라 동시에 기다릴 수 있는 수" 를 컨슈머에서 확인했다. 웹에서도 같은지, 그리고 WebFlux 의 실제 차이(스레드 · 지연 분포 · 끊김)를 본다.

## 결정
1. **네 판을 같은 도구로 비교한다**: MVC 블로킹(대조군, `join`) · MVC 비동기(지금 기본) · WebFlux 코루틴 · WebFlux Reactor. `account.verification.web-mode` 로 기동 때 하나만 (소문자 그대로, 그 밖은 기동 실패). `apps/api` 는 `async`(기본) | `blocking`, `apps/api-reactive` 는 `coroutine`(기본) | `reactor`.
2. **WebFlux 는 별도 앱 `apps/api-reactive`** (포트 8081) — 1원인증 요청 하나만. Spring 은 한 앱에 MVC 와 WebFlux 가 같이 있으면 MVC 를 고른다. Kafka · 스케줄러 · 배치 · Flyway 는 넣지 않는다 (스키마는 `apps/api` 소유).
3. **판정 · 바깥 어댑터는 한 벌**: 판정 순수 함수 `VerificationRequestRules`(account — 기존 `AccountVerificationServiceTest` 는 그대로 통과), JPA 저장소 · 은행 응답 해석(새 모듈 `account-infra`). 판마다 다른 것은 기다리는 방법과 블로킹 코드를 도는 스레드뿐. 계획 중 드러난 조정: 공유 패키지의 엔티티 스캔은 `@AutoConfigurationPackage` 로 등록하는데 `basePackages` 만 적으면 앱 자기 패키지가 빠져서 `basePackageClasses = [앱 클래스]` 를 같이 적는다 (코드 확인됨, 커밋 64801f6). `BankMaintenanceGuard` 는 두 앱이 쓰므로 `bank-gateway` 로 옮겼다.
4. **JDBC 는 이벤트 루프에서 돌지 않는다**: 코루틴 `withContext(Dispatchers.IO)`, Reactor `publishOn(boundedElastic())`. 실험 E1 (코드 확인됨, 커밋 bc79516): 코루틴 `await` 뒤와 Reactor `map` 은 둘 다 게이트웨이 이벤트 루프 스레드(`multiThreadIoEventLoopGroup-4-1`)에서 이어졌다 — 그대로 JDBC 를 부르면 은행 응답 처리 루프가 막힌다.
5. **클라이언트가 끊으면 취소되게 둔다**: 실험 E2 · E3 (코드 확인됨) — 코루틴 · Reactor 모두 끊기면 취소되고, 게이트웨이 future 의 `isCancelled` 가 true 였다. E4: 은행 응답 시간 초과는 두 판 모두 `BankTimeoutException` 그대로 올라온다. 1원 이체 응답을 기다리는 중 끊기면 1원이 나갔을 수 있어 마스킹한 경고(`stage=transfer`)를 남기고, 사용자는 다시 요청한다(대가는 최악에 1원 두 번 — 돈을 잃지 않는다). `NonCancellable` 은 쓰지 않는다: 응답을 못 받은 사용자는 인증 건 id 를 몰라 저장해도 확인할 수 없다. `CancellationException` 은 오류 응답으로 바꾸지 않고 다시 던진다. MVC 비동기는 끊겨도 끝까지 가서 저장한다 (실험 E2 MVC, 코드 확인됨).
6. **오류 응답은 계약 테스트로 맞춘다**: 웹 계층 코드는 공유하지 않고(API 가 다르다), 기대표 하나(`VerificationErrorContract`, account-infra test fixtures)를 두 앱 테스트가 읽는다. 갈라지면 테스트가 깨진다.
7. **운영 기본은 MVC 비동기 그대로** — WebFlux 앱은 비교용이다. 측정 뒤에도 유지 (결과 절). 단 MVC 비동기의 콜백 실행기 크기를 정하는 일을 남긴다.

## 결과
- **측정** (2026-11-05, 82회 모두 정확성 통과, [측정 결과](../perf/p6-2-web-reactive.md) — 이 PC 기준 상대 비교): 은행 지연 500ms(요청 ≈ 1초) · 초당 400건에서 MVC 블로킹 53.5건/s (k6 시간 초과 4,999건), MVC 비동기 · 코루틴 · Reactor 400.0건/s. 블로킹의 Tomcat 스레드를 200 → 400 으로 늘리면 386.1건/s — 천장은 스레드 수다.
- **판단 기준 결과**: 본 측정 모든 칸에서 비동기 세 판의 처리량은 도착률 그대로라 **같다** (5% 안). 차이는 스레드 수(코루틴 96 ~ 110 일정, Reactor 약 190, MVC 비동기 63 ~ 163)와 p99(500ms · 400건/s: 1.04 · 1.03 · 1.15초).
- **예상 밖**: MVC 비동기는 초당 800건에서 491건/s · p99 4.3초로 무너졌다. 은행 응답 뒤처리 · 저장을 도는 앱 실행기(Boot 기본 8스레드)가 원인 — `spring.task.execution.pool.core-size=64` 로 같은 칸 800.0건/s · p99 1.7초 (확인 1회). 코루틴 · Reactor 는 800건/s 를 그대로, 1,600건/s 에서 960 · 973건/s 로 PC CPU(같은 PC 의 k6 포함)가 먼저 찼다.
- **운영 기본 판단**: MVC 비동기 유지. 이 부하까지 WebFlux 와 같고, 별도 앱 · 끊김 취소 비용 · 운영 단위가 하나 더 생기는 것을 감수할 이득이 이 측정에서는 나오지 않았다. 대신 MVC 비동기의 콜백 실행기(크기 · 큐 · 지표)를 따로 정한다 — 기본 8스레드는 은행이 느릴 때 천장이 된다. 한계 탐색의 멈춤 조건 "dropped > 0" 은 측정 구간 처리량이 도착률의 99% 미만일 때만 적용했다 (dropped 는 낮은 부하에도 워밍업 VU 증설 지연으로 생긴다 — 결과 문서 §7-2).
- 검증 (코드 확인됨): 블로킹 대조군은 Tomcat 스레드 2 · 은행 250ms × 2 · 동시 4건에서 1074ms, `join` 을 future 반환으로 바꾸면 546ms 라 테스트가 잡는다 (커밋 01b0185).
- **변이 검사** (커밋하지 않고 되돌림, 커밋 0b894b8) 4종 모두 테스트가 잡았다: ① `withContext(io)` 제거 → 코루틴 판 "… IO 디스패처에서 저장한다" 테스트 ② `publishOn` 제거 → Reactor 판 "… publishOn 스케줄러에서 저장한다" 테스트 ③ 취소를 `BankUnavailableException` 으로 바꿈 → 코루틴 판 "… CancellationException 그대로" 테스트 ④ 블로킹 `join` → future 반환 → "Tomcat 스레드 2개면 … 두 번에 나뉘어 끝난다" 테스트.
- 테스트: 전체 빌드 514개, 실패 · 건너뜀 0 (이 저장소 전체 수).
- 대가: 코드가 두 벌의 실행 모델(`apps/api` · `apps/api-reactive`)로 늘고, 스레드 경계를 손으로 지켜야 한다 (테스트가 지킨다). 이 단계의 모의 은행 입금 기록은 측정이 O(n²) 복사에 흔들리지 않도록 `ConcurrentLinkedQueue` 로 바꿨다.

## 검토한 대안
- **출금 접수도 WebFlux 로**: 은행을 기다리지 않아 차이가 작을 것으로 추정 — 범위에서 뺐다.
- **R2DBC**: 저장 계층(차감 전략 4종 · 아웃박스)을 다시 써야 한다. 이번 비교의 질문("은행을 기다리는 동안 스레드")과 다르다 — 범위 밖.
- **한 앱에 MVC 와 WebFlux**: Spring 이 MVC 를 고른다 (불가).
- **얇은 판(지금 CompletableFuture 서비스를 await 만)**: 코드는 가장 적지만 서버 모델 하나만 비교되고 코루틴다운 코드가 없다.
- **`NonCancellable` 로 1원 이체부터 저장까지 묶기**: 위 결정 5.
- **1원인증 동시 호출 상한**: 지금 없다. 한 판에만 두면 비교가 공정하지 않다 — 범위 밖.
