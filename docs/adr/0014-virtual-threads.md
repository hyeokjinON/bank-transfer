# ADR-0014: 가상 스레드 적용 판단 — 성능은 코루틴만큼, Java 21 에서는 기본 끔

- 상태: 채택 (2026-11-09, [#23](https://github.com/hyeokjinON/bank-transfer/pull/23))
- 관련: [설계](../superpowers/specs/2026-11-09-p7-virtual-threads-design.md) · [계획](../superpowers/plans/2026-11-09-p7-virtual-threads.md) · [ADR-0012](0012-consumer-coroutines.md) (컨슈머 코루틴) · [ADR-0013](0013-web-verification-webflux-comparison.md) (웹 WebFlux 비교) · [측정 결과](../perf/p7-virtual-threads.md)

## 맥락
- P6 에서는 은행을 기다리는 동안 스레드를 놓게 하려고 코드를 다시 썼다 — 컨슈머는 suspend 리스너(P6-1), 웹은 서블릿 비동기 · WebFlux 코루틴 · Reactor(P6-2). 블로킹 웹 판은 Tomcat 200 천장(지연 500ms · 400건/s 에서 53.5건/s), 서블릿 비동기 판은 뒤처리 실행기 8 천장(800건/s 에서 490.9건/s)에 막혔다.
- Java 21 가상 스레드는 코드를 그대로 두고 JVM 이 기다림을 처리한다. Spring Boot 는 설정 하나(`spring.threads.virtual.enabled`)로 켠다. 지원 공고에도 "Coroutines, WebFlux, Virtual Thread" 가 함께 나온다.
- 이 저장소는 Java 21 이다. Java 21 은 `synchronized` 안에서 블로킹하면(모니터 입장 대기 포함) 가상 스레드가 캐리어를 붙잡는다 (pinning — JDK 24 의 JEP 491 이 고침).

## 결정
1. **켜는 방법은 Boot 설정 하나** `spring.threads.virtual.enabled` — 실행기를 손으로 바꾸지 않는다. `application.yml` 에 기본 `false` 를 명시하고 `/actuator/info` 의 `threads.virtual` 로 노출한다 (측정 스크립트가 회차 인자와 같은지 확인).
2. **기본값은 끈 채로 둔다** (Java 21). 미리 정한 판단 기준(설계 §6-5) 셋 중 기준 1 이 어긋났다:

| 기준 | 결과 | 판정 |
|---|---|---|
| 1. 실험 V2 와 측정 회차 모두 pinning 0건 | park 중 pinning(`jdk.VirtualThreadPinned`)은 V2(0ms) · 측정 VT 회차(20ms) 0건. 그러나 **냉시동 교착 4/4** (아래), 측정 회차에도 가상 스레드의 모니터 입장 대기 2건 | 불충족 |
| 2. 웹 모든 칸에서 VT 판 처리량 ≥ async(끔) | 400건/s: 400.0 · 399.1 vs 399.8 (같다), 800건/s: 800.0 · 800.0 vs 495.8 | 충족 |
| 3. 컨슈머 VT 처리량이 끈 판의 ±5% 안 | 20.6 vs 20.7건/s (−0.5%) | 충족 |

3. **냉시동 교착 대처는 측정 도구에만** 넣는다: 웹 측정 회차는 판 · VT 여부와 상관없이 k6 전에 1원인증 3건을 순차로 보낸다 (정상 상태를 재려고). 앱에 기동 워밍업을 넣는 일은 이 ADR 범위 밖이다 — 켤 때 함께 정한다.

## 무엇이 가상 스레드가 되나 (실험 V1, 확인)
Boot 4.1.1 에서 설정을 켜면 네 곳이 바뀐다: Tomcat 요청(`TomcatVirtualThreadsWebServerFactoryCustomizer`, 요청마다 가상 스레드 — `server.tomcat.threads.max` 가 천장이 아니게 된다), 앱 실행기 `applicationTaskExecutor`(`SimpleAsyncTaskExecutor`, 풀 없음), `@Scheduled`(`SimpleAsyncTaskScheduler`), Kafka 리스너 컨테이너. Netty 게이트웨이 이벤트 루프 · Hikari · Kafka 클라이언트 내부 스레드는 그대로다 (추정).

## pinning (실험 V2 · 측정 · 재현)
- **JFR 이 세는 범위**: JDK 21 에서 `jdk.VirtualThreadPinned` 를 만드는 곳은 `VirtualThread.parkOnCarrierThread` 하나다 (JDK `src.zip` 확인) — **모니터를 쥔 채 park** 할 때만 남고, `synchronized` **입장 대기**로 캐리어를 붙잡는 것은 남지 않는다. 입장 대기는 `jdk.JavaMonitorEnter` 로 따로 본다.
- **park 중 pinning 0건**: JFR `jdk.VirtualThreadPinned` 를 기준 0ms 로 켜고 async · blocking 1원인증, 블로킹 출금(발행 → 컨슈머 → 은행 → `COMPLETED`)을 돌려 0건. 대조(`synchronized` 안 `sleep`) 1 ~ 3건으로 검출이 동작함을 함께 확인 (확인).
- **1회성**: 첫 요청의 `DispatcherServlet` 초기화가 Tomcat `StandardWrapper.allocate` 의 `synchronized` 안에서 돌아, 그 안의 로그가 출력 잠금을 기다리면 pinned 된다 (테스트에서 확인). `spring.mvc.servlet.load-on-startup` 으로 피할 수 있다 (추정). 공유 Kafka 프로듀서의 첫 생성도 같은 모양이다 — kafka-clients `AppInfoParser.registerAppInfo` 가 `static synchronized` 안에서 버전 로그를 남긴다 (바이트코드 확인, P7 머지 뒤 CI 에서 2ms 1건으로 드러남).
- **Kafka 컨슈머 시작 · 리밸런스 (머지 뒤 확인)**: 가상 스레드 리스너가 그룹에 합류하거나(`AbstractCoordinator.requestRejoin`) 오프셋을 초기화할 때(`SubscriptionState.maybeSeekUnvalidated`) Kafka 컨슈머의 `synchronized` 안에서 INFO 로그를 남기고, 그 로그가 출력 잠금(logback · Gradle 출력의 `ReentrantLock`)을 기다리면 pinned 된다 — 캐리어를 4개로 줄이면(`-Djdk.virtualThreadScheduler.parallelism=4`, CI 러너와 같은 수) 로컬에서도 재현된다 (스택 확인). 처리 경로가 아니라 시작 · 리밸런스 때지만, 리스너 컨테이너 하나가 컨슈머 3개라 리스너가 많을수록(이 앱 15개) 캐리어 수를 넘기 쉽다 — 냉시동 교착과 같은 조건이다. 켤 때 리밸런스 중 동작도 재현 시험에 넣는다.
- **냉시동 교착 (핵심 발견)**: 막 뜬 앱에 동시 요청이 곧바로 몰리면 앱이 통째로 멈췄다 — VT 켬 4/4, 끔 2/2 정상, 데운 VT 앱 정상 (확인). 스레드 덤프: 가상 스레드 하나가 Jackson 3 `SerializerCache` 의 모니터를 쥔 채 Kotlin 리플렉션 `ReentrantLock` 을 기다리며 캐리어째 멈추고(`parkOnCarrierThread`), 나머지 15개는 그 모니터 입장을 기다리며 캐리어를 붙잡아 캐리어 16개가 모두 묶였다. 같은 락을 기다리는 가상 스레드 4개는 내려와 park 중이고, 락이 넘어가도 다음 대기자가 올라탈 캐리어가 없다 (덤프 확인 · 마지막 고리는 추정). 입장 대기 15개는 `VirtualThreadPinned` 를 만들지 않고, 모니터를 쥔 1개는 park 가 끝나지 않아 기록되지 않는다 — 이 교착은 JFR pinning **0건으로 보인다**. "JFR 0건 = 안전" 이 아니다.
- **측정 회차의 모니터 입장 대기** (사후 집계, `jdk.JavaMonitorEnter` 20ms 이상): VT 웹 13회 0건, 컨슈머 VT 1회차에 가상 리스너 스레드 2건(각 22.6ms, Hibernate 첫 UPDATE 쿼리 번역의 `synchronized`). "처음 한 번" 라이브러리 초기화라는 점에서 냉시동 교착과 같은 모양이다 (추정). 워밍업이 모든 "처음" 을 덮지는 못한다 — 처음 보는 오류 응답이 몰리는 순간 등 (추정).

## 결과
- **측정** (2026-11-09, 웹 28회 · 컨슈머 6회 모두 정확성 통과, [측정 결과](../perf/p7-virtual-threads.md) — 이 PC 기준 상대 비교): 지연 500ms · 400건/s 에서 블로킹 59.0 → **400.0건/s** (p99 9,967 → 1,033ms), 800건/s 에서 비동기 495.8 → **800.0건/s**, 블로킹 + VT 800.0건/s — 코루틴 판(800.0건/s)과 같다. 플랫폼 스레드 최대는 VT 판 42 로 일정. 컨슈머는 20.7 → 20.6건/s 로 그대로 (은행 최대 동시 3 → 3).
- **손 확인 (머지 전, 개발 DB · 개발용 모의 은행, 2026-11-09)**: 실행 구성 `2. API (8080)` + `--spring.threads.virtual.enabled=true` → `/actuator/info` `threads.virtual: true`. Postman 으로 한 건씩 `user-p41` 1원인증 `SENT` → `VERIFIED`, 출금 2건 `COMPLETED 0000` (접수 → 완료 0.2 ~ 0.6초). 스레드 덤프(요청 전후): Kafka 리스너 15개(출금 · 재시도 3 · DLT) · `@Scheduled` 2개가 `virtual`, Tomcat 요청 풀 `http-nio-8080-exec-*` 0개 (요청마다 가상 스레드라 상주 풀이 없다), 캐리어 `ForkJoinPool-1-worker-*` 23개 — 기본 병렬도 16 보다 많다 (캐리어 보충으로 추정, 원인 확인 안 함).
- **해석**: 가상 스레드는 "동시에 기다릴 수 있는 수" 를 스레드 풀 크기에서 풀어 준다 (블로킹 · 실행기 천장이 사라짐). 그러나 동시성을 만들지는 않는다 — 한 건씩 처리하는 컨슈머는 그대로다 (동시성은 P6-1 처럼 코드가 만든다).
- **풀이 없어진다**: 켜면 Tomcat · 앱 실행기의 풀 크기가 막아 주던 과부하 보호가 사라진다. 은행 · DB 동시 호출은 Hikari(20) · 은행 응답 시간 초과(3초)만 막는다. 켤 때는 동시 호출 상한(세마포어 등)을 함께 정한다 (범위 밖).
- **다시 볼 조건**: ① Java 24+ (JEP 491 — `synchronized` 안에서도 캐리어를 놓음) 로 올리고 냉시동 재현이 사라지는지 확인, 또는 ② Java 21 그대로라면 기동 워밍업(첫 직렬화기 생성) 뒤에 레디니스를 여는 장치 + 냉시동 재현 시험 + 동시 호출 상한을 함께 넣을 때. 켜자는 결정이 나더라도 기본값 변경은 그 결정의 정리 PR 에서 한다.
- 테스트: 전체 524개 (P6-2 518 + 기본값 고정 2 + 실험 4). 실험(V1 · V2 · V2 대조 · V3)은 회귀 테스트로 남는다 — JDK 를 24+ 로 올리면 V2 대조가 0건이 되어 실패하므로 Java 버전 변화가 테스트로 드러난다.
- 대가: 측정 도구에 순차 워밍업이 들어가 P6-2 와 스레드 수를 직접 비교하지 않는다 (같은 날 같은 스크립트 칸끼리만).

## 검토한 대안
- **코루틴(P6-1) · WebFlux(P6-2)**: 같은 처리량을 내지만 코드를 다시 써야 한다. 대신 취소 전파 · 백프레셔 · 동시성 상한을 코드로 표현할 수 있다. 컨슈머처럼 동시성을 만들어야 하는 곳은 이쪽만 된다.
- **실행기 크기 조정**: 서블릿 비동기의 뒤처리 실행기를 64로 키우면 800건/s 를 받는다 (P6-2 확인 1회). 블로킹 판의 Tomcat 천장에는 효과가 없다 (Tomcat 스레드를 키우면 플랫폼 스레드가 그만큼 는다 — 400 에서 386.1건/s).
- **지금 켜고 앱에 기동 워밍업을 넣기**: 가능하지만 어떤 첫 요청 경로가 `synchronized` 를 지나는지 모두 알 수 없다 (이번엔 응답 직렬화기였다). Java 21 에서는 위험 대비 이득이 이 서비스에 필요하지 않다 — 운영 기본(MVC 비동기)이 400건/s 를 이미 받는다.
- **Java 25 로 올리기**: 근본 해결로 보이지만 런타임 변경은 별도 단계다 — 범위 밖.
- **리액티브 앱에 가상 스레드**: Reactor Netty 이벤트 루프 · `Dispatchers.IO` · `boundedElastic` 은 Boot 설정 대상이 아니고 이미 스레드를 놓는다 — 만들지 않았다 (설계 §2 결정 3).
