# P7 가상 스레드(Virtual Thread) 적용 비교 — 설계

> 상태: 설계 승인 (2026-11-09) · 다음: 구현 계획 · 결정 기록: ADR-0014 (측정 후 작성)
> 앞 단계: [P6-2 설계](2026-11-05-p6-2-web-reactive-design.md) (네 판 · 측정 격리 · `load/verification`), [P6-1 설계](2026-11-04-p6-1-consumer-coroutines-design.md) (컨슈머 측정 · `load/consumer`), [ADR-0013](../../adr/0013-web-verification-webflux-comparison.md), [ADR-0012](../../adr/0012-consumer-coroutines.md)

## 1. 목적과 범위
P6 에서 "기다리는 동안 스레드를 놓게" 하려고 코드를 다시 썼다 — 컨슈머는 suspend 리스너(P6-1), 웹은 서블릿 비동기 · WebFlux 코루틴 · Reactor(P6-2). P7 은 **코드를 다시 쓰지 않고** JVM 이 같은 일을 하게 하는 가상 스레드를 같은 PC · 같은 측정 도구로 비교한다.

- **바꾸는 것**: `apps/api` 를 `spring.threads.virtual.enabled=true` 로 띄울 수 있게 한다 (Boot 설정 하나). 업무 코드는 바꾸지 않는다.
- **가설 1 (웹, 추정)**: MVC 블로킹 판의 Tomcat 200 천장(P6-2: 지연 500ms · 400건/s 에서 53.5건/s)과 MVC 비동기 판의 앱 실행기 8 천장(P6-2: 800건/s 에서 490.9건/s)이 모두 사라져, 블로킹 코드 그대로 코루틴 판(800.0건/s)만큼 받는다.
- **가설 2 (컨슈머, 추정)**: 블로킹 컨슈머의 처리량은 그대로다. 파티션 3개를 리스너 3개가 한 건씩 처리하므로 동시에 은행을 기다리는 수가 3 그대로다. "가상 스레드는 동시에 **기다릴 수** 있게 할 뿐 동시에 **일을 만들지는** 않는다" 는 대조.
- **기본값**: 측정이 끝날 때까지 기본은 꺼 둔다. 켤지는 §6-5 기준으로 ADR-0014 에서 정한다.
- **범위 밖**: 리액티브 앱(`apps/api-reactive`)의 가상 스레드 (§2 결정 3), 코루틴 컨슈머 + 가상 스레드, Java 25 로 올리기, 은행 · DB 동시 호출 상한(세마포어), ThreadLocal · MDC 점검, Tomcat `max-connections` 조정, 끊김 동작 실험, 1,600건/s 한계 탐색, 웹 판 기본값(`async`) 다시 고르기, CI 에서의 측정.

## 2. 결정 요약
| # | 결정 | 이유 |
|---|---|---|
| 1 | 켜는 방법은 Boot 설정 `spring.threads.virtual.enabled` 하나 | 실무에서 실제로 켜는 방법이 이것이다. 실행기를 손으로 바꾸면 "무엇이 가상 스레드가 됐는지" 가 코드마다 달라진다 |
| 2 | 웹(blocking · async 판)과 컨슈머(blocking) 둘 다 잰다 | 웹은 "천장이 사라진다", 컨슈머는 "그래도 그대로다" — 둘이 같이 있어야 가상 스레드의 효과와 한계가 함께 보인다 |
| 3 | 리액티브 앱은 가상 스레드 판을 만들지 않고 코루틴 판을 기준선으로만 다시 잰다 | Reactor Netty 이벤트 루프 · `Dispatchers.IO` · `boundedElastic` 은 Boot 설정과 상관없는 자체 스레드다 (추정 — Boot 4.1.1 jar 의 자동 설정 목록에 없음). 이미 스레드를 놓는 앱이라 가상 스레드가 할 일이 저장 한 번(몇 ms)뿐이다 |
| 4 | 기준 칸(끈 판)도 같은 날 다시 잰다 | P6-2 · P6-1 숫자를 가져다 쓰면 날짜 · PC 상태 차이가 섞인다 |
| 5 | pinning 은 실험(테스트 경로)과 측정(실제 부하) 두 번 본다 | Java 21 은 `synchronized` 안에서 블로킹하면 캐리어를 붙잡는다 (JDK 24 에서 고쳐짐). 이 저장소는 Java 21 이다 |
| 6 | 범위를 줄인다 (2026-11-09 사용자 결정) | 결과가 대부분 예상 가능하다. 끊김 실험 · 1,600 한계 탐색 · 컨슈머 코루틴 재측정은 빼고 P6 값을 인용한다 |

## 3. 무엇이 가상 스레드가 되나 (Boot 4.1.1, jar 로 확인 — 실험 V1 에서 다시 확인)
| 곳 | 꺼짐 (지금) | 켜짐 |
|---|---|---|
| Tomcat 요청 처리 | 플랫폼 스레드 풀 최대 200 (`server.tomcat.threads.max`) | 요청마다 가상 스레드 (`TomcatVirtualThreadsWebServerFactoryCustomizer`) — 최대 스레드 설정이 천장이 아니게 된다 |
| 앱 실행기 `applicationTaskExecutor` (async 판의 은행 응답 뒤처리) | `ThreadPoolTaskExecutor` 코어 8 · 큐 무제한 | `SimpleAsyncTaskExecutor` — 작업마다 가상 스레드, 풀 없음 |
| `@Scheduled` (발행기 · 정리 작업 · 대사 · 보존) | `ThreadPoolTaskScheduler` 1 | `SimpleAsyncTaskScheduler` (가상 스레드) |
| Kafka 리스너 컨테이너 (블로킹 컨슈머 3) | 컨테이너가 만든 플랫폼 스레드 | `SimpleAsyncTaskExecutor.setVirtualThreads(true)` 를 리스너 실행기로 (`kafkaListenerContainerFactoryConfigurerVirtualThreads`). 앱에 직접 만든 컨테이너 팩토리가 없어 적용된다 (코드 확인됨) |
| Netty 은행 게이트웨이 이벤트 루프 · Hikari 하우스키핑 · Kafka 클라이언트 내부 스레드 | 그대로 | 그대로 (추정 — Boot 설정 대상 아님) |

- 블로킹 판이 기다리는 곳은 `CompletableFuture.join()` 이다 (코드 확인됨). `join()` 은 `LockSupport.park` 로 기다려 가상 스레드를 캐리어에서 내린다 (JDK 동작, 추정 — V2 로 확인).
- 앱 코드(`apps/api` · `modules/*`)에 `synchronized` · `@Synchronized` 는 없다 (grep 확인 — 모의 은행 `BankJournal` 에만 있고, 모의 은행은 별도 프로세스다). pinning 이 나면 라이브러리(MySQL 드라이버 9.7.0 · Hikari · Kafka 클라이언트 · Logback) 쪽이다.

## 4. 진행 조건 실험 (구현 · 측정 전, 통과해야 다음 Task)
`apps/api` 테스트 `experiment/VirtualThreadExperimentTest` — Testcontainers(MySQL · Kafka) + 진짜 모의 은행(Netty), `spring.threads.virtual.enabled=true` 컨텍스트.

| # | 질문 | 방법 | 예상 (추정) | 다르면 |
|---|---|---|---|---|
| V1 | 설정을 켜면 무엇이 가상 스레드가 되나 | 네 곳에서 실제로 도는 스레드를 기록해 `Thread.isVirtual()` 확인: Tomcat 요청(1원인증 요청이 지나는 서블릿 필터 — Tomcat 실행기는 판과 상관없이 같다), 앱 실행기(`applicationTaskExecutor` 빈에 작업 하나), `@Scheduled`(테스트용 예약 작업 — 앱과 같은 Boot 스케줄러), Kafka 리스너(테스트용 리스너 — 앱 리스너와 같은 Boot 컨테이너 팩토리) | 넷 다 `true` | 바뀌지 않는 곳은 측정 해석 · 결과 문서에서 그 판의 의미를 다시 적는다 |
| V2 | 실제 경로에 pinning 이 있나 | JDK `jdk.jfr.consumer.RecordingStream` 으로 `jdk.VirtualThreadPinned` 를 **기준 0ms** (측정 기본 20ms 보다 민감하게) 로 켜고, async 1원인증(HTTP) 한 건 + blocking 1원인증 한 건(블로킹 컨트롤러가 하는 `service.request(...).join()` 을 가상 스레드에서 그대로) + 블로킹 출금 한 건(접수 → 가상 스레드에서 발행 → 컨슈머 → 은행 → `COMPLETED`)을 끝까지 돌린 뒤 이벤트 수를 센다. 같은 녹화 방식으로 `synchronized` 안 `sleep` 이 실제로 잡히는지 보는 대조 테스트를 함께 둔다. 추가 의존성 없음 | 0건 (대조는 1건 이상) | 위치(스택 맨 위 몇 줄)를 기록하고, 단언을 "실제로 이렇다" 로 바꿔 남긴다 (P6-2 E1 과 같은 방식). 측정 해석 · ADR-0014 기준 1 에 반영. 우회 코드는 범위 밖 |
| V3 | `jvm.threads.live` 가 가상 스레드를 세나 | Micrometer `JvmThreadMetrics` 가 읽는 값(`ThreadMXBean.threadCount`)을, 가상 스레드 200개를 래치에 묶어 둔 채 읽는다 (Spring 컨텍스트 없이) | 200 만큼 늘지 않는다 (플랫폼 스레드만 센다) | 결과 문서의 스레드 열 이름 · 해석을 바꾼다 |

- 실험은 머지 뒤에도 회귀 테스트로 남는다 (P6-1 `SuspendListenerExperimentTest`, P6-2 `WebFluxExperimentTest` 와 같다).

## 5. 설정 (기본값)
| 키 | 기본 | 비고 |
|---|---|---|
| `spring.threads.virtual.enabled` | `false` | `application.yml` 에 명시. 켤지는 ADR-0014 |
| `/actuator/info` 의 `threads.virtual` | 위 값 | 측정 사전 확인이 회차 인자와 같은지 읽는다 (P6-2 의 `web-mode` 와 같은 방식) |

- 기존 `account.verification.web-mode` (`async` | `blocking`), `transfer.consumer.mode` (`blocking` | `coroutine`) 는 그대로다. 가상 스레드는 이 판들과 곱해지는 별도 축이다.

## 6. 측정
### 6-1. 웹 1원인증 (`load/verification`, 은행 지연 500ms)
| 판 | 400건/s | 800건/s | 보려는 것 |
|---|---|---|---|
| blocking (끔) | 3회 | — | 무너지는 기준선 (800 은 더 무너질 뿐이라 뺀다) |
| **blocking + VT** | 3회 | 3회 | 가설 1 — Tomcat 200 천장이 사라지나 |
| async (끔) | 3회 | 3회 | 앱 실행기 8 천장의 기준선 |
| **async + VT** | 3회 | 3회 | 실행기가 가상 스레드로 바뀌어 천장이 사라지나 |
| coroutine (끔) | 3회 | 3회 | 리액티브 기준선 (Reactor 는 P6-2 에서 코루틴과 같아 뺀다) |

- 본 측정 27회 + drift 1회(마지막에 blocking + VT · 400 한 번 더). 회차 하나 = 워밍업 10초 + 측정 30초, 앱 재기동 포함 약 40분 (추정).
- 회차는 판 사이를 번갈아 돈다 (1회차 다섯 판 → 2회차 → 3회차).
- 지연 100ms 를 빼는 이유: P6-2 에서 400건/s 까지 네 판 모두 도착률 그대로였다 — 차이가 드러나지 않는다.

### 6-2. 출금 컨슈머 (`load/consumer`, 은행 지연 100ms)
| 판 | 회차 | N | 보려는 것 |
|---|---|---|---|
| blocking (끔) | 3회 | 1,500 | 기준선 (P6-1: 20.9건/s) |
| **blocking + VT** | 3회 | 1,500 | 가설 2 — 처리량이 같아야 한다 |

- 6회, 약 20분 (추정). 두 판을 번갈아 돈다. "동시성을 늘리면 오른다" 는 대조는 P6-1 의 코루틴 상한 64 값(387.3건/s)을 인용한다 — 다시 재지 않는다.

### 6-3. 도구 변경
- 두 `run.sh` 에 환경변수 `VT=1` (기본 0). 켜면:
  - 앱 인자 `--spring.threads.virtual.enabled=true`, 사전 확인에서 `/actuator/info` 의 `threads.virtual` 이 `true` 인지 확인 (끈 회차는 `false` 인지)
  - JVM 인자 `-XX:StartFlightRecording=name=p7,settings=default,filename=<WORK>/app.jfr` — JDK 21 `default.jfc` 는 `jdk.VirtualThreadPinned` 를 기준 20ms 로 켠다
  - 앱을 내리기 **전에** `jcmd <PID> JFR.dump name=p7 filename=<WORK>/app.jfr` (강제 종료는 녹화를 남기지 않는다), 이어 `jfr print --events jdk.VirtualThreadPinned` 로 이벤트 수를 세어 JSON `pinned_events` 에 넣는다. 0 이 아니면 스택 맨 위 줄별 수를 `<WORK>/pinned.txt` 에 남긴다
  - 결과 이름에 `-vt` (웹 `blocking-vt-d500-r800-1`, 컨슈머 `blocking-vt-d100-h20-1`), JSON `virtual_threads: true`
- 웹 `VT=1` 은 MVC 판(blocking · async)에만, 컨슈머 `VT=1` 은 blocking 에만 허용한다 (그 밖은 인자 오류로 멈춤).
- 원자료 폴더를 환경변수 `RAW_SUB` 로 바꿀 수 있게 한다 (`docs/perf/raw/<RAW_SUB>`, 기본은 각 도구의 기존 `p6-2` · `p6-1` 그대로). k6 요약 파일 경로도 같은 값을 따른다. P7 측정 절차(OPERATION §13)는 끈 판 · VT 판 모두 `RAW_SUB=p7` 로 돌린다. 작업 파일은 `build/load-p62/` · `build/load-p61/` 그대로.
- 표는 새 `load/p7/summarize.sh` 가 `raw/p7` 하나로 웹 · 컨슈머 두 표를 만든다 (VT · `pinned` 열, "플랫폼 스레드 최대"). P6 의 `summarize.sh` 두 개는 고치지 않는다 — P6 결과 문서를 그대로 다시 만들 수 있게.
- 측정 사용자 · 격리 · 정확성 확인은 P6-2 · P6-1 규칙 그대로 (웹 `p62-u001` ~ `u100`, 컨슈머 `load-p61-u001` ~ `u100`, 끝나면 `cleanup`).

### 6-4. 한 회차에 기록하는 것 (기존 + 추가)
- 기존: 처리량, p50 · p95 · p99, 시간 초과 · `dropped`, 시스템 CPU, 정확성 (웹: 201 수 = 행 수 = 1원 수 / 컨슈머: 거래번호마다 은행 한 번)
- 추가: `pinned_events`, `virtual_threads`, 플랫폼 스레드 최대 (가상 스레드 수는 재지 않는다 — §8)

### 6-5. 판단 기준 (미리 정함, ADR-0014)
`apps/api` 의 가상 스레드 기본값을 **켜는 쪽으로 제안**하려면 셋 다 만족해야 한다:
1. V2 와 측정 회차 모두 pinning 0건
2. 웹 모든 칸에서 blocking + VT · async + VT 의 처리량이 async(끔) 이상 (5% 안이면 "같다")
3. 컨슈머 blocking + VT 의 처리량이 끈 판의 ±5% 안

하나라도 어긋나면 끈 채로 두고 이유와 다시 볼 조건(예: Java 25)을 적는다. 켜자고 제안하더라도 실제로 기본값을 바꾸는 일은 ADR 채택(머지) 뒤 정리 PR 에서 한다.

### 6-6. 예상 (추정 — 결과 문서 "예상과 비교" 에 쓴다)
| 칸 | 예상 |
|---|---|
| blocking + VT · 400 | 약 400건/s, p50 약 1,020ms (동시에 기다리는 요청 약 400 = 가상 스레드 약 400) |
| blocking + VT · 800 | 약 800건/s. 저장 한 번이 커넥션을 몇 ms 만 쥐어 Hikari 20 은 남는다 (800 × 약 5ms ≈ 4) |
| async + VT · 800 | 약 800건/s — P6-2 실행기 64 확인 실험(800.0건/s)과 같은 모양 |
| 플랫폼 스레드 최대 | VT 판이 끈 판보다 작다 (Tomcat 200 · 실행기 스레드가 사라짐) |
| 컨슈머 blocking + VT | 약 21건/s (끈 판 ±5%) |

## 7. 테스트
| 위치 | 테스트 |
|---|---|
| `apps/api` `experiment/VirtualThreadExperimentTest` | V1 · V2 · V3 (§4) |
| `apps/api` 설정 테스트 | 설정을 주지 않으면 Tomcat 요청 · 앱 실행기가 플랫폼 스레드다 (결정 전까지 기존 동작 고정), `/actuator/info` 에 `threads.virtual` 이 보인다 |
| 기존 | 518개 그대로 통과. CI 에 Testcontainers 컨텍스트 하나가 늘어난다 |

- **변이 검사** (커밋하지 않고 되돌림): 실험 컨텍스트에서 설정을 빼기 → V1 이 잡아야 한다. pinning 검출이 실제로 동작하는지는 V2 의 대조 테스트(`synchronized` 안 `sleep` → 1건 이상)가 늘 확인한다.
- 측정 도구: `bash -n` 문법 확인, `VT=1` 인자 검사(허용 안 되는 판에서 멈춤), 짧은 시험 회차(`smoke`) 하나씩 — 결과는 `.json` 으로 남기지 않는다.
- **손 확인** (머지 전): `--spring.threads.virtual.enabled=true` 로 API 를 띄워 1원인증 → 201 → `/confirm` → `VERIFIED` → 출금 → `COMPLETED`. `/actuator/info` 의 `threads.virtual` 확인.

## 8. 위험과 한계
- 한 PC · 인스턴스 하나의 상대 비교다. 절대 수치는 운영으로 옮기지 못한다.
- **가상 스레드 수는 재지 않는다**: `jvm.threads.live` 는 플랫폼 스레드만 센다 (V3). 동시에 기다리는 요청 수는 리틀의 법칙(처리량 × 지연)으로 추정만 한다.
- **풀이 없어진다**: 켜면 Tomcat · 앱 실행기가 요청 · 작업마다 새 가상 스레드라, 풀 크기가 막아 주던 과부하 보호가 사라진다. 은행 · DB 에 몰리는 동시 수는 Hikari(20) · 은행 응답 시간 초과(3초)만 막는다. 이번 측정 범위(800건/s)에서는 문제가 없을 것으로 보지만(추정), 제한 장치는 범위 밖으로 남긴다.
- **pinning 측정은 20ms 기준이다**: 그보다 짧은 pinning 은 측정 회차에서 세지 않는다 (V2 는 0ms 로 본다).
- JFR 녹화가 앱에 약간의 부담을 준다 (기본 설정 기준 1% 안팎, 일반 정보 — 추정). 끈 판에는 JFR 을 붙이지 않으므로, VT 판이 끈 판보다 약간 불리한 쪽이다.
- CPU · DB 천장은 그대로다. 가상 스레드는 "앱이 스레드에 막히지 않게" 할 뿐이다.

## 9. 결정 기록 · 문서
- **ADR-0014** "가상 스레드 적용 판단" (측정 후 작성, 제안 → 머지 후 채택): 무엇이 가상 스레드가 되나(V1), pinning(V2 · 측정), 측정 결과, §6-5 기준 판정, 다시 볼 조건 (Java 25 · 동시 호출 상한)
- 결과 문서 `docs/perf/p7-virtual-threads.md`: 결론, 그림(플랫폼 · 가상 스레드가 기다리는 모습), 조건, 결과, 예상과 비교, 해석, 한계, 원자료
- OPERATION §13 (측정 절차), ARCHITECTURE (스레드 모델에 가상 스레드 행), README (로드맵 P7 · 테스트 수), SETUP (테스트 수), HANDOVER (P7 범위 밖 목록), CHANGELOG
- 브랜치 `feat/p7-virtual-threads`, PR 하나. 머지 후 정리 PR, velog #13 (비공개, "면접 질문 + 내 숫자로 답하기" 절)
