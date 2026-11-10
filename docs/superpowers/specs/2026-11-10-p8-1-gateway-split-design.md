# P8-1 은행 게이트웨이 서비스 분리 — 설계

> 상태: 설계 승인 (2026-11-10) · 결정 기록: ADR-0015 (구현 중 작성) · 결과: `docs/perf/p8-1-gateway-split.md` (측정 뒤 작성)
> 앞 단계: [P7 설계](2026-11-09-p7-virtual-threads-design.md), [P6-2 설계](2026-11-05-p6-2-web-reactive-design.md) (`load/verification`, 판 비교 방식), [P6-1 설계](2026-11-04-p6-1-consumer-coroutines-design.md) (`load/consumer`), [ADR-0001](../../adr/0001-modular-monolith-ports.md) ("P8 에서 게이트웨이를 별도 서비스로 떼어낼 때 `BankPort` 구현만 바꾸면 된다")
> P8 전체: **P8-1 게이트웨이 분리 (이 문서)** → P8-2 앱 이미지 · Compose 로 전체 실행 → P8-3 Kubernetes

## 1. 목적과 범위
지금은 `apps/api`(와 비교용 `apps/api-reactive`)가 프로세스 안에서 은행 FEP 에 Netty 로 직접 붙는다. api 를 여러 대로 늘리면 두 가지가 깨진다.
- **은행 연결이 대수만큼 늘어난다.** 실제 FEP 는 접속 수가 정해져 있고, 응답 매칭(전문번호 → future)이 프로세스 메모리에 있어 요청과 응답이 같은 인스턴스여야 한다.
- 스케줄(정리 작업 · 대사 · 보존)이 대수만큼 돈다 — 이건 P8-3 에서 다룬다.

P8-1 은 첫째를 푼다: **은행 연결을 혼자 가진 게이트웨이 앱을 떼어내고, api 는 HTTP 로 부른다.**

- **바꾸는 것**
  - 새 실행 앱 `apps/bank-gateway`: 은행 연결 1개, 내부 HTTP API 3개(성명조회 · 이체 · 결과조회), 점검 시간 송신 보류, 회선 상태 health.
  - 새 모듈 `modules/bank-gateway-http`: HTTP 계약(DTO · 오류 코드), HTTP 클라이언트, 실패 분류.
  - `modules/bank-gateway` 에 인터페이스 `BankGateway` — 기존 `BankGatewayClient` 가 구현한다. 처리 코드는 이 인터페이스만 본다.
  - `apps/api` · `apps/api-reactive`: `bank.gateway.mode: direct | http` 로 구현을 고른다. **기본은 `direct`** (지금 동작 · 실행 구성 · 테스트 그대로). P8-2 의 Compose 실행에서 `http` 가 기본이 된다.
- **비교 판** (면접에서 차이를 설명하고 구현할 수 있게 — 사용자 요청 2026-11-10): 게이트웨이 서버 **WebFlux(기본) / MVC 비동기**, 클라이언트 **WebClient(기본) / JDK `HttpClient`**. 같은 계약 테스트 · 같은 측정 도구로 비교한다. 판은 "같은 일을 다르게 구현한 코드" 이고, 한 회차에 게이트웨이는 1개만 뜬다 (분산 · 이중화가 아니다).
- **분리 비용 측정**: 분리 전후를 같은 도구로 잰다. 망 지연은 Toxiproxy 로 넣는다 (같은 PC 루프백의 한계를 줄임).
- **도메인 · 처리 규칙은 바꾸지 않는다.** "안 보냄만 다시 보낸다, 모르면 묻는다"(ADR-0005 · 0006 · 0007)가 네트워크 구간이 하나 늘어도 그대로여야 한다.

## 2. 결정 요약
| # | 결정 | 이유 | 버린 대안 |
|---|---|---|---|
| 1 | 첫 분리 대상은 은행 게이트웨이 | 은행 연결 · 응답 매칭은 대수를 늘리면 바로 깨지는 유일한 상태다. ADR-0001 이 예고한 경계 | 도메인별 분리(1원인증 / 출금, DB 분리) — 범위가 크고, 지금 확장을 막는 원인이 아니다 |
| 2 | api ↔ 게이트웨이는 **동기 HTTP** | 은행 호출은 결과를 알아야 다음 상태를 정하는 요청/응답이다. 출금은 이미 아웃박스 → Kafka 뒤에서 호출되므로 사용자 쪽 비동기와 버퍼는 이미 있다. 처리량은 전송 방식이 아니라 동시에 기다릴 수 있는 수가 정한다 (P6-1: 같은 Kafka 컨슈머가 블로킹 20.9 → 코루틴 387건/s) | **Kafka 명령/결과 이벤트**(게이트웨이 재배달 시 이중 송신 → 게이트웨이 멱등 · P3 처리 구조 재설계 필요, 범위 약 2배), **Kafka 요청/응답**(응답 짝짓기 · 늦은 응답 · 타임아웃을 직접 만듦), **gRPC**(도구 · 학습 비용 대비 얻는 것 적음) |
| 3 | 실패 분류는 기존 규칙 그대로: **`BankNotSentException` 만 "안 보냄"**, 나머지는 "모름" | `classifyBankFailure` · `BankInquiryAdapter` 가 이미 이 규칙이다. HTTP 클라이언트는 "은행에 확실히 안 갔다" 를 증명할 수 있을 때만 `BankNotSentException` 을 만든다 | HTTP 오류를 일괄 "안 보냄" 으로 → 이중 출금 |
| 4 | 서버 기본은 WebFlux | DB · Kafka 없이 HTTP → 은행 TCP 를 기다리기만 하는 앱이라 이벤트 루프가 맞다. `Mono.fromFuture(f, suppressCancel = true)` 로 "끊겨도 은행 송신은 취소하지 않음" 을 코드로 강제 | MVC 비동기 (비교 판으로 구현) |
| 5 | 클라이언트 기본은 WebClient | `toFuture()` 로 `CompletableFuture` 인터페이스 유지, 연결 풀 조절 가능, P9 에서 Micrometer 지표가 바로 붙음 | JDK `HttpClient` (비교 판으로 구현), `RestClient` + 가상 스레드 (말로만 비교 — P7 로 설명 가능) |
| 6 | 계약 DTO 는 한 모듈(`modules/bank-gateway-http`)을 서버 · 클라이언트가 같이 쓴다 | 같은 저장소 · 같은 버전 배포라 어긋날 일이 없다 | OpenAPI · 계약 테스트 도구 — 팀이 갈라지면 필요 (범위 밖) |
| 7 | 게이트웨이는 1대 | 분리 목적이 은행 연결을 한 곳으로 모으는 것 | 여러 대 · 이중화 → P8-3 (active-standby) |
| 8 | 기본 모드는 `direct` 유지 | 기존 524개 테스트 · 실행 구성 · 손 확인 절차가 그대로 돈다. 분리 비용을 같은 도구로 비교할 수 있다 | 바로 `http` 기본 → P8-2 에서 |
| 9 | 망 지연 주입은 Toxiproxy (Compose 프로필 `chaos`, 사용자 결정 2026-11-10) | 같은 PC 루프백은 망 지연이 거의 0 이라 분리 비용이 작게 나온다. 끊김도 실제 망처럼 재현 | 계산으로만 설명 |

## 3. 구조
```
                     ┌──────────── mode=direct (기본, 지금 그대로) ────────────┐
[apps/api] ──────────┤                                                          ├──▶ BankGatewayClient ──TCP──▶ [mock-bank]
[apps/api-reactive] ─┤                                                          │    (프로세스 안)
                     └── mode=http ──▶ HttpBankGateway ──HTTP──▶ [apps/bank-gateway :8082] ──▶ BankGatewayClient ──TCP(1개)──▶ [mock-bank]
```

| 위치 | 내용 | 의존 |
|---|---|---|
| `modules/bank-gateway` (기존) | `interface BankGateway { inquireName · transfer · inquire : CompletableFuture<…Response> }` 추가, `BankGatewayClient : BankGateway`. 예외 3종(`BankNotSentException` · `BankTimeoutException` · `BankUnavailableException`)은 그대로 여기 | `bank-protocol` |
| `modules/bank-gateway-http` (신규) | 계약 DTO · 오류 코드 · 경로 상수, 실패 분류(§4-3) 한 곳, 클라이언트 2판 `WebClientBankGateway` · `JdkHttpBankGateway` (둘 다 `BankGateway`), 설정 값 객체 | `bank-gateway`, `spring-webflux`(클라이언트만), Jackson. **Spring Boot 의존 없음** |
| `apps/bank-gateway` (신규) | Spring Boot 앱, 포트 8082. 컨트롤러 2판(§5-1), 오류 → HTTP 변환 한 곳, 송신 보류(점검 시간 — 기존 `SendGate`), `bankConnection` health, `/actuator/info` 에 판 표시. **DB · Kafka 없음** | `bank-gateway`, `bank-gateway-http`, webflux + webmvc 스타터 |
| `apps/api` | `BankGatewayClient` 를 직접 받던 곳(`BankGatewayAdapter` · `MessagingConfiguration.transferProcessor` · `ConsumerConfiguration` 코루틴 리스너 · `ReconcileConfiguration`)을 `BankGateway` 로 바꾼다. `bank.gateway.mode` 로 빈 하나를 고른다. 점검 시간 접수 503(`BankMaintenanceGuard`)은 지금처럼 api 가 판단한다 | + `bank-gateway-http` |
| `apps/api-reactive` | 같은 방식 (`CoroutineVerificationFlow` · `ReactorVerificationFlow`) | + `bank-gateway-http` |
| `apps/mock-bank` | 바뀌지 않음 | |

- 기존 `BankGatewayAdapter` 의 `thenApplyAsync(…, executor)` 는 그대로 둔다. HTTP 판도 future 를 이벤트 루프(Reactor Netty)나 JDK 클라이언트 스레드가 완료하므로, 뒤처리(DB 저장)를 그 스레드에서 돌리면 안 되는 이유가 같다.
- 전문번호는 http 모드에서 게이트웨이 앱이 매긴다. 응답 · 504 본문에 실어 두 앱 로그를 잇는다.

## 4. HTTP 계약
### 4-1. 경로 (모두 `POST`, JSON, 내부망 전용)
| 경로 | 요청 | 200 응답 |
|---|---|---|
| `/internal/v1/bank/name-inquiries` | `bankCode`, `accountNo` | 헤더(`tranNo` · `tranDateTime` · `responseCode`) + `bankCode` · `accountNo` · `holderName` |
| `/internal/v1/bank/transfers` | `bankCode`, `accountNo`, `amount`, `memo`, `reference` | 헤더 + `bankCode` · `accountNo` · `amount` · `memo` · `reference` |
| `/internal/v1/bank/inquiries` | `reference` | 헤더 + `reference` · `result` |

- 200 응답은 은행 응답 전문을 그대로 옮긴 것이다. 클라이언트는 이걸 `NameInquiryResponse` 등 기존 전문 타입으로 되살려 돌려준다 — 그 뒤의 해석(`holderNameOf` · `requireOk` · `outcomeOf` · `BankInquiryAdapter.classify`)은 한 줄도 바뀌지 않는다.
- **은행의 거절(`1001` 등)은 실패가 아니라 정상 응답이다** → 200.
- 계좌번호는 로그에 마스킹(기존 규칙). 요청 · 응답 본문 전체를 로그로 남기지 않는다.

### 4-2. 게이트웨이 안의 결과 → HTTP
| 게이트웨이 안에서 | HTTP | 본문 |
|---|---|---|
| 은행 응답 받음 | 200 | §4-1 |
| `BankNotSentException` (연결 없음, 점검 시간 송신 보류) | 503 | `{"code":"BANK_NOT_SENT","message":…}` |
| `BankTimeoutException` (보냈는데 응답 시간 초과 3초) | 504 | `{"code":"BANK_TIMEOUT","tranNo":…}` |
| `BankUnavailableException` (쓰기 실패, 보낸 뒤 끊김, 종료 중) | 502 | `{"code":"BANK_UNAVAILABLE",…}` |
| 요청 형식 오류 (검증) | 400 | `{"code":"INVALID_REQUEST",…}` |
| 그 밖 | 500 | `{"code":"INTERNAL",…}` |

### 4-3. 클라이언트의 해석 (HTTP → 기존 예외) — 두 클라이언트 판이 같은 분류 함수를 쓴다
| HTTP 구간에서 | 예외 | 의미 |
|---|---|---|
| 200 | (응답) | |
| 503 + `BANK_NOT_SENT` | `BankNotSentException` | 안 보냄 → 출금은 재시도 토픽, 결과조회는 그 주기 중단 |
| **연결 실패** (거부 · 연결 시간 초과 · 이름 풀이 실패) | `BankNotSentException` | 게이트웨이가 요청을 받지 못했다 |
| **연결 풀에서 연결을 얻지 못함** (WebClient `pendingAcquireTimeout`) | `BankNotSentException` | 요청을 쓰기 전이다 |
| 504 + `BANK_TIMEOUT` | `BankTimeoutException` | 모름 → 결과조회 |
| 502 · 500 · 503(코드 없음) · 그 밖의 상태 | `BankUnavailableException` | 모름 |
| **응답 읽기 시간 초과** | `BankUnavailableException` | 모름 — 요청은 이미 갔을 수 있다 |
| **응답 전에 연결이 끊김** (`PrematureCloseException`, `IOException` 등) | `BankUnavailableException` | 모름 |
| 응답 본문을 읽지 못함 (JSON 오류) | `BankUnavailableException` | 모름 |
| 위 어디에도 안 맞는 예외 | `BankUnavailableException` | 모름 (기존 원칙: 다시 보내는 쪽이 더 위험) |

- 연결 단계 예외 이름은 판마다 다르다 (WebClient: `ConnectException` · `ConnectTimeoutException` · `PoolAcquireTimeoutException` / JDK: `ConnectException` · `HttpConnectTimeoutException`). 각 판은 "연결 단계에서 실패했는가" 만 판별해 공통 분류에 넘긴다. 정확한 예외 이름은 구현 때 라이브러리 버전으로 확인하고 계약 테스트로 고정한다.
- **503 이라도 `BANK_NOT_SENT` 코드가 있을 때만** 안 보냄이다. 프록시 · 로드밸런서가 만든 503 은 요청이 게이트웨이에 닿았는지 모르므로 모름으로 둔다 (P8-3 에서 앞단이 생길 때 중요).

### 4-4. 시간과 재시도
| 순서 | 값 | 설정 |
|---|---|---|
| 연결 시간 초과 | 1s | `bank.gateway.http.connect-timeout` |
| 연결 풀 대기 (WebClient) | 1s — 기본 45s 면 컨슈머가 묶인다 | `bank.gateway.http.pending-acquire-timeout` |
| 게이트웨이의 은행 응답 시간 초과 | 3s (기존) | 게이트웨이 앱 `bank.gateway.response-timeout` |
| **HTTP 응답 시간 초과** | **5s = 3s + 여유 2s** — api 가 먼저 끊으면 게이트웨이는 답을 받았는데 api 는 "모름" 이 되는 건이 쓸데없이 는다 | `bank.gateway.http.response-timeout` |

- **HTTP 클라이언트 자동 재시도는 끈다** (WebClient: Reactor Netty `disableRetry(true)`. JDK: POST 를 다시 보내지 않는 기본값을 테스트로 고정). 재시도는 지금처럼 재시도 토픽 · 정리 작업만 한다.
- **api 가 끊어도 게이트웨이는 은행 송신을 취소하지 않는다.** 보낸 전문은 되돌릴 수 없다. 늦게 온 응답은 버려지고(대기 표 항목은 은행 응답 시간 초과 3초 뒤 지워짐 — 기존 동작), api 쪽 건은 모름 → 결과조회로 확정된다. P6-2 의 "끊기면 취소" 는 1원인증 앱이 *다음 단계로 진행하지 않는* 것이지 보낸 것을 되돌리는 게 아니다.

## 5. 비교 판
### 5-1. 서버 — 게이트웨이 앱 하나, 기동 설정으로 고름
`spring.main.web-application-type: reactive | servlet`. 두 스타터를 다 넣고 판별 컨트롤러는 `@ConditionalOnWebApplication(type = REACTIVE | SERVLET)`. 오류 → HTTP 변환(§4-2)과 송신 보류는 공통 코드 하나.

| 판 | 컨트롤러 반환 | 요청이 지나가는 스레드 | 취소 |
|---|---|---|---|
| **WebFlux** (기본) | `Mono.fromFuture(future, true)` | Reactor Netty 이벤트 루프가 받음 → 은행 Netty 이벤트 루프가 future 완료 → 이벤트 루프가 응답을 씀. 요청당 스레드 없음 | 연결이 끊기면 구독 취소. `suppressCancel = true` 라 은행 future 는 취소되지 않는다 |
| **MVC 비동기** | `CompletableFuture<ResponseEntity<…>>` | Tomcat 스레드가 받고 future 를 돌려준 뒤 반납 → 은행 응답에 future 완료 → `AsyncContext` 재디스패치 → **Tomcat 스레드가 다시 잡혀** 응답을 씀 | 비동기 요청 시간 초과 · 끊김이 은행 future 를 취소하지 않는지 테스트로 고정 (`spring.mvc.async.request-timeout` 은 응답 시간 초과보다 길게) |

- 면접 포인트: 둘 다 "기다리는 동안 스레드를 잡지 않는다" 는 같다. 다른 점은 받기 · 쓰기에 Tomcat 스레드가 두 번 드는 것, 연결 수 한계(Tomcat `max-connections`), 취소 전달 방식, 스레드 덤프의 모양.

### 5-2. 클라이언트 — `bank.gateway.http.client: webclient | jdk`
| 판 | 비동기 결과 | 응답 처리 스레드 | 연결 풀 | POST 자동 재시도 |
|---|---|---|---|---|
| **WebClient** (기본) | `Mono` → `toFuture()` | Reactor Netty 이벤트 루프 | 최대 연결 · 대기 시간 조절 가능. **기본 최대 연결은 `max(코어, 8) × 2` (이 PC 16코어면 32, 일반 지식 — 구현 때 버전 확인)** → 명시적으로 500 | 연결 재사용 중 끊기면 다시 보낼 수 있다 → `disableRetry(true)` |
| **JDK `HttpClient`** | `sendAsync` → `CompletableFuture` 그대로 | 셀렉터 스레드 + 실행기 (**기본 실행기 그대로** — 늘어나는 캐시 풀. 이것 자체가 비교 대상이다) | 개수 제한 없음, 조절 불가 | 기본으로 하지 않는다 (테스트로 고정) |

- HTTP/1.1 keep-alive 로 고정한다 (JDK 기본은 HTTP/2 시도 → `HTTP_1_1` 지정). HTTP/2 다중화는 이 규모에서 얻는 게 적고 두 판의 비교 조건을 다르게 만든다.
- 말로만 비교: `RestClient`(블로킹) + 가상 스레드 — 코드 발췌와 흐름만 perf 문서 · 가이드에 둔다.

## 6. 설정 (기본값)
| 앱 | 키 | 기본 | 비고 |
|---|---|---|---|
| api · api-reactive | `bank.gateway.mode` | `direct` | `http` 면 아래 `bank.gateway.http.*` 사용, Netty 클라이언트 빈은 만들지 않는다 |
| | `bank.gateway.http.base-url` | `http://localhost:8082` | |
| | `bank.gateway.http.client` | `webclient` | `jdk` |
| | `bank.gateway.http.max-connections` | 500 | WebClient 만 |
| | `bank.gateway.http.connect-timeout` · `pending-acquire-timeout` · `response-timeout` | 1s · 1s · 5s | §4-4 |
| bank-gateway | `server.port` | 8082 | |
| | `spring.main.web-application-type` | `reactive` | `servlet` |
| | `bank.gateway.host` · `port` · `response-timeout` · `poll-interval` | api 와 같은 값 (localhost · 9090 · 3s · 30s) | |
| | `bank.maintenance.*` | api 와 같은 값 (23:50 ~ 00:10) | 송신 보류. api 의 접수 503 과 같은 창이어야 한다 (두 곳 설정 — 위험 §10) |

- 시작 검사: `http` 모드에서 `response-timeout` 이 게이트웨이 은행 응답 시간 초과(3s)보다 짧으면 안 된다 — api 는 게이트웨이 값을 모르므로 **5s 미만이면 기동 경고**만 남긴다 (실패로 막지 않는다 — 측정 · 테스트에서 일부러 줄일 수 있게).

## 7. 테스트
| 단계 | 어디 | 내용 |
|---|---|---|
| 단위 | `modules/bank-gateway-http` | §4-3 분류표 전 줄 (판별 예외 → 공통 분류), DTO ↔ 전문 타입 왕복 |
| **계약 테스트** — 서버 2 × 클라이언트 2 = 4조합 매개변수화 | `apps/bank-gateway` (앱을 임의 포트로, 모의 은행 픽스처) | ① `0000` 200 ② 거절 `1001` 200 ③ 은행 연결 없음 503 → `BankNotSentException` ④ 점검 송신 보류 503 → `BankNotSentException` ⑤ 은행 무응답 504 → `BankTimeoutException` ⑥ 게이트웨이 꺼짐(연결 거부) → `BankNotSentException` ⑦ **받고 응답 전에 끊는 가짜 서버** → 모름 ⑧ 연결 풀 고갈(WebClient 풀 1 + 느린 요청) → `BankNotSentException` ⑨ 읽기 시간 초과(응답을 늦추는 가짜 서버) → 모름 ⑩ 클라이언트가 끊어도 은행 송신은 취소되지 않음 (모의 은행 거래 기록에 이체 1건, 대기 표 3초 뒤 비움) ⑪ 자동 재시도 없음 (가짜 서버가 받은 요청 수 = 1) |
| 망 끊김 (Toxiproxy) | `apps/bank-gateway` (게이트웨이 앱을 띄워야 하므로) | Testcontainers Toxiproxy 모듈로 api → 게이트웨이 사이 `reset_peer` · `timeout` toxic → 모름 / 지연 toxic → 응답 시간 초과 경계 |
| 흐름 (`mode=http`) | `apps/api` (테스트 지원 클래스가 게이트웨이 앱을 같은 JVM 에 임의 포트로 띄움) | 출금 `COMPLETED` · 1원인증 `SENT → VERIFIED` / **게이트웨이 내린 채 출금 → `NotSent` → 재시도 토픽 → 게이트웨이 올리면 `COMPLETED`** / **게이트웨이가 받고 끊음 → `UNKNOWN` → 정리 작업 결과조회(HTTP) → 확정, 이체 전문 1번** / 코루틴 컨슈머 모드 1건 |
| 흐름 (`mode=http`) | `apps/api-reactive` | 1원인증 1건 |
| 기존 | 전 모듈 | 기본 `direct` 로 기존 524개 그대로 통과 |

- 흐름 테스트에서 "받고 끊음" 은 게이트웨이 앞에 끊는 프록시(가짜 서버 또는 Toxiproxy)를 두어 만든다 — 게이트웨이 코드에 테스트용 분기를 넣지 않는다.
- P7 교훈: 별도 컨텍스트를 띄우는 테스트는 `@DirtiesContext` 비용 · 닫히지 않은 리스너를 확인한다 (CI 4코어).

## 8. 측정
### 8-1. 묶음 (회차마다 기존 정확성 확인 통과 필수 — 은행 거래 기록 수 = 처리 행 수, 이중 송신 0)
| 묶음 | 도구 · 조건 | 비교 | 회차 | 보려는 것 |
|---|---|---|---|---|
| A 분리 비용 | `load/consumer` 블로킹 d100 · 코루틴 d100 상한 64 | direct vs http(기본 조합) | 2 × 2 × 3 = 12 | 은행 지연이 지배하는 조건에서 차이가 오차 안인가 |
| B 순수 오버헤드 | `load/consumer` 코루틴 d0 상한 64 | direct vs http | 2 × 3 = 6 | HTTP 구간 몫 (처리량 · p50 · p95) |
| B' 망 지연 | 같은 조건, Toxiproxy 지연 1ms · 5ms | http + 지연 | 2 × 3 = 6 | 다른 PC 를 부르는 상황에 가까운 비용 |
| C 판 비교 | `load/consumer` 코루틴 d10 상한 128 | 서버 2 × 클라이언트 2 | 4 × 2 = 8 | 처리량 · 지연 · **게이트웨이 · api 스레드 수 · CPU** |
| D 연결 풀 천장 | `load/consumer` 코루틴 d100 상한 128 | WebClient 풀 32 vs 500 | 2 × 2 = 4 | 기본 풀이 새 천장이 되는 모습 |
| E 웹 1원인증 | `load/verification` async 판 d500 · 400건/s | direct vs http | 2 × 3 = 6 | 사용자가 기다리는 경로의 p95 증가 |
| | | | **42** | |

### 8-2. 도구 변경
- `load/consumer/run.sh` · `load/verification/run.sh`: 환경 변수 `GATEWAY=direct|http`, `SERVER=reactive|servlet`, `CLIENT=webclient|jdk`, `GW_POOL`(WebClient 최대 연결), `NET_DELAY`(Toxiproxy 지연 ms). http 면 스크립트가 측정용 게이트웨이(포트 **18082**, 측정용 모의 은행 9190 에 연결)를 직접 띄우고 내린다. 원자료 이름에 `gw-<판>` 을 붙이고 `RAW_SUB=p8-1`.
- Toxiproxy: 프록시 **28082 → `host.docker.internal:18082`**, 제어 API 8474. 스크립트가 회차 시작 때 toxic 을 걸고 끝나면 지운다 (남은 toxic 확인).
- 공통 라이브러리(`load/lib/`)에 게이트웨이 기동 · 대기 · 종료, 프로세스 표본(`proc.sh` 재사용 — 게이트웨이 PID 도), 스레드 덤프 1회(`jcmd Thread.dump_to_file`)를 둔다.
- `summarize.sh`: 묶음별 표 (중앙값 · 범위), 게이트웨이 CPU · 스레드 수 열.
- 개발용 포트(8080 · 8082 · 9090)와 겹치지 않게 한다 — 측정 중에도 개발용 앱을 켜 둘 수 있는 P6-2 방식 유지 (컨슈머 측정은 기존처럼 8080 을 비워야 함).

### 8-3. 예상 (추정 — 결과 문서 "예상과 비교" 에 쓴다)
- A: 차이가 회차 간 흔들림 안. 처리량 = 동시성 ÷ (은행 지연 + α) 에서 α 가 수 ms 늘 뿐.
- B: 한 건당 수 ms 증가 (루프백). B': 지연을 넣은 만큼 p50 이 늘고, 동시성이 충분하면 처리량은 거의 그대로 (리틀의 법칙).
- C: 처리량은 비슷하고, 스레드 모양만 다르다 — MVC 판은 Tomcat 스레드, JDK 판은 실행기 스레드가 보인다.
- D: 풀 32 에서 ≈ 32 ÷ (0.1s + α) ≈ 260건/s 근처 천장, 500 이면 상한 128 이 한계.
- E: p95 가 수 ms 늘고 처리량은 400 그대로.

### 8-4. 판단 기준 (미리 정함, ADR-0015)
- 기본 판(WebFlux · WebClient)을 바꾸는 조건: 다른 판이 C 에서 처리량 10% 이상 높고 두 회차 모두 같은 방향일 때만. 아니면 유지.
- `mode` 기본은 P8-1 에서 `direct` 유지 — 측정 결과와 상관없이 P8-2 에서 `http` 로 바꾼다 (분리의 목적이 성능이 아니라 확장 · 연결 소유이므로). A · E 에서 차이가 예상보다 크면 원인을 찾아 문서에 남긴다.

## 9. 손 확인 (머지 전, 개발 DB · 개발용 모의 은행)
1. 모의 은행 9090 → 게이트웨이 8082 → api 8080(`--bank.gateway.mode=http`) 순으로 띄운다. 실행 구성(`.run`)에 게이트웨이를 추가한다.
2. 게이트웨이 `/actuator/health` 의 `bankConnection` `UP`, `/actuator/info` 의 판 표시. 모의 은행을 내리면 `DOWN`, 올리면 `UP`.
3. Postman 으로 1원인증 `SENT → VERIFIED`, 출금 `COMPLETED 0000` (`user-p41`).
4. **게이트웨이를 내린 채 출금 접수 → 재시도 토픽 경로 → 게이트웨이를 올리면 `COMPLETED`** (`sending_at` · 재시도 시각으로 경로 확인).
5. 게이트웨이 스레드 덤프 (WebFlux 판 · MVC 판 각 1회).

## 10. 위험과 한계
- **점검 시간 설정이 api 와 게이트웨이 두 곳**이다. 어긋나면 api 는 접수하고 게이트웨이는 보류(안 보냄 → 재시도)하거나 그 반대가 된다 — 돈은 안전하지만(보류는 안 보냄) 동작이 헷갈린다. P8-2 Compose · P8-3 ConfigMap 에서 한 곳으로 모은다. 지금은 문서 · 기본값을 같게 둔다.
- 측정은 한 PC 다. 루프백 · Toxiproxy 지연은 실제 망의 패킷 손실 · 대역폭을 흉내 내지 않는다.
- 게이트웨이 1대는 단일 장애점이다. 내려가면 출금은 재시도 토픽 → DLT → 재발행 한도(약 26분) 뒤 `FAILED E001`, 1원인증은 503 류 오류. 이중화는 P8-3.
- 서비스 간 인증이 없다 (내부망 전제). 게이트웨이 포트가 외부에 열리면 누구나 이체 전문을 보낼 수 있다 → P8-3 NetworkPolicy, 문서에 경고.
- 서버 두 스타터를 한 앱에 넣는 방식은 Boot 가 지원하지만(설정으로 타입 지정), 자동 설정이 섞여 예상 밖 빈이 생길 수 있다 (추정 — 계약 테스트가 두 판 모두 뜨는 것을 확인).

## 11. 범위 밖
게이트웨이 이중화 · 여러 대 (P8-3), 앱 컨테이너 이미지 · Compose 전체 실행 (P8-2), Kafka 3대 브로커 장애 실험 (P8-2 후보), 서비스 간 인증 · mTLS · NetworkPolicy (P8-3), 분산 추적 (P9), 일마감 대사 파일 읽기를 게이트웨이로 옮기기, Kafka 명령/결과 방식 구현, `RestClient` + 가상 스레드 판 구현, OpenAPI 계약, 스케줄 중복 실행 (P8-3), 게이트웨이 쪽 동시 송신 상한.

## 12. 결정 기록 · 문서 · 작업 순서
- 문서: ADR-0015(결정 · 버린 대안 · 측정 판단), `docs/perf/p8-1-gateway-split.md`(묶음 A ~ E · B', 판별 스레드 덤프, 흐름 그림), ARCHITECTURE(구조도 · 모듈 표 · 의존 방향), README(로드맵 P8-1 줄 · 기술 스택), SETUP · OPERATION(게이트웨이 실행 · 장애 때 보는 법 · 측정 §), `infra/README.md`(`chaos` 프로필), HANDOVER · CHANGELOG, 실행 구성 `.run`.
- 머지 뒤: job-prep 면접 가이드 `09-p8-1-gateway-split.md` (판별 코드 비교 · 흐름 · 질문) · `07` Docker 문서 보강 (Toxiproxy · 프로필), velog #14 (비공개).
- 작업 순서 (1 ~ 4 가 끝나면 분리 자체는 완성):
  1. `BankGateway` 인터페이스, 호출부 교체 (`direct`, 동작 그대로)
  2. `modules/bank-gateway-http`: 계약 DTO · 분류 · WebClient 클라이언트 · 단위 테스트
  3. `apps/bank-gateway`: WebFlux 판 · 오류 변환 · 송신 보류 · health · 계약 테스트
  4. api · api-reactive `mode=http`, 흐름 테스트
  5. MVC 판 · JDK 클라이언트, 계약 테스트 4조합, 취소되지 않음 · 재시도 없음 고정
  6. Toxiproxy (Compose `chaos` 프로필, Testcontainers 끊김 테스트)
  7. 측정 도구 확장 · 측정 42회 · perf 문서
  8. 문서 · 실행 구성
