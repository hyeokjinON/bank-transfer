# ADR-0016: 앱 이미지 · Compose 로 전체 실행 · api 2대 — Dockerfile 하나, nginx 앞단, 하루 배치만 ShedLock, `mode` 기본 `http`

- 상태: 제안 (2026-11-11) — 머지 뒤 정리 PR 에서 `채택`
- 관련: [설계](../superpowers/specs/2026-11-11-p8-2-compose-design.md) · [계획](../superpowers/plans/2026-11-11-p8-2-compose.md) · [ADR-0015](0015-bank-gateway-service.md) (게이트웨이 분리 — 기본 `http` 는 여기서) · [ADR-0004](0004-idempotency-key-and-transactional-outbox.md) (멱등키) · [ADR-0005](0005-kafka-delivery-and-consumer-idempotency.md) (아웃박스 · 컨슈머 멱등) · [ADR-0010](0010-daily-reconciliation.md) (일마감 대사) · [ADR-0011](0011-data-retention.md) (보존 배치) · [측정 결과](../perf/p8-2-compose.md)

## 맥락
- P8-1 이 "api 를 늘려도 은행 연결은 한 곳" 을 만들었다. 실제로 여러 대로 띄우면 **인스턴스 안에 있던 상태**가 드러난다. 하나씩 따져 보면 대부분은 이미 DB 가 지켜 주고, 하나만 새로 막아야 한다 (설계 §2).

| 인스턴스 안에 있던 것 | 2대가 되면 | 처리 |
|---|---|---|
| 은행 연결 · 응답 매칭 표 | 응답이 다른 인스턴스로 | **이미 안전** — 게이트웨이 분리 (P8-1). 이번에 `mode` 기본을 `http` 로 |
| 하루 배치 (일마감 대사 · 보존 삭제) | 두 번 돈다 | **이번에 처리** — ShedLock |
| 아웃박스 발행 (0.5초) · 정리 작업 (10초) | 같은 행 두 번 처리 | **이미 안전** — `FOR UPDATE SKIP LOCKED` · 조건부 UPDATE (ADR-0005 · 0006 · 0007). 락을 걸지 않는다 (걸면 한 대만 일한다) |
| 중복 요청 검사 | 다른 인스턴스에선 통과 | **이미 안전** — DB UNIQUE 멱등키 (ADR-0004) |
| 잔고 동시 차감 | 로컬 락은 서로 안 보임 | **이미 안전** — 조건부 UPDATE · DB 락 (ADR-0008 · 0009) |
| 대사 파일 디렉터리 | 인스턴스마다 다름 | **이번에 처리** — 공유 볼륨 `bank-files` (운영이면 SFTP · 오브젝트 스토리지) |
| 세션 · 로컬 캐시 | — | **이미 안전** — 없음 (무상태, 헤더 `X-User-Id`) |
| Kafka 컨슈머 | 그룹 안 컨슈머 6 · 파티션 3 → 3개는 논다 | **드러내기만** — 그대로 두고 보인다 |

- 이 단계의 코드 변경은 설정 · 하루 배치 락 · 배포물뿐이다. 돈 불변식(접수 때 차감, 잔고 = 원장 합, 같은 거래번호 은행 송신 1번, 안 보냄만 다시 보내고 모르면 묻는다)은 건드리지 않는다.
- **처리량을 늘리려는 단계가 아니다.** 한 PC 의 2대는 같은 CPU 를 나눠 쓴다. 가용성 · 정합성이 지켜지는지와 이미지 비용을 본다.

## 결정
1. **루트 `Dockerfile` 하나 · 타깃 3개(`api` · `bank-gateway` · `mock-bank`) · 멀티 스테이지.** 세 앱이 같은 Gradle 멀티 모듈을 쓰므로 빌드 단계를 한 번만 돌리고 BuildKit 이 타깃들에 재사용한다. 줄마다 이유:
   - **멀티 스테이지** — JDK · Gradle · 소스는 빌드 단계에만 있고 최종 이미지에는 없다.
   - **덜 바뀌는 것 먼저 `COPY`** (`gradlew` · 빌드 스크립트 → `gradle/` → 소스) — 래퍼 · 스크립트가 그대로면 Gradle 배포판 내려받는 층을 다시 쓴다.
   - **`sh ./gradlew`** — Windows 에서 보낸 빌드 컨텍스트의 실행 비트에 기대지 않는다.
   - **캐시 마운트 `--mount=type=cache,target=/root/.gradle/caches`** — Gradle 의존성 캐시가 이미지 층에 들어가지 않는다. `/root/.gradle` 전체가 아니라 **`caches` 만** 마운트한다 (앞 층에서 받은 래퍼 배포판 `/root/.gradle/wrapper` 를 가리지 않게 — 이 계획에서 바꾼 점).
   - **`-x test`** — 테스트는 CI `build` 잡이 돌린다. Kotlin 컴파일은 `in-process` (컴파일 데몬을 따로 띄우지 않는다).
   - **Boot jar 레이어 추출** (`-Djarmode=tools extract --layers --launcher`) — `dependencies` · `spring-boot-loader` · `snapshot-dependencies` · `application` 4층. 코드 한 줄을 고치면 맨 위 `application` 층만 다시 만든다.
   - **`eclipse-temurin:21-jre-alpine`** — JRE 만 있어 작고, 헬스체크용 `wget` · `nc` 가 busybox 에 있다. **`USER app`** (root 아님) · `/data/bank-files` 를 `app` 소유로 미리 만든다 (처음 붙는 이름 있는 볼륨이 소유자를 물려받는다).
   - **`ENV TZ=Asia/Seoul`** — 로그 · Spring Batch 메타 시각 · 모의 은행 파일 날짜가 KST.
   - **exec 형식 `ENTRYPOINT`** — PID 1 = java 라 `docker stop` 의 SIGTERM 이 java 에 직접 가 graceful shutdown 이 된다.
   - **`-XX:MaxRAMPercentage=75`** — 힙은 컨테이너 메모리 한도의 75%, 나머지는 스택 · 메타스페이스 · 네이티브. **`-XX:+ExitOnOutOfMemoryError`** — 반쯤 죽은 채 버티지 않고 끝내서 `restart` 가 새로 띄우게 한다.
   - **`.dockerignore`** — `infra/.env` 를 뺀다 (비밀은 실행 때 Compose 가 환경변수로 넣는다. 이미지에 굳지 않는다, CI `app-smoke` 가 확인). `infra/` · `.github/` · `**/*.md` · `gradlew.bat` 도 뺀다 — 앱 코드가 아닌 파일이 바뀌어도 `COPY . .` 층(→ Gradle 단계)이 무효화되지 않게 (E1 에서 찾아 고쳤다).
2. **PC 에 여는 포트는 nginx `8000` 하나, `127.0.0.1` 에만** (인증 없는 `/admin/*` 가 LAN 에 열리지 않게). 게이트웨이 · 모의 은행 · api 는 내부망 전용이다 (ADR-0015 위험 "게이트웨이 포트가 열리면 누구나 전문을 보낸다" 의 첫 조치). IntelliJ(8080 · 8082 · 9090) · 측정 포트와 안 겹친다. 디버깅은 `docker compose exec` · `logs`.
3. **nginx 는 라운드 로빈, 재시도는 연결 단계 실패만** (`proxy_next_upstream error timeout`, `non_idempotent` 없음). 이미 보낸 POST 를 다른 서버로 다시 보내지 않는다 — 출금은 멱등키로도 막히지만 **1원인증 요청에는 키가 없어** 다시 보내면 두 번 처리될 수 있다. P8-1 의 "연결 단계 실패만 안 보냄" 과 같은 규칙이다. `max_fails=3 fail_timeout=10s`, 연결 1초, 읽기 15초 (1원인증은 api 가 게이트웨이를 차례로 2번 부른다 — 성명조회 → 1원 이체, 최악 5s + 5s. 안쪽이 먼저 끝나야 nginx 가 504 를 주는 동안 api 가 1원을 보내는 일이 없다). `upstream api` 에 `zone api 64k;` 를 둔다 (없으면 워커 18개가 각자 라운드 로빈을 세어 처음 14건이 api-1 로 쏠렸다 — Task 5 에서 발견).
4. **`bank.gateway.mode` 기본은 `http`** (api · api-reactive). ADR-0015 가 미뤄 둔 것을 했다. 운영 모양이 기본이 된다 — 기본이 `direct` 일 때 게이트웨이를 안 거치는 줄 모르고 확인한 일이 있었다 (#25). 테스트 공통 기반 · 측정 스크립트 · 측정용 실행 구성은 `direct` 를 명시한다. IntelliJ 는 모의 은행 → 게이트웨이 → api 순서가 평소가 된다.
5. **ShedLock — 하루 배치 2개에만** (`daily-recon` 50분 · `data-retention` 30분, `lockAtLeastFor` 1분).
   - JDBC 제공자 + **`usingDbTime()`**: 서버 시계 차이와 무관하게 DB 시각으로 잡는다. 락 행은 `shedlock` 표 (V6), `locked_by` 는 `info.instance` (api-1 · api-2, IntelliJ 는 `local`).
   - 스케줄러 방식은 `PROXY_METHOD`(기본)라 `@SchedulerLock` 메서드를 가진 빈을 CGLIB 프록시로 감싼다 → **스케줄러 클래스와 `run()` 을 `open` 으로** 바꿨다 (Kotlin 기본 final).
   - **락은 1차, 멱등은 2차.** 락이 풀리거나 시계가 어긋나 두 대가 돌아도 돈이 틀어지지 않는다: 대사는 Spring Batch 잡 인스턴스(같은 날짜 · 동시 실행 하나)와 이슈 `uk_recon_issue (날짜, 종류, 키)` UNIQUE 가, 보존 삭제는 다시 지워도 같은 결과(조건 삭제)가 막는다.
   - 아웃박스 발행 · 정리 작업에는 걸지 않는다 (위 표).
   - `lockAtMostFor` 는 실행 시간보다 넉넉하게, 다음 회차 간격(60분)보다 짧게 — 개발 DB 에서 대사 평균 1.6초 · 최대 2.2초, 보존 평균 0.2초 ([측정 §8-2](../perf/p8-2-compose.md)). 운영 규모에서는 다시 잰다.
6. **Kafka 파티션 3 · api 당 `concurrency: 3` 유지.** 2대면 컨슈머 6 중 3은 논다 — 일부러 그대로 두고 `kafka-consumer-groups --describe` 로 보인다 (E2). "파티션 = 병렬도 상한", "노는 컨슈머 = 장애 대기 인력" 을 실험으로 보이는 것이 목적이고, 파티션 수는 병목을 잰 뒤 정하는 용량 결정이다 (한 PC 에서는 늘려도 이득을 증명 못 한다). 늘리는 기준은 lag 가 계속 늘고 컨슈머 CPU 에 여유가 있을 때 ([측정 §10](../perf/p8-2-compose.md)).
7. **graceful shutdown: 안쪽이 바깥보다 짧게.** api `server.shutdown=graceful`, `spring.lifecycle.timeout-per-shutdown-phase: 20s`, Compose `stop_grace_period: 30s` (bank-gateway 도 같은 규칙: `server.shutdown=graceful` · 20s). 배포 · 재시작 때 처리 중인 요청 · 메시지를 끝내고 내려간다. 주의: **20s 는 종료 단계(웹 서버 · Kafka 리스너 …) 하나당 상한**이다. "20s < 30s" 는 한 단계씩 봤을 때 성립하고, 단계들이 모두 상한까지 쓰는 최악에는 합이 30s 를 넘을 수 있다 (그때는 Docker 가 SIGKILL — 처리 중이던 건은 위 표의 안전장치로 이어진다). 대가: 종료가 최대 30초.
8. **점검 설정은 Compose 공통 블록(`x-bank-maintenance`) 하나에서 api · 게이트웨이에 같은 값**을 준다. 컨테이너 실행에서는 두 곳 설정이 어긋날 수 없다. 공통 블록 구성: `x-java-app`(restart · stop_grace_period) · `x-bank-maintenance`(점검 3변수) · `x-api-env`(api 환경변수) · `x-api`(api 두 대). `MYSQL_PORT=3306` 은 명시한다 — `infra/.env` 의 `MYSQL_PORT` 는 PC 쪽 포트라 그대로 넘기면 컨테이너 안에서 틀린 포트로 붙는다.

## 결과
측정 (2026-11-11, 이 PC 기준 — 운영 수치가 아니다, [측정 결과](../perf/p8-2-compose.md)):
- **한 줄 결론: 처리량은 재지 않았다 — 한 PC 의 2대는 같은 CPU 를 나눠 쓴다.** 늘었다고 주장하지 않는다. 확인한 것은 가용성 · 정합성 · 이미지 비용이다.
- **E1 (이미지)**: api 512MB · bank-gateway 381MB · mock-bank 297MB (추정 ~300MB 보다 큼). 코드 한 줄 수정 빌드 26.5s, 새로 만든 층은 `application` **1.67MB** 뿐 (`dependencies` 116MB 층 그대로). 기동 → healthy 30.3s. 측정 중 `.dockerignore` 누락을 찾아 고쳤다 (아무것도 안 바꾼 빌드 27.3s → 3.9s).
- **E2 (분배)**: `/actuator/info` 100번 → api-1 50 · api-2 50. Kafka 컨슈머 6 중 파티션을 받은 것 3, **노는 컨슈머 3**, lag 0.
- **E3 (롤링 재시작, 2회)**: 첫 시도 **5xx 0**, 접수 키 = 출금 행, 은행 누락 · 중복 0. 파티션은 0.96s · 3.2s 만에 api-2 로 넘어갔다 (api-1 이 정상 탈퇴).
- **E4 (강제 종료, 3회)**: 은행 누락 · 중복 0, 환불 0, 접수 키 = 출금 행. 그러나 **처리 지연 45s** — SIGKILL 은 탈퇴 신호가 없어 브로커가 `session.timeout.ms` 기본 45s 를 기다린 뒤에야 api-2 가 45.6s · 45.6s 만에 파티션을 이어받았다. kill-3 에서 1201건 중 5s 넘음 268건(22%), 30s 넘음 119건, 최대 45.04s. 잃은 것은 없다 — 늦을 뿐이다.
  - **502 1건** (kill-3): SIGKILL 순간 api-1 이 처리 중이던 POST. nginx 는 다시 보내지 않았고, 클라이언트가 1초 뒤 **같은 멱등키**로 다시 보내 한 번만 접수됐다.
  - `docker kill` 한 컨테이너를 Docker 는 다시 띄우지 않았다 (`restart: unless-stopped` 는 kill 을 사람이 멈춘 것으로 본다. OOM · 크래시는 다시 띄운다).
- **정합성 이중 0**: 6회차 모두 은행 누락 0 · 같은 거래번호 두 번 0, `COMPLETED` 아님 0 · 환불 0 · 음수 잔고 0 · 잔고 ≠ 원장 0.
- **E5 (ShedLock)**: 2분마다 cron 으로 약 10분, 6회차 모두 **한 컨테이너만** 시도 (api-2 5회 · api-1 1회), 같은 분에 두 줄 없음. 실제 앱의 `@Bean` 스케줄러가 프록시로 감싸졌음을 컨테이너 2대에서 확인.

## 위험 · 한계
- **컨테이너 api 와 IntelliJ API 를 동시에 띄우지 않는다.** 같은 MySQL · 같은 Kafka 그룹이라 출금이 다른 모의 은행으로 흘러 대사가 어긋난다. 컨테이너 api 도 06:30 보존 배치를 **개발 DB** 에 돌린다 (HANDOVER 경고 그대로 적용).
- **게이트웨이 1대 = 단일 장애점.** 이중화는 P8-3. 게이트웨이가 죽는 경우는 이번에 실험하지 않았다.
- **nginx 는 기동 때 upstream 이름을 한 번만 푼다.** api 컨테이너를 다시 만들면(설정을 바꿔 `up -d`) nginx 도 `restart` 해야 한다 (OPERATION §14-3).
- **강제 종료 시 최대 45초 처리 지연** (파티션 1/3 분량). 손잡이는 `session.timeout.ms` · heartbeat 이고 이번에는 바꾸지 않았다 — 줄이면 GC · 느린 처리에도 불필요한 리밸런스가 늘어난다.
- **점검 설정은 IntelliJ 실행에서는 여전히 두 `application.yml`** (같은 값을 직접 맞춘다). 컨테이너 실행만 공통 블록 하나다.
- **`shedlock` 시각은 UTC** (타입은 `DATETIME(3)`, 값만 UTC — KST 정책의 예외, [database.md §4-10](../database.md)).
- 한 PC · 같은 CPU, 표본이 작다 (재시작 2회 · 강제 종료 3회 · E5 6회). 502 가 3회 중 1회 나온 것은 타이밍이다. 배치 실행 시간은 개발 DB 규모다. 이미지 처음 빌드 (캐시 없는 CI 러너) 시간은 PR 뒤 CI 로 채운다.

## 검토한 대안
- **앱마다 Dockerfile**: 빌드가 3번 돈다. 하나로 두면 빌드 단계를 BuildKit 이 재사용한다.
- **`bootBuildImage` (Buildpacks)**: 설명할 게 적고 느리다. 줄마다 이유를 설명하고 싶은 목적과 맞지 않는다.
- **Jib**: 플러그인 의존, mock-bank(Boot 앱이 아님)는 별도 처리가 필요하다.
- **`--scale api=2`**: nginx 가 기동 때 이름을 한 번만 풀고 인스턴스 구분이 흐려진다. `api-1` · `api-2` 로 나눠 로그 · 장애 실험 · 확인이 인스턴스별로 또렷하다 (YAML anchor 로 중복을 줄인다).
- **api 포트를 PC 에 노출**: 내부망 전용 원칙(결정 2)에 어긋난다.
- **nginx 재시도 허용 · `least_conn`**: 보낸 POST 를 다시 보내면 1원인증 이중 처리 위험이 있다 (결정 3). 라운드 로빈이면 충분하다.
- **설정으로 배치를 한 대만 돌리기**: 그 한 대가 죽으면 배치가 안 돈다.
- **락 없이 멱등만**: 안전은 하지만 두 대가 같은 일을 하고 결과가 불확실하다. 락이 1차, 멱등이 2차다 (결정 5).
- **6파티션**: 파티션은 줄일 수 없고 측정 기준선이 깨진다. 한 PC 에서는 이득도 증명 못 한다.
- **컨슈머 축소 (api 당 1~2개)**: 한 대가 죽으면 느려진다. 노는 컨슈머가 장애 대기 인력이다.
- **게이트웨이에 점검 시간을 묻기 · 설정 서버**: 호출이 늘거나 구성요소가 늘어난다. 지금은 Compose 공통 블록으로 충분하다 (코드 수준에서 한 곳으로 모으는 것은 범위 밖).
- **distroless**: 헬스체크용 `wget` · `nc` 가 없다. JRE Alpine 으로 충분하다 (대가: musl — JVM 은 문제없고 네이티브 라이브러리를 쓰면 확인 필요).

## 다음 (P8-3)
- Deployment · Service · Ingress.
- readiness 에 게이트웨이 `bankConnection` 을 넣는다. **liveness 에는 넣지 않는다** (은행 장애 때 재시작 루프 — HANDOVER P8-1 메모).
- NetworkPolicy (서비스 간 인증 없음 보완), 게이트웨이 이중화 (active-standby).
- 범위 밖으로 둔 것: 이미지 레지스트리 푸시 · 서명 · 취약점 스캔, 처리량 확장 측정(여러 노드 필요), 파티션 수 늘리기, api-reactive 컨테이너, 분산 추적 · 지표(P9).
