# P8-2 앱 이미지 · Docker Compose 전체 실행 · api 2대 — 설계

> 날짜: 2026-11-11 · 브랜치 `feat/p8-2-compose` · 기준 main `9d782ff` (P8-1 #25 · 정리 #26 머지)
> P8 전체: P8-1 게이트웨이 분리 (완료) → **P8-2 앱 이미지 · Compose 로 전체 실행 (이 문서)** → P8-3 Kubernetes
> 앞 단계가 넘긴 것 (ADR-0015): 앱 컨테이너 이미지 · Compose 전체 실행, `mode` 기본을 `http` 로, 점검 설정 한 곳으로.

## 1. 목표와 성공 기준
P8-1 은 "api 를 늘려도 은행 연결은 한 곳" 을 위해 게이트웨이를 떼어냈다. P8-2 는 그걸 **실제로 여러 대로 띄워** 확인한다.

- `docker compose --profile app up -d --build` 한 번으로 인프라 + 모의 은행 + 게이트웨이 1대 + **api 2대** + nginx 가 뜬다. PC 에 JDK 가 없어도 된다.
- 2대가 되면 드러나는 "인스턴스 안의 상태" 를 하나씩 처리하고 근거를 남긴다 (§2 표).
- CI 가 같은 구성을 띄워 nginx 를 통해 1원인증 · 출금 `COMPLETED` 를 확인한다.
- 실험으로 가용성 · 정합성을 확인한다: 롤링 재시작 · 강제 종료에서 이중 0, 접수된 출금 전부 완료.
- **처리량 증가는 목표가 아니다.** 한 PC 에서 2대는 같은 CPU 를 나눠 쓴다 — 늘었다고 주장하지 않는다.

## 2. 2대가 되면 깨지는 것과 처리
| 인스턴스 안에 있던 것 | 2대가 되면 | 처리 |
|---|---|---|
| 은행 연결 · 응답 매칭 표 | 응답이 다른 인스턴스로 | 게이트웨이 분리 (P8-1) — 이번에 `mode` 기본 `http` |
| 하루 배치 (일마감 대사 · 보존 삭제) | 두 번 돈다 | **ShedLock** (§6) |
| 아웃박스 발행 (0.5초) · 정리 작업 (10초) | 같은 행 두 번 처리 | 이미 안전 — `FOR UPDATE SKIP LOCKED` · 조건부 UPDATE (ADR-0005 · 0006 · 0007). 락을 걸지 않는다 (걸면 한 대만 일한다) |
| 중복 요청 검사 | 다른 인스턴스에선 통과 | 이미 DB UNIQUE 멱등 키 (ADR-0004) |
| 잔고 동시 차감 | 로컬 락은 서로 안 보임 | 이미 조건부 UPDATE · DB 락 (ADR-0008 · 0009) |
| 대사 파일 디렉터리 | 인스턴스마다 다름 | 공유 볼륨 `bank-files` (운영이면 SFTP · 오브젝트 스토리지) |
| 세션 · 로컬 캐시 | — | 없음 (무상태, 헤더 `X-User-Id`) |
| Kafka 컨슈머 | 그룹 안 컨슈머 6 · 파티션 3 → 3개는 논다 | **그대로 두고 드러낸다** (§5-4) |

## 3. 결정
| # | 결정 | 고른 이유 | 버린 대안 | 대가 |
|---|---|---|---|---|
| 1 | 루트 `Dockerfile` 하나 · 타깃 3개 (`api` · `bank-gateway` · `mock-bank`), 멀티 스테이지 | 세 앱이 같은 Gradle 멀티 모듈을 쓴다 — 빌드 단계를 한 번만 돌리고 BuildKit 이 타깃들에 재사용. 줄 단위로 설명 가능 | 앱마다 Dockerfile (빌드 3번), `bootBuildImage` (설명할 게 적고 느림), Jib (플러그인 의존, mock-bank 별도) | Dockerfile 직접 관리 |
| 2 | 실행 이미지 `eclipse-temurin:21-jre-alpine`, root 아닌 사용자, exec 형식 `ENTRYPOINT` | 작다 · 헬스체크용 `wget` 기본 포함 · 최소 권한 · SIGTERM 이 java 에 직접 | `-jdk` (크다), distroless (헬스체크 도구 없음) | Alpine(musl) — JVM 은 문제없음, 네이티브 라이브러리 쓰면 확인 필요 |
| 3 | Boot jar 를 레이어로 풀어 4층 복사 (`-Djarmode=tools extract --layers --launcher`) | 코드 한 줄 수정에 맨 위 층만 다시 | fat jar 통째로 (매번 100MB 층) | 단계가 하나 늘어남 |
| 4 | Compose `app` 프로필 — nginx 1 · api 2 (`api-1` · `api-2` 이름 분리) · 게이트웨이 1 · 모의 은행 1 | 인프라만 띄우는 지금 개발 방식 유지. 이름을 나눠 로그 · 장애 실험 · 확인이 인스턴스별로 또렷 | `--scale api=2` (nginx 가 기동 때 한 번만 이름을 풂, 인스턴스 구분이 흐림) | 서비스 정의 2개 (YAML anchor 로 중복 최소화) |
| 5 | PC 에 여는 포트는 nginx `8000` 하나 | 게이트웨이 · 모의 은행은 내부망 전용 — ADR-0015 위험 "게이트웨이 포트가 열리면 누구나 전문을 보낸다" 의 첫 조치. IntelliJ(8080 · 8082 · 9090) · 측정 포트와 안 겹침 | api 포트 각각 노출 | 디버깅은 `docker compose exec` · `logs` |
| 6 | nginx 라운드 로빈, `proxy_next_upstream error timeout` 만 (`non_idempotent` 없음) | POST 를 **보낸 뒤** 다른 서버로 다시 보내지 않는다 (이중 접수 방지). 연결조차 못 한 요청은 안전하게 넘긴다 — P8-1 "연결 단계 실패만 안 보냄" 과 같은 규칙 | 재시도 허용, least_conn | — |
| 7 | `bank.gateway.mode` 기본 `http` (api · api-reactive) | ADR-0015 계획. 운영 모양이 기본이 된다. 기본 `direct` 때문에 게이트웨이를 안 거치는 줄 모르고 확인한 일이 있었다 (#26) | 코드 기본 `direct` + 컨테이너만 `http` | IntelliJ 는 모의 은행 → 게이트웨이 → api 순서. 테스트 · 측정은 `direct` 명시 |
| 8 | 하루 배치 2개에만 ShedLock (JDBC, `usingDbTime()`) | 한 대가 죽어도 다른 대가 돈다. 서버 시계 차이와 무관 | 설정으로 한 대만 (그 한 대가 죽으면 배치가 안 돈다), 락 없이 멱등만 (결과 불확실) | 의존성 1 · 마이그레이션 1 |
| 9 | Kafka 파티션 3 · api 당 컨슈머 3 유지 | 파티션 수는 병목 측정 뒤 정하는 용량 결정 — 한 PC 에서는 늘려도 이득을 증명 못 함. "파티션 = 병렬도 상한" · "노는 컨슈머 = 장애 대기 인력" 을 실험으로 보인다 | 6파티션 (줄일 수 없음 · 측정 기준선 깨짐), 컨슈머 축소 (한 대 죽으면 느려짐) | 처리 병렬도 3 그대로 |
| 10 | graceful shutdown: api `server.shutdown=graceful` · 단계당 20s, Compose `stop_grace_period: 30s` | 배포 때마다 처리 중인 요청 · 메시지를 끝내고 내려간다. 안쪽(20s)이 바깥(30s)보다 짧게 | 기본(즉시 종료) | 종료가 최대 30초 |
| 11 | 점검 설정은 Compose 공통 블록 하나에서 api · 게이트웨이에 같은 값 | 컨테이너 실행에서 두 곳 설정이 어긋날 수 없다 | 게이트웨이에 물어보기 (호출 추가), 설정 서버 | IntelliJ 실행은 여전히 두 `application.yml` (같은 값) — ADR 에 한계로 |

## 4. 이미지
### 4-1. 루트 `Dockerfile`
```dockerfile
# syntax=docker/dockerfile:1.7
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
RUN ./gradlew --version                                   # 래퍼 내려받기까지 캐시
COPY . .
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew :apps:api:bootJar :apps:bank-gateway:bootJar :apps:mock-bank:installDist -x test --no-daemon
RUN java -Djarmode=tools -jar apps/api/build/libs/api-*-SNAPSHOT.jar extract --layers --launcher --destination /out/api \
 && java -Djarmode=tools -jar apps/bank-gateway/build/libs/bank-gateway-*-SNAPSHOT.jar extract --layers --launcher --destination /out/bank-gateway \
 && cp -r apps/mock-bank/build/install/mock-bank /out/mock-bank

FROM eclipse-temurin:21-jre-alpine AS runtime-base
RUN addgroup -S app && adduser -S app -G app
ENV TZ=Asia/Seoul
WORKDIR /app
USER app

FROM runtime-base AS api
COPY --from=build /out/api/dependencies/ ./
COPY --from=build /out/api/spring-boot-loader/ ./
COPY --from=build /out/api/snapshot-dependencies/ ./
COPY --from=build /out/api/application/ ./
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "org.springframework.boot.loader.launch.JarLauncher"]
# bank-gateway: 같은 모양. mock-bank: lib/ 복사, ENTRYPOINT ["java", …, "-cp", "lib/*", "…MockBankServerKt"], CMD ["9090", "--bank-files=/data/bank-files"]
```
- jar 파일 이름 · 레이어 디렉터리 이름 · `JarLauncher` 패키지는 Boot 4.1.1 의 실제 출력으로 구현 때 확인한다 (`plain` jar 와 섞이지 않게).
- mock-bank 는 대사 파일을 `/data/bank-files` 에 쓴다 — 볼륨 소유자가 `app` 사용자여야 쓸 수 있다 (구현 때 확인, 필요하면 디렉터리를 이미지에서 미리 만들고 `chown`).

### 4-2. 줄마다 이유 (문서 · 면접 가이드에 그대로)
멀티 스테이지(빌드 도구는 최종 이미지에 없음) · 덜 바뀌는 것 먼저 `COPY`(층 캐시) · `--mount=type=cache`(Gradle 의존성 캐시, 이미지에 안 들어감) · `-x test`(테스트는 CI `build` 잡) · 레이어 추출(코드 층만 다시) · JRE Alpine · `USER app`(최소 권한) · `TZ`(모의 은행 파일 날짜 KST) · exec 형식 `ENTRYPOINT`(PID 1 = java → SIGTERM 직접 → graceful) · `MaxRAMPercentage=75`(컨테이너 한도의 75% 를 힙으로, 나머지는 스택 · 메타스페이스 · 네이티브) · `ExitOnOutOfMemoryError`(반쯤 죽은 채 버티지 않고 종료 → `restart` 가 새로).

### 4-3. `.dockerignore`
`build/` `**/build/` `.gradle/` `.kotlin/` `.git/` `.idea/` `.postman/` `bank-files/` `docs/` `load/` `postman/` `.run/` `.superpowers/` `http/` **`infra/.env`**. 비밀값은 실행 때 Compose 가 `infra/.env` 에서 환경변수로 넣는다 — 이미지에 굳지 않는다 (CI 가 확인, §7-2).

## 5. Compose `app` 프로필
### 5-1. 서비스
| 서비스 | 이미지 / 타깃 | 포트 (PC) | 주요 환경변수 | healthcheck | 기동 조건 |
|---|---|---|---|---|---|
| `mock-bank` | 타깃 `mock-bank` | 없음 | `TZ` | TCP 9090 열림 | — |
| `bank-gateway` | 타깃 `bank-gateway` | 없음 | `BANK_GATEWAY_HOST=mock-bank`, 점검 공통 블록 | `wget /actuator/health` (bankConnection 포함) | mock-bank healthy |
| `api-1` · `api-2` | 타깃 `api` (같은 이미지) | 없음 | `MYSQL_HOST=mysql`, **`MYSQL_PORT=3306`**, `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`, `REDIS_HOST=redis`, `BANK_GATEWAY_HTTP_BASEURL=http://bank-gateway:8082`, `DAILY_RECON_FILE_DIR=/data/bank-files`, `INFO_INSTANCE=api-1\|api-2`, 점검 공통 블록, DB 계정은 `infra/.env` | `wget /actuator/health/readiness` | mysql · kafka · redis · bank-gateway healthy |
| `nginx` | `nginx:1.27-alpine` | **`8000:80`** | — | `wget /nginx-health` (nginx 자체) | api-1 · api-2 healthy |

- `MYSQL_PORT=3306` 을 **명시**한다: `infra/.env` 의 `MYSQL_PORT` 는 PC 쪽 포트(바꿔 쓸 수 있음)라, 그대로 넘기면 컨테이너 안에서 틀린 포트로 붙는다.
- 환경변수 이름은 Spring 의 느슨한 바인딩(`bank.gateway.http.base-url` → `BANK_GATEWAY_HTTP_BASEURL`)을 따른다. 바인딩은 스모크로 확인한다.
- 볼륨 `bank-files`: mock-bank 는 쓰고 api 는 읽는다 (`:ro`).
- `restart: unless-stopped`, `stop_grace_period: 30s` (api · 게이트웨이), 메모리 한도 `mem_limit` (api 768m · 게이트웨이 512m · 모의 은행 256m — E1 에서 실제 사용량을 보고 조정).
- 공통 블록: `x-api` (api 두 대), `x-bank-maintenance` (점검 3변수, api · 게이트웨이), `x-java-app` (restart · stop_grace_period · TZ).

### 5-2. `infra/nginx/nginx.conf`
```nginx
upstream api {
    server api-1:8080 max_fails=3 fail_timeout=10s;
    server api-2:8080 max_fails=3 fail_timeout=10s;
}
server {
    listen 80;
    location = /nginx-health { return 200 "ok\n"; }
    location / {
        proxy_pass http://api;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_next_upstream error timeout;      # non_idempotent 를 넣지 않는다 — 보낸 POST 는 다른 서버로 다시 보내지 않는다 (이중 접수 방지)
        proxy_next_upstream_tries 2;
        proxy_connect_timeout 1s;
        proxy_read_timeout 10s;                 # api 가 게이트웨이(5s)를 기다리는 시간보다 길게 — 안쪽이 먼저 끝나게
        add_header X-Upstream $upstream_addr always;
    }
}
```

### 5-3. 기동 순서와 동시 기동
인프라 healthy → mock-bank → bank-gateway(은행 연결 UP) → api-1 · api-2 → nginx. 두 api 가 동시에 떠도 Flyway 는 DB 잠금으로 마이그레이션을 한 번만 적용하고, 토픽 생성(`NewTopic`)은 이미 있으면 그대로다.

### 5-4. Kafka 컨슈머 6 · 파티션 3
`transfer.requested` 는 코드에서 3파티션으로 만든다(`TransferTopics.PARTITIONS`). `concurrency: 3` 이라 api 한 대가 컨슈머 3개 → 2대면 6개 중 3개는 파티션 없음. 그대로 두고 `kafka-consumer-groups --describe` 로 보이며, 강제 종료 실험(E4)에서 노는 컨슈머가 리밸런스로 이어받는 것을 본다. 파티션을 늘리는 판단 기준(lag 가 계속 늘고 컨슈머 CPU 에 여유)은 문서에.

### 5-5. 운영 주의
`app` 프로필과 IntelliJ 개발 API 는 같은 MySQL · 같은 Kafka 그룹을 쓴다 — 동시에 띄우면 출금이 다른 모의 은행으로 흘러 대사가 어긋난다. **동시에 띄우지 않는다** (SETUP · OPERATION). 컨테이너 api 도 06:30 보존 배치를 개발 DB 에 돌린다 (HANDOVER 경고 그대로 적용).

## 6. ShedLock
- 의존성: `net.javacrumbs.shedlock:shedlock-spring` · `shedlock-provider-jdbc-template` — Spring Boot 4.1.1 / Spring 7 과 맞는 버전을 구현 때 확인해 고정.
- `SchedulerLockConfiguration`: `@EnableSchedulerLock(defaultLockAtMostFor = "PT30M")`, `JdbcTemplateLockProvider` + `usingDbTime()`.
- Flyway `V6__shedlock.sql`: `shedlock(name VARCHAR(64) PK, lock_until TIMESTAMP(3), locked_at TIMESTAMP(3), locked_by VARCHAR(255))`.
- `DailyReconScheduler.run()`: `@SchedulerLock(name = "daily-recon", lockAtMostFor = "PT50M", lockAtLeastFor = "PT1M")` — 매시 회차(01 ~ 06시)라 60분보다 짧게.
- `DataRetentionScheduler.run()`: `@SchedulerLock(name = "data-retention", lockAtMostFor = "PT30M", lockAtLeastFor = "PT1M")`.
- 발행 · 정리 작업에는 걸지 않는다 (§2).
- 락은 1차, 멱등은 2차: 대사 이슈 `(날짜, 종류, 키)` UNIQUE · Spring Batch 잡 인스턴스, 보존 삭제는 다시 지워도 같음 — 구현 때 코드로 확인해 ADR 에.
- 실행 시간: 대사 · 보존 한 회차가 실제로 몇 초인지 로그로 보고 문서에 (lockAtMostFor 의 근거).

## 7. 검증
### 7-1. 자동 테스트
| 테스트 | 증명 |
|---|---|
| `BankGatewayDefaultModeTest` (api · api-reactive) | 설정 없이 뜨면 `mode=http` — `HttpBankGateway` 빈, `BankGatewayClient` 없음 |
| 기존 전체 | 공통 테스트 기반(`ApiIntegrationTest` · `ReactiveIntegrationTest`)이 `bank.gateway.mode=direct` 를 등록 → 그대로 통과 |
| `SchedulerLockTest` (MySQL Testcontainers) | ① 같은 `LockProvider` 의 스케줄러 둘을 동시에 `run()` → 실행기 1번, `locked_by` = 이긴 쪽 ② `lockAtLeastFor` 지난 뒤 다음 회차는 돈다 ③ `lock_until` 지난 행(죽은 서버)은 가져간다 ④ 실제 스케줄러 빈 두 개에 `@SchedulerLock` 이름이 붙어 있다 |
| `GracefulShutdownConfigTest` | api 컨텍스트에 `server.shutdown=graceful` · 단계 20s |

### 7-2. CI `app-smoke` 잡 (15분 제한)
이미지 빌드 (BuildKit 캐시 `type=gha`) → `docker compose --profile app up -d --wait` → `infra/app-smoke.sh` (nginx 8000 만 사용):
1. 1원인증 요청 → `docker compose logs mock-bank` 에서 적요 코드 4자리 → 확인 → `VERIFIED`
2. 충전 → 출금(`Idempotency-Key`) → 조회 반복 → `COMPLETED`
3. 같은 키로 출금 다시 → 같은 출금 id
4. 요청 20번의 `X-Upstream` 에 api-1 · api-2 둘 다
5. 게이트웨이 health `UP` (`docker compose exec`), PC 쪽 8082 · 9090 은 연결 거부
6. 각 컨테이너 `id -u` ≠ 0, 이미지에 `infra/.env` 의 비밀번호 값이 없음 (`docker history` · 이미지 파일 검색)
실패 시 전체 로그, 항상 `down -v`. CI 는 `infra/.env.example` 을 쓴다.

### 7-3. 실험 (로컬, `docs/perf/p8-2-compose.md`)
측정용 사용자 `load-p82-*` 만 쓰고 끝나면 정리 (개발 DB 측정 행 규칙).
| # | 실험 | 확인 |
|---|---|---|
| E1 | 이미지 | 앱별 크기 · 층별 크기, 처음 / 캐시 빌드 시간, 코드 한 줄 수정 뒤 새로 만든 층 크기, 기동 → healthy 시간, 실제 메모리 사용 |
| E2 | 분배 | 조회 100번의 api-1 / api-2 비율, `kafka-consumer-groups --describe` (6 중 3 파티션 없음) |
| E3 | 롤링 재시작 | k6 출금 일정 부하 60초 중 `docker compose restart api-1`: 5xx 0, 접수 전부 `COMPLETED`, 은행 기록 = 출금 행, 이중 0, 리밸런스 시각 |
| E4 | 강제 종료 | 같은 부하 중 `docker kill api-1`: 502 수, nginx 가 빼는 시간, Kafka 가 파티션을 넘기는 시간(세션 시간 초과), 최종 정합성(재배달 · `SENDING` 정리 포함, 이중 0) |
| E5 | ShedLock | 두 api 의 대사 cron 을 1분마다로 5분: 회차마다 한쪽만 (`shedlock.locked_by` · 로그) |
도구: `load/compose/` (k6 스크립트 · 실행 스크립트 · 정확성 SQL — 기존 `load/consumer` · `load/verification` 의 정확성 확인을 재사용). k6 는 Compose `load` 프로필 컨테이너에서 `nginx:80` 으로.

### 7-4. Postman 직접 확인 (머지 전, 사용자)
먼저: IntelliJ 의 API · 게이트웨이 · 모의 은행을 모두 끄고 `docker compose --profile app up -d --build` → `docker compose ps` 5개 healthy.
| # | 할 일 | 기대 결과 |
|---|---|---|
| 1 | base URL `http://localhost:8000` 으로 1원인증 → 확인 | `VERIFIED` (적요 코드: `docker compose logs mock-bank`) |
| 2 | 충전 → 출금 | 202 `RECEIVED` → `COMPLETED`, 응답 헤더 `X-Upstream` 이 요청마다 바뀜 |
| 3 | 같은 `Idempotency-Key` 로 출금 다시 | 같은 출금 id, 잔고는 한 번만 감소 |
| 4 | `docker compose stop api-1` 후 출금 → `start` | 계속 성공, `X-Upstream` 은 api-2 만 → 다시 번갈아 |
| 5 | `localhost:8082` · `localhost:9090` 접속 | 연결 거부 (내부망 전용) |
| 6 | nginx 로 `/actuator/info` 여러 번 | `instance: api-1` / `api-2` |

## 8. 설정 변경 목록
- `apps/api` · `apps/api-reactive` `application.yml`: `bank.gateway.mode: http`. api: `server.shutdown: graceful`, `spring.lifecycle.timeout-per-shutdown-phase: 20s`.
- 테스트 공통 기반: `bank.gateway.mode=direct` 등록.
- `load/lib/gateway.sh`: `GATEWAY` 없을 때 `--bank.gateway.mode=direct` 를 명시 (P6 ~ P8-1 측정 기준선 유지).
- 실행 구성: `0. Mock Bank + API` 에 `11. Bank Gateway` 추가 (모의 은행 → 게이트웨이 → API), `12. API (http 게이트웨이)` → `12. API (direct 직결)` (`--bank.gateway.mode=direct`). `2. API (8080)` 파일은 건드리지 않는다 (사용자의 커밋하지 않은 수정 — 기본값을 따라 http 가 된다).

## 9. 위험
- 컨테이너 api 와 IntelliJ API 동시 실행 → 대사 어긋남 (§5-5). 문서 · `app-smoke.sh` 시작 시 8080 · 9090 이 쓰이고 있으면 경고.
- 빌드 시간: CI 에서 처음 빌드는 Gradle 의존성 내려받기로 길다 — `type=gha` 캐시로 줄인다. 15분 넘으면 잡을 나누거나 캐시 범위를 다시 본다.
- Windows Docker Desktop 의 볼륨 권한 — 구현 때 확인. `gradlew` 줄바꿈은 `.gitattributes` 가 이미 LF 로 고정한다 (확인함).
- ShedLock 버전과 Spring 7 호환 — 구현 첫 작업에서 확인.
- 한 PC 자원: 컨테이너 5 + 인프라 3 + k6. 메모리 한도로 묶고 E1 에서 실제 사용량 확인.

## 10. 범위 밖
게이트웨이 이중화(active-standby), 서비스 간 인증 · mTLS · NetworkPolicy, Kubernetes (Deployment · Service · Ingress · probe — P8-3), 이미지 레지스트리 푸시 · 서명 · 취약점 스캔, 처리량 확장 측정(여러 노드 필요), 파티션 수 늘리기, api-reactive 컨테이너, 분산 추적 · 지표 수집(P9), 점검 설정을 코드 수준에서 한 곳으로(설정 서버).

## 11. 문서 · 작업 순서
- 문서: ADR-0016 (이미지 방식 · 실행 구성 · ShedLock · 파티션 유지 · 버린 대안), ADR-0015 에 "P8-2 에서 `mode` 기본 `http`" 덧붙임, `docs/perf/p8-2-compose.md` (E1 ~ E5), ARCHITECTURE (컨테이너 실행 그림) · README (로드맵 P8-2 · 기술 스택) · SETUP (컨테이너 실행 · IntelliJ 순서 · 실행 구성 이름) · OPERATION (컨테이너 장애 때 보는 법 · `shedlock` 쿼리 · 롤링 재시작 절차) · `infra/README.md` (`app` 프로필) · HANDOVER · CHANGELOG.
- job-prep 면접 가이드 `10-p8-2-containers-compose.md`: 용어(ShedLock · `lockAtMostFor` · 멱등과 nginx 재시도 · graceful shutdown · 리밸런스 · 오프셋 커밋 · 파티션 상한 · 병목 찾기(USE) · Service/Ingress/Deployment/readiness/liveness), 코드(Dockerfile 줄 단위 · Compose · nginx.conf · ShedLock 설정 · 테스트), 실험 숫자, 면접 예상 질문과 30초 답. `07` 보강 · `00` 색인.
- 작업 순서: ① `mode` 기본 `http` + direct 명시 + 실행 구성 ② graceful · 인스턴스 구분 ③ ShedLock ④ Dockerfile · `.dockerignore` ⑤ Compose `app` 프로필 · nginx ⑥ `app-smoke.sh` · CI 잡 ⑦ 실험 E1 ~ E5 · 결과 문서 ⑧ ADR-0016 · 문서 · 전체 빌드.
