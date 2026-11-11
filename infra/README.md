# 로컬 인프라 (MySQL · Kafka · Redis)

설계 이유는 [ADR-0003](../docs/adr/0003-local-infra-docker-compose.md).

| 서비스 | 이미지 | 내 PC 에서 접속 | 컨테이너끼리 |
|---|---|---|---|
| MySQL | `mysql:8.4` | `localhost:3306` (db `bank_transfer`, user `bank`) | `mysql:3306` |
| Kafka (KRaft) | `apache/kafka:4.1.0` | `localhost:9092` | `kafka:29092` |
| Redis | `redis:7.4-alpine` | `localhost:6379` | `redis:6379` |
| Kafka UI (선택) | `kafbat/kafka-ui` | http://localhost:8089 | – |
| nginx (`app` 프로필) | `nginx:1.27-alpine` | http://localhost:8000 (127.0.0.1 만) | – |
| 모의 은행 · 게이트웨이 · api-1 · api-2 (`app` 프로필) | `bank-transfer/*:dev` (루트 `Dockerfile`) | 열지 않음 | `mock-bank:9090` · `bank-gateway:8082` · `api-1:8080` · `api-2:8080` |

## 처음 한 번 (Windows)
1. **WSL2 설치** — 관리자 PowerShell 에서 `wsl --install` → 재부팅.
2. **Docker Desktop 설치** — docker.com 에서 받아 설치, *Use WSL 2 based engine* 체크 → 실행 후 고래 아이콘이 초록색이 될 때까지 기다린다.
3. 확인: `docker version`, `docker compose version`.
4. (선택) WSL 메모리 제한: `%UserProfile%\.wslconfig` 에 `[wsl2]` / `memory=4GB`.

## 매일
> Windows 는 **Git Bash** 에서 실행한다. PowerShell 의 `bash` 는 WSL 로 연결되어 동작하지 않는다 (아래 "자주 막히는 곳").

```bash
cp infra/.env.example infra/.env          # 처음 한 번
docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait
bash infra/verify.sh                       # MySQL · Kafka · Redis 실제 읽기/쓰기 확인
docker compose -f infra/docker-compose.yml --env-file infra/.env ps
docker compose -f infra/docker-compose.yml --env-file infra/.env down       # 멈추기 (데이터 유지)
docker compose -f infra/docker-compose.yml --env-file infra/.env down -v    # 데이터까지 삭제
```

## 직접 만져 보기
```bash
# MySQL
docker compose -f infra/docker-compose.yml exec mysql mysql -ubank -p bank_transfer
# Kafka: 토픽 목록 · 보내기 · 받기
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic infra.check
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic infra.check --from-beginning
# Redis
docker compose -f infra/docker-compose.yml exec redis redis-cli
```

### 망 지연 주입 — `chaos` 프로필 (P8-1)
api 와 게이트웨이 앱을 같은 PC 에서 띄우면 망 지연이 거의 0 이다. Toxiproxy 를 사이에 두어 "다른 서버를 부르는" 지연을 만든다 (측정은 `latency` 만, 테스트는 지연 · 응답 막힘). 연결 끊김은 Toxiproxy 가 아니라 테스트 픽스처 `TcpCutProxy` 로 다룬다.
- 띄우기: `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile chaos up -d toxiproxy`
- 제어 API `http://localhost:8474` — 측정 스크립트(`load/lib/gateway.sh`)가 프록시 `p81-gateway`(:28082 → `host.docker.internal:18082`)와 지연 toxic 을 만들고 지운다.
- 손으로: `curl -s localhost:8474/proxies` 로 남은 프록시 · toxic 확인, `curl -X DELETE localhost:8474/proxies/p81-gateway` 로 지운다.
- 내리기: `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile chaos stop toxiproxy`

### 앱까지 컨테이너로 — `app` 프로필 (P8-2)
모의 은행 · 게이트웨이 · api 2대 · nginx 를 같은 Compose 에서 띄운다 (설계 이유 [ADR-0016](../docs/adr/0016-app-images-compose.md)). **먼저 IntelliJ 의 API · 게이트웨이 · 모의 은행을 끈다** — 같은 DB · 같은 Kafka 그룹이라 함께 두면 대사가 어긋난다.
- 띄우기 (이미지 빌드 포함, 처음엔 몇 분): `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app up -d --build --wait`
- 확인: `bash infra/app-smoke.sh` (nginx `http://localhost:8000` 만으로 1원인증 · 출금 · 멱등 · 분배 · 포트 · 이미지 비밀값), `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app ps` (5개 healthy)
- 내리기 (앱만): `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app stop nginx api-1 api-2 bank-gateway mock-bank`
- 기동 순서는 `depends_on: condition: service_healthy` 가 정한다: 인프라 → mock-bank → bank-gateway → api-1 · api-2 → nginx. nginx 의 `/nginx-health` 는 **nginx 자신만** 답한다 (api 가 살았는지가 아니다 — api 상태는 `ps` 의 healthy 나 `/actuator/info`).
- **공통 블록** (`docker-compose.yml` 위쪽 YAML anchor): `x-java-app`(restart · `stop_grace_period 30s`) · `x-bank-maintenance`(점검 3변수 — api · 게이트웨이에 같은 값) · `x-api-env`(api 환경변수 전체) · `x-api`(api 두 대의 공통: 이미지 · healthcheck · depends_on · 읽기 전용 `bank-files`). 서비스 `api-1` · `api-2` 는 `INFO_INSTANCE` 만 다르다.
- **`MYSQL_PORT: "3306"` 을 명시한 이유**: `infra/.env` 의 `MYSQL_PORT` 는 PC 쪽 포트(바꿔 쓸 수 있다)다. 그대로 컨테이너에 넘기면 컨테이너 안에서 틀린 포트로 붙는다.
- 점검 시간 · 대사 cron 을 바꾸는 보간 변수 (기본값이 있고, 셸 환경이나 `infra/.env` 에 두면 바뀐다): `BANK_MAINTENANCE_ENABLED`(`true`) · `BANK_MAINTENANCE_START`(`23:50`) · `BANK_MAINTENANCE_END`(`00:10`) · `API_DAILY_RECON_CRON`(`0 0 1-6 * * *`, 실험 E5 만 2분마다로 바꾼다). 바꾼 뒤에는 `up -d` 로 컨테이너를 다시 만들고 **`restart nginx`** 도 한다.
- 로그 · 안에서 보기 · 롤링 재시작 · 장애: [OPERATION §14](../OPERATION.md#14-컨테이너-실행-p8-2).

## 자주 막히는 곳
| 증상 | 원인 | 해결 |
|---|---|---|
| `port is already allocated` | PC 에 MySQL · Redis 가 이미 떠 있음 | `infra/.env` 에서 `MYSQL_PORT=3307` 처럼 바꾸기 |
| `up --wait` 이 오래 걸리다 실패 | 첫 실행에 이미지 내려받기 · MySQL 초기화 | `docker compose ... logs mysql` 확인 후 다시 실행 |
| 앱에서 Kafka 접속은 되는데 메시지가 안 감 | `advertised.listeners` 주소 불일치 | 앱은 `localhost:9092`, 컨테이너는 `kafka:29092` |
| Kafka 가 `Invalid cluster.id` 로 안 뜸 | `KAFKA_CLUSTER_ID` 를 바꿨는데 예전 볼륨이 남음 | `down -v` 후 다시 `up` |
| WSL 메모리 부족 · PC 느려짐 | Docker 가 메모리를 많이 씀 | `.wslconfig` 로 제한, 안 쓸 때 `down` |
| nginx 502 · 한쪽만 응답 | api 컨테이너를 다시 만들었는데 nginx 가 옛 IP | `--profile app restart nginx` |
| api-1 이 unhealthy 로 멈춤 | DB · Kafka 연결 또는 메모리 한도 | `logs api-1`, `docker stats` |
| PowerShell 에서 `bash infra/verify.sh` → `execvpe(/bin/bash) failed` | PowerShell 의 `bash` 는 WSL 실행기다. WSL 배포판이 `docker-desktop` 하나뿐이라 bash 가 없다 | Git Bash 로 실행: `& "C:\Program Files\Git\bin\bash.exe" infra/verify.sh` (또는 IntelliJ 터미널을 Git Bash 로) |
| Git Bash 에서 `docker compose exec kafka /opt/kafka/...` → 파일 없음 | Git Bash 가 `/opt/...` 를 `C:/Program Files/Git/opt/...` 로 바꿔 넘김 | 앞에 `MSYS_NO_PATHCONV=1` 을 붙이거나 `//opt/kafka/...` 로 쓰기 (`verify.sh` 는 이미 처리) |
