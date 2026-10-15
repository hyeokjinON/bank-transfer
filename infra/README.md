# 로컬 인프라 (MySQL · Kafka · Redis)

설계 이유는 [ADR-0003](../docs/adr/0003-local-infra-docker-compose.md).

| 서비스 | 이미지 | 내 PC 에서 접속 | 컨테이너끼리 |
|---|---|---|---|
| MySQL | `mysql:8.4` | `localhost:3306` (db `bank_transfer`, user `bank`) | `mysql:3306` |
| Kafka (KRaft) | `apache/kafka:4.1.0` | `localhost:9092` | `kafka:29092` |
| Redis | `redis:7.4-alpine` | `localhost:6379` | `redis:6379` |
| Kafka UI (선택) | `kafbat/kafka-ui` | http://localhost:8089 | – |

## 처음 한 번 (Windows)
1. **WSL2 설치** — 관리자 PowerShell 에서 `wsl --install` → 재부팅.
2. **Docker Desktop 설치** — docker.com 에서 받아 설치, *Use WSL 2 based engine* 체크 → 실행 후 고래 아이콘이 초록색이 될 때까지 기다린다.
3. 확인: `docker version`, `docker compose version`.
4. (선택) WSL 메모리 제한: `%UserProfile%\.wslconfig` 에 `[wsl2]` / `memory=4GB`.

## 매일
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

## 자주 막히는 곳
| 증상 | 원인 | 해결 |
|---|---|---|
| `port is already allocated` | PC 에 MySQL · Redis 가 이미 떠 있음 | `infra/.env` 에서 `MYSQL_PORT=3307` 처럼 바꾸기 |
| `up --wait` 이 오래 걸리다 실패 | 첫 실행에 이미지 내려받기 · MySQL 초기화 | `docker compose ... logs mysql` 확인 후 다시 실행 |
| 앱에서 Kafka 접속은 되는데 메시지가 안 감 | `advertised.listeners` 주소 불일치 | 앱은 `localhost:9092`, 컨테이너는 `kafka:29092` |
| Kafka 가 `Invalid cluster.id` 로 안 뜸 | `KAFKA_CLUSTER_ID` 를 바꿨는데 예전 볼륨이 남음 | `down -v` 후 다시 `up` |
| WSL 메모리 부족 · PC 느려짐 | Docker 가 메모리를 많이 씀 | `.wslconfig` 로 제한, 안 쓸 때 `down` |
