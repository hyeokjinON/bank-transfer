# ADR-0003 로컬 인프라는 Docker Compose 한 파일로 — MySQL 8.4 · Kafka 4.1(KRaft 단일 노드) · Redis 7.4

- 상태: 채택 (2026-10-16, PR #1 머지)
- 단계: P3-0 (P3 Kafka 비동기 이체 · P4 동시성 비교의 준비)

## 맥락
- 다음 단계부터 이체 요청을 DB 에 저장하고(MySQL), Kafka 로 비동기 처리하고, Redis 로 분산락을 비교한다.
- 예전 회사는 VM 에 MariaDB Galera · ZooKeeper · Kafka 를 직접 설치했고, Kafka 데이터 경로가 `/tmp` 였다.
  누구 PC 에서든 같은 환경을 1분 안에 띄우고, 지우고, 다시 만들 수 있어야 한다.

## 결정
1. **Docker Compose 파일 하나**(`infra/docker-compose.yml`)로 세 서비스를 띄운다. 앱은 지금처럼 IntelliJ 에서 실행한다.
2. **MySQL 8.4 (LTS)**: 공고의 MySQL 과 맞추고, 문자셋은 `utf8mb4`, 시간대는 +09:00.
3. **Kafka 4.1 KRaft 단일 노드**: 4.x 는 ZooKeeper 가 없다. 한 컨테이너가 브로커와 컨트롤러를 함께 맡는다.
   - 리스너 2개: 컨테이너끼리는 `kafka:29092`, 내 PC 의 앱은 `localhost:9092`.
   - 토픽 자동 생성은 끈다 (오타 토픽이 몰래 생기는 것 방지).
   - 복제 수는 1 이다. 장애 실습(브로커 다운 · ISR · min.insync.replicas)은 나중에 3노드 파일을 따로 만든다.
4. **Redis 7.4**: AOF 를 켜서 재기동해도 데이터가 남게 한다.
5. **healthcheck + `up --wait`**: "컨테이너가 떴다"가 아니라 "접속해서 응답한다"를 준비 완료 기준으로 삼는다.
   `infra/verify.sh` 는 한 단계 더 가서 실제로 쓰고 읽어 본다.
6. **CI 연기 테스트**: GitHub Actions 에서 같은 파일로 인프라를 띄우고 `verify.sh` 를 실행한다.
   설정 파일이 깨지면 PR 에서 바로 드러난다.
7. 비밀번호는 `infra/.env`(git 제외)에 두고, 예시는 `infra/.env.example` 로 제공한다.

## 결과
- `cp infra/.env.example infra/.env` → `docker compose ... up -d --wait` → `bash infra/verify.sh` 세 줄로 준비 끝.
- 데이터는 이름 있는 볼륨에 남고 `down -v` 로만 지워진다.
- 한계: 단일 노드라 Kafka 의 복제 · 장애 동작은 볼 수 없다. Testcontainers 기반 통합 테스트는 P3 에서 앱 코드와 함께 추가한다.

## 검토한 다른 선택지
| 선택지 | 버린 이유 |
|---|---|
| PC 에 직접 설치 | 버전 · 설정이 사람마다 달라지고, 지우고 다시 만들기 어렵다 |
| Kafka + ZooKeeper (3.x) | 4.x 에서 ZooKeeper 가 제거되었다. 예전 구조는 학습 문서로만 남긴다 |
| 처음부터 Kafka 3노드 | 지금 단계(앱 연결)에는 과하다. 장애 실습 단계에서 별도 파일로 |
| Testcontainers 만 사용 | 테스트에는 좋지만 IntelliJ 로 앱을 띄워 직접 만져 보기 어렵다 → 둘 다 쓴다 |
