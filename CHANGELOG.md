# CHANGELOG

형식은 [Keep a Changelog](https://keepachangelog.com/ko/1.1.0/) 를 따르고, 릴리스 단위는 로드맵 단계(P0~P9)다.
항목은 "무엇이 바뀌었나"를 쓰고, 이유는 PR · ADR 로 연결한다.

## [Unreleased]
### Added
- 프로젝트 안내 문서: `CLAUDE.md`, `ARCHITECTURE.md`, `SETUP.md`, `OPERATION.md`, `HANDOVER.md`, `CHANGELOG.md`.
- P3 설계 문서 (`docs/superpowers/specs/2026-10-19-p3-transfer-design.md`).
- DB 문서 `docs/database.md`: ERD, 테이블 정의, 인덱스 용도, 잠금 방식, 시간대 정책(KST 저장), 마이그레이션 규칙, 운영 쿼리.

## [P5-1] 일마감 대사 · 은행 점검 시간 · 알림 — 2026-10-30 ([#15](https://github.com/hyeokjinON/bank-transfer/pull/15), 머지 후 정리 [#16](https://github.com/hyeokjinON/bank-transfer/pull/16))
### Added
- 대사 파일 형식 v0.3 (`ReconFileFormat`, 헤더 · 상세 · 트레일러, 59바이트 레코드), 모의 은행 거래 기록 · 재기동 복원 · 00:05 일마감 · `--close-day` · `--bank-files`.
- `BankMaintenanceWindow` · `SendGate` (점검 시간에는 은행으로 보내지 않음), 웹 503 `BANK_MAINTENANCE` + `Retry-After`.
- Spring Batch 6 잡 `dailyReconciliationJob`(6단계) · `StatementMatcher` · `StatementSettler` · 응답코드 `E002`, Flyway V4(Batch 메타 · `bank_statement_line` · `recon_issue`).
- 스케줄 01 ~ 06시 매시 · `POST /admin/reconciliations`(200 / 409 `RECON_ALREADY_COMPLETED` · `RECON_RUNNING` / 400 `RECON_DATE_NOT_CLOSED`), 마지막 회차의 "대사 미완료" 알림(실패 · 실행 중 모두).
- `AlertNotifier` · `TelegramNotifier` · `LogNotifier`, 설정 `bank.maintenance.*` · `daily-recon.*` · `alert.telegram.*`.
- 실행 구성 `6. API (대사 손 확인)`, `http/daily-recon.http`, Postman 6 폴더, ADR-0010, 전문 규격 v0.3, 테스트 240 → 370개.
### Changed
- `Clock.systemUTC()` 배선을 `Clock` 빈 하나로.
- 기존 경고 3곳(`CONFLICT` · DLT · 결과조회 한도)에 WARN 알림.
- Gradle `run` · `bootRun` 작업 디렉터리 = 저장소 루트.
- 스케줄 풀 2 → 3.
- `spring-boot-starter-batch-jdbc` 추가.
- 부하 측정 실행 구성에 `--daily-recon.schedule.enabled=false`.
### Fixed (최종 리뷰)
- ③ 자동 확정(완료 전이 · `E002` 실패 + 환불)은 `sending_at` 이 아직 대사일 안일 때만 — 상태와 같은 조건부 UPDATE 의 조건(`TransferStore.transition` · `fail` 의 `sentWithin`). 읽은 뒤 다시 보내진 출금을 환불해 이중 지급이 되던 틈을 막았다. 그런 건은 새 판정 `RESENT_LATER`(확정 · 이슈 없음), 요약 `resentLater`.
- ④ · 줄 수에서 빼는 1원인증은 `V…` 이면서 1원인 줄만 — 1원이 아닌 `V…` 줄은 `UNKNOWN_REFERENCE` + 알림.
- 대사 파일 날짜 · 시각 엄격 해석(`20260230` · `240000` 거부).
- Postman 컬렉션 변수 `manualWalletBusy` 중복 키(lint 오류) 제거, 6-4 도 `reconDate` 기본값.
### Fixed (마감 후)
- 수동 대사는 마감된 날(어제 이전, KST)만 — 오늘 이후 400 `RECON_DATE_NOT_CLOSED`(`RECON_DATE_IN_FUTURE` 대체). 오늘을 대사하면 아직 은행 파일에 없는 방금 보낸 출금을 `E002` 환불 → 은행이 그 뒤 처리하면 이중 지급. 손 확인용 설정 `daily-recon.allow-today`(기본 `false`, 실행 구성 6 만 `true`).
- `StatementSettler.settle` 의 범위 인자 기본값 제거(③ 은 D일 범위, ④ 는 `null` 명시) + ③ 쓰기가 범위를 넘기는 배선을 잡 수준 테스트로 고정.

## [P4-2] 잔고 차감 락 전략 비교 — 2026-10-28 ([#13](https://github.com/hyeokjinON/bank-transfer/pull/13), 머지 후 정리 [#14](https://github.com/hyeokjinON/bank-transfer/pull/14))
### Added
- `WalletDebit` 이음새와 전략 4종(`conditional` · `pessimistic` · `optimistic` · `redis`), 설정 `wallet.*` · `GET /actuator/info` 의 전략.
- 오류 코드 `WALLET_BUSY`(503) · `CONCURRENT_UPDATE`(409) + `Retry-After: 1`.
- Redisson 4.8.0, 테스트용 Redis 컨테이너(`redis:7.4-alpine`).
- `load/` (k6 2.3.0 핫스팟 · 분산, 시드 · 정리 · 검증 · `run.sh` · `summarize.sh`), compose `k6` (`load` 프로필), 실행 구성 `5. API (부하 측정)`.
- ADR-0009, 측정 결과 `docs/perf/p4-2-lock-comparison.md` · 원본 `docs/perf/raw/` (24회 + drift 확인 1회).
### Changed
- Hikari 풀 20 명시 · 락 대기 5초(`innodb_lock_wait_timeout`, 모든 커넥션).
- MySQL 1205 · 1213 → 503 (전에는 500).
- `saveWithEvent` 를 바깥 트랜잭션 안에서 부르면 거부.
- 차감 SQL 이 `WalletLedger` 에서 전략으로.

## [P4-1] 원화 잔고 — 2026-10-26 ([#11](https://github.com/hyeokjinON/bank-transfer/pull/11), 머지 후 정리 [#12](https://github.com/hyeokjinON/bank-transfer/pull/12))
### Added
- `wallet` · `wallet_ledger`(V3), 잔고 SQL 한 곳 `WalletLedger`, 지갑 API(`POST /api/v1/wallet/deposits` 관리 · 테스트용 충전, `GET /api/v1/wallet` 잔고 · 보류).
- `TransferStore.fail()`: 실패 확정과 환불을 한 트랜잭션에. 오류 코드 `USER_ID_REQUIRED` · `INSUFFICIENT_BALANCE`.
- ADR-0008, 동시성 테스트(출금 100건 동시 → 정확히 10건 · 잔고 0, 같은 키 20건 → 차감 1번), 손 확인(`http/transfer.http` 9절 · Postman 5 폴더).
### Changed (**호환 깨짐**)
- 모든 출금 · 1원인증 API 에 `X-User-Id` 헤더 필수.
- 출금은 접수할 때 잔고에서 차감 (충전이 필요하고, 모자라면 422).
- 멱등키 UNIQUE 를 `(user_id, idempotency_key)` 로 (사용자마다 같은 키를 쓸 수 있다).
- 받는 계좌는 내가 인증한 계좌만. 남의 출금 조회 · 남의 인증 확인은 404.
- `transition(…, FAILED)` 금지: 실패 확정 4경로 모두 `fail()`.
- `CONFLICT` 경고에 "잔고 그대로".

## [P3-3b] 재시도 토픽 · 제한 재발행 — 2026-10-26 ([#9](https://github.com/hyeokjinON/bank-transfer/pull/9), 머지 후 정리 [#10](https://github.com/hyeokjinON/bank-transfer/pull/10))
### Added
- 재시도 토픽 `transfer.requested-retry-0/1/2` · `transfer.requested-dlt` (5s · 30s · 60s, 설정 `transfer.retry.*`), DLT 핸들러(이유별 로그: 은행 연결 없음 · 잘못된 key 는 WARN, 그 밖의 예외는 자동 재처리가 없어 ERROR + ack), key 가 UUID 가 아니면 재시도 없이 바로 DLT (`InvalidTransferKeyException`).
- 정리 작업 ⓪ 재발행 · 포기: 5분(`transfer.reconcile.republish-after`) 넘게 `RECEIVED` 이고 마지막 재발행 이후 은행에 보내려 한 흔적(`sending_at > republished_at`)이 있으면 아웃박스로 다시 발행, 3번(`max-republish`)을 다 쓰면 `FAILED` `E001`.
- `TransferStore.findRepublishable` · `republish`(조건부 UPDATE 와 아웃박스 INSERT 를 한 트랜잭션에), `InquiryOutcome.Unavailable`, V2 마이그레이션(`republish_count` · `republished_at`).
- 모의 은행 `pause` / `resume` (은행 장애 재현: 붙은 연결도 끊고 같은 포트로 다시 연다).
- ADR-0007, 손 확인(`http/transfer.http` 8절 · Postman 4 폴더 · 실행 구성 `4. API (재발행 손 확인)`).
### Changed
- 블로킹 `kafkaErrorHandler` 삭제: 재시도 중인 메시지가 같은 파티션의 뒤 메시지를 막지 않는다. 수동 ack 는 유지하고 넘긴 레코드의 오프셋은 프레임워크가 단계마다 커밋한다.
- 상태 전이 `RECEIVED → FAILED` 추가 (정리 작업 ⓪ 만 사용, `bank_response_code = E001` = 은행에 간 적 없음).
- 리스너 그룹 · 동시성을 yml 한 곳으로 (`spring.kafka.consumer.group-id`, `listener.concurrency: 3`). 재시도 · DLT 그룹은 접미사로 나뉜다.
- 결과조회 중 연결 없음은 그 주기의 남은 조회를 멈추고 `inquiry_attempts` 를 올리지 않는다.
- 재시도 토픽 전용 1스레드 스케줄러 (`RetryTopicSchedulerWrapper`).

### Fixed (머지 후 정리)
- DLT ERROR 로그의 `cause=` 가 spring-kafka 래퍼 문구 대신 원래 예외 메시지를 보여 준다 (실제 4.1 헤더 모양 "래퍼; 원인" 으로 가른다).

## [P3-3a] 결과조회와 정리 작업 — 2026-10-21 ([#7](https://github.com/hyeokjinON/bank-transfer/pull/7))
### Added
- 전문 규격 v0.2: 이체 개별부에 이용기관 거래번호 20 추가(52 → 72바이트), 신규 업무 `INQ` 결과조회(요청 거래번호 20, 응답 거래번호 20 + 처리결과 4: `0000` 처리됨 / `2001` 원거래 없음) ([protocol.md](docs/protocol.md)).
- `BankInquiryPort` · `BankInquiryAdapter`: 결과조회는 확실한 답(`0000` · `2001`)만 확정하고 나머지(조회 거절 · 시간 초과 · 연결 없음)는 모름으로 둔다. 예외를 던지지 않는다.
- 정리 작업 `TransferReconciler`(10초 고정 지연, 한 번에 100건): 30초 넘게 `SENDING` 인 건 → `UNKNOWN`, `UNKNOWN` → 결과조회로 `COMPLETED` / `FAILED`, 10번 모르면 멈추고 경고(일마감 대사 대상). 대상은 `status` 로 고른다. 설정은 `transfer.reconcile.*`.
- 결과조회는 `sending_at` 30초 유예 뒤에 한다(컨슈머가 3초 타임아웃으로 적은 `UNKNOWN` 도 포함): 느린 은행의 `2001` 로 처리 중인 건이 `FAILED` 가 되지 않게. 결과조회 응답의 거래번호가 물은 것과 다르면 모름으로 둔다.
- `TransferStore.findSendingBefore` · `findUnknownForInquiry` · `recordInquiryAttempt`(조건부, `UNKNOWN` 에서만 증가).
- 모의 은행: 이체의 거래번호를 기억해 결과조회에 답하고, `9990000000002` 는 처리하고 응답만 보내지 않는다(1원 이체는 정상 응답). 테스트에서 `9990000000001` 은 처리도 응답도 하지 않는다.
- ADR-0006, `http/transfer.http` 의 "손 확인: 처리 후 무응답" 절.
### Changed
- 이체 전문 72바이트: 출금이체는 저장된 `transfer.reference`, 1원인증은 API 어댑터가 만든 `V` 거래번호를 싣는다(저장 안 함).
- 컨슈머 경합: 정리 작업이 먼저 `UNKNOWN` 으로 바꿨어도 은행의 확정 결과(완료 · 거절)를 받으면 `UNKNOWN → COMPLETED/FAILED` 로 기록한다. 이미 반대 결과로 확정돼 있으면 `CONFLICT` 경고를 남기고 넘어간다(던지지 않는다).
- 스케줄러 스레드 2개(`spring.task.scheduling.pool.size`): 발행기와 정리 작업이 서로 막지 않는다.
- 스키마 변경 없음 (`inquiry_attempts` 는 V1 에 있었다).

## [P3-2] 출금이체 처리 — 2026-10-21 ([#5](https://github.com/hyeokjinON/bank-transfer/pull/5))
### Added
- 폴링 퍼블리셔 `OutboxRelay`: 미발행 `outbox_event` 를 `FOR UPDATE SKIP LOCKED` 로 잡아 `transfer.requested` 토픽에 발행하고, 브로커 확인 뒤 `published_at` 기록 (적어도 한 번). 간격 · 배치 크기는 `transfer.outbox.relay.*`.
- Kafka 설정: 프로듀서 `acks=all` + 멱등, 컨슈머 수동 ack, 토픽 `transfer.requested`(파티션 3, key = 이체 id), 로컬 Kafka Testcontainers.
- 컨슈머 `TransferRequestedListener` (그룹 `transfer-processor`, 컨슈머 3) 와 처리 규칙 `TransferProcessor`: 조건부 `RECEIVED → SENDING`, 은행 호출은 트랜잭션 밖, 결과 4분류(완료 · 거절 · 결과 모름 · 미전송). 결과 모름(포트 예외 포함)은 `UNKNOWN` 으로 두고 다시 보내지 않는다.
- `TransferStore.transition`: 현재 상태가 맞을 때만 바꾸는 조건부 UPDATE (`sending_at` KST 기록).
- `BankTransferAdapter`: 게이트웨이 예외를 완료 · 거절 · 결과 모름 · 미전송으로 분류.
- 컨슈머 에러 핸들러: 은행에 못 보내면 1초부터 두 배씩(최대 10초) 5번 재시도 후 로그를 남기고 넘어감 (블로킹 재시도, P3-3 에서 재시도 토픽 · DLT 로 교체).
- ADR-0005 (제안), `http/transfer.http` 2-1) · Postman `2-9` 처리 결과 확인 요청.
### Changed
- 게이트웨이: 전문을 쓰기 전 연결이 없으면 `BankNotSentException`(`BankUnavailableException` 하위)으로 구분 — 은행에 안 갔으므로 다시 보내도 안전.
- 요청 형식 오류(본문 JSON 깨짐 · 검증 실패 · 경로 변수 타입 오류 …)도 `code: INVALID_REQUEST`, 그 밖의 표준 Spring 오류는 HTTP 상태 이름을 `code` 로 응답. 잘못된 상태 전이는 409 `ILLEGAL_TRANSITION`.
- `Transfer.toString()` 은 계좌번호 뒤 4자리만 남기고 적요는 찍지 않는다.
- API 실행에 Kafka 가 필요하다.

## [P3-1] 출금이체 접수 — 2026-10-20 ([#3](https://github.com/hyeokjinON/bank-transfer/pull/3))
### Added
- `modules/transfer`: 상태 전이 규칙, 멱등 접수 서비스, 이용기관 거래번호 발급 (Spring·JPA 의존 없음).
- `POST /api/v1/transfers` (Idempotency-Key 필수, 202/200), `GET /api/v1/transfers/{id}`.
- MySQL + Flyway `V1__init.sql` (account_verification · transfer · outbox_event), 시각 KST 저장.
- 트랜잭셔널 아웃박스: 이체와 `TransferRequested` 이벤트를 한 트랜잭션에 저장.
- Testcontainers MySQL 통합 테스트 (공유 컨텍스트), ADR-0004, `http/transfer.http`.
- 멱등 키 · 거래번호는 `ascii_bin` 으로 바이트 비교 (키는 보이는 ASCII 1~64자). 금액이 소수 · 문자열이면 400.
### Changed
- 1원인증 저장소: 메모리 → MySQL (`SELECT … FOR UPDATE`). 재시작해도 인증 기록이 남는다.
- 은행 응답 뒤 처리를 Netty 이벤트 루프 대신 앱 실행기에서 실행 (`thenApplyAsync`).
- API 실행에 MySQL 이 필요하다.

## [P3-0] 로컬 인프라 — 2026-10-16 ([#1](https://github.com/hyeokjinON/bank-transfer/pull/1))
### Added
- `infra/docker-compose.yml`: MySQL 8.4, Kafka 4.1 (KRaft 단일 노드), Redis 7.4 (AOF), 선택 도구 kafka-ui(`--profile tools`).
- `infra/verify.sh`: MySQL 쓰기/읽기(utf8mb4), Kafka 토픽 생성→produce→consume, Redis SET/GET/TTL 확인.
- CI `infra-smoke` 잡: 같은 Compose 파일을 띄우고 `verify.sh` 실행, 실패 시 로그, 항상 `down -v`.
- ADR-0003 로컬 인프라 결정 기록.
### Fixed
- `.gitattributes`: 기존 `gradlew`·`*.bat`·`*.jar` 규칙을 덮어쓴 실수 복원, `*.sh`·`*.yml` LF 고정 추가.
- Windows Git Bash 에서 `verify.sh` 의 Kafka 단계 실패 (MSYS 경로 변환) → `MSYS_NO_PATHCONV=1`. PowerShell `bash`(WSL 실행기) 대신 Git Bash 안내.

## [chore] IntelliJ 실행 구성 — 2026-10-07
### Added
- `.run/` 실행 구성 4개 (Mock Bank + API, 각각 실행, 전체 테스트), `http/account-verification.http` 요청 모음.

## [P0–P2] 은행 전문 게이트웨이와 1원 계좌인증 — 2026-10-01
### Added
- P0: Gradle 9 멀티모듈(Kotlin DSL), Kotlin 2.3, Spring Boot 4.1, Java 21, GitHub Actions `build` 잡.
- P1: 1원 계좌인증 API (`POST /api/v1/account-verifications`, `POST /{id}/confirm`) — 성명조회 → 1원 이체 → 코드 확인, 해시 저장, 5분 유효, 5회 잠금, 서블릿 비동기.
- P2: 모의 은행 FEP(Netty 서버), 게이트웨이(폴링 `IdleStateHandler`, 지수 백오프 재접속, 전문번호 → `CompletableFuture` 응답 매칭, 3초 타임아웃).
- 전문 규격 v0.1 (`docs/protocol.md`), ADR-0001(모듈러 모놀리스 + 포트), ADR-0002(프레이밍 · 응답 매칭).
- 테스트 29개 (직렬화, 쪼개진 TCP 패킷, 동시 50건 매칭, 타임아웃, 재접속, API 통합).
### Changed
- CI 액션을 v6 으로 올려 Node 20 경고 제거.
