# CHANGELOG

형식은 [Keep a Changelog](https://keepachangelog.com/ko/1.1.0/) 를 따르고, 릴리스 단위는 로드맵 단계(P0~P9)다.
항목은 "무엇이 바뀌었나"를 쓰고, 이유는 PR · ADR 로 연결한다.

## [Unreleased]
### Added
- 프로젝트 안내 문서: `CLAUDE.md`, `ARCHITECTURE.md`, `SETUP.md`, `OPERATION.md`, `HANDOVER.md`, `CHANGELOG.md`.
- P3 설계 문서 (`docs/superpowers/specs/2026-10-19-p3-transfer-design.md`).
- DB 문서 `docs/database.md`: ERD, 테이블 정의, 인덱스 용도, 잠금 방식, 시간대 정책(KST 저장), 마이그레이션 규칙, 운영 쿼리.

### 진행 중
- P3 입출금 비동기 처리 설계 (3단계 분할, 폴링 퍼블리셔, 인증 계좌로만 출금). → [HANDOVER](HANDOVER.md)

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
