# HANDOVER — 지금 상태와 다음 할 일

> 세션(주말 작업)이 끝날 때마다 이 파일을 갱신한다. 다음 사람(또는 다음 세션의 AI)이 이것만 읽고 이어갈 수 있어야 한다.
> 마지막 갱신: 2026-10-19

## 1. 지금 어디까지 왔나
| 단계 | 상태 | 비고 |
|---|---|---|
| P0 골격 · P1 1원인증 · P2 Netty 모의 은행 | ✅ 완료 (2026-10-01) | 테스트 29개 |
| P3-0 로컬 인프라 | ✅ 완료 (2026-10-16, PR #1) | 로컬(Windows 11 + Docker Desktop)·CI 모두 `verify.sh` 통과 |
| P3 입출금 비동기 | 🟡 설계 문서 검토 중 | 아래 §2 |
| P4 ~ P9 | ⬜ 예정 | README 로드맵 |

## 2. 진행 중: P3 설계 (합의된 것)
설계 섹션 1~5 모두 승인(2026-10-19) → **[설계 문서](docs/superpowers/specs/2026-10-19-p3-transfer-design.md)** 로 정리, 사람 검토 대기.
검토가 끝나면 P3-1 구현 계획 → `feat/p3-1-transfer-intake` 브랜치.

| 결정 | 내용 |
|---|---|
| 분할 | P3-1 접수 / P3-2 처리 / P3-3 실패, 단계마다 브랜치·PR·velog 1편 |
| 범위 | 서비스 모계좌 → 사용자 계좌 **출금이체만**. 잔액·락 비교는 P4 |
| 수취 계좌 | **1원인증 VERIFIED 계좌만**. 그래서 1원인증 저장소도 MySQL(JPA) 로 이전 (P3-1) |
| 아웃박스 발행 | **폴링 퍼블리셔** (0.5초, `FOR UPDATE SKIP LOCKED`), 적어도 한 번 발행. Debezium 은 검토 후 보류 |
| 모듈 | `modules/transfer` 순수 Kotlin. 포트 `TransferStore.saveWithEvent`, `VerifiedAccounts`, `BankTransferPort` |
| 상태 | `RECEIVED → SENDING → COMPLETED / FAILED / UNKNOWN`, `UNKNOWN → 결과조회 → 확정` |
| 멱등 | API: `Idempotency-Key` 필수 + `UNIQUE` + `request_hash` (다른 내용 재사용 422). 컨슈머: **상태 조건부 UPDATE** (`processed_event` 테이블은 쓰지 않기로 수정) |
| 실패 | 거절 → FAILED / 연결 없음(미전송) → RECEIVED 로 되돌리고 `@RetryableTopic` (1s·10s·60s) → DLT / 타임아웃 → **재전송 금지**, UNKNOWN → 결과조회 |
| 규격 v0.2 | 이체 전문에 `이용기관 거래번호 AN(20)` 추가, 신규 업무 `INQ` 결과조회 (`0000` 처리됨 / `2001` 원거래 없음) |
| 정리 작업 | 10초마다: 30초 넘은 SENDING → UNKNOWN, UNKNOWN → 결과조회 |
| 테스트 | 도메인 단위(가짜 포트) / Testcontainers MySQL 어댑터 / MySQL+Kafka+모의 은행 흐름 통합 + Awaitility. 단계별 '증명 테스트' 목록은 설계 문서 §7 |

## 3. 다음 할 일 (순서대로)
1. 설계 문서 검토 (수정 요청 반영).
2. 구현 계획 작성 → `feat/p3-1-transfer-intake` 브랜치에서 P3-1 구현 (TDD) → PR → velog `#3`.
3. ADR-0004 (아웃박스 · 멱등) 작성, 상태 "제안".

## 4. 알려진 한계 · 주의
- 1원인증 저장소가 아직 메모리 (`InMemoryVerificationRepository`) → 재시작하면 인증 기록이 사라진다. P3-1 에서 해결.
- 게이트웨이 응답 매칭은 단일 연결 · 단일 인스턴스 전제 (ADR-0002 한계).
- Kafka 는 단일 노드라 복제 · ISR 장애 실험 불가 (ADR-0003 한계).
- Windows: 셸 스크립트는 Git Bash 로. PowerShell `bash` 는 WSL 실행기라 실패한다.

## 5. 최근 배운 것 (같은 실수 반복 방지)
- 설정 파일(`.gitattributes` 등)은 추가할 때도 먼저 읽고 합친다. 덮어써서 기존 규칙을 지운 적이 있다.
- CI(리눅스) 통과 ≠ 개발 PC 통과. Git Bash 는 `/opt/...` 인자를 Windows 경로로 바꾼다 → `MSYS_NO_PATHCONV=1`.

## 6. 관련 링크
- 블로그: [#1 Netty 게이트웨이와 1원인증](https://velog.io/@hyeokjinon/bank-transfer-01-netty-gateway-1won-verification) · #2 로컬 인프라 (비공개 검토 중)
- PR: [#1 P3-0](https://github.com/hyeokjinON/bank-transfer/pull/1)
