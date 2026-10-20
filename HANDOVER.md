# HANDOVER — 지금 상태와 다음 할 일

> 세션(주말 작업)이 끝날 때마다 이 파일을 갱신한다. 다음 사람(또는 다음 세션의 AI)이 이것만 읽고 이어갈 수 있어야 한다.
> 마지막 갱신: 2026-10-20

## 1. 지금 어디까지 왔나
| 단계 | 상태 | 비고 |
|---|---|---|
| P0 골격 · P1 1원인증 · P2 Netty 모의 은행 | ✅ 완료 (2026-10-01) | 테스트 29개 |
| P3-0 로컬 인프라 | ✅ 완료 (2026-10-16, PR #1) | 로컬(Windows 11 + Docker Desktop)·CI 모두 `verify.sh` 통과 |
| P3-1 접수 | ✅ 완료 (2026-10-20, [#3](https://github.com/hyeokjinON/bank-transfer/pull/3)) | ADR-0004 채택, 테스트 73개. 손 확인: 로컬 API · Postman 컬렉션으로 접수 흐름, DB 에서 아웃박스 미발행 · KST 저장 확인 |
| P3-2 처리 · P3-3 실패 | ⬜ 다음 | 설계 문서 §5·§6 |
| P4 ~ P9 | ⬜ 예정 | README 로드맵 |

## 2. P3 설계 (합의된 것)
설계 섹션 1~5 모두 승인(2026-10-19) → **[설계 문서](docs/superpowers/specs/2026-10-19-p3-transfer-design.md)** 로 정리.
P3-1 은 머지됐다 (PR #3). 아래 표 중 P3-2 · P3-3 항목은 아직 구현 전이다.

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
| 시간대 | DB 시각은 **KST** `DATETIME(6)`, 도메인은 `Instant`, 변환은 JDBC·Hibernate 설정 한 곳 ([docs/database.md §2](docs/database.md)) |
| 정리 작업 | 10초마다: 30초 넘은 SENDING → UNKNOWN, UNKNOWN → 결과조회 |
| 테스트 | 도메인 단위(가짜 포트) / Testcontainers MySQL 어댑터 / MySQL+Kafka+모의 은행 흐름 통합 + Awaitility. 단계별 '증명 테스트' 목록은 설계 문서 §7 |

## 3. 다음 할 일 (순서대로)
1. velog `#3` (P3-1 접수: 멱등키 · 아웃박스 · 최종 리뷰에서 잡은 collation · 금액 강제 변환).
2. P3-2 구현 계획 작성 (폴링 퍼블리셔 · Kafka · 컨슈머). 아래 "P3-2 로 넘기는 것"을 계획의 제약에 넣는다.
3. P3-3 (재시도 토픽 · DLT · 결과조회, 규격 v0.2) 구현 계획.

### P3-2 로 넘기는 것 (P3-1 리뷰에서 나온 항목)
- `Transfer` 의 `toString` 이 계좌번호 · 적요를 그대로 출력 → 컨슈머가 로그를 남기기 전에 마스킹.
- `TransferEntity.from()` 은 INSERT 전용 (version = null). 상태 변경은 조회한 엔티티를 고치거나 조건부 UPDATE 로.
- `moveTo` 로 `bankResponseCode` 를 설정할 수 없다. `IllegalTransition` 은 HTTP 매핑이 없어 500.
- 깨진 JSON · 누락 필드 · 잘못된 UUID 는 `code` 없는 400 → `INVALID_REQUEST` 핸들러.
- `TransferService.request` / `JpaTransferStore.saveWithEvent` 를 바깥 트랜잭션 안에서 호출하지 않는다 (KDoc).
- UNIQUE 위반 구분은 오류 메시지의 제약 이름에 의존. 경합 롤백 때 드문 데드락(1213)은 500 → 재시도 안내 또는 매핑.
- 동시 중복 요청마다 Hibernate 가 클라이언트 키를 WARN 으로 남긴다 (예상된 동작, 로그 잡음).

## 4. 알려진 한계 · 주의
- 게이트웨이 응답 매칭은 단일 연결 · 단일 인스턴스 전제 (ADR-0002 한계).
- Kafka 는 단일 노드라 복제 · ISR 장애 실험 불가 (ADR-0003 한계).
- Windows: 셸 스크립트는 Git Bash 로. PowerShell `bash` 는 WSL 실행기라 실패한다.

## 5. 최근 배운 것 (같은 실수 반복 방지)
- 설정 파일(`.gitattributes` 등)은 추가할 때도 먼저 읽고 합친다. 덮어써서 기존 규칙을 지운 적이 있다.
- CI(리눅스) 통과 ≠ 개발 PC 통과. Git Bash 는 `/opt/...` 인자를 Windows 경로로 바꾼다 → `MSYS_NO_PATHCONV=1`.

## 6. 관련 링크
- 블로그: [#1 Netty 게이트웨이와 1원인증](https://velog.io/@hyeokjinon/bank-transfer-01-netty-gateway-1won-verification) · #2 로컬 인프라 (비공개 검토 중)
- PR: [#1 P3-0](https://github.com/hyeokjinON/bank-transfer/pull/1)
