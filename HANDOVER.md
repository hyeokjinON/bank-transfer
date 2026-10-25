# HANDOVER — 지금 상태와 다음 할 일

> 세션(주말 작업)이 끝날 때마다 이 파일을 갱신한다. 다음 사람(또는 다음 세션의 AI)이 이것만 읽고 이어갈 수 있어야 한다.
> 마지막 갱신: 2026-10-22

## 1. 지금 어디까지 왔나
| 단계 | 상태 | 비고 |
|---|---|---|
| P0 골격 · P1 1원인증 · P2 Netty 모의 은행 | ✅ 완료 (2026-10-01) | 테스트 29개 |
| P3-0 로컬 인프라 | ✅ 완료 (2026-10-16, PR #1) | 로컬(Windows 11 + Docker Desktop)·CI 모두 `verify.sh` 통과 |
| P3-1 접수 | ✅ 완료 (2026-10-20, [#3](https://github.com/hyeokjinON/bank-transfer/pull/3)) | ADR-0004 채택, 테스트 73개. 손 확인: 로컬 API · Postman 컬렉션으로 접수 흐름, DB 에서 아웃박스 미발행 · KST 저장 확인 |
| P3-2 처리 | ✅ 완료 (2026-10-21, [#5](https://github.com/hyeokjinON/bank-transfer/pull/5)) | ADR-0005 채택, 테스트 107개. CI `build` · `infra-smoke` 통과. 손 확인 절차: Postman 2-1 → 2-9 `COMPLETED`, kafka-ui 로 메시지 · 컨슈머 Lag (OPERATION.md Kafka 절) |
| P3-3a 결과조회 · 정리 작업 | ✅ 완료 (2026-10-21, [#7](https://github.com/hyeokjinON/bank-transfer/pull/7)) | ADR-0006 채택, 테스트 135개. CI `build` · `infra-smoke` 통과. 손 확인: Postman 3-1 → 3-5 로 무응답 계좌 출금 5건 모두 `COMPLETED 0000`, `sending_at` 뒤 32~38초에 확정(30초 유예 + 주기), `inquiry_attempts` 0 |
| P3-3b 재시도 | 🟡 PR [#9](https://github.com/hyeokjinON/bank-transfer/pull/9) 리뷰 중 (손 확인 대기) | ADR-0007 제안(머지 뒤 채택), 테스트 161개. 재시도 토픽 · DLT · 제한 재발행 · `FAILED E001`. 손 확인 절차: `http/transfer.http` 8절 · Postman 4 폴더 · 실행 구성 `4. API (재발행 손 확인)` — 머지 전에 사람이 한다. 범위 밖으로 남긴 것은 아래 "P3-3b 범위 밖으로 남긴 것" |
| P4 ~ P9 | ⬜ 예정 | README 로드맵 |

## 2. P3 설계 (합의된 것)
설계 섹션 1~5 모두 승인(2026-10-19) → **[설계 문서](docs/superpowers/specs/2026-10-19-p3-transfer-design.md)** 로 정리.
P3-1 (PR #3) · P3-2 (PR #5) · P3-3a (PR #7) 는 머지됐다. P3-3b (PR #9) 는 구현을 마치고 리뷰 중이다.

| 결정 | 내용 |
|---|---|
| 분할 | P3-1 접수 / P3-2 처리 / P3-3a 결과조회 · 정리 작업 / P3-3b 재시도 토픽 · DLT · 제한 재발행, 단계마다 브랜치·PR·velog 1편 |
| 범위 | 서비스 모계좌 → 사용자 계좌 **출금이체만**. 잔액·락 비교는 P4 |
| 수취 계좌 | **1원인증 VERIFIED 계좌만**. 그래서 1원인증 저장소도 MySQL(JPA) 로 이전 (P3-1) |
| 아웃박스 발행 | **폴링 퍼블리셔** (0.5초, `FOR UPDATE SKIP LOCKED`), 적어도 한 번 발행. Debezium 은 검토 후 보류 |
| 모듈 | `modules/transfer` 순수 Kotlin. 포트 `TransferStore.saveWithEvent` + `transition`, `VerifiedAccounts`, `BankTransferPort` |
| 상태 | `RECEIVED → SENDING → COMPLETED / FAILED / UNKNOWN`, `UNKNOWN → 결과조회 → 확정`, `RECEIVED → FAILED`(E001, 재발행 한도 뒤, P3-3b) |
| 멱등 | API: `Idempotency-Key` 필수 + `UNIQUE` + `request_hash` (다른 내용 재사용 422). 컨슈머: **상태 조건부 UPDATE** (`processed_event` 테이블은 쓰지 않기로 수정) |
| 실패 | 거절 → FAILED / 연결 없음(미전송) → RECEIVED 로 되돌리고 `@RetryableTopic` (5s·30s·60s) → DLT → 정리 작업이 최대 3번 재발행 → `FAILED E001` / 타임아웃 → **재전송 금지**, UNKNOWN → 결과조회 |
| 규격 v0.2 (P3-3a 구현) | 이체 전문에 `이용기관 거래번호 AN(20)` 추가, 신규 업무 `INQ` 결과조회 (`0000` 처리됨 / `2001` 원거래 없음). 1원인증은 API 어댑터가 만든 `V` 번호(저장 안 함) |
| 시간대 | DB 시각은 **KST** `DATETIME(6)`, 도메인은 `Instant`, 변환은 JDBC·Hibernate 설정 한 곳 ([docs/database.md §2](docs/database.md)) |
| 정리 작업 (P3-3a 구현) | 10초마다: 30초 넘은 `status = SENDING` → UNKNOWN, `sending_at` 이 30초 넘은 UNKNOWN → 결과조회(묻기만 함) → 확정. 10번 모르면 멈추고 경고 → 일마감 대사. 스케줄러 스레드 2개 |
| 정리 작업 ⓪ 재발행 (P3-3b 구현) | `RECEIVED` + `updated_at` 5분 전 + 은행 시도 흔적(`sending_at > republished_at`) → 아웃박스로 재발행(`republish_count + 1`), 3회를 다 쓰면 `FAILED E001`. 결과조회 중 연결 없음은 그 주기 조회를 멈추고 횟수를 세지 않음. 근거: [ADR-0007](docs/adr/0007-retry-topic-and-bounded-republish.md) |
| 테스트 | 도메인 단위(가짜 포트) / Testcontainers MySQL 어댑터 / MySQL+Kafka+모의 은행 흐름 통합 + Awaitility. 단계별 '증명 테스트' 목록은 설계 문서 §7 |

## 3. 다음 할 일 (순서대로)
1. **P3-3b 손 확인** (PR #9 머지 전): `http/transfer.http` 8절 순서대로 — 은행 Stop → 접수 → DLT → 은행 Run → 재발행으로 `COMPLETED` (입금 1건). 끝나면 이 표의 P3-3b 를 ✅ 로, ADR-0007 을 채택으로 바꾼다.
2. velog `#5` (P3-3a: 모르면 묻는다 — 거래번호, 정리 작업, 경합, 최종 리뷰에서 잡은 "유예 없는 결과조회" 버그). `#2` ~ `#4` 는 비공개 초안으로 올라가 있다.
3. velog `#6` (P3-3b: 재시도 토픽 · 은행 시도 흔적 기준 재발행 · 오프셋 커밋 함정 확인).
4. P4 (잔액 동시성) 계획.

### P3-3b 범위 밖으로 남긴 것
- 게이트웨이 쓰기 실패 경로(`write failed` → `UNKNOWN`)의 테스트를 추가한다.
- 릴레이는 인스턴스가 여럿이면 키별 순서를 보장하지 않는다 (`ORDER BY id` 라도 `SKIP LOCKED` 로 건너뛴다). 이벤트 종류를 늘리기 전에 이 점을 정리한다.
- (P3-3a 리뷰) 은행이 응답하지 않으면 한 번의 정리 작업이 최대 100건 × 응답 타임아웃(3초) ≈ 5분 걸린다 (스케줄러 스레드가 2개라 발행기는 안 막힌다). 연결 없음은 이제 바로 멈춘다.
- (P3-3a 리뷰) 늦게 온 이체 응답 로그(`late or unknown response`)에 거래번호가 없다 → 거래번호를 남겨 대사에 쓰게 한다.
- (P3-3a 리뷰) 정리 작업 · 발행기 스케줄러의 실패 로그가 `e.message` 만 남긴다 → 예외 전체를 남긴다. `@EnableScheduling` 이 `MessagingConfiguration` 에 붙어 있어 그 클래스에 묶여 있다.
- (P3-3b 최종 리뷰) 재시작 직후 정리 작업 ⓪ 이 바로 돌아, 앱이 5분 넘게 내려가 있던 동안의 시간도 대기로 친다 → 재시도 중이던 건이 재발행 1회를 일찍 쓰거나(이미 3회면 바로 `E001`) 한다. 돈은 안전하다(은행에 간 적 없음). 개선: 기동 후 `republish-after` 동안 ⓪ 건너뛰기.
- (P3-3b 최종 리뷰) 인스턴스 간 시계 차이: `republished_at`(정리 작업 시계) vs `sending_at`(컨슈머 시계). 정리 작업 쪽 시계가 빠르면 재발행 기준이 영영 안 맞아 경고 없이 `RECEIVED` 로 남을 수 있다. 지금은 단일 인스턴스.
- (P3-3b 최종 리뷰) DLT 에 온 이유가 은행 연결 없음이 아닌 이체(DB 장애 등)는 자동 재처리가 없다. ERROR 로그를 보고 수동 확인 · 재처리한다.
- 은행 점검 시간대에는 접수 자체를 거절한다 (P5 후보): 지금은 26분이 넘는 점검이면 그 사이 접수된 출금이 `FAILED E001` 이 된다.

### P3-2 에서 남은 것 (P3-1 리뷰 항목 중 아직 안 한 것)
- `TransferService.request` / `JpaTransferStore.saveWithEvent` 를 바깥 트랜잭션 안에서 호출하지 않는다 (KDoc).
- UNIQUE 위반 구분은 오류 메시지의 제약 이름에 의존. 경합 롤백 때 드문 데드락(1213)은 500 → 재시도 안내 또는 매핑.
- 동시 중복 요청마다 Hibernate 가 클라이언트 키를 WARN 으로 남긴다 (예상된 동작, 로그 잡음).

## 4. 알려진 한계 · 주의
- (P3-3b) 은행 장애가 약 26분((재시도 약 95초 + 대기 5분) × 4 ≈ 26분, 정리 작업 주기 최대 10초 별도)보다 길면 그 사이 접수된 출금은 `FAILED E001` 이 된다. 복구 직후 밀린 건이 한꺼번에 은행에 간다(컨슈머 3개가 상한). 재시도 토픽 컨슈머가 5분 넘게 밀리면 재발행과 재시도가 겹칠 수 있다 (은행 호출은 조건부 전이로 한 번뿐).
- (P3-3a) `UNKNOWN` · 멈춘 `SENDING` 은 정리 작업이 결과조회로 확정한다. 단 원거래가 은행 안에서 아직 처리 중일 때 조회하면 `2001` 이 올 수 있다. `sending_at` 30초 유예(정리 작업이 바꾼 `UNKNOWN` 과 컨슈머가 적은 `UNKNOWN` 모두)로 줄였지만, 은행이 30초보다 더 늦으면 `2001` → `FAILED` 로 확정된 뒤 늦은 이체 응답은 로그만 남는다(최종 안전망은 P5 대사). 10번 물어도 모르는 건은 `UNKNOWN` 으로 남아 경고만 난다. 인스턴스가 여러 대면 같은 `UNKNOWN` 을 중복 조회할 수 있다(결과는 같다).
- (P3-2) 컨슈머 스레드가 은행 응답을 블로킹으로 기다린다(최대 3초) → P4 에서 측정, P6 · P7 에서 비교.
- 게이트웨이 응답 매칭은 단일 연결 · 단일 인스턴스 전제 (ADR-0002 한계).
- Kafka 는 단일 노드라 복제 · ISR 장애 실험 불가 (ADR-0003 한계).
- Windows: 셸 스크립트는 Git Bash 로. PowerShell `bash` 는 WSL 실행기라 실패한다.

## 5. 최근 배운 것 (같은 실수 반복 방지)
- 설정 파일(`.gitattributes` 등)은 추가할 때도 먼저 읽고 합친다. 덮어써서 기존 규칙을 지운 적이 있다.
- CI(리눅스) 통과 ≠ 개발 PC 통과. Git Bash 는 `/opt/...` 인자를 Windows 경로로 바꾼다 → `MSYS_NO_PATHCONV=1`.

## 6. 관련 링크
- 블로그: [#1 Netty 게이트웨이와 1원인증](https://velog.io/@hyeokjinon/bank-transfer-01-netty-gateway-1won-verification) · #2 로컬 인프라 (비공개 검토 중)
- PR: [#1 P3-0](https://github.com/hyeokjinON/bank-transfer/pull/1)
