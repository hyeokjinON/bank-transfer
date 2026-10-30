# HANDOVER — 지금 상태와 다음 할 일

> 세션(주말 작업)이 끝날 때마다 이 파일을 갱신한다. 다음 사람(또는 다음 세션의 AI)이 이것만 읽고 이어갈 수 있어야 한다.
> 마지막 갱신: 2026-10-29

## 1. 지금 어디까지 왔나
| 단계 | 상태 | 비고 |
|---|---|---|
| P0 골격 · P1 1원인증 · P2 Netty 모의 은행 | ✅ 완료 (2026-10-01) | 테스트 29개 |
| P3-0 로컬 인프라 | ✅ 완료 (2026-10-16, PR #1) | 로컬(Windows 11 + Docker Desktop)·CI 모두 `verify.sh` 통과 |
| P3-1 접수 | ✅ 완료 (2026-10-20, [#3](https://github.com/hyeokjinON/bank-transfer/pull/3)) | ADR-0004 채택, 테스트 73개. 손 확인: 로컬 API · Postman 컬렉션으로 접수 흐름, DB 에서 아웃박스 미발행 · KST 저장 확인 |
| P3-2 처리 | ✅ 완료 (2026-10-21, [#5](https://github.com/hyeokjinON/bank-transfer/pull/5)) | ADR-0005 채택, 테스트 107개. CI `build` · `infra-smoke` 통과. 손 확인 절차: Postman 2-1 → 2-9 `COMPLETED`, kafka-ui 로 메시지 · 컨슈머 Lag (OPERATION.md Kafka 절) |
| P3-3a 결과조회 · 정리 작업 | ✅ 완료 (2026-10-21, [#7](https://github.com/hyeokjinON/bank-transfer/pull/7)) | ADR-0006 채택, 테스트 135개. CI `build` · `infra-smoke` 통과. 손 확인: Postman 3-1 → 3-5 로 무응답 계좌 출금 5건 모두 `COMPLETED 0000`, `sending_at` 뒤 32~38초에 확정(30초 유예 + 주기), `inquiry_attempts` 0 |
| P3-3b 재시도 | ✅ 완료 (2026-10-26, [#9](https://github.com/hyeokjinON/bank-transfer/pull/9)) | ADR-0007 채택, 테스트 161개. CI `build` · `infra-smoke` 통과. 재시도 토픽 · DLT · 제한 재발행 · `FAILED E001`. 손 확인: 은행을 내린 채 접수한 2건이 재시도 토픽 경로로 은행 복구 뒤 `COMPLETED 0000` (재발행 0회, `sending_at` 은 접수 약 95초 뒤 = 5s·30s·60s 마지막 재시도). 재발행 경로(DLT → 재발행 → `COMPLETED` / 3회 뒤 `E001`)는 자동 흐름 테스트로만 증명 — 손으로 보려면 실행 구성 `4. API (재발행 손 확인)` 으로 은행을 15초 넘게 내려 둔다 |
| P4-1 원화 잔고 | ✅ 완료 (2026-10-26, [#11](https://github.com/hyeokjinON/bank-transfer/pull/11)) | ADR-0008 채택, 테스트 206개. CI `build` · `infra-smoke` 통과. 손 확인: Postman 5 폴더로 `user-p41` 출금 19건(COMPLETED 6 · 은행 거절 FAILED 1001 13) — 환불 13건이 거절 건수와 정확히 같고, 잔고 1,500,000 = 원장 합 |
| P4-2 락 전략 비교 | ✅ 완료 (2026-10-28, [#13](https://github.com/hyeokjinON/bank-transfer/pull/13)) | ADR-0009 채택, 테스트 240개. CI `build` · `infra-smoke` 통과. 차감 전략 4종 · 409/503 · k6 도구. 측정 24회 ([docs/perf](docs/perf/p4-2-lock-comparison.md)): 핫스팟 접수 조건부 106/s > 비관적 99 > Redis 81 > 낙관적 45(409 77%), 분산은 차이 없음. 손 확인: `redis` 전략 + Redis 정지 상태에서 API 기동 → 출금 접수 503 `WALLET_BUSY` + `Retry-After: 1`, 남은 출금 없음 (Postman 5-8 · curl, 인증 사용자 `user-p41`). `manualWalletBusy` 를 켜지 않고 Send 하면 5-8 이 나가지 않음(응답 없음)도 확인 |
| P5-1 일마감 대사 | ✅ 완료 (2026-10-30, [#15](https://github.com/hyeokjinON/bank-transfer/pull/15)) | ADR-0010 채택, 테스트 370개. CI `build` · `infra-smoke` 통과. 대사 파일 v0.3 · 점검 시간 · Spring Batch 잡 · 알림. 손 확인 (`http/daily-recon.http` 순서를 curl 로, `user-1`): 정상 출금 `COMPLETED` · 무응답 계좌 출금 `UNKNOWN` → `--close-day=20261030` → 실행 구성 6 으로 오늘 대사 200 `COMPLETED` (출금 2 · 일치 1 · 자동 완료 1 · 불일치 0, 0.8초), `recon_issue` 에 `AUTO_COMPLETED` 한 줄, 무응답 건 `COMPLETED 0000`, 잔고 97,000 = 원장(충전 100,000 − 출금 3,000). 다시 보내면 409 `RECON_ALREADY_COMPLETED`, 2099-12-31 은 400 `RECON_DATE_NOT_CLOSED`. 기본 구성 + 지금을 포함한 점검(09:50 ~ 10:40)으로 다시 띄우면 오늘 날짜 대사도 400 `RECON_DATE_NOT_CLOSED`, 출금 · 1원인증 503 `BANK_MAINTENANCE` + `Retry-After: 2339` (10:40 까지 남은 초), 잔고 조회 200. 봇을 설정하지 않아 알림은 로그(`ALERT … [INFO] 일마감 대사 2026-10-30 완료 — 불일치 없음`) |
| P5-2 ~ P9 | ⬜ 예정 | README 로드맵 |

## 2. P3 설계 (합의된 것)
설계 섹션 1~5 모두 승인(2026-10-19) → **[설계 문서](docs/superpowers/specs/2026-10-19-p3-transfer-design.md)** 로 정리.
P3-1 (PR #3) · P3-2 (PR #5) · P3-3a (PR #7) · P3-3b (PR #9) 모두 머지됐다. **P3 완료.**

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

### P4-1 설계 (합의된 것)
설계 섹션 모두 승인(2026-10-26) → **[설계 문서](docs/superpowers/specs/2026-10-26-p4-1-wallet-balance-design.md)**. 구현 중 조정한 것(출금 INSERT 를 차감보다 먼저)은 설계 문서 §4-1 에 메모했다.

| 결정 | 내용 |
|---|---|
| 잔액 모델 | 사용자 원화 잔고 `wallet` + append-only 원장 `wallet_ledger` — "잔고 = 원장 합"을 언제든 맞춰 본다 (P5 대사의 근거) |
| 사용자 식별 | `X-User-Id` 헤더 (영숫자 · 하이픈 1~36자), 없거나 틀리면 400 `USER_ID_REQUIRED`. 웹 계층 `RequestUserResolver` 한 곳. 값은 신뢰(인증은 범위 밖) |
| 차감 시점 · 동시성 | **접수 트랜잭션**에서 조건부 UPDATE 한 문장(`WHERE balance >= ?`) + `CHECK (balance >= 0)`. 0건이면 롤백, 422 `INSUFFICIENT_BALANCE`. 잔고 SQL 은 `WalletLedger` 한 곳 (P4-2 이음새) |
| 환불 | 실패 확정 4경로를 `TransferStore.fail()` 하나로 — 조건부 전이 1건일 때만 같은 트랜잭션에서 환불 + `REFUND`. `transition(…, FAILED)` 는 거부. `RECEIVED` · `SENDING` · `UNKNOWN` 은 보류, `COMPLETED` 는 확정, `CONFLICT` 는 잔고를 건드리지 않고 경고 |
| 멱등키 | UNIQUE 를 `(user_id, idempotency_key)` 로 |
| 계좌 소유 | 1원인증에 `user_id`. 출금은 내가 인증한 계좌만, 확인 · 조회는 본인 것만 (남이면 404) |
| 근거 | [ADR-0008](docs/adr/0008-wallet-debit-on-intake-refund-on-failure.md) (제안 — 머지 때 채택으로) |

### P4-2 설계 (합의된 것)
설계 섹션 모두 승인(2026-10-27) → **[설계 문서](docs/superpowers/specs/2026-10-27-p4-2-lock-comparison-design.md)**. 근거: [ADR-0009](docs/adr/0009-wallet-debit-strategies-and-lock-comparison.md) (채택).

| 결정 | 내용 |
|---|---|
| 전략 선택 | `wallet.debit-strategy = conditional \| pessimistic \| optimistic \| redis` (기본 `conditional`). 기동 때 하나만 — 요청마다 고르면 운영 API 에 실험 손잡이가 생기고 네 빈이 자원을 나눠 쓴다 |
| 이음새 | `WalletDebit` 하나에 바깥(`around`, 트랜잭션을 감쌈) · 안(`withdraw`, 트랜잭션 안) 두 지점. 낙관적 재시도와 Redis 락은 트랜잭션 바깥이어야 한다 |
| 실패 응답 | 낙관적 재시도 소진 409 `CONCURRENT_UPDATE`, 락을 못 잡음 503 `WALLET_BUSY`, 둘 다 `Retry-After: 1`. 잔고 부족(422)과 구분 |
| 락 대기 한도 | MySQL `innodb_lock_wait_timeout` 을 커넥션마다 5초 (기본 50초). 넘으면 503. 네 전략에 같은 조건 |
| 커넥션 풀 | Hikari `maximum-pool-size` 20 명시. 네 전략에 같은 조건 |
| Redis 장애 | `redis` 전략에서 Redis 에 닿지 않으면 503 (fail-closed) — 돈이 걸린 판단을 락 없이 진행하지 않는다 |
| 환불 · 충전 | 전략과 무관하게 원자적 UPDATE (`balance = balance + :amt`). 비교 대상은 차감이다 |
| 안전망 | `CHECK (balance >= 0)` 유지 — 어느 전략이 틀려도 DB 가 음수를 막는다 |
| 스키마 | 바꾸지 않는다 — `wallet.version` 은 V3 부터 있고 모든 UPDATE 가 이미 올린다 |

### P5-1 설계 (합의된 것)
설계 섹션 모두 승인(2026-10-29) → **[설계 문서](docs/superpowers/specs/2026-10-29-p5-1-daily-reconciliation-design.md)**. 근거: [ADR-0010](docs/adr/0010-daily-reconciliation.md) (제안 — 머지 때 채택으로).

| 결정 | 내용 |
|---|---|
| 은행 쪽 기록 | 모의 은행이 만든 **일자별 대사 파일**(고정길이 EUC-KR, 헤더 · 상세 · 트레일러, 규격 v0.3). 전문으로 내역을 페이징해 받는 것은 실제와 거리가 있고 Batch 의 파일 읽기 · 재시작을 못 쓴다 |
| 자동 보정 범위 | **결과를 모르는 출금(`UNKNOWN` · `SENDING`)만** 파일로 확정 (있으면 `COMPLETED`, 없으면 `FAILED E002` + 환불). 이미 확정된 건은 기록 + 알림 — 뒤집으면 보정이 새 사고가 된다. 확정은 `sending_at` 이 아직 D일 안일 때만(같은 조건부 UPDATE) — 다시 보내진 출금은 `RESENT_LATER` 로 넘긴다. 1원인증 줄 = `V…` 이면서 1원만(1원이 아닌 `V…` 는 `UNKNOWN_REFERENCE`) |
| 실행 위치 | **API 앱 안의 Spring Batch 잡**(JDBC 잡 저장소). 확정 · 환불에 기존 `TransferStore` 경로를 그대로 쓴다. 별도 배치 앱은 P8 |
| 중복 실행 · 재시작 | 잡 파라미터 `reconDate`, 날짜마다 인스턴스 하나: 끝난 날짜는 거부(409), 실패하면 멈춘 곳부터, 동시 실행은 하나(`SingleRunGuard` 가 Spring Batch 6 시작 경로의 틈을 막음) |
| 수동 대사 날짜 | **마감된 날(어제 이전, KST)만** — 오늘 이후 400 `RECON_DATE_NOT_CLOSED`. 오늘을 대사하면 아직 파일에 없는 출금을 `E002` 환불 → 은행이 그 뒤 처리하면 이중 지급. 손 확인만 `daily-recon.allow-today=true`(실행 구성 6) |
| 날짜 경계 | **은행 점검 시간 23:50 ~ 00:10 (KST)**: 은행을 건드리는 웹 요청 503 + 은행 송신 보류 → "D일 송신" = "파일 D" |
| 알림 | `AlertNotifier` + 텔레그램 Bot API(토큰 · chat id 가 둘 다 있을 때) / 로그. 실패는 삼키고 대사에 영향 없음. 기존 경고 3곳도 WARN |
| 이름 | 일마감 대사 = `dailyrecon` (잡 `dailyReconciliationJob`) — 10초 주기 "정리 작업"(`reconcile`)과 구분 |

## 3. 다음 할 일 (순서대로)
1. velog `#2` ~ `#9` 는 비공개로 올라가 있다 (`#9` = P5-1 일마감 대사). 시리즈 지정 · 글마다 "핵심 3줄" 직접 작성 후 공개.
2. (선택) 텔레그램 봇 설정 ([SETUP §2-6](SETUP.md#2-6-텔레그램-알림-선택-p5-1)) 후 대사 요약이 휴대폰으로 오는지 확인 — 손 확인은 로그 알림으로만 했다.
3. P5-2 설계 (보존 배치). 부하를 다시 잴 때는 다른 API 인스턴스(IntelliJ 등)를 모두 끄고, 끝나면 `bash load/run.sh cleanup` ([OPERATION §8](OPERATION.md#8-부하-측정-p4-2-k6)).

### P5-1 범위 밖으로 남긴 것
- 보존 배치 (발행된 `outbox_event`, 만료된 `account_verification`) → P5-2.
- 원장 파티셔닝 · 보관, `bank_statement_line` · `recon_issue` 보존.
- 불일치 해결 API (지금은 SQL 로 `resolved_at` 기록).
- 알림 재전송 큐 (기록의 원본은 `recon_issue`).
- 관리 API 인증.
- 별도 배치 서버 (P8).
- `transfer.sending_at` 단독 인덱스 (③ 이 하루 범위를 전체 훑기로 훑는다).

### P4-2 범위 밖으로 남긴 것
- 환불 · 충전의 락 전략 (지금의 원자적 UPDATE 그대로).
- 다중 인스턴스 실측 (Redis 락의 진짜 이점은 서버가 여러 대일 때다).
- 운영 기본 전략 변경 (ADR-0009 에 판단만 남긴다).
- k6 의 CI 실행 (CI 는 전략별 정확성 테스트만).
- Redis 락 장애 주입(네트워크 지연 등) 실험.
- 컨슈머 블로킹 대기 · 정리 작업 한 주기 시간 (측정은 접수 API 만 한다 — 컨슈머 · 발행기 · 정리 작업은 끄고 잰다).

### P4-1 범위 밖으로 남긴 것
- 인증: `X-User-Id` 는 값을 그대로 믿는다 ("원래는 인증 토큰에서 꺼내는 값").
- 은행 입금 통지로 잔고를 채우기, 충전 멱등 (지금은 관리 · 테스트용 충전).
- 락 비교 · 부하 테스트 (P4-2).
- `CONFLICT` 자동 보정 없음 — 경고만, 수동 확인 · 일마감 대사가 다음 날 기록 · 알림 (P5-1).
- P3 시절 출금(`user_id` NULL)은 아무도 조회할 수 없다 (소유자가 없다). 차감한 적이 없어 환불도 없다.
- 원장 보존 정책(파티셔닝 · 보관)은 P5-2 이후.
- (P4-1 리뷰) 정리 작업은 ⓪ 재발행 · 포기와 ② 결과조회 확정을 건마다 예외 격리한다: 환불이 예외를 던지는 깨진 행(지갑 행 없음, REFUND 중복 등)은 건너뛰고 `onItemError` 로 ERROR 로그(`reconcile skipped transfer …`)를 남긴 채 나머지를 계속 처리한다. 그 행은 상태가 그대로라 손으로 고칠 때까지 매 주기 다시 시도되고 다시 ERROR 가 찍힌다 (`ReconcileResult.errors` 로 센다).
- (P4-1 리뷰) 충전 API 는 인증 없이 항상 켜져 있다 → 운영이라면 설정 플래그(`wallet.deposit-api.enabled`)나 관리자 인증 뒤로.
- (P4-1 리뷰) `fail()` 은 `refund() = false`(`WITHDRAW` 원장 없음)를 조용히 넘긴다: `user_id` 가 있는데 원장이 없는 비정상 행도 P3 시절 출금과 같이 상태만 바뀐다 (경고 로그 후보).
- (P4-1 리뷰) 충전 응답의 `balance` 와 `pending` 은 같은 스냅샷이 아니다 (별도 쿼리).

### P3-3b 범위 밖으로 남긴 것
- 게이트웨이 쓰기 실패 경로(`write failed` → `UNKNOWN`)의 테스트를 추가한다.
- 릴레이는 인스턴스가 여럿이면 키별 순서를 보장하지 않는다 (`ORDER BY id` 라도 `SKIP LOCKED` 로 건너뛴다). 이벤트 종류를 늘리기 전에 이 점을 정리한다.
- (P3-3a 리뷰) 은행이 응답하지 않으면 한 번의 정리 작업이 최대 100건 × 응답 타임아웃(3초) ≈ 5분 걸린다 (스케줄러 스레드가 2개라 발행기는 안 막힌다). 연결 없음은 이제 바로 멈춘다.
- (P3-3a 리뷰) 늦게 온 이체 응답 로그(`late or unknown response`)에 거래번호가 없다 → 거래번호를 남겨 대사에 쓰게 한다.
- (P3-3a 리뷰) 정리 작업 · 발행기 스케줄러의 실패 로그가 `e.message` 만 남긴다 → 예외 전체를 남긴다. `@EnableScheduling` 이 `MessagingConfiguration` 에 붙어 있어 그 클래스에 묶여 있다.
- (P3-3b 최종 리뷰) 재시작 직후 정리 작업 ⓪ 이 바로 돌아, 앱이 5분 넘게 내려가 있던 동안의 시간도 대기로 친다 → 재시도 중이던 건이 재발행 1회를 일찍 쓰거나(이미 3회면 바로 `E001`) 한다. 돈은 안전하다(은행에 간 적 없음). 개선: 기동 후 `republish-after` 동안 ⓪ 건너뛰기.
- (P3-3b 최종 리뷰) 인스턴스 간 시계 차이: `republished_at`(정리 작업 시계) vs `sending_at`(컨슈머 시계). 정리 작업 쪽 시계가 빠르면 재발행 기준이 영영 안 맞아 경고 없이 `RECEIVED` 로 남을 수 있다. 지금은 단일 인스턴스.
- (P3-3b 최종 리뷰) DLT 에 온 이유가 은행 연결 없음이 아닌 이체(DB 장애 등)는 자동 재처리가 없다. ERROR 로그를 보고 수동 확인 · 재처리한다.
- 은행 점검 시간대에는 접수 자체를 거절한다 → P5-1 에서 구현 (503 `BANK_MAINTENANCE`). 점검을 26분보다 길게 잡으면 점검 전에 접수된 출금이 `FAILED E001` 이 될 수 있다.

### P3-2 에서 남은 것 (P3-1 리뷰 항목 중 아직 안 한 것)
- `TransferService.request` / `JpaTransferStore.saveWithEvent` 를 바깥 트랜잭션 안에서 호출하지 않는다 (KDoc).
- UNIQUE 위반 구분은 오류 메시지의 제약 이름에 의존. 경합 롤백 때 드문 데드락(1213)은 500 → 재시도 안내 또는 매핑. P4-1 에서 잔고 부족 롤백이 같은 모양으로 겹친다 (아래 §4 의 (P4-1) 항목). → P4-2 에서 503 매핑.
- 동시 중복 요청마다 Hibernate 가 클라이언트 키를 WARN 으로 남긴다 (예상된 동작, 로그 잡음).

## 4. 알려진 한계 · 주의
- (P5-1) 점검 시작 직전에 보낸 출금을 은행이 20분 넘게 붙잡았다가 처리하면 그날 회차는 자동 실패(`E002` + 환불)로 확정하고, 다음 날 회차의 ④ 가 `REFUNDED_BUT_PAID` 로 잡아 알린다 — 자동 복구는 없다. 확정된 건의 어긋남은 사람이 SQL 로 처리한다. 개발 PC 에서는 스케줄이 PC 가 켜져 있을 때만 돌고, 모의 은행이 00:05 에 떠 있지 않았으면 파일이 없어 06시에 "대사 미완료" 알림이 온다. 관리 API 에 인증이 없다. 텔레그램은 외부 서비스라 가린 계좌 · 금액 · 거래번호가 나간다. 배치가 API 와 같은 프로세스에서 돈다(하루 한 번 · 수 초 규모라 측정하지 않았다). Spring Batch 메타 표(`BATCH_*`)의 시각은 KST 가 아니라 JVM 기본 시간대다. 앱이 대사 도중 죽으면 실행이 `STARTED` 로 남아 409 `RECON_RUNNING` 이 계속된다 ([OPERATION §9-6](OPERATION.md) 의 SQL 로 `FAILED` 처리). 텔레그램 호출이 컨슈머 · 정리 작업 스레드에서 최대 8초 걸릴 수 있다 (`CONFLICT` · DLT · 한도 소진 때만). 은행 파일은 한 번 보내면 바뀌지 않는다는 가정이다 (같은 줄 번호의 다른 내용은 조용히 무시). 거래번호가 ASCII 가 아닌 파일은 ② 적재에서 ERROR 로 실패한다.
- (P4-1) 잔고 행이 핫스팟이다: 같은 사용자의 출금은 행 락을 기다린다 (P4-2 에서 네 전략으로 측정). `CONFLICT` (예: 정리 작업이 `FAILED` 로 확정 · 환불한 뒤 은행이 완료라고 답함)는 **환불된 돈이 실제로 나간** 상태일 수 있다 — 자동 보정하지 않는다 — 일마감 대사가 다음 날 기록 · 알린다 (P5-1). 충전은 인증 · 멱등키가 없어 같은 요청을 두 번 보내면 두 번 충전된다. 같은 `(사용자, 키)` 요청이 **3개 이상 동시에** 오고 잔고가 모자라면, 먼저 들어간 쪽이 롤백될 때 기다리던 쪽끼리 MySQL 데드락(1213)을 만날 수 있다 — 같은 `catch` 가 503 `WALLET_BUSY` 로 매핑하지만 테스트로 재현하지 않았다 (이 데드락은 Hibernate `saveAndFlush` 의 INSERT 에서 나고 `HibernateJpaDialect` 를 거쳐 번역되는데, 테스트한 1205 는 JdbcTemplate 경로다. 출금 INSERT 를 먼저 두는 순서의 대가, 위 `P3-2 에서 남은 것`의 1213 항목과 같은 뿌리).
- (P4-2) `innodb_lock_wait_timeout` 5초는 Hikari `connection-init-sql` 이라 풀의 모든 커넥션에 적용된다 — 발행기 · 정리 작업 트랜잭션도 (지금은 짧아 영향이 없다고 봤다). 그쪽 트랜잭션이 길어지면 락 대기 5초에 걸릴 수 있다. 진짜 1205 → 503 은 테스트로 증명했고, 1213 은 같은 `catch` 가 매핑하지만 테스트로 일으키지는 않았다.
- (P4-2) `redis` 전략: (a) `tryLock` 응답이 시간 초과(명령 응답 대기 1초, 재시도 1번)로 끝났는데 Redis 는 락을 실제로 줬다면, 그 락은 Redisson 내부 임대(30초)가 만료될 때까지 남아 그 사용자의 출금이 최대 약 30초 503 이다 — 실패 쪽으로 닫히는 동작이라 잔고는 맞다. (b) Redis 가 락 경합으로 대기하는 중에 죽으면 `tryLock` 이 예외 대신 `false` 를 돌려줄 수 있어 우리 로그는 INFO `not acquired` 이고 ERROR 는 Redisson 이 따로 남긴다. (c) watchdog 연장이 끊기면(긴 GC) 락이 풀릴 수 있다 — 안쪽 조건부 UPDATE 로 잔고는 안전하다. (d) 앱이 한 대라 인스턴스 사이 직렬화라는 Redis 락의 진짜 이점은 재지 않았다.
- (P4-2) `optimistic` 전략의 재시도는 톰캣 스레드에서 잔다 (최대 약 200ms). 100건 동시 테스트에서는 충돌을 재시도하다 잔고가 0 이 되면 422 로 끝나 409 가 0건이다 — 재시도 소진(409)은 훅 기반 테스트가 증명한다.
- (P4-2) 측정은 한 PC · 서버 한 대 기준의 상대 비교다. 측정 중에는 다른 API 인스턴스(IntelliJ 등)를 모두 끈다 — 같은 DB 를 쓰는 발행기와 컨슈머가 부하용 출금을 은행으로 보낸다. 측정 뒤 `bash load/run.sh cleanup` 은 필수.
- (P3-3b) 은행 장애가 약 26분((재시도 약 95초 + 대기 5분) × 4 ≈ 26분, 정리 작업 주기 최대 10초 별도)보다 길면 그 사이 접수된 출금은 `FAILED E001` 이 된다. 복구 직후 밀린 건이 한꺼번에 은행에 간다(컨슈머 3개가 상한). 재시도 토픽 컨슈머가 5분 넘게 밀리면 재발행과 재시도가 겹칠 수 있다 (은행 호출은 조건부 전이로 한 번뿐).
- (P3-3a) `UNKNOWN` · 멈춘 `SENDING` 은 정리 작업이 결과조회로 확정한다. 단 원거래가 은행 안에서 아직 처리 중일 때 조회하면 `2001` 이 올 수 있다. `sending_at` 30초 유예(정리 작업이 바꾼 `UNKNOWN` 과 컨슈머가 적은 `UNKNOWN` 모두)로 줄였지만, 은행이 30초보다 더 늦으면 `2001` → `FAILED` 로 확정된 뒤 늦은 이체 응답은 로그만 남는다(최종 안전망은 일마감 대사 — P5-1). 10번 물어도 모르는 건은 `UNKNOWN` 으로 남아 경고만 난다. 인스턴스가 여러 대면 같은 `UNKNOWN` 을 중복 조회할 수 있다(결과는 같다).
- (P3-2) 컨슈머 스레드가 은행 응답을 블로킹으로 기다린다(최대 3초) → P4 에서 측정, P6 · P7 에서 비교.
- 게이트웨이 응답 매칭은 단일 연결 · 단일 인스턴스 전제 (ADR-0002 한계).
- Kafka 는 단일 노드라 복제 · ISR 장애 실험 불가 (ADR-0003 한계).
- Windows: 셸 스크립트는 Git Bash 로. PowerShell `bash` 는 WSL 실행기라 실패한다.

## 5. 최근 배운 것 (같은 실수 반복 방지)
- 제약 이름으로 예외를 가르는 코드는 인덱스 이름을 바꿀 때 같이 고친다 (`uk_transfer_idempotency` 는 `uk_transfer_user_idempotency` 의 부분 문자열이 아니다 — 그대로 두면 동시 같은 키가 500 이 된다).
- 동시성 테스트는 조건을 일부러 지워 실패하는지 한 번 확인한다 (P4-1: `AND balance >= ?` 를 지우면 `{202=10, 500=90}` 으로 깨졌다).
- 통합 테스트가 공유 DB 를 쓰면 테스트마다 새 사용자 id 를 쓰고, `RECEIVED` 를 남기는 테스트는 모의 은행에 없는 계좌를 쓴다 (다른 테스트의 정리 작업이 건드려도 잔고 단언이 흔들리지 않게).
- 설정 파일(`.gitattributes` 등)은 추가할 때도 먼저 읽고 합친다. 덮어써서 기존 규칙을 지운 적이 있다.
- 트랜잭션 바깥에서 돌아야 하는 것(재시도 · 분산락)은 문서로 두지 말고 `check` 로 강제한다 (P4-2: `saveWithEvent` 가 바깥 트랜잭션 안이면 거부).
- 분산락 안쪽에서 읽고-계산하고-쓰면 락을 잡지 않는 다른 쓰기(환불 · 충전)가 덮어써 사라진다 — 안쪽은 조건부 UPDATE 로.
- CI(리눅스) 통과 ≠ 개발 PC 통과. Git Bash 는 `/opt/...` 인자를 Windows 경로로 바꾼다 → `MSYS_NO_PATHCONV=1`.
- 시각 판단은 `Instant` 를 업무 시간대(KST)로 바꿔서 한다 — `LocalTime.now(clock)` 은 시계의 시간대(서버는 UTC)를 쓴다.
- Spring Batch 6 의 `chunk(size)` 는 트랜잭션 관리자를 넘기지 않으면 트랜잭션이 없다(Resourceless) — 단계마다 명시한다 (`DailyReconJobTest` 가 리플렉션으로 고정).
- 같은 트랜잭션 안의 "다시 읽기" 는 REPEATABLE READ 에서 첫 읽기의 스냅숏을 본다 — 조건부 UPDATE 0건 뒤 다시 판정하려면 READ COMMITTED (`StatementSettlerIsolationTest`).
- YAML 1.1 은 `23:50` 을 60진수 정수로 읽는다 — 시각은 따옴표로.

## 6. 관련 링크
- 블로그: [#1 Netty 게이트웨이와 1원인증](https://velog.io/@hyeokjinon/bank-transfer-01-netty-gateway-1won-verification) · #2 로컬 인프라 (비공개 검토 중)
- PR: [#1 P3-0](https://github.com/hyeokjinON/bank-transfer/pull/1)
