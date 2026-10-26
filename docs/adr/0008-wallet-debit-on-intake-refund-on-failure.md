# ADR-0008 원화 잔고는 접수 트랜잭션에서 조건부 UPDATE 로 차감하고, 실패 확정 시 같은 트랜잭션에서 환불한다 (원장은 append-only)

- 상태: 제안 (2026-10-26)
- 단계: P4-1 · 설계: [P4-1 설계](../superpowers/specs/2026-10-26-p4-1-wallet-balance-design.md) · [ADR-0004](0004-idempotency-key-and-transactional-outbox.md) (멱등키 범위를 바꾼다) · [ADR-0007](0007-retry-topic-and-bounded-republish.md) (E001 포기도 환불) · [database.md](../database.md) §4-4 · §4-5 · §6

## 맥락
- 거래소의 "내 원화"처럼 사용자별 잔고가 있어야 출금 동시성 · 락 비교(P4-2)를 할 수 있다. 같은 사용자의 출금이 몰려도 잔고가 음수가 되면 안 된다 (P4 의 정합성 기준선).
- 결과를 모르는 돈(`UNKNOWN`)을 다른 출금에 쓰면 안 된다.
- P3 까지는 멱등키 · 받는 계좌 확인이 전역이었다. 다른 사용자가 같은 키로 남의 출금 정보를 200 으로 받을 수 있었고, 남이 인증한 계좌로 출금할 수 있었다.

## 결정
1. **`wallet`(잔고) + append-only `wallet_ledger`(원장)**. `wallet` 은 `CHECK (balance >= 0)` 과 `version`, 원장은 `DEPOSIT` · `WITHDRAW` · `REFUND`, `amount > 0`, `balance_after`, `uk_ledger_transfer_type (transfer_id, type)`. "잔고 = 원장 합"을 언제든 맞춰 볼 수 있다 (P5 대사의 근거, 운영 쿼리는 [database.md §9](../database.md)).
2. **접수 트랜잭션에서 차감** (`JpaTransferStore.saveWithEvent`, 한 트랜잭션): `INSERT transfer` → `UPDATE wallet SET balance = balance - ?, version = version + 1 … WHERE user_id = ? AND balance >= ?` (0건이면 전체 롤백, 지갑이 없어도 0건 = 잔고 0, 422 `INSUFFICIENT_BALANCE`) → 원장 `WITHDRAW` → `INSERT outbox_event`. 별도 락은 없고 행 락 한 번이다.
   - 출금 INSERT 를 먼저 두는 이유: 같은 `(사용자, 키)` 경합의 진 쪽이 UNIQUE 위반에서 먼저 멈춰 지갑을 건드리지 않고 멱등 응답(200)을 받는다. 차감을 먼저 하면 잔고가 한 건 몫뿐일 때 진 쪽이 422 를 받는다. 아웃박스를 마지막에 두면 그 INSERT 가 실패할 때 차감까지 되돌아가는 것을 테스트로 보일 수 있다.
   - `userId` 없는 출금은 저장하지 않는다 (차감 없이 돈이 나가는 길을 막는다).
3. **실패 확정 4경로를 `TransferStore.fail(id, from, at, code): Boolean` 하나로**: 컨슈머의 은행 거절(`SENDING → FAILED`) · 늦은 거절(`UNKNOWN → FAILED`), 정리 작업 ② 결과조회 `2001`, 정리 작업 ⓪ 재발행 소진 `E001`. 한 트랜잭션에서 조건부 전이가 1건일 때만 원장의 `WITHDRAW` 금액만큼 `balance += amount` 와 `REFUND` 1줄을 쓴다. 0건이면 아무것도 하지 않는다. `WITHDRAW` 가 없는 P3 시절 출금(`user_id` NULL)은 상태만 바꾼다. 기존 `transition(…, FAILED)` 는 `require` 로 거부해, 새 실패 경로가 생겨도 환불을 빠뜨릴 수 없다. 이중 환불은 조건부 전이 + `uk_ledger_transfer_type` 두 겹으로 막는다.
4. **상태별 잔고**: `RECEIVED` · `SENDING` · `UNKNOWN` = 빠진 채(보류, `GET /wallet` 의 `pending`), `COMPLETED` = 빠진 채(확정), `FAILED` = 복원, `CONFLICT` = 잔고를 손대지 않고 경고(수동 확인 · P5 대사).
5. **사용자 식별**: `X-User-Id` 헤더 (영숫자 · 하이픈 1~36자, 없거나 틀리면 400 `USER_ID_REQUIRED`). 꺼내고 검사하는 곳은 웹 계층의 `RequestUserResolver` 하나다. 값은 신뢰한다 (인증은 범위 밖 — "원래는 인증 토큰에서 꺼내는 값"). DB 는 `ascii_bin` 이라 대소문자가 다르면 다른 사용자다. 멱등 UNIQUE 는 `(user_id, idempotency_key)`. 1원인증 · 출금 계좌 확인 · 조회는 본인 것만 본다 (남의 인증 확인 404 `VERIFICATION_NOT_FOUND`, 남의 출금 조회 404 `TRANSFER_NOT_FOUND`, 남이 인증한 계좌로 출금 422 `ACCOUNT_NOT_VERIFIED`).
6. **충전 API 는 관리 · 테스트용**: `POST /api/v1/wallet/deposits` 1회 1 ~ 1억 원, 인증 · 멱등키 없음. 실제로는 은행 입금 통지가 채우고 멱등 처리가 필요하다.
7. **잔고 SQL 은 `WalletLedger` 한 곳** (`apps/api/persistence`). 도메인 모듈은 잔고를 모른다. P4-2 에서 차감 방식(비관적 · 낙관적 · Redis 분산락)을 바꿔 끼우는 이음새다.

## 결과
- 증명 테스트:
  - **동시성** (`WalletConcurrencyTest` 2개): 잔고 10만 원에 1만 원 출금 100건 동시 → 정확히 10건 접수 · 90건 `INSUFFICIENT_BALANCE` · 잔고 0 · 원장 합 = 잔고 / 같은 `(사용자, 키)` 20건 동시 → 출금 1건 · 차감 1번
  - **저장소** (`JpaTransferStoreTest`): 접수가 출금 · 차감 · 원장 · 아웃박스를 한 트랜잭션에 쓴다 / 잔고 부족 · 지갑 없음 → 아무것도 남지 않는다 / 아웃박스 INSERT 실패 → 차감 롤백 / `userId` 없는 출금 거부 / 같은 키 · 다른 사용자는 별개 / `transition(…, FAILED)` 거부 / `fail()` 은 전이 1건일 때만 환불, 두 번 불러도 환불 1번 / 환불이 실패하면 전이도 롤백 / P3 시절 출금(`user_id` NULL)은 환불 없음
  - **원장** (`WalletLedgerTest`): 충전 · 차감 · 환불 SQL, 같은 출금에 REFUND 두 번 → UNIQUE 가 막고 잔고 증가도 롤백, `CHECK` 가 음수 UPDATE 를 거부 (조건부 UPDATE 를 우회한 SQL 에 대한 2차 방어선), 잔고 = 원장 합 · KST 저장
  - **흐름** (Kafka + 모의 은행): 은행 거절(1001) · 결과조회 `2001` · 멈춘 `SENDING` · 재발행 포기 `E001` → 잔고 복원과 원장 `WITHDRAW` + `REFUND`, `COMPLETED` 는 빠진 채 `pending` 0, `UNKNOWN` 은 `pending` 으로 보류, 재발행으로 끝내 완료돼도 차감은 한 번
  - **API**: `X-User-Id` 없음 · 형식 오류 400 (`UserIdHeaderApiTest`), 충전 → 잔고 · `pending` · 금액 범위 · 형식 오류 400 (`WalletApiTest`), 남이 인증한 계좌 422 · 남의 출금 조회 404 · 남의 인증 확인 404 (`TransferApiTest` · `AccountVerificationApiTest`)
- **테스트가 실제로 잡아내는지 확인** (일회성 변이 검사, 커밋하지 않음): `WalletLedger.withdraw` 의 UPDATE 에서 `AND balance >= :amount` 조건을 지우고 `WalletConcurrencyTest` 를 돌렸다. 100건 동시 테스트가 실패했다 — 상태 코드 분포가 `{202=10, 500=90}` 이라 "90건 `INSUFFICIENT_BALANCE`" 단언이 깨졌다. 10건은 잔고를 정확히 0 으로 만들며 통과했고, 나머지 90건은 UPDATE 가 잔고를 음수로 만들려다 `CHECK` 제약(`ck_wallet_balance_non_negative`)에 막혀 500 이 됐다. 즉 조건부 UPDATE 가 빠지면 응답 코드로 바로 드러나고, 그때도 잔고가 음수가 되지는 않는다 (`CHECK` 가 2차 방어선으로 동작). 조건을 되돌린 뒤 같은 테스트가 다시 통과한다.
- **이 동시성 테스트의 한계**: Hikari 풀 크기를 정하지 않아 기본값 10 이다. 100개 스레드가 동시에 요청을 보내도 DB 에 닿는 커넥션은 최대 10개라, 실제로는 **10-way DB 경합**이다 (나머지는 커넥션 대기). 정확성(음수 불가 · 정확히 10건)을 보이기에는 충분하지만 락 대기 시간 비교에는 쓸 수 없다. **P4-2 에서 락 전략을 비교할 때는 풀 크기를 명시적으로 설정한다.**
- 한계:
  - 잔고 행이 핫스팟이다. 같은 사용자의 출금은 행 락 대기를 한다 (P4-2 에서 측정).
  - `CONFLICT` (예: 정리 작업이 `FAILED` 로 확정 · 환불한 뒤 은행이 완료라고 답함)는 환불된 돈이 실제로 나간 상태일 수 있다. 자동 보정하지 않고 경고만 한다 (P5 대사).
  - 충전에 인증 · 멱등키가 없다. 같은 요청을 두 번 보내면 두 번 충전된다.
  - `X-User-Id` 는 신뢰한다. 누구든 다른 사용자의 id 를 쓸 수 있다.
  - 같은 `(사용자, 키)` 요청이 3개 이상 동시에 오고 잔고가 모자라면, 먼저 들어간 쪽이 롤백될 때 기다리던 쪽끼리 MySQL 데드락(1213)을 만나 한 요청이 500 이 될 수 있다. 출금 INSERT 를 먼저 두는 순서의 대가이며 HANDOVER 의 기존 1213 항목과 같은 뿌리다. 드물고 보통 클라이언트 재시도로 풀린다.
  - 정리 작업 루프의 `fail()` 호출에는 건별 `catch` 가 없다. 환불이 예외를 던지는 깨진 행이 있으면 그 주기의 나머지 대상이 밀리고, 같은 행이 매 주기 앞에서 다시 던질 수 있다. 또 `fail()` 은 `refund() = false`(`WITHDRAW` 원장 없음)를 조용히 넘긴다 — `user_id` 가 있는데 원장이 없는 비정상 행도 P3 시절 출금(정상)과 같이 상태만 바뀐다. 둘 다 HANDOVER 에 남긴다.
  - 원장 보존 정책(파티셔닝 · 보관)은 P5.

## 검토한 다른 선택지
| 선택지 | 버린 이유 |
|---|---|
| 컨슈머(은행에 보내기 직전)에서 차감 (B안) | 잔고 부족을 접수 뒤에야 알고, 보류를 표현할 상태가 더 필요하다 |
| 잔고 행 `SELECT … FOR UPDATE` 후 계산 (비관적 락) | 한 문장 조건부 UPDATE 와 결과가 같고 왕복이 하나 더 많다 → P4-2 에서 비교 |
| `version` 낙관적 락 | 경합 시 재시도가 폭증한다 → P4-2 에서 비교 |
| Redis 분산락 | 외부 의존이 늘고 락 만료 위험이 있다 → P4-2 에서 비교 |
| 원장 없이 잔고만 | 대사 근거가 없다 |
| 원장 합으로 잔고 계산 | 읽을 때마다 합산해야 하고, 잔고 조건 차감을 원자적으로 못 한다 |
| 전역 멱등 UNIQUE 유지 | 다른 사용자에게 남의 출금 정보가 노출된다 |
| `CONFLICT` 자동 보정 | 돈과 기록이 어긋난 상태에서 자동 이동이 더 위험하다 |
