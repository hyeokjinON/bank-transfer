#!/usr/bin/env bash
# P8-2 앱 컨테이너 스모크 (스펙 §7-2). Compose app 프로필이 떠 있을 때 nginx(:8000) 만으로 확인한다:
#   1) 1원인증 요청 → 모의 은행 로그의 적요 코드 → 확인 → VERIFIED
#   2) 충전 → 출금(Idempotency-Key) → COMPLETED        3) 같은 키로 다시 → 같은 출금 id
#   4) 요청 20번이 api-1 · api-2 둘 다로 (X-Upstream 두 값, /actuator/info 의 instance 두 값)
#   5) 게이트웨이 health UP (컨테이너 안에서), 게이트웨이 · 모의 은행 · api 포트는 PC 에 열리지 않음
#   6) 컨테이너 사용자 ≠ root, 이미지(층 기록 · 파일)에 infra/.env 의 비밀번호 값이 없음
#   7) (CI 만) 대사 파일 디렉터리 바인딩 — 모의 은행이 쓴 파일을 api 가 읽는다 (DB 에 2000-01-01 대사 행이 남아 로컬 개발 DB 에서는 하지 않는다)
# 사용 (Git Bash · Linux): bash infra/app-smoke.sh
#   먼저: docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app up -d --build --wait
# 개발 DB 에는 사용자 load-p82-smoke-<시각> 의 행만 쓰고 끝에서 지운다 (측정 행 규칙).
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash 가 /data/... 같은 인자를 Windows 경로로 바꾸지 않게
cd "$(dirname "$0")"

ENV_FILE=.env
[ -f "$ENV_FILE" ] || ENV_FILE=.env.example
dc() { docker compose -f docker-compose.yml --env-file "$ENV_FILE" --profile app "$@"; }
# 비밀번호는 mysql 컨테이너가 이미 가진 환경변수로만 (PC 명령행에 나오지 않게 — load/run.sh 와 같은 방식)
sql() { dc exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" exec mysql -u"$MYSQL_USER" "$MYSQL_DATABASE" --default-character-set=utf8mb4 -N'; }
ok() { printf '  \033[32mOK\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1" >&2; exit 1; }
port_open() { timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$1" 2>/dev/null; }
json() { { grep -o "\"$1\":\"[^\"]*\"" || true; } | head -1 | cut -d'"' -f4; }   # 평평한 JSON 에서 문자열 값 하나 (없으면 빈 줄 — pipefail 로 여기서 끝나지 않게)
code() { tail -1 <<<"$1"; }   # curl -w '\n%{http_code}' 결과의 상태 코드
body() { sed '$d' <<<"$1"; }

BASE=http://localhost:8000
USER_ID="load-p82-smoke-$(date +%s)"
ACCOUNT=1002123456789
PW=$(mktemp)

cleanup() {
  rm -f "$PW"
  sql >/dev/null 2>&1 <<SQL || echo "  (정리 실패 — 사용자 $USER_ID 의 행을 손으로 지운다: load/compose/cleanup.sql 과 같은 순서)" >&2
DELETE o FROM outbox_event o JOIN transfer t ON t.id = o.aggregate_id WHERE t.user_id = '$USER_ID';
DELETE FROM wallet_ledger WHERE user_id = '$USER_ID';
DELETE FROM transfer WHERE user_id = '$USER_ID';
DELETE FROM account_verification WHERE user_id = '$USER_ID';
DELETE FROM wallet WHERE user_id = '$USER_ID';
SQL
}
trap cleanup EXIT

post() {   # $1 = 경로, $2 = JSON 본문(표준입력으로 — 한글이 셸 인자 변환을 거치지 않게), 나머지 = curl 인자
  local path=$1 data=$2; shift 2
  printf '%s' "$data" | curl -s -w '\n%{http_code}' -X POST "$BASE$path" -H 'Content-Type: application/json' -H "X-User-Id: $USER_ID" "$@" --data-binary @-
}

echo "[0/7] 준비"
for p in 8080 8082 9090; do
  if port_open "$p"; then echo "  경고: PC 포트 $p 가 쓰이고 있다 — IntelliJ 의 API · 게이트웨이 · 모의 은행을 끈다 (같은 DB · 같은 컨슈머 그룹, 스펙 §5-5)" >&2; fi
done
for s in mock-bank bank-gateway api-1 api-2 nginx; do
  id=$(dc ps -q "$s")
  [ -n "$id" ] || fail "$s 컨테이너가 없다 — 먼저 --profile app up -d --build --wait"
  h=$(docker inspect -f '{{.State.Health.Status}}' "$id")
  [ "$h" = healthy ] || fail "$s 가 $h"
done
ok "app 프로필 5개 healthy"

echo "[1/7] 1원인증"
R=$(post /api/v1/account-verifications "{\"bankCode\":\"090\",\"accountNo\":\"$ACCOUNT\",\"holderName\":\"홍길동\"}")
[ "$(code "$R")" = 201 ] || fail "1원인증 요청 $(code "$R"): $(body "$R")"
VID=$(body "$R" | json id)
VCODE=$(dc logs --no-color --no-log-prefix mock-bank 2>/dev/null | grep -a "$ACCOUNT" | grep -ao "BT[0-9]\{4\}" | tail -1 | cut -c3- || true)
[ -n "$VCODE" ] || fail "모의 은행 로그에서 적요 코드(BT + 4자리)를 찾지 못했다"
R=$(post "/api/v1/account-verifications/$VID/confirm" "{\"code\":\"$VCODE\"}")
[ "$(body "$R" | json status)" = VERIFIED ] || fail "확인 $(code "$R"): $(body "$R")"
ok "1원인증 → VERIFIED (적요 코드는 모의 은행 로그에서)"

echo "[2/7] 충전 → 출금 → COMPLETED"
R=$(post /api/v1/wallet/deposits '{"amount":10000}')
[ "$(code "$R")" = 200 ] || fail "충전 $(code "$R"): $(body "$R")"
KEY="smoke-$USER_ID"
TRANSFER="{\"bankCode\":\"090\",\"accountNo\":\"$ACCOUNT\",\"amount\":1000,\"memo\":\"smoke\"}"
R=$(post /api/v1/transfers "$TRANSFER" -H "Idempotency-Key: $KEY")
[ "$(code "$R")" = 202 ] || fail "출금 접수 $(code "$R"): $(body "$R")"
TID=$(body "$R" | json id)
S=
for _ in $(seq 60); do
  S=$(curl -s "$BASE/api/v1/transfers/$TID" -H "X-User-Id: $USER_ID" | json status)
  [ "$S" = COMPLETED ] && break
  sleep 1
done
[ "$S" = COMPLETED ] || fail "60초 안에 COMPLETED 가 아니다 (지금 $S)"
ok "출금 $TID → COMPLETED"

echo "[3/7] 같은 멱등키로 다시"
R=$(post /api/v1/transfers "$TRANSFER" -H "Idempotency-Key: $KEY")
[ "$(code "$R")" = 200 ] || fail "같은 키 재요청 $(code "$R"): $(body "$R")"
[ "$(body "$R" | json id)" = "$TID" ] || fail "다른 출금 id: $(body "$R")"
ok "같은 출금 id, 200"

echo "[4/7] 분배"
UPS=$(for _ in $(seq 20); do { curl -s -D - -o /dev/null "$BASE/actuator/info" || true; } | tr -d '\r' | awk -F': ' 'tolower($1) == "x-upstream" { print $2 }'; done | sort -u)
[ "$(grep -c . <<<"$UPS")" = 2 ] || fail "X-Upstream 이 두 값이 아니다: $(tr '\n' ' ' <<<"$UPS")"
INST=$(for _ in $(seq 20); do { curl -s "$BASE/actuator/info" || true; } | json instance; done | sort -u | tr '\n' ' ')
[ "$INST" = "api-1 api-2 " ] || fail "instance 가 api-1 · api-2 둘이 아니다: $INST"
ok "X-Upstream $(tr '\n' ' ' <<<"$UPS")· instance $INST"

echo "[5/7] 게이트웨이 · 포트"
dc exec -T bank-gateway wget -qO- http://127.0.0.1:8082/actuator/health | grep -q '"status":"UP"' || fail "게이트웨이 health 가 UP 이 아니다"
for sp in "bank-gateway 8082" "mock-bank 9090" "api-1 8080" "api-2 8080"; do
  set -- $sp
  out=$(dc port "$1" "$2" 2>/dev/null || true)
  [ -z "$out" ] || [ "$out" = ":0" ] || fail "$1:$2 가 PC 에 열려 있다 ($out) — app 프로필에서 PC 포트는 nginx 8000 하나"
done
dc port nginx 80 | grep -q ':8000$' || fail "nginx 가 8000 에 열려 있지 않다"
if [ "${CI:-false}" = true ]; then
  for p in 8080 8082 9090; do port_open "$p" && fail "PC 포트 $p 가 열려 있다"; done
fi
ok "게이트웨이 UP, PC 포트는 nginx 8000 하나"

echo "[6/7] 비root · 이미지 비밀"
for s in mock-bank bank-gateway api-1 api-2; do
  u=$(dc exec -T "$s" id -u | tr -d '\r')
  [ "$u" != 0 ] || fail "$s 가 root 로 돈다"
done
# 비밀번호 값은 파일로만 다룬다 (명령행 · 출력에 나오지 않게). .env.example 의 예시 값과 같은 값은 뺀다 — 예시 값은 application.yml 의 기본값이기도 하다
pw_values() { grep -E '^(MYSQL_PASSWORD|MYSQL_ROOT_PASSWORD)=' "$1" | cut -d= -f2- | tr -d '\r' | grep -v '^$' || true; }
pw_values "$ENV_FILE" | grep -vxF -f <(pw_values .env.example) > "$PW" || true
if [ -s "$PW" ]; then
  for img in bank-transfer/api:dev bank-transfer/bank-gateway:dev bank-transfer/mock-bank:dev; do
    n=$(docker history --no-trunc "$img" | grep -cF -f "$PW" || true)
    [ "$n" = 0 ] || fail "$img 의 층 기록에 비밀번호 값"
    cid=$(docker create "$img")
    n=$(docker export "$cid" | grep -acF -f "$PW" || true)
    docker rm "$cid" >/dev/null
    [ "$n" = 0 ] || fail "$img 의 파일에 비밀번호 값"
  done
  ok "비root, 이미지 세 개에 비밀번호 값 없음"
else
  ok "비root (비밀번호가 .env.example 의 예시 값이라 이미지 검색은 건너뜀 — CI 는 실행마다 임의 값으로 확인)"
fi

echo "[7/7] 대사 파일 디렉터리 바인딩"
if [ "${CI:-false}" = true ]; then
  dc exec -T mock-bank java -Xmx32m -cp 'lib/*' io.github.hyeokjinon.banktransfer.mockbank.MockBankServerKt --bank-files=/data/bank-files --close-day=20000101 >/dev/null
  R=$(curl -s -X POST "$BASE/admin/reconciliations?date=2000-01-01")
  grep -Eq '"status":"COMPLETED"|RECON_ALREADY_COMPLETED' <<<"$R" || fail "api 가 모의 은행의 대사 파일을 읽지 못했다 (DAILYRECON_FILEDIR · 볼륨): $R"
  ok "모의 은행이 쓴 RECON_20000101.dat 를 api 가 읽어 대사 COMPLETED"
else
  echo "  (건너뜀 — CI 에서만. 개발 DB 에 2000-01-01 대사 행이 남는다)"
fi

echo "app-smoke 통과"
