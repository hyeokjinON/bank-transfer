#!/usr/bin/env bash
# P8-3a 실험 (스펙 §6-3 · §7 · §9): kind 입구(Envoy, 제어 노드 NodePort 30080)에 출금을 일정 속도로 넣으면서 동작 하나를 한다.
#   steady  아무것도 안 함 (기준)                          rolling  api 이미지 태그를 바꿔 롤링 업데이트 (R1 — preStop 은 지금 배포된 값)
#   podkill api Pod 하나 강제 삭제 (R2)                    tagtrap  같은 태그로 다시 apply → 롤링 안 일어남 (R3, 부하 없음)
#   bank    모의 은행 replicas 0 → BANK_DOWN_S 뒤 1 (B1 · B2 — 게이트웨이 liveness 설정은 지금 배포된 값)
#   drain   워커 drain → 부하 끝나면 uncordon (D1)          node     워커 노드 컨테이너 docker stop → NODE_DOWN_S 뒤 start (D2 · D2')
#   cleanup 측정 사용자 행 지우기 (끝나면 반드시)
# 확인 (P8-2 load/compose/run.sh 와 같은 기준): 접수된 키 = 출금 행, 전부 COMPLETED, 은행 거래 기록 = 출금 행(같은 거래번호 두 번 0), 잔고 = 원장 합.
#   통과 못 하면 요약을 .json.invalid 로. 시간 지표는 timeline.sh(1 ~ 2초 간격) → timeline.awk.
# 사용 (Git Bash · Linux): bash load/k8s/run.sh <동작> <회차>      먼저: bash infra/k8s/up.sh
# 환경변수: RATE(20) DURATION(60s) ACTION_AT(20) SETTLE(300) BANK_DOWN_S(60) NODE_DOWN_S(420) NODE(drain · node 대상) OVERLAY(tagtrap, local)
# 결과: docs/perf/raw/p8-3a/<동작>-<회차>.json · .k6.json · .timeline.txt · .rebalance.txt, 작업 파일 build/load-p83a/<동작>-<회차>/
set -euo pipefail
. "$(dirname "$0")/../../infra/k8s/lib.sh"
RUN=setup
die() { echo "[$RUN] $*" >&2; exit 1; }
USERS="user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)\$'"

# 종료 중(deletionTimestamp 있음)인 Pod 는 세지 않는다 — rollout restart 직후 옛 Pod 가 preStop · graceful 동안 남아 있다
# live_pods <app>: 살아 있는 Pod 의 "이름 노드" 줄 (kubectl 실패는 그대로 실패)
live_pods() {
  local out; out=$(k get pods -l "app=$1" -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.nodeName}{" "}{.metadata.deletionTimestamp}{"\n"}{end}') || return 1
  printf '%s\n' "$out" | tr -d '\r' | awk 'NF == 2'
}
# 종료 중인 api · bank-gateway Pod 이름 (없으면 빈 출력)
terminating_pods() {
  local out; out=$(k get pods -l 'app in (api,bank-gateway)' -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.deletionTimestamp}{"\n"}{end}') || return 1
  printf '%s\n' "$out" | tr -d '\r' | awk 'NF >= 2 {print $1}'
}
# 종료 중인 Pod 가 없어질 때까지 (최대 120초, 2초 간격). 0 = 없음, 1 = 남음, 2 = kubectl 실패
wait_no_terminating() {
  local i t
  for i in $(seq 60); do
    t=$(terminating_pods) || return 2
    [ -z "$t" ] && return 0
    sleep 2
  done
  echo "[$RUN] 종료 중인 Pod 가 120초 뒤에도 남아 있다: $(echo $t)" >&2
  return 1
}

if [ "${1:-}" = cleanup ]; then
  sql < load/compose/cleanup.sql
  echo "cleanup 완료 (클러스터 DB 의 load-p82-u001 ~ u020)"
  exit 0
fi

ACTION=${1:?동작: steady | rolling | podkill | tagtrap | bank | drain | node | cleanup}
ROUND=${2:?회차: 1, 2 …}
case "$ACTION" in steady|rolling|podkill|tagtrap|bank|drain|node) ;; *) die "모르는 동작: $ACTION" ;; esac
[[ "$ROUND" =~ ^[0-9]{1,2}$ ]] || die "회차는 숫자 1 ~ 2자리: $ROUND"
RUN="$ACTION-$ROUND"
OUT_DIR=docs/perf/raw/p8-3a; OUT="$OUT_DIR/$RUN.json"; WORK="build/load-p83a/$RUN"
rm -rf "$WORK"; mkdir -p "$OUT_DIR" "$WORK"; rm -f "$OUT" "$OUT.invalid" "$OUT.tmp" "$OUT_DIR/$RUN.k6.json"

# --- R3: 같은 태그로 다시 apply (부하 없음) ---
if [ "$ACTION" = tagtrap ]; then
  TAG=$(deployed_tag)
  [ "$(api_tag)" = "$TAG" ] || die "api 태그 $(api_tag) ≠ 게이트웨이 태그 $TAG — 먼저: bash -c '. infra/k8s/lib.sh; apply_overlay local $TAG'"
  GEN0=$(k get deploy api -o jsonpath='{.metadata.generation}'); RS0=$(k get rs -l app=api -o name | sort | tr '\n' ' ')
  apply_overlay "${OVERLAY:-local}" "$TAG" > "$WORK/apply.txt"
  sleep 5
  GEN1=$(k get deploy api -o jsonpath='{.metadata.generation}'); RS1=$(k get rs -l app=api -o name | sort | tr '\n' ' ')
  LINE=$(grep 'deployment.apps/api ' "$WORK/apply.txt" | tr -d '\r' || true)
  printf '{"run":"%s","action":"tagtrap","tag":"%s","generation_before":%s,"generation_after":%s,"replicasets_before":"%s","replicasets_after":"%s","api_apply":"%s"}\n' \
    "$RUN" "$TAG" "$GEN0" "$GEN1" "$RS0" "$RS1" "$LINE" > "$OUT"
  [ "$GEN0" = "$GEN1" ] && [ "$RS0" = "$RS1" ] || { mv -f "$OUT" "$OUT.invalid"; die "같은 태그인데 스펙이 바뀌었다 — $OUT.invalid"; }
  echo "[$RUN] 통과 — 같은 태그는 롤링을 일으키지 않는다"; cat "$OUT"; exit 0
fi

DEF_DURATION=60s; [ "$ACTION" = rolling ] && DEF_DURATION=150s   # 롤링은 서로 이어진 두 번의 교체(새 Pod 준비 → 옛 Pod preStop · 종료)라 부하가 더 길어야 R1 비교가 의미 있다
RATE=${RATE:-20}; DURATION=${DURATION:-$DEF_DURATION}; ACTION_AT=${ACTION_AT:-20}; SETTLE=${SETTLE:-300}
BANK_DOWN_S=${BANK_DOWN_S:-60}; NODE_DOWN_S=${NODE_DOWN_S:-420}
for v in RATE ACTION_AT SETTLE BANK_DOWN_S NODE_DOWN_S; do [[ "${!v}" =~ ^[1-9][0-9]*$ ]] || die "$v 는 양의 정수: ${!v}"; done
[[ "$DURATION" =~ ^([1-9][0-9]*)s$ ]] || die "DURATION 은 <숫자>s: $DURATION"
DURATION_S=${BASH_REMATCH[1]}
if [ "$ACTION" != steady ] && [ "$ACTION_AT" -ge "$DURATION_S" ]; then die "ACTION_AT($ACTION_AT) < DURATION(${DURATION_S}s) 이어야 한다"; fi

# --- 1. 사전 확인 · 대상 ---
for dpl in mock-bank bank-gateway api; do
  r=$(k get deploy "$dpl" -o jsonpath='{.status.readyReplicas}'); w=$(k get deploy "$dpl" -o jsonpath='{.spec.replicas}')
  [ "${r:-0}" = "$w" ] || die "$dpl 준비 ${r:-0}/$w"
done
[ -z "$(ka get nodes --no-headers | awk '$2 != "Ready"')" ] || die "Ready 가 아닌 노드가 있다"
wait_no_terminating || die "종료 중인 api · bank-gateway Pod 가 사라지지 않는다 (또는 kubectl 실패) — 잠시 뒤 다시"
k get pods -o wide > "$WORK/pods-before.txt"
LIVE_API=$(live_pods api) || die "api Pod 목록을 못 읽었다"
LIVE_GW=$(live_pods bank-gateway) || die "bank-gateway Pod 목록을 못 읽었다"
[ "$(printf '%s\n' "$LIVE_API" | awk 'NF {print $2}' | sort -u | grep -c . || true)" = 2 ]   || die "살아 있는 api Pod 두 대가 서로 다른 워커에 있지 않다 — kubectl -n bank-transfer rollout restart deploy/api 후 다시"
GW_NODE=$(printf '%s\n' "$LIVE_GW" | awk 'NF {print $2; exit}')
[ -n "$GW_NODE" ] || die "살아 있는 bank-gateway Pod 가 없다"
case "$ACTION" in
  drain) NODE=${NODE:-$GW_NODE} ;;   # 기본: 게이트웨이가 있는 워커 — 1대짜리가 옮겨 가는 동안까지 본다
  node)  NODE=${NODE:-$(printf '%s\n' "$LIVE_API" | awk 'NF {print $2}' | { grep -vx "$GW_NODE" || true; } | head -1)} ;;   # 기본: 게이트웨이가 없는 api 워커 — api 복구만 본다
  *)     NODE=- ;;
esac
[ -n "$NODE" ] || die "대상 워커를 못 정했다 (게이트웨이 없는 api 워커가 없다) — NODE=… 로 지정"
if [ "$NODE" != - ]; then [[ "$NODE" == "$CLUSTER_NAME-worker"* ]] || die "대상은 워커여야 한다: $NODE"; fi
PRESTOP=$(k get deploy api -o jsonpath='{.spec.template.spec.containers[0].lifecycle.preStop.sleep.seconds}'); PRESTOP=${PRESTOP:-0}
LIVE_BANK=$(k get deploy bank-gateway -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="MANAGEMENT_ENDPOINT_HEALTH_GROUP_LIVENESS_INCLUDE")].value}')
TOL=$(k get deploy api -o jsonpath='{.spec.template.spec.tolerations[?(@.key=="node.kubernetes.io/unreachable")].tolerationSeconds}'); TOL=${TOL:-300}
NEW=
if [ "$ACTION" = rolling ]; then   # 같은 이미지에 새 태그 → kind 노드에 미리 넣어 둔다 (부하 중에 kind load 하지 않게)
  CUR=$(api_tag); NEW="${CUR%-r*}-r$(date +%s)"
  d image inspect "bank-transfer/api:$CUR" >/dev/null 2>&1 || die "PC 에 bank-transfer/api:$CUR 가 없다 — up.sh 로 만든 태그여야 한다"
  d tag "bank-transfer/api:$CUR" "bank-transfer/api:$NEW"
  kindc load docker-image "bank-transfer/api:$NEW" --name "$CLUSTER_NAME" >/dev/null
fi

# --- 2. 시드 ---
sql < load/compose/cleanup.sql
sql < load/compose/seed.sql
T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# --- 3. 표본 · 부하 · 동작 ---
bash load/k8s/timeline.sh "$WORK/timeline.txt" &
SAMPLER=$!
# 삭제 · 축출된 Pod 의 로그는 나중에 못 읽는다 — 시작 때 있던 api Pod 의 로그를 따라간다 (끝난 뒤 --since-time 조회와 합친다)
MSYS_NO_PATHCONV=1 kubectl --context "$CTX" -n "$NAMESPACE" logs -f -l app=api --prefix --timestamps --tail=0 --max-log-requests=10 > "$WORK/api-follow.log" 2>&1 &
FOLLOW=$!
K6_NAME="p83a-k6-$RUN"
RESTORE=   # 동작이 시작돼 되돌려야 할 것 (drain | node | bank) — 정상 흐름에서 되돌리면 비운다
stop_bg() {
  kill "$SAMPLER" "$FOLLOW" 2>/dev/null || true; d rm -f "$K6_NAME" >/dev/null 2>&1 || true
  case "$RESTORE" in
    drain) ka uncordon "$NODE" >/dev/null 2>&1 || true; echo "[$RUN] 비정상 종료 — 노드 $NODE uncordon" >&2 ;;
    node)  d start "$NODE" >/dev/null 2>&1 || true; echo "[$RUN] 비정상 종료 — 노드 $NODE docker start (Ready 는 직접 확인)" >&2 ;;
    bank)  k scale deploy/mock-bank --replicas=1 >/dev/null 2>&1 || true; echo "[$RUN] 비정상 종료 — mock-bank replicas 1 로 복구" >&2 ;;
  esac
}
trap stop_bg EXIT
# 다음 회차를 위해: api 두 대가 한 워커에 몰렸으면 다시 나눈다 (ScheduleAnyway · drain 뒤). 판정과 무관한 최선 노력
spread_nodes() { live_pods api 2>/dev/null | awk 'NF {print $2}' | sort -u | grep -c . || true; }
restore_spread() {
  k rollout status deploy/api --timeout=300s >/dev/null 2>&1 || true
  wait_no_terminating || true
  local n; n=$(spread_nodes)
  if [ "$n" != 2 ]; then
    echo "[$RUN] api 두 대가 한 워커에 — rollout restart 로 다시 나눈다" >&2
    k rollout restart deploy/api >/dev/null 2>&1 || true; k rollout status deploy/api --timeout=420s >/dev/null 2>&1 || true
    wait_no_terminating || true
    n=$(spread_nodes)
    # 그래도 한 워커면: 그 워커의 살아 있는 api Pod 하나만 지워 다시 뜨게 한다 (최대 3번)
    local try victim
    for try in 1 2 3; do
      [ "$n" = 2 ] && break
      victim=$(live_pods api 2>/dev/null | awk 'NF {print $1; exit}')
      [ -n "$victim" ] || break
      echo "[$RUN] 아직 한 워커 — api Pod $victim 삭제 ($try/3)" >&2
      k delete pod "$victim" >/dev/null 2>&1 || true
      k rollout status deploy/api --timeout=300s >/dev/null 2>&1 || true
      wait_no_terminating || true
      n=$(spread_nodes)
    done
    echo "[$RUN] api 분산 복구 결과: 워커 ${n}곳" >&2
  fi
}
d rm -f "$K6_NAME" >/dev/null 2>&1 || true
echo "[$RUN] k6 ${RATE}건/s × $DURATION → $CLUSTER_NAME-control-plane:30080, 동작 $ACTION @ ${ACTION_AT}s (대상 $NODE)"
d run --rm --name "$K6_NAME" --network kind \
  -v "$(host_path load):/scripts:ro" -v "$(host_path docs/perf/raw):/out" \
  -e BASE_URL="http://$CLUSTER_NAME-control-plane:30080" -e RUN="$RUN" -e RATE="$RATE" -e DURATION="$DURATION" -e OUT_SUBDIR=p8-3a \
  "$K6_IMAGE" run --quiet /scripts/compose/withdraw.js > "$WORK/k6.out" 2>&1 &
K6_PID=$!
TA_MS0=$(date +%s%3N)   # k6 시작 — 부하 끝 = TA_MS0 + DURATION_S 초
T_ACTION=null; T_ACTION_END=null; TA_MS=0; TE_MS=0; VICTIM=; VERIFY_DURING=; WITHIN=null
if [ "$ACTION" != steady ]; then
  sleep "$ACTION_AT"
  TA_MS=$(date +%s%3N); T_ACTION="\"$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)\""
  case "$ACTION" in
    rolling) k set image deploy/api "api=bank-transfer/api:$NEW" >/dev/null
             k rollout status deploy/api --timeout=300s > "$WORK/rollout.txt" 2>&1 || echo "[$RUN] 주의: 롤링이 300초 안에 안 끝났다" >&2 ;;
    podkill) VICTIM=$(k get pods -l app=api -o jsonpath='{.items[0].metadata.name}')
             k delete pod "$VICTIM" --grace-period=0 --force > "$WORK/delete.txt" 2>&1 ;;
    bank)    RESTORE=bank; k scale deploy/mock-bank --replicas=0 >/dev/null
             sleep 15   # 장애 중 1원인증은 어떤 코드로 끝나나 (동기 경로 — 503 · 504 등을 그대로 기록)
             VERIFY_DURING=$(printf '%s' '{"bankCode":"090","accountNo":"1002123456789","holderName":"LOAD"}' \
               | curl -s -o /dev/null -w '%{http_code}' -m 20 -X POST http://127.0.0.1:8000/api/v1/account-verifications \
                   -H 'Content-Type: application/json' -H 'X-User-Id: load-p82-u001' --data-binary @- || true)
             VERIFY_DURING=${VERIFY_DURING:-000}   # 연결 실패면 curl 이 000 (문자열로 기록)
             rest=$(( BANK_DOWN_S - ($(date +%s%3N) - TA_MS) / 1000 )); [ "$rest" -gt 0 ] && sleep "$rest"
             k scale deploy/mock-bank --replicas=1 >/dev/null; RESTORE= ;;
    drain)   RESTORE=drain; ka drain "$NODE" --ignore-daemonsets --delete-emptydir-data --timeout=240s > "$WORK/drain.txt" 2>&1 \
               || echo "[$RUN] 주의: drain 이 240초 안에 안 끝났다 — $WORK/drain.txt" >&2 ;;
    node)    RESTORE=node; d stop "$NODE" >/dev/null ;;
  esac
  TE_MS=$(date +%s%3N); T_ACTION_END="\"$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)\""
  if [ "$TE_MS" -le $(( TA_MS0 + DURATION_S * 1000 )) ]; then WITHIN=true; else
    WITHIN=false; echo "[$RUN] 주의: 동작이 부하(${DURATION_S}초)보다 늦게 끝났다 — DURATION 을 늘려 다시" >&2
  fi
fi
wait "$K6_PID" || die "k6 가 실패했다 — $WORK/k6.out"
[ -f "$OUT_DIR/$RUN.k6.json" ] || die "k6 요약이 없다 — $WORK/k6.out"
case "$ACTION" in
  drain) ka uncordon "$NODE" >/dev/null; RESTORE= ;;
  node)  rest=$(( NODE_DOWN_S - ($(date +%s%3N) - TA_MS) / 1000 )); [ "$rest" -gt 0 ] && { echo "[$RUN] 노드 $NODE 정지 유지 ${rest}초"; sleep "$rest"; }
         d start "$NODE" >/dev/null; RESTORE=
         ka wait "node/$NODE" --for=condition=Ready --timeout=300s >/dev/null || die "노드 $NODE 가 돌아오지 않는다 — down.sh → up.sh (스펙 §13)" ;;
esac

# --- 4. 완료 대기 ---
P=-1; SETTLE_S=0
for SETTLE_S in $(seq "$SETTLE"); do
  P=$(sql <<<"SELECT COUNT(*) FROM transfer WHERE $USERS AND status <> 'COMPLETED'" | tr -d '\r')
  [ "$P" = 0 ] && break
  sleep 1
done
kill "$SAMPLER" "$FOLLOW" 2>/dev/null || true

# --- 5. 정확성 ---
declare -A C
while IFS=$'\t' read -r key val; do C[$key]=$val; done < <(sql < load/compose/verify.sql | tr -d '\r')
K6J="$OUT_DIR/$RUN.k6.json"
k6v() { grep -o "\"$1\": *[0-9.]*" "$K6J" | head -1 | sed 's/.*: *//'; }
ACCEPTED=$(k6v accepted_keys)
sql < load/compose/refs.sql | tr -d '\r' | sort > "$WORK/refs.txt"
{ k exec deploy/mock-bank -- sh -c 'cat /data/bank-files/journal/*.log 2>/dev/null' || true; } | tr -d '\r' | grep -a '^D' | cut -c2-21 | sort > "$WORK/journal-refs.txt" || true
JOURNAL_MISSING=$(comm -23 "$WORK/refs.txt" <(sort -u "$WORK/journal-refs.txt") | wc -l | tr -d ' ')
JOURNAL_DUP=$({ grep -xF -f "$WORK/refs.txt" "$WORK/journal-refs.txt" || true; } | uniq -d | wc -l | tr -d ' ')

# --- 6. 로그 · 시간 지표 ---
k logs -l app=api --prefix --timestamps --tail=-1 --since-time="$T0" > "$WORK/api.log" 2>&1 || true
k logs -l app=bank-gateway --prefix --timestamps --tail=-1 --since-time="$T0" > "$WORK/gateway.log" 2>&1 || true
ka -n envoy-gateway-system logs -l gateway.envoyproxy.io/owning-gateway-name=bank-transfer --all-containers --tail=-1 --since-time="$T0" > "$WORK/envoy.log" 2>&1 || true
cat "$WORK/api.log" "$WORK/api-follow.log" | { grep -aE "partitions (assigned|revoked)|Adding newly assigned partitions|Revoke previously assigned partitions" || true; } | sort -u | sort -k2,2 -s > "$WORK/rebalance.txt" || true
if [ "$ACTION" != steady ] && [ ! -s "$WORK/rebalance.txt" ]; then echo "[$RUN] 주의: 리밸런스 줄이 0 — 동작이 일어났는지 api 로그로 확인" >&2; fi
declare -A M
while IFS='=' read -r key val; do M[$key]=$val; done < <(awk -v ta="$TA_MS" -v node="$NODE" -f load/k8s/timeline.awk "$WORK/timeline.txt")

num() { if [[ "${1:-}" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then echo "$1"; else echo null; fi; }
str() { if [ -n "${1:-}" ]; then echo "\"$1\""; else echo null; fi; }
cat > "$OUT.tmp" <<JSON
{"run":"$RUN","action":"$ACTION","rate":$RATE,"duration":"$DURATION","action_at_s":$ACTION_AT,"t0":"$T0","t_action":$T_ACTION,"t_action_end":$T_ACTION_END,
 "prestop_s":$PRESTOP,"gateway_liveness_bank":$(str "$LIVE_BANK"),"api_unreachable_toleration_s":$TOL,"node":$(str "${NODE#-}"),"victim":$(str "$VICTIM"),"new_tag":$(str "$NEW"),
 "verify_during_outage":$(str "$VERIFY_DURING"),"action_within_load":$WITHIN,"settle_s":$SETTLE_S,
 "first_202":$(num "$(k6v first_202)"),"first_5xx":$(num "$(k6v first_5xx)"),"first_502":$(num "$(k6v first_502)"),"first_neterr":$(num "$(k6v first_neterr)"),
 "retried_requests":$(num "$(k6v retried_requests)"),"accepted_keys":$(num "$ACCEPTED"),"gave_up_keys":$(num "$(k6v gave_up_keys)"),"p95_ms":$(num "$(k6v p95_ms)"),
 "transfers":$(num "${C[transfers]:-}"),"not_completed":$(num "${C[not_completed]:-}"),"refunds":$(num "${C[refunds]:-}"),
 "negative_balance":$(num "${C[negative_balance]:-}"),"balance_ne_ledger":$(num "${C[balance_ne_ledger]:-}"),
 "transfer_ne_withdraw":$(num "${C[transfer_ne_withdraw]:-}"),"unpublished_outbox":$(num "${C[unpublished_outbox]:-}"),
 "journal_missing":$JOURNAL_MISSING,"journal_dup":$JOURNAL_DUP,"rebalance_lines":$(wc -l < "$WORK/rebalance.txt" | tr -d ' '),
 "api_ready_min":${M[api_ready_min]:-null},"api_recovered_s":${M[api_recovered_s]:-null},"gw_not_ready_s":${M[gw_not_ready_s]:-null},"gw_restarts":${M[gw_restarts]:-null},
 "node_notready_s":${M[node_notready_s]:-null},"pod_notready_s":${M[pod_notready_s]:-null},"replacement_ready_s":${M[replacement_ready_s]:-null}}
JSON
FAILS=()
[[ "$ACCEPTED" =~ ^[1-9][0-9]*$ ]] || FAILS+=("접수된 키가 양의 정수가 아님: ${ACCEPTED:-없음}")
ITER=$(k6v iterations); GAVE=$(k6v gave_up_keys)
[ $(( ${ACCEPTED:-0} + ${GAVE:-0} )) = "${ITER:-x}" ] || FAILS+=("접수 ${ACCEPTED:-?} + 포기 ${GAVE:-?} ≠ k6 반복 ${ITER:-?}")
REFS_N=$(wc -l < "$WORK/refs.txt" | tr -d ' ')
[ "${C[transfers]:-x}" = "$REFS_N" ] || FAILS+=("출금 행 ${C[transfers]:-?} ≠ 거래번호 $REFS_N")
[ "${C[not_completed]:-x}" = 0 ] || FAILS+=("COMPLETED 아님 ${C[not_completed]:-?} (대기 ${SETTLE_S}초)")
for key in refunds negative_balance balance_ne_ledger transfer_ne_withdraw unpublished_outbox; do
  [ "${C[$key]:-x}" = 0 ] || FAILS+=("$key ${C[$key]:-?}")
done
[ "${C[transfers]:-x}" = "$ACCEPTED" ] || FAILS+=("출금 행 ${C[transfers]:-?} ≠ 접수된 키 $ACCEPTED")
[ "$JOURNAL_MISSING" = 0 ] || FAILS+=("은행 기록에 없는 출금 $JOURNAL_MISSING")
[ "$JOURNAL_DUP" = 0 ] || FAILS+=("은행에 두 번 간 거래번호 $JOURNAL_DUP")
# 판정이 먼저 — 결과 파일 이름이 정해진 뒤에 되돌리기 · 복사 (최선 노력, 판정을 바꾸지 않는다)
if [ ${#FAILS[@]} -gt 0 ]; then FINAL="$OUT.invalid"; else FINAL="$OUT"; fi
mv -f "$OUT.tmp" "$FINAL"
cp "$WORK/timeline.txt" "$OUT_DIR/$RUN.timeline.txt" || true
cp "$WORK/rebalance.txt" "$OUT_DIR/$RUN.rebalance.txt" || true
restore_spread || true
if [ ${#FAILS[@]} -gt 0 ]; then
  die "정확성 확인 실패 — ${FAILS[*]} ($OUT.invalid, 작업 파일 $WORK)"
fi
# 5xx · 재시작은 판정이 아니라 결과다 (문서에 원인과 함께 적는다)
echo "[$RUN] 통과 — $OUT"
cat "$OUT"
