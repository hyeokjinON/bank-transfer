# P9a 실험 원자료 (스펙 §8) — load/k8s/run.sh 가 PHASE=p9a 일 때 source 한다 (run.sh 의 변수 · 함수를 그대로 쓴다).
# 남기는 것: docs/perf/raw/p9a/<동작>-<회차>.prom-*.json (Prometheus 질의 결과) · .alert-sink.txt · .alertmanager.txt · .mock-bank-sessions.txt, 숫자 요약 .p9a.json
# (.log 는 .gitignore 가 무시해 .txt). 숫자: 지표로 본 은행 재연결 vs 모의 은행 로그 재연결 (M1 ≤ 5초), 리더 0 구간, 알림 도착 · 해제 (M2 · M3),
# 상태 조회 시간 (M4 > 50ms 면 V7), 전체 시계열 수 (B2), 카운터 vs DB (B3), 죽은 Pod 시계열이 끊긴 시각 (B4), 점검 중 · 뒤 알림 (M5),
# 억제 증거 (M3 — Alertmanager 가 BankDisconnected 를 suppressed 로 본 표본 수), ScrapeTargetDown Pod 별 첫 firing (M1 노드 kill).

# --- M5: 점검 창을 "지금" 으로 ---
# 점검 창을 저장소 값(overlays/local)으로 되돌린다 — env 는 Pod 시작 때 읽으므로 다시 시작해야 한다
restore_maint() {
  apply_overlay local "$MAINT_TAG" >/dev/null 2>&1 || true
  k rollout restart deploy/api deploy/bank-gateway >/dev/null 2>&1 || true
  # 다음 실험의 사전 확인("준비 x/y")이 걸리지 않게 끝날 때까지 기다린다 (최선 노력 — 여러 번 불려도 된다)
  k rollout status deploy/bank-gateway --timeout=300s >/dev/null 2>&1 || true
  k rollout status deploy/api --timeout=420s >/dev/null 2>&1 || true
  echo "[$RUN] 점검 창을 overlays/local 값으로 되돌렸다 — 재시작 중 (kubectl -n bank-transfer rollout status deploy/api)" >&2
}

# 점검 창 = (지금 + MAINT_PREP_S, 분 단위 올림) ~ + MAINT_S. KST "HH:mm" — KST 는 서머타임이 없어 UTC + 9시간.
# app-config 를 고치고 api · 게이트웨이를 다시 시작한다 (설정은 기동 때 읽는다). 점검 길이는 재발행 포기(약 26분)보다 짧게 (기본 10분)
maint_prepare() {
  MAINT_TAG=$(deployed_tag)
  [ "$(api_tag)" = "$MAINT_TAG" ] || die "api 태그 $(api_tag) ≠ 게이트웨이 태그 $MAINT_TAG — 먼저: bash -c '. infra/k8s/lib.sh; apply_overlay local $MAINT_TAG'"
  MAINT_START_EPOCH=$(( ( $(date +%s) + MAINT_PREP_S + 59 ) / 60 * 60 ))
  MAINT_END_EPOCH=$(( MAINT_START_EPOCH + MAINT_S ))
  MAINT_START=$(date -u -d "@$(( MAINT_START_EPOCH + 9 * 3600 ))" +%H:%M)
  MAINT_END=$(date -u -d "@$(( MAINT_END_EPOCH + 9 * 3600 ))" +%H:%M)
  echo "[$RUN] 점검 창 $MAINT_START ~ $MAINT_END (KST) — api · 게이트웨이 재시작"
  RESTORE=maint
  trap restore_maint EXIT   # stop_bg 트랩이 들어서기 전에 죽어도 되돌린다
  trap 'exit 130' INT TERM  # Ctrl-C · 종료 신호도 EXIT 트랩을 거치게
  k patch configmap app-config --type merge \
    -p "{\"data\":{\"BANK_MAINTENANCE_ENABLED\":\"true\",\"BANK_MAINTENANCE_START\":\"$MAINT_START\",\"BANK_MAINTENANCE_END\":\"$MAINT_END\"}}" >/dev/null
  k rollout restart deploy/bank-gateway deploy/api >/dev/null
  k rollout status deploy/bank-gateway --timeout=300s >/dev/null || die "게이트웨이 재시작이 300초 안에 끝나지 않았다"
  k rollout status deploy/api --timeout=420s >/dev/null || die "api 재시작이 420초 안에 끝나지 않았다"
  wait_no_terminating || die "종료 중인 Pod 가 남아 있다"
  [ "$(date +%s)" -lt $(( MAINT_START_EPOCH - MAINT_LEAD_S )) ] || die "재시작이 길어 점검 시작 ${MAINT_LEAD_S}초 전에 부하를 걸 수 없다 — MAINT_PREP_S 를 늘려 다시"
}

# 부하 시작 직전: 점검 시작 MAINT_LEAD_S 초 전까지 기다린다 (그래야 동작 시각 = 점검 시작)
maint_wait_start() {
  local w=$(( MAINT_START_EPOCH - MAINT_LEAD_S - $(date +%s) ))
  if [ "$w" -gt 0 ]; then sleep "$w"; fi
  return 0
}

# --- Alertmanager 표본 (M3 억제 증거) ---
# /api/v2/alerts 는 "지금 살아 있는" 알림(억제 · 침묵 포함)만 준다 — 끝난 뒤엔 사라지므로 실험 내내 15초마다 한 줄씩 남긴다: "epoch_ms <본문 한 줄>"
am_snapshot() { km exec deploy/alertmanager -- wget -qO- 'http://127.0.0.1:9093/api/v2/alerts?silenced=true&inhibited=true&active=true&unprocessed=true' 2>/dev/null | tr -d '\r\n'; }
p9a_am_loop() {
  while :; do
    printf '%s %s\n' "$(date +%s%3N)" "$(am_snapshot || true)" >> "$WORK/alertmanager.txt"
    sleep 15
  done
}
p9a_am_start() { : > "$WORK/alertmanager.txt"; p9a_am_loop & AM_POLL=$!; }
# $1 = 알림 이름, $2 = 상태 (suppressed | active) → 그 상태로 보인 표본 수 (알림 하나가 한 표본에 한 번)
am_seen() {
  awk -v n="\"alertname\":\"$1\"" -v s="\"state\":\"$2\"" '{
    c = split($0, a, "{\"annotations\":")
    for (i = 2; i <= c; i++) if (index(a[i], n) && index(a[i], s)) k++
  } END { printf "%d", k }' "$WORK/alertmanager.txt" 2>/dev/null || echo 0
}

# --- 원자료 ---
p9a_range() {   # $1 = 이름, $2 = PromQL. T0 ~ 지금, 5초 간격 (수집 간격과 같다)
  prom_range "$2" "$(date -u -d "$T0" +%s)" "$(date +%s)" 5 > "$OUT_DIR/$RUN.prom-$1.json" 2>/dev/null || echo '{}' > "$OUT_DIR/$RUN.prom-$1.json"
}
# query_range 결과(시계열 하나) → "epoch초 값" 줄
p9a_points() { { grep -o '\[[0-9.]*,"[^"]*"\]' "$OUT_DIR/$RUN.prom-$1.json" || true; } | tr -d '[]"' | tr ',' ' '; }
# alert-sink 로그 → "epoch_ms 알림이름 firing|resolved" (웹훅 본문 한 줄 = 한 번, 묶음 = alertname, 첫 "status" = 묶음 상태)
p9a_alerts() {
  local ts body name st
  { grep -a '"groupLabels"' "$OUT_DIR/$RUN.alert-sink.txt" || true; } | while read -r ts body; do
    name=$(grep -o '"groupLabels":{"alertname":"[^"]*"' <<<"$body" | cut -d'"' -f6)
    st=$(grep -o '"status":"[a-z]*"' <<<"$body" | head -1 | cut -d'"' -f4)
    printf '%s %s %s\n' "$(date -d "$ts" +%s%3N)" "$name" "$st"
  done
}
alert_s() { since_ta "$(awk -v n="$1" -v s="$2" -v t="$TA_MS" '$2 == n && $3 == s && $1 >= t { print $1; exit }' "$WORK/alerts.txt")"; }
delta() { if [[ "${1:-}" =~ ^[0-9.]+$ && "${2:-}" =~ ^[0-9.]+$ ]]; then awk -v a="$1" -v b="$2" 'BEGIN { printf "%d", b - a }'; else echo null; fi; }
# ALERTS 범위 질의 결과에서 ScrapeTargetDown 시계열마다 "pod=동작뒤_첫_firing_초" (M1 노드 kill — Pod 당 약 1분 뒤 하나)
p9a_scrape_down() {
  { grep -o '{"metric":{[^}]*"alertname":"ScrapeTargetDown"[^}]*},"values":\[\[[0-9.]*' "$OUT_DIR/$RUN.prom-alerts.json" || true; } \
    | awk -v ta="$(( TA_MS / 1000 ))" '{
        p = ""; if (match($0, /"pod":"[^"]*"/)) p = substr($0, RSTART + 7, RLENGTH - 8)
        t = $0; sub(/.*\[\[/, "", t)
        if (p != "") printf "%s=%.0fs ", p, t - ta
      }' | sed 's/ $//'
}

p9a_capture() {
  if [ "$ACTION" = maint ]; then
    local until=$(( MAINT_END_EPOCH + MAINT_WATCH_S )) now
    now=$(date +%s)
    if [ "$now" -lt "$until" ]; then
      echo "[$RUN] 점검이 끝난 뒤 ${MAINT_WATCH_S}초까지 알림을 지켜본다 (남은 $(( until - now ))초)"
      sleep $(( until - now ))
    fi
  fi
  if [ -n "${AM_POLL:-}" ]; then kill "$AM_POLL" 2>/dev/null || true; wait "$AM_POLL" 2>/dev/null || true; AM_POLL=; fi
  local ta_s=$(( TA_MS / 1000 )) t0_s
  t0_s=$(date -u -d "$T0" +%s)
  p9a_range connected 'max(banktransfer_gateway_bank_connected) or vector(0)'
  p9a_range leaders 'sum(banktransfer_gateway_leader) or vector(0)'
  p9a_range leader-pods 'banktransfer_gateway_leader'
  p9a_range open-oldest 'max by (status) (banktransfer_transfer_open_oldest_seconds)'
  p9a_range alerts 'ALERTS{alertstate="firing"}'
  p9a_range db-query-max 'max(banktransfer_metrics_db_query_seconds_max)'
  p9a_range head-series 'max(prometheus_tsdb_head_series)'
  if [ -n "${VICTIM:-}" ]; then p9a_range victim "max(banktransfer_gateway_leader{pod=\"$VICTIM\"})"; fi
  km logs deploy/alert-sink --timestamps --since-time="$T0" > "$OUT_DIR/$RUN.alert-sink.txt" 2>&1 || true
  { grep -a 'session ' "$WORK/mock-bank.log" || true; } > "$OUT_DIR/$RUN.mock-bank-sessions.txt"
  printf '%s %s\n' "$(date +%s%3N)" "$(am_snapshot || true)" >> "$WORK/alertmanager.txt"   # 마지막 표본
  cp "$WORK/alertmanager.txt" "$OUT_DIR/$RUN.alertmanager.txt" || true
  p9a_alerts > "$WORK/alerts.txt"

  local -A G L
  local key val
  while IFS='=' read -r key val; do G[$key]=$val; done < <(p9a_points connected | awk -v ta="$ta_s" -v name=metric_bank -f load/k8s/promgap.awk)
  while IFS='=' read -r key val; do L[$key]=$val; done < <(p9a_points leaders | awk -v ta="$ta_s" -v name=leaders -f load/k8s/promgap.awk)
  local log_open_ms log_open_s diff=null victim_end=null db_max_ms head_max acc0 acc1 done0 done1 firing_names sink_firing scrape_down
  log_open_ms=$({ grep -a 'session opened' "$WORK/mock-bank.log" || true; } | awk '{print $1}' \
    | while read -r ts; do date -d "$ts" +%s%3N 2>/dev/null; done | awk -v t="$TA_MS" '$1 >= t' | sort -n | head -1)
  log_open_s=$(since_ta "$log_open_ms")
  if [ "${G[metric_bank_back_s]:-null}" != null ] && [ "$log_open_s" != null ]; then
    diff=$(awk -v a="${G[metric_bank_back_s]}" -v b="$log_open_s" 'BEGIN { d = a - b; if (d < 0) d = -d; printf "%.1f", d }')
  fi
  if [ -n "${VICTIM:-}" ]; then victim_end=$(p9a_points victim | awk -v ta="$ta_s" 'NF { last = $1 } END { if (last != "") printf "%.1f", last - ta; else print "null" }'); fi
  db_max_ms=$(p9a_points db-query-max | awk '$2 != "NaN" { n++; if ($2 + 0 > m) m = $2 + 0 } END { if (n) printf "%.1f", m * 1000; else print "null" }')
  head_max=$(p9a_points head-series | awk '$2 != "NaN" { n++; if ($2 + 0 > m) m = $2 + 0 } END { if (n) printf "%d", m; else print "null" }')
  acc0=$(prom_value 'sum(banktransfer_transfer_accepted_total{result="created"})' "$t0_s"); acc1=$(prom_value 'sum(banktransfer_transfer_accepted_total{result="created"})')
  done0=$(prom_value 'sum(banktransfer_transfer_completed_seconds_count{status="COMPLETED"})' "$t0_s"); done1=$(prom_value 'sum(banktransfer_transfer_completed_seconds_count{status="COMPLETED"})')
  firing_names=$({ grep -o '"alertname":"[^"]*"' "$OUT_DIR/$RUN.prom-alerts.json" || true; } | cut -d'"' -f4 | sort -u | tr '\n' ' ' | sed 's/ $//')
  sink_firing=$(awk -v t="$TA_MS" '$3 == "firing" && $1 >= t' "$WORK/alerts.txt" | wc -l | tr -d ' ')
  scrape_down=$(p9a_scrape_down)
  cat > "$OUT_DIR/$RUN.p9a.json" <<JSON
{"run":"$RUN","t0":"$T0","t_action_ms":$TA_MS,
 "metric_bank_zero_s":${G[metric_bank_zero_s]:-null},"metric_bank_back_s":${G[metric_bank_back_s]:-null},"metric_bank_gap_s":${G[metric_bank_gap_s]:-null},
 "log_bank_reconnect_s":$log_open_s,"bank_gap_diff_s":$diff,
 "leaders_zero_s":${L[leaders_zero_s]:-null},"leaders_back_s":${L[leaders_back_s]:-null},"leaders_gap_s":${L[leaders_gap_s]:-null},
 "victim_series_end_s":$victim_end,
 "alert_BankDisconnected_firing_s":$(alert_s BankDisconnected firing),"alert_BankDisconnected_resolved_s":$(alert_s BankDisconnected resolved),
 "alert_GatewayNoLeader_firing_s":$(alert_s GatewayNoLeader firing),"alert_GatewayNoLeader_resolved_s":$(alert_s GatewayNoLeader resolved),
 "prom_firing_alerts":"$firing_names","sink_firing_notifications":$sink_firing,
 "am_BankDisconnected_suppressed_samples":$(am_seen BankDisconnected suppressed),"am_BankDisconnected_active_samples":$(am_seen BankDisconnected active),
 "am_GatewayNoLeader_active_samples":$(am_seen GatewayNoLeader active),
 "scrape_target_down_first_s":$(str "$scrape_down"),
 "maint_start":$(str "${MAINT_START:-}"),"maint_end":$(str "${MAINT_END:-}"),
 "db_query_max_ms":$db_max_ms,"head_series_max":$head_max,
 "accepted_counter_delta":$(delta "$acc0" "$acc1"),"completed_counter_delta":$(delta "$done0" "$done1"),"db_transfers":$(num "${C[transfers]:-}")}
JSON
  echo "[$RUN] P9a 원자료 — $OUT_DIR/$RUN.p9a.json"
  cat "$OUT_DIR/$RUN.p9a.json"
}
