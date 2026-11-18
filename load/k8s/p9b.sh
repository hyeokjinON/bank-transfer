# P9b 실험 원자료 (스펙 §7) — load/k8s/run.sh 가 PHASE=p9b 일 때 source 한다 (run.sh 의 변수 · 함수를 그대로 쓴다).
# 남기는 것: docs/perf/raw/p9b/<동작>-<회차>.p9b.json (숫자 요약) · .traces.txt (표본 trace 원자료 — 한 줄 = 출금 id + Tempo JSON) · .tempo-search-*.json
#   T1 추적 비용: TRACING=on|off (off = app-config 샘플링 0.0). 회차마다 api · 게이트웨이를 다시 시작해 조건을 같게 (JIT 데우기 포함 — 상대 비교)
#      고정 도착률이라 "초당 접수" 는 같다 → 처리량 = 출금 수 / (부하 + 마무리 대기) 초, CPU = 부하 동안 process_cpu_usage 평균의 Pod 합
#   T2 모양: 출금 TRACE_SAMPLE 건의 trace → load/k8s/traceshape.py (Python 3 — PC 전용)
#   T3 은행 끊기(bank): 재시도 토픽 소비 구간이 든 trace 수 · 그중 접수 HTTP 가 root 인 수, 재발행 건이 transfer.id 로 trace 2개 이상 · 정리 작업 root
#   T4 리더 교대(gwkill · gwroll): bank.request 의 not_sent 사유
#   T5 (덤) exemplar: Prometheus 에 저장된 exemplar 수 · 그 trace 가 Tempo 에 있나
# Tempo · Prometheus 질의는 Prometheus Pod 의 busybox wget (lib.sh tempo_get · prom_get — PC 포트를 열지 않는다)
# Tempo 검색은 요청 뒤 ~8초 지나야 보인다 → 검색은 마무리 대기 뒤에, 못 찾으면 제한된 횟수만 다시 (P9B_SEARCH_TRIES x P9B_SEARCH_WAIT_S)

p9b_tag() {
  P9B_TAG=$(deployed_tag)
  [ "$(api_tag)" = "$P9B_TAG" ] || die "api 태그 $(api_tag) ≠ 게이트웨이 태그 $P9B_TAG — 먼저: bash -c '. infra/k8s/lib.sh; apply_overlay local $P9B_TAG'"
}

# 샘플링을 저장소 값(overlays/local 의 1.0)으로 되돌린다 — env 는 Pod 시작 때 읽으므로 다시 시작한다 (여러 번 불려도 된다)
# 태그는 지금 떠 있는 것(deployed_tag)으로 — 중간에 바뀌었어도 맞춘다. RESTORE 슬롯과 따로 TRACING_DIRTY 깃발로 돈다
restore_tracing() {
  local tag; tag=$(deployed_tag 2>/dev/null || true); tag=${tag:-$P9B_TAG}
  apply_overlay local "$tag" >/dev/null 2>&1 || true
  k rollout restart deploy/api deploy/bank-gateway >/dev/null 2>&1 || true
  k rollout status deploy/bank-gateway --timeout=300s >/dev/null 2>&1 || true
  k rollout status deploy/api --timeout=420s >/dev/null 2>&1 || true
  echo "[$RUN] 샘플링을 overlays/local 값으로 되돌렸다 — 재시작 중 (kubectl -n bank-transfer rollout status deploy/api)" >&2
}

# 대상 선택(run.sh 사전 확인) 전에 부른다 — 재시작이 Pod 를 바꾸기 때문
p9b_prepare() {
  p9b_tag
  local cur
  cur=$(k get configmap app-config -o jsonpath='{.data.MANAGEMENT_TRACING_SAMPLING_PROBABILITY}' 2>/dev/null || true)
  if [ "$TRACING" = off ]; then
    TRACING_DIRTY=1
    trap restore_tracing EXIT   # stop_bg 트랩이 들어서기 전에 죽어도 되돌린다
    trap 'exit 130' INT TERM
    k patch configmap app-config --type merge -p '{"data":{"MANAGEMENT_TRACING_SAMPLING_PROBABILITY":"0.0"}}' >/dev/null
  elif [ "$cur" != "1.0" ]; then
    echo "[$RUN] 주의: 샘플링이 '$cur' 였다 (이전 실험이 못 되돌렸나) — 1.0 으로 맞춘다" >&2
    k patch configmap app-config --type merge -p '{"data":{"MANAGEMENT_TRACING_SAMPLING_PROBABILITY":"1.0"}}' >/dev/null
  fi
  echo "[$RUN] 추적 $TRACING — api · 게이트웨이 재시작 (회차마다 같은 조건)"
  k rollout restart deploy/bank-gateway deploy/api >/dev/null
  k rollout status deploy/bank-gateway --timeout=300s >/dev/null || die "게이트웨이 재시작이 300초 안에 끝나지 않았다"
  k rollout status deploy/api --timeout=420s >/dev/null || die "api 재시작이 420초 안에 끝나지 않았다"
  wait_no_terminating || die "종료 중인 Pod 가 남아 있다"
  # 리더 라벨이 붙을 때까지 (제한 120초) — 대상 선택이 새 Pod 를 보게
  local i
  for (( i = 0; i < 60; i++ )); do [ -n "$(gw_leader)" ] && break; sleep 2; done
  [ -n "$(gw_leader)" ] || echo "[$RUN] 주의: 게이트웨이 리더 라벨이 120초 안에 안 붙었다 (선출을 끈 구성이면 정상)" >&2
}

P9B_SEARCH_TRIES=${P9B_SEARCH_TRIES:-4}; P9B_SEARCH_WAIT_S=${P9B_SEARCH_WAIT_S:-5}

# TraceQL 검색 → JSON (실패면 {}). 범위 = T0 ~ 지금 (+60초 — 시계 어긋남 여유)
tempo_search() {
  { tempo_get "/api/search?q=$(urlenc "$1")&limit=${2:-20}&spss=3&start=$(date -u -d "$T0" +%s)&end=$(( $(date +%s) + 60 ))" 2>/dev/null || echo '{}'; } | tr -d '\r'
}
# 검색 JSON → traceID 줄 (32자로 채운다 — Tempo 는 앞 0 을 뗄 수 있다)
trace_ids() { { grep -o '"traceID":"[0-9a-f]*"' || true; } | cut -d'"' -f4 | while read -r t; do printf '%32s\n' "$t" | tr ' ' 0; done; }
count_lines() { grep -c . || true; }
# 검색 + 못 찾으면 기다렸다 다시 (제한된 횟수) — 결과 JSON 을 그대로 낸다. $3 = 최소 trace 수 (기본 1)
tempo_search_retry() {
  local i out n want=${3:-1}
  for (( i = 1; i <= P9B_SEARCH_TRIES; i++ )); do
    out=$(tempo_search "$1" "${2:-20}")
    n=$(trace_ids <<<"$out" | count_lines)
    if [ "$n" -ge "$want" ] || [ "$i" -eq "$P9B_SEARCH_TRIES" ]; then printf '%s\n' "$out"; return 0; fi
    sleep "$P9B_SEARCH_WAIT_S"
  done
}
# 진짜 Python 만 쓴다 — Git Bash 의 python3 는 Windows Store 스텁일 수 있어 출력으로 확인한다
run_py() {
  local py
  for py in python3 python; do
    if [ "$("$py" -c 'print(1)' 2>/dev/null | tr -d '\r')" = 1 ]; then "$py" "$@"; return; fi
  done
  echo null
}

p9b_capture() {
  local load_end_s=$(( TA_MS0 / 1000 + DURATION_S )) win_s
  win_s=$(( $(date +%s) - $(date -u -d "$T0" +%s) ))
  local api_cpu gw_cpu p95 per_s export_err
  api_cpu=$(prom_value "sum(avg_over_time(process_cpu_usage{app=\"api\"}[${DURATION_S}s]))" "$load_end_s")
  gw_cpu=$(prom_value "sum(avg_over_time(process_cpu_usage{app=\"bank-gateway\"}[${DURATION_S}s]))" "$load_end_s")
  p95=$(prom_value "histogram_quantile(0.95, sum by (le) (increase(banktransfer_transfer_completed_seconds_bucket{status=\"COMPLETED\"}[${win_s}s])))")
  per_s=$(awk -v n="${C[transfers]:-0}" -v d="$DURATION_S" -v s="$SETTLE_S" 'BEGIN { if (d + s > 0) printf "%.2f", n / (d + s) }')
  # api.log · gateway.log 만 센다 (follow 로그는 같은 줄이라 두 번 센다)
  export_err=$(cat "$WORK/api.log" "$WORK/gateway.log" 2>/dev/null | { grep -ac 'Failed to export' || true; })

  # T2 모양 — Tempo 를 내린 회차(tempozero)는 그 동안의 trace 를 잃었으므로 건너뛴다
  local shape=null tid t doc
  : > "$OUT_DIR/$RUN.traces.txt"
  if [ "$TRACING" = on ] && [ "$ACTION" != tempozero ]; then
    sql <<<"SELECT id FROM transfer WHERE $USERS ORDER BY RAND() LIMIT $TRACE_SAMPLE" | tr -d '\r' > "$WORK/trace-sample.txt"
    while read -r tid; do
      [ -n "$tid" ] || continue
      t=$(tempo_search_retry "{ span.transfer.id = \"$tid\" } && { resource.service.name = \"bank-gateway\" }" 5 | trace_ids | sed -n 1p)
      doc='{}'
      if [ -n "$t" ]; then
        doc=$(tempo_get "/api/traces/$t" 2>/dev/null | tr -d '\r\n' || true)
        [ -n "$doc" ] || doc='{}'
      fi
      printf '%s\t%s\n' "$tid" "$doc" >> "$OUT_DIR/$RUN.traces.txt"
    done < "$WORK/trace-sample.txt"
    shape=$(run_py load/k8s/traceshape.py "$OUT_DIR/$RUN.traces.txt" || true); [ -n "$shape" ] || shape=null
  fi

  # T3 재시도 체인 · 재발행
  local retry_n=null retry_rooted=null repub=null repub_found=null repub_recon=null s
  if [ "$ACTION" = bank ]; then
    tempo_search_retry '{ name =~ "transfer.requested-retry-.* process" }' 1000 > "$OUT_DIR/$RUN.tempo-search-retry.json"
    retry_n=$(trace_ids < "$OUT_DIR/$RUN.tempo-search-retry.json" | count_lines)
    retry_rooted=$({ grep -o '"rootTraceName":"[^"]*"' "$OUT_DIR/$RUN.tempo-search-retry.json" || true; } | { grep -c '"rootTraceName":"http post /api/v1/transfers"' || true; })
    sql <<<"SELECT id FROM transfer WHERE $USERS AND republish_count > 0" | tr -d '\r' > "$WORK/republished.txt"
    repub=$(count_lines < "$WORK/republished.txt"); repub_found=0; repub_recon=0
    local cap_end=$(( $(date +%s) + 300 ))   # 재발행 검색 전체 상한 5분
    while read -r tid; do
      [ -n "$tid" ] || continue
      if [ "$(date +%s)" -ge "$cap_end" ]; then echo "[$RUN] 주의: 재발행 trace 검색이 5분 상한에 걸려 멈췄다" >&2; break; fi
      s=$(tempo_search_retry "{ span.transfer.id = \"$tid\" }" 20 2)
      if [ "$(trace_ids <<<"$s" | count_lines)" -ge 2 ]; then repub_found=$(( repub_found + 1 )); fi
      if [ "$(grep -c '"rootTraceName":"transfer.reconcile"' <<<"$s" || true)" -ge 1 ]; then repub_recon=$(( repub_recon + 1 )); fi
    done < "$WORK/republished.txt"
  fi

  # T4 리더 교대 — 안 보냄 사유
  local notsent=null reasons=
  if [ "$ACTION" = gwkill ] || [ "$ACTION" = gwroll ]; then
    tempo_search_retry '{ name = "bank.request" && span.bank.outcome = "not_sent" } | select(span.bank.not_sent.reason)' 1000 > "$OUT_DIR/$RUN.tempo-search-notsent.json"
    notsent=$(trace_ids < "$OUT_DIR/$RUN.tempo-search-notsent.json" | count_lines)
    reasons=$({ grep -o '"key":"bank.not_sent.reason","value":{"stringValue":"[a-z_]*"' "$OUT_DIR/$RUN.tempo-search-notsent.json" || true; } \
      | sed 's/.*"stringValue":"//; s/"$//' | sort | uniq -c | awk '{ printf "%s=%s ", $2, $1 }' | sed 's/ $//')
  fi

  # T5 (덤) exemplar
  local ex ex_n ex_tid ex_doc ex_found=false
  ex=$(prom_get "/api/v1/query_exemplars?query=$(urlenc banktransfer_transfer_completed_seconds_bucket)&start=$(date -u -d "$T0" +%s)&end=$(date +%s)" 2>/dev/null || true)
  ex_n=$({ grep -o '"trace_id":"[0-9a-f]*"' <<<"$ex" || true; } | count_lines)
  ex_tid=$({ grep -o '"trace_id":"[0-9a-f]*"' <<<"$ex" || true; } | sed -n 1p | cut -d'"' -f4)
  if [ -n "$ex_tid" ]; then
    ex_doc=$(tempo_get "/api/traces/$ex_tid" 2>/dev/null || true)
    if [ "$(grep -c '"spanId"' <<<"$ex_doc" || true)" -ge 1 ]; then ex_found=true; fi
  fi

  cat > "$OUT_DIR/$RUN.p9b.json" <<JSON
{"run":"$RUN","tracing":"$TRACING","rate":$RATE,"duration_s":$DURATION_S,"settle_s":$SETTLE_S,"transfers":$(num "${C[transfers]:-}"),
 "completed_per_s":$(num "$per_s"),"accept_p95_ms":$(num "$(k6v p95_ms)"),"complete_p95_s":$(num "$p95"),"api_cpu_avg":$(num "$api_cpu"),"gw_cpu_avg":$(num "$gw_cpu"),"export_errors":$(num "$export_err"),
 "trace_shape":$shape,
 "retry_traces":$retry_n,"retry_traces_rooted":$retry_rooted,"republished":$repub,"republished_found":$repub_found,"republished_reconcile_root":$repub_recon,
 "not_sent_traces":$notsent,"not_sent_reasons":"$reasons","exemplars":$(num "$ex_n"),"exemplar_trace_found":$ex_found}
JSON
  echo "[$RUN] P9b 원자료 — $OUT_DIR/$RUN.p9b.json"
}
