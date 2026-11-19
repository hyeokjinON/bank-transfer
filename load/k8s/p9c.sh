# P9c 실험 원자료 (스펙 §7) — load/k8s/run.sh 가 PHASE=p9c 일 때 source 한다 (run.sh 의 변수 · 함수를 그대로 쓴다).
# 남기는 것: docs/perf/raw/p9c/<동작>-<회차>.p9c.json (숫자 요약) · .l1.txt · .res.txt · .loki-*.json (Loki 지표 질의 결과) · .alert-sink.txt · .inhibit.txt · .errfault-probes.txt
#   로그 원문은 남기지 않는다 (작업 폴더 build/load-p9c/ 에만).
#   L1 수집 지연 (steady): 부하 동안 api 컨테이너 PID 1 의 표준 출력에 표지 줄을 쓰고 Loki 에서 보일 때까지 0.5초 간격으로 묻는다 (해상도 약 1초 — kubectl exec 왕복 포함)
#   L2 비용: JSONLOG=on|off (off = app-config 의 LOGGING_STRUCTURED_FORMAT_CONSOLE 를 지운다 — 회차마다 api · 게이트웨이 재시작). 서비스별 줄 · 바이트/s, Loki · Alloy CPU · 메모리(/metrics 10초 표본)
#   L3 podkill: 희생 api Pod 의 노드 로그 파일을 부하 시작 때부터 tail -f 로 떠 둔다 (강제 삭제 뒤 kubelet 이 파일을 지운다) — F 줄 수 vs Loki 줄 수
#   L4 lokizero: Loki 0대 동안 api 가 쓴 줄 수 vs 복구 뒤 Loki 에 들어온 수 (emptyDir — 그 전 줄은 사라진다, 위험 R4. 기준 없이 기록)
#   L5 alloykill: api Pod(첫째)가 있는 노드의 Alloy Pod 삭제 — 그 api Pod 의 kubectl logs 줄 수 vs Loki 줄 수 (잃음 · 중복)
#   L6 ① 모든 회차: 5분 ERROR 수 최대 (규칙 식 그대로 · DLT 발행 줄 포함 둘 다), ② errfault: 1원인증 저장 실패 주입 → ErrorLogSurge 도착 vs 계산값, ApiHigh5xx 안 울림,
#      ③ bank: BankDisconnected 가 울리는 동안 시험 ErrorLogSurge 를 넣어 Alertmanager 가 suppressed 로 보는지 · alert-sink 에 안 오는지
#   L7 모든 회차: 금지 패턴 0건 + 가린 모양이 실제로 있음 (양성 대조)
# Loki · Prometheus · Alertmanager 질의는 Prometheus · Alertmanager Pod 의 busybox wget (lib.sh loki_get · prom_get — PC 포트를 열지 않는다)

P9C_N=${P9C_N:-10}   # ErrorLogSurge 임계 — infra/k8s/monitoring/loki/rules.yml 의 "> 10" 과 같은 값 (L6 ① 뒤 바꾸면 둘 다)
P9C_SURGE='{namespace="bank-transfer", app=~"api|bank-gateway"} | level="ERROR" != "DeadLetterPublishingRecovererFactory"'
P9C_ERR_ALL='{namespace="bank-transfer", app=~"api|bank-gateway"} | level="ERROR"'
P9C_FORBID='{namespace="bank-transfer"} |~ `(^|\\[nrtbf]|\\u[0-9A-Fa-f]{4}|[^0-9A-Za-z])[0-9]{10,14}([^0-9A-Za-z]|$)|BT[0-9]{4}|[0-9]\x{C6D0}`'
P9C_PIDS=; P9C_TAIL=; P9C_INHIBIT=; P9C_VICTIM=; P9C_LOSS_POD=; P9C_NODE=

p9c_tag() {
  P9C_TAG=$(deployed_tag)
  [ "$(api_tag)" = "$P9C_TAG" ] || die "api 태그 $(api_tag) ≠ 게이트웨이 태그 $P9C_TAG — 먼저: bash -c '. infra/k8s/lib.sh; apply_overlay local $P9C_TAG'"
}

# 구조화 로그를 저장소 값(overlays/local 의 ecs)으로 되돌린다 — env 는 Pod 시작 때 읽으므로 다시 시작한다 (여러 번 불려도 된다)
restore_jsonlog() {
  local tag; tag=$(deployed_tag 2>/dev/null || true); tag=${tag:-$P9C_TAG}
  apply_overlay local "$tag" >/dev/null 2>&1 || true
  k rollout restart deploy/api deploy/bank-gateway >/dev/null 2>&1 || true
  k rollout status deploy/bank-gateway --timeout=300s >/dev/null 2>&1 || true
  k rollout status deploy/api --timeout=420s >/dev/null 2>&1 || true
  local now; now=$(k get configmap app-config -o jsonpath='{.data.LOGGING_STRUCTURED_FORMAT_CONSOLE}' 2>/dev/null | tr -d '\r' || true)
  if [ "$now" = ecs ]; then
    echo "[$RUN] 구조화 로그를 overlays/local 값(ecs)으로 되돌렸다 — 확인함 (api · 게이트웨이 재시작 뒤 준비 확인: kubectl -n bank-transfer rollout status deploy/api)" >&2
  else
    echo "[$RUN] !!! 경고: 구조화 로그를 못 되돌렸다 (app-config 값 '${now:-없음}') — 직접: bash -c '. infra/k8s/lib.sh; apply_overlay local $tag' 후 kubectl -n bank-transfer rollout restart deploy/api deploy/bank-gateway" >&2
    return 1
  fi
}

# 대상 선택(run.sh 사전 확인) 전에 부른다 — 재시작이 Pod 를 바꾸고, 새 Pod 라야 로그 파일이 0.log 하나다 (L3 · L5)
p9c_prepare() {
  p9c_tag
  local cur i
  cur=$(k get configmap app-config -o jsonpath='{.data.LOGGING_STRUCTURED_FORMAT_CONSOLE}' 2>/dev/null || true)
  if [ "$JSONLOG" = off ]; then
    JSONLOG_DIRTY=1
    trap restore_jsonlog EXIT   # stop_bg 트랩이 들어서기 전에 죽어도 되돌린다
    trap 'exit 130' INT TERM
    if [ -n "$cur" ]; then k patch configmap app-config --type json -p '[{"op":"remove","path":"/data/LOGGING_STRUCTURED_FORMAT_CONSOLE"}]' >/dev/null; fi
  elif [ "$cur" != ecs ]; then
    echo "[$RUN] 주의: 구조화 로그가 '$cur' 였다 (이전 실험이 못 되돌렸나) — ecs 로 맞춘다" >&2
    k patch configmap app-config --type merge -p '{"data":{"LOGGING_STRUCTURED_FORMAT_CONSOLE":"ecs"}}' >/dev/null
  fi
  km rollout status deploy/loki --timeout=180s >/dev/null || die "Loki 가 준비되지 않았다 (kubectl -n monitoring get pods -l app=loki)"
  km rollout status ds/alloy --timeout=180s >/dev/null || die "Alloy 가 3/3 이 아니다 (kubectl -n monitoring get pods -l app=alloy)"
  sql_root <<<"DROP TRIGGER IF EXISTS p9c_fault_verification;" >/dev/null 2>&1 || true   # 이전 errfault 가 못 지웠어도 깨끗하게
  echo "[$RUN] 구조화 로그 $JSONLOG — api · 게이트웨이 재시작 (회차마다 같은 조건 · 새 로그 파일)"
  k rollout restart deploy/bank-gateway deploy/api >/dev/null
  k rollout status deploy/bank-gateway --timeout=300s >/dev/null || die "게이트웨이 재시작이 300초 안에 끝나지 않았다"
  k rollout status deploy/api --timeout=420s >/dev/null || die "api 재시작이 420초 안에 끝나지 않았다"
  wait_no_terminating || die "종료 중인 Pod 가 남아 있다"
  for (( i = 0; i < 60; i++ )); do [ -n "$(gw_leader)" ] && break; sleep 2; done
  [ -n "$(gw_leader)" ] || echo "[$RUN] 주의: 게이트웨이 리더 라벨이 120초 안에 안 붙었다" >&2
}

# --- 자원 표본 (L2) ---
# Prometheus Pod 의 wget 으로 /metrics 한 값 (없으면 빈 값). 값은 1.2e+08 처럼 지수일 수 있다 — awk 가 숫자로 읽는다
p9c_metric() { { km exec deploy/prometheus -- wget -qO- "$1" 2>/dev/null || true; } | awk -v m="$2" '$1 == m { print $2; exit }'; }
# "epoch_s loki_cpu_s loki_rss_b alloy_cpu_s_합 alloy_rss_b_최대" 한 줄
p9c_res_sample() {
  local lc lr ips ip c r ac=0 ar=0
  lc=$(p9c_metric http://loki.monitoring.svc.cluster.local:3100/metrics process_cpu_seconds_total)
  lr=$(p9c_metric http://loki.monitoring.svc.cluster.local:3100/metrics process_resident_memory_bytes)
  ips=$(km get pods -l app=alloy -o jsonpath='{range .items[*]}{.status.podIP}{"\n"}{end}' 2>/dev/null | tr -d '\r' || true)
  for ip in $ips; do
    c=$(p9c_metric "http://$ip:12345/metrics" process_cpu_seconds_total); r=$(p9c_metric "http://$ip:12345/metrics" process_resident_memory_bytes)
    ac=$(awk -v a="$ac" -v b="${c:-0}" 'BEGIN { printf "%.3f", a + b }'); ar=$(awk -v a="$ar" -v b="${r:-0}" 'BEGIN { printf "%.0f", (b + 0 > a + 0) ? b : a }')
  done
  printf '%s %s %s %s %s\n' "$(date +%s)" "${lc:-NaN}" "${lr:-NaN}" "$ac" "$ar"
}
p9c_res_loop() { while :; do p9c_res_sample >> "$WORK/res.txt"; sleep 10; done; }
# $1 ~ $2 (epoch 초) 의 평균 CPU(코어) · 최대 RSS(MiB) → "loki_cpu loki_rss_mb alloy_cpu alloy_rss_mb" (재시작으로 누적값이 줄면 null)
p9c_res_summary() {
  awk -v a="$1" -v b="$2" '$1 >= a && $1 <= b && $2 != "NaN" {
      if (!n++) { t0 = $1; l0 = $2; c0 = $4 }
      t1 = $1; l1 = $2; c1 = $4
      if ($3 + 0 > lr) lr = $3 + 0
      if ($5 + 0 > ar) ar = $5 + 0
    }
    END {
      if (n < 2 || t1 <= t0) { print "null null null null"; exit }
      lc = (l1 - l0) / (t1 - t0); ac = (c1 - c0) / (t1 - t0)
      printf "%s %.0f %s %.0f\n", (lc < 0 ? "null" : sprintf("%.3f", lc)), lr / 1048576, (ac < 0 ? "null" : sprintf("%.3f", ac)), ar / 1048576
    }' "$WORK/res.txt" 2>/dev/null || echo "null null null null"
}

# --- L1 수집 지연 ---
# 표지 줄을 api 컨테이너 PID 1(java)의 표준 출력에 쓴다 → 컨테이너 로그 파일 → Alloy → Loki. "i 쓴_ms 찾은_ms|null [exec_failed]"
p9c_l1_loop() {
  local pod i t_w found end=$(( $(date +%s) + DURATION_S ))
  pod=$(live_pods api | awk 'NF {print $1; exit}')
  for (( i = 1; i <= L1_SAMPLES; i++ )); do
    [ "$(date +%s)" -lt "$end" ] || break
    if ! k exec "$pod" -c api -- sh -c "echo 'p9c-l1 $RUN $i end' > /proc/1/fd/1" >/dev/null 2>&1; then
      printf '%s %s null exec_failed\n' "$i" "$(date +%s%3N)" >> "$WORK/l1.txt"; sleep 2; continue
    fi
    t_w=$(date +%s%3N); found=null
    for _ in $(seq 60); do
      if [ "$(loki_count "{namespace=\"bank-transfer\", app=\"api\"} |= \"p9c-l1 $RUN $i end\"" 300)" = 1 ]; then found=$(date +%s%3N); break; fi
      sleep 0.5
    done
    printf '%s %s %s\n' "$i" "$t_w" "$found" >> "$WORK/l1.txt"
    sleep 1
  done
}
# → "표본수 p50_ms p95_ms 못찾음" (가장 가까운 순위)
p9c_l1_summary() {
  awk '$3 == "null" { miss++; next } NF >= 3 { n++; v[n] = $3 - $2 }
    END {
      if (!n) { printf "0 null null %d\n", miss + 0; exit }
      for (i = 1; i <= n; i++) for (j = i + 1; j <= n; j++) if (v[j] < v[i]) { t = v[i]; v[i] = v[j]; v[j] = t }
      printf "%d %d %d %d\n", n, v[int((n - 1) * 0.5) + 1], v[int((n - 1) * 0.95) + 1], miss + 0
    }' "$WORK/l1.txt" 2>/dev/null || echo "0 null null 0"
}

# --- L6 ② 1원인증 저장 실패 주입 ---
# 비밀번호는 mysql Pod 가 이미 가진 환경변수로만 (명령행 · 출력에 나오지 않게 — lib.sh sql() 과 같은 방식). 트리거 생성은 binlog 때문에 root 로
sql_root() { k exec -i mysql-0 -- sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot "$MYSQL_DATABASE" -N'; }
p9c_fault_on() {
  sql_root <<'SQL'
DROP TRIGGER IF EXISTS p9c_fault_verification;
CREATE TRIGGER p9c_fault_verification BEFORE INSERT ON account_verification FOR EACH ROW
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p9c fault injection (L6)';
SQL
}
# 지운 뒤 SHOW TRIGGERS 로 정말 없는지 확인 (최대 5번). 못 지우면 크게 경고하고 실패(1) — 남은 트리거는 1원인증을 막는다
p9c_fault_off() {
  local i left=x
  for (( i = 0; i < 5; i++ )); do
    if sql_root <<<"DROP TRIGGER IF EXISTS p9c_fault_verification;" >/dev/null 2>&1 \
       && left=$(sql_root <<<"SHOW TRIGGERS WHERE \`Trigger\`='p9c_fault_verification';" 2>/dev/null | tr -d '\r') && [ -z "$left" ]; then
      return 0
    fi
    sleep 2
  done
  echo "[${RUN:-p9c}] !!! 경고: 1원인증 저장 실패 트리거(p9c_fault_verification)를 못 지웠다 — 1원인증이 계속 실패한다. 직접: kubectl -n bank-transfer exec -i mysql-0 -- sh -c 'MYSQL_PWD=\"\$MYSQL_ROOT_PASSWORD\" mysql -uroot \"\$MYSQL_DATABASE\" -e \"DROP TRIGGER IF EXISTS p9c_fault_verification\"'" >&2
  return 1
}
# $1 초 동안 VERIFY_EVERY_S 마다 1원인증 요청 (모의 은행 예금주 홍길동 — 성명조회 · 1원은 통과하고 저장에서 500 이 기대값). "epoch_ms 코드" 줄
p9c_verify_probes() {
  local until=$(( $(date +%s) + $1 )) code
  while [ "$(date +%s)" -lt "$until" ]; do
    code=$(printf '%s' '{"bankCode":"090","accountNo":"1002123456789","holderName":"홍길동"}' \
      | curl -s -o /dev/null -w '%{http_code}' -m 20 -X POST http://127.0.0.1:8000/api/v1/account-verifications \
          -H 'Content-Type: application/json' -H 'X-User-Id: load-p82-u001' --data-binary @- || true)
    printf '%s %s\n' "$(date +%s%3N)" "${code:-000}" >> "$WORK/errfault-probes.txt"
    sleep "$VERIFY_EVERY_S"
  done
}

# --- L6 ③ 억제 ---
# BankDisconnected 가 울리면 시험 ErrorLogSurge(25초 뒤 저절로 끝남 — 상태 확인 15초보다 길고, 은행 복구 · BankDisconnected 해소 전에 끝난다)를 Alertmanager 에 넣고, 15초 뒤 그 알림 상태를 남긴다
p9c_inhibit_probe() {
  local i fired= ends
  for (( i = 0; i < 60; i++ )); do
    [ "$(prom_value 'count(ALERTS{alertname="BankDisconnected",alertstate="firing"})')" = 1 ] && { fired=1; break; }
    sleep 2
  done
  [ -n "$fired" ] || { echo "no_bank_disconnected" > "$WORK/inhibit.txt"; return 0; }
  ends=$(date -u -d '+25 seconds' +%Y-%m-%dT%H:%M:%SZ)
  km exec deploy/alertmanager -- wget -qO- --header='Content-Type: application/json' \
    --post-data="[{\"labels\":{\"alertname\":\"ErrorLogSurge\",\"app\":\"api\",\"severity\":\"warning\",\"source\":\"logs\",\"p9c_probe\":\"$RUN\"},\"annotations\":{\"summary\":\"p9c L6 inhibition probe\"},\"endsAt\":\"$ends\"}]" \
    http://127.0.0.1:9093/api/v2/alerts >/dev/null 2>&1 || { echo "post_failed" > "$WORK/inhibit.txt"; return 0; }
  sleep 15
  km exec deploy/alertmanager -- wget -qO- 'http://127.0.0.1:9093/api/v2/alerts?inhibited=true&silenced=true&active=true&filter=alertname%3D%22ErrorLogSurge%22' 2>/dev/null | tr -d '\r\n' > "$WORK/inhibit.txt" || true
}

# --- 배경 작업 (run.sh 가 부하 직전에 부른다) ---
p9c_alloy_pod_on() { km get pods -l app=alloy -o jsonpath="{range .items[?(@.spec.nodeName==\"$1\")]}{.metadata.name}{\"\n\"}{end}" | tr -d '\r' | head -1; }
p9c_start_bg() {
  : > "$WORK/res.txt"; : > "$WORK/l1.txt"
  p9c_res_loop & P9C_PIDS="$P9C_PIDS $!"
  if [ "$ACTION" = steady ]; then p9c_l1_loop & P9C_PIDS="$P9C_PIDS $!"; fi
  local uid node
  case "$ACTION" in
    podkill)   # 희생 Pod 를 미리 정하고 (run.sh 가 같은 Pod 를 지운다) 노드의 로그 파일을 끝까지 떠 둔다 — 지워져도 열린 fd 는 남는다
      P9C_VICTIM=$(k get pods -l app=api -o jsonpath='{.items[0].metadata.name}')
      uid=$(k get pod "$P9C_VICTIM" -o jsonpath='{.metadata.uid}'); node=$(k get pod "$P9C_VICTIM" -o jsonpath='{.spec.nodeName}')
      d exec "$node" timeout "$(( DURATION_S + 30 ))" tail -n +1 -f "/var/log/pods/${NAMESPACE}_${P9C_VICTIM}_${uid}/api/0.log" > "$WORK/l3-victim-cri.txt" 2> "$WORK/l3-victim-cri.err" &
      P9C_TAIL=$! ;;
    alloykill)
      P9C_LOSS_POD=$(k get pods -l app=api -o jsonpath='{.items[0].metadata.name}')
      P9C_NODE=$(k get pod "$P9C_LOSS_POD" -o jsonpath='{.spec.nodeName}') ;;
    bank) p9c_inhibit_probe & P9C_INHIBIT=$! ;;
  esac
}
p9c_stop_bg() { kill $P9C_PIDS ${P9C_TAIL:+"$P9C_TAIL"} ${P9C_INHIBIT:+"$P9C_INHIBIT"} 2>/dev/null || true; P9C_PIDS=; }

# --- 원자료 ---
# 기대 줄 수에 닿거나 60초가 지나면 마지막 값 (수집 · 재시도 지연을 기다린다). $1 선택자, $2 범위 초, $3 기대, $4 시각
p9c_loki_settle() {
  local i v=
  for (( i = 0; i < 12; i++ )); do
    v=$(loki_count "$1" "$2" "${4:-}")
    if [[ "$v" =~ ^[0-9]+$ ]] && [ "$v" -ge "$3" ]; then break; fi
    sleep 5
  done
  echo "${v:-null}"
}
# 살아 있는 Pod: (T0, 지금−30초] 의 kubectl logs 줄 수 vs Loki 줄 수 → "kubectl loki" (Loki 범위 (시작, 끝] 과 맞춘다 — 초 단위 문자열 비교)
p9c_loss_live() {
  local pod=$1 t0_s end_s from to k_n l_n
  t0_s=$(date -u -d "$T0" +%s); end_s=$(( $(date +%s) - 30 ))
  from=$(date -u -d "@$t0_s" +%Y-%m-%dT%H:%M:%S); to=$(date -u -d "@$end_s" +%Y-%m-%dT%H:%M:%S)
  k_n=$(k logs "$pod" -c api --timestamps --since-time="$T0" 2>/dev/null | awk -v a="$from" -v b="$to" '{ t = substr($1, 1, 19) } t >= a && t < b' | wc -l | tr -d ' ')
  l_n=$(p9c_loki_settle "{namespace=\"bank-transfer\", app=\"api\"} | pod=\"$pod\"" $(( end_s - t0_s )) "$k_n" "$end_s")
  echo "$k_n $l_n"
}
# 5분 ERROR 수의 최대 (T0 ~ 지금, 30초 간격, 모든 app 중). $2 = 원자료 이름
p9c_err5m_max() {
  local q="sum by (app) (count_over_time($1 [5m]))"
  { loki_get "/loki/api/v1/query_range?query=$(urlenc "$q")&start=$(date -u -d "$T0" +%s)&end=$(date +%s)&step=30" 2>/dev/null || true; } | tr -d '\r' > "$OUT_DIR/$RUN.loki-$2.json"
  # 질의가 실패했으면(성공 응답이 아님) null — 0 으로 바꾸지 않는다. 성공했는데 계열이 없으면 ERROR 가 없었다는 뜻이라 0
  if ! grep -q '"status":"success"' "$OUT_DIR/$RUN.loki-$2.json" 2>/dev/null; then echo null; return 0; fi
  { grep -o '\[[0-9.]*,"[0-9.]*"\]' "$OUT_DIR/$RUN.loki-$2.json" || true; } | tr -d '[]"' | awk -F, '{ if ($2 + 0 > m) m = $2 + 0 } END { printf "%d", m + 0 }'
}
# 동작 뒤 5분 ERROR 수(규칙 식)가 처음 N 을 넘은 시각 (동작 시각 기준 초, 5초 간격) 또는 null
p9c_cross_s() {
  local q="sum by (app) (count_over_time($P9C_SURGE [5m]))" ta_s=$(( TA_MS / 1000 ))
  { loki_get "/loki/api/v1/query_range?query=$(urlenc "$q")&start=$ta_s&end=$(date +%s)&step=5" 2>/dev/null || true; } | tr -d '\r' > "$OUT_DIR/$RUN.loki-surge-cross.json"
  { grep -o '\[[0-9.]*,"[0-9.]*"\]' "$OUT_DIR/$RUN.loki-surge-cross.json" || true; } | tr -d '[]"' \
    | awk -F, -v n="$P9C_N" -v ta="$ta_s" '$2 + 0 > n { t = $1 - ta; if (best == "" || t < best) best = t } END { if (best == "") print "null"; else printf "%.0f\n", best }'
}
# alert-sink 원자료에서 알림 $1 의 상태 $2(firing|resolved) 첫 도착 epoch_ms (동작 뒤) — 웹훅 본문 첫 "status" = 묶음 상태
p9c_sink_first() {
  local ts body
  { grep -a "\"alertname\":\"$1\"" "$OUT_DIR/$RUN.alert-sink.txt" | { grep -av 'p9c_probe' || true; } || true; } | while read -r ts body; do
    [ "$(grep -o '"status":"[a-z]*"' <<<"$body" | head -1 | cut -d'"' -f4)" = "$2" ] || continue
    date -d "$ts" +%s%3N 2>/dev/null
  done | awk -v t="$TA_MS" '$1 >= t' | sort -n | head -1
}
# app 별 초당 값 JSON: $1 = sum by (app) 질의, $2 = 시각 → {"api":1.23,...} (DURATION_S 로 나눔)
p9c_by_app() {
  { loki_get "/loki/api/v1/query?query=$(urlenc "$1")&time=$2" 2>/dev/null || true; } | tr -d '\r' \
    | { grep -o '"app":"[a-z-]*"},"value":\[[0-9.]*,"[0-9.]*"' || true; } \
    | awk -F'"' -v d="$DURATION_S" 'BEGIN { printf "{" } { printf "%s\"%s\":%.2f", (n++ ? "," : ""), $4, $(NF - 1) / d } END { printf "}" }'
}

p9c_capture() {
  kill $P9C_PIDS 2>/dev/null || true; P9C_PIDS=
  local load_end_s=$(( TA_MS0 / 1000 + DURATION_S )) t0_s win_s
  t0_s=$(date -u -d "$T0" +%s); win_s=$(( $(date +%s) - t0_s ))
  cp "$WORK/res.txt" "$OUT_DIR/$RUN.res.txt" 2>/dev/null || true
  cp "$WORK/l1.txt" "$OUT_DIR/$RUN.l1.txt" 2>/dev/null || true

  # 기본 (P9b 와 같은 정의 — 고정 도착률이라 처리량 = 출금 수 / (부하 + 마무리 대기) 초)
  local api_cpu gw_cpu p95 per_s
  api_cpu=$(prom_value "sum(avg_over_time(process_cpu_usage{app=\"api\"}[${DURATION_S}s]))" "$load_end_s")
  gw_cpu=$(prom_value "sum(avg_over_time(process_cpu_usage{app=\"bank-gateway\"}[${DURATION_S}s]))" "$load_end_s")
  p95=$(prom_value "histogram_quantile(0.95, sum by (le) (increase(banktransfer_transfer_completed_seconds_bucket{status=\"COMPLETED\"}[${win_s}s])))")
  per_s=$(awk -v n="${C[transfers]:-0}" -v d="$DURATION_S" -v s="$SETTLE_S" 'BEGIN { if (d + s > 0) printf "%.2f", n / (d + s) }')

  # L2
  local lines_json bytes_json loki_cpu loki_rss alloy_cpu alloy_rss
  lines_json=$(p9c_by_app "sum by (app) (count_over_time({namespace=\"bank-transfer\"} [${DURATION_S}s]))" "$load_end_s")
  bytes_json=$(p9c_by_app "sum by (app) (bytes_over_time({namespace=\"bank-transfer\"} [${DURATION_S}s]))" "$load_end_s")
  read -r loki_cpu loki_rss alloy_cpu alloy_rss < <(p9c_res_summary "$(( TA_MS0 / 1000 ))" "$load_end_s")

  # L1
  local l1_n l1_p50 l1_p95 l1_miss
  read -r l1_n l1_p50 l1_p95 l1_miss < <(p9c_l1_summary)

  # L3 · L4 · L5 — 잃음 · 중복
  local loss_pod= file_n= loki_n= lost= dup= pod ta_s te_s from to
  case "$ACTION" in
    podkill)
      if [ -n "$P9C_TAIL" ]; then wait "$P9C_TAIL" 2>/dev/null || true; P9C_TAIL=; fi
      loss_pod=$P9C_VICTIM
      file_n=$(awk '$3 == "F"' "$WORK/l3-victim-cri.txt" 2>/dev/null | wc -l | tr -d ' ')
      loki_n=$(p9c_loki_settle "{namespace=\"bank-transfer\", app=\"api\"} | pod=\"$P9C_VICTIM\"" 3600 "$file_n") ;;
    lokizero)
      ta_s=$(( TA_MS / 1000 + 1 )); te_s=$(( TE_MS / 1000 ))
      from=$(date -u -d "@$ta_s" +%Y-%m-%dT%H:%M:%S); to=$(date -u -d "@$te_s" +%Y-%m-%dT%H:%M:%S)
      file_n=0
      for pod in $(live_pods api | awk 'NF {print $1}'); do
        file_n=$(( file_n + $(k logs "$pod" -c api --timestamps --since-time="$T0" 2>/dev/null | awk -v a="$from" -v b="$to" '{ t = substr($1, 1, 19) } t >= a && t < b' | wc -l) ))
      done
      loki_n=$(p9c_loki_settle '{namespace="bank-transfer", app="api"}' $(( te_s - ta_s )) "$file_n" "$te_s") ;;
    alloykill)
      loss_pod=$P9C_LOSS_POD
      read -r file_n loki_n < <(p9c_loss_live "$P9C_LOSS_POD") ;;
  esac
  # 기준(파일 · kubectl logs 줄 수)이 0 이거나 숫자가 아니면 수집 실패 — 가짜 0 · 가짜 중복을 남기지 않고 null
  case "$ACTION" in podkill|lokizero|alloykill)
    if ! [[ "${file_n:-}" =~ ^[0-9]+$ ]] || [ "${file_n:-0}" -le 0 ]; then
      echo "[$RUN] 주의: 기준 줄 수가 ${file_n:-없음} — 로그 수집 실패로 보고 file_lines · lost_lines · dup_lines 를 null 로 남긴다 ($WORK/l3-victim-cri.err 등 확인)" >&2
      file_n=; lost=; dup=; loki_n=
    fi ;;
  esac
  if [[ "${file_n:-}" =~ ^[0-9]+$ ]] && [[ "${loki_n:-}" =~ ^[0-9]+$ ]]; then
    lost=$(( file_n > loki_n ? file_n - loki_n : 0 )); dup=$(( loki_n > file_n ? loki_n - file_n : 0 ))
  fi

  # L6 ① 정상 상태 (모든 회차) · ② · ③
  local err_max err_max_all cross=null exp_min=null exp_max=null api5xx=null probes= inhibit=null surge_fired surge_sink
  err_max=$(p9c_err5m_max "$P9C_SURGE" surge)
  err_max_all=$(p9c_err5m_max "$P9C_ERR_ALL" err-all)
  km logs deploy/alert-sink --timestamps --since-time="$T0" > "$OUT_DIR/$RUN.alert-sink.txt" 2>&1 || true
  surge_fired=$(since_ta "$(p9c_sink_first ErrorLogSurge firing)")
  surge_sink=$({ grep -a '"alertname":"ErrorLogSurge"' "$OUT_DIR/$RUN.alert-sink.txt" || true; } | { grep -av 'p9c_probe' || true; } | { grep -c '"status":"firing"' || true; })
  if [ "$ACTION" = errfault ]; then
    cross=$(p9c_cross_s)
    if [ "$cross" != null ]; then   # 계산값 = 넘은 시각 + for 2m + 묶기 10초, 늦게는 + ruler 평가 간격 1m + 수집 지연 여유 10초
      exp_min=$(( cross + 130 )); exp_max=$(( cross + 200 ))
    fi
    if [ -n "$(prom_value "max_over_time(ALERTS{alertname=\"ApiHigh5xx\",alertstate=\"firing\"}[${win_s}s])")" ]; then api5xx=true; else api5xx=false; fi
    probes=$(awk '{ c[$2]++ } END { for (k in c) printf "%s=%d ", k, c[k] }' "$WORK/errfault-probes.txt" 2>/dev/null | sed 's/ $//')
    cp "$WORK/errfault-probes.txt" "$OUT_DIR/$RUN.errfault-probes.txt" 2>/dev/null || true
  fi
  if [ "$ACTION" = bank ]; then
    if [ -n "$P9C_INHIBIT" ]; then wait "$P9C_INHIBIT" 2>/dev/null || true; P9C_INHIBIT=; fi
    cp "$WORK/inhibit.txt" "$OUT_DIR/$RUN.inhibit.txt" 2>/dev/null || true
    if grep -q '"inhibitedBy":\["[0-9a-f]' "$WORK/inhibit.txt" 2>/dev/null && grep -q '"state":"suppressed"' "$WORK/inhibit.txt"; then inhibit=true; else inhibit=false; fi
  fi

  # L7 가리기 — 금지 패턴 0 + 가린 모양이 실제로 있다 (양성 대조)
  local forb m_acct m_code m_amt
  forb=$(loki_count "$P9C_FORBID" "$win_s")
  m_acct=$(loki_count '{namespace="bank-transfer"} |~ `\*{6,10}[0-9]{4}`' "$win_s")
  m_code=$(loki_count '{namespace="bank-transfer"} |= "BT****"' "$win_s")
  m_amt=$(loki_count '{namespace="bank-transfer"} |~ `\*\*\*\x{C6D0}`' "$win_s")

  cat > "$OUT_DIR/$RUN.p9c.json" <<JSON
{"run":"$RUN","jsonlog":"$JSONLOG","rate":$RATE,"duration_s":$DURATION_S,"settle_s":$SETTLE_S,"transfers":$(num "${C[transfers]:-}"),
 "completed_per_s":$(num "$per_s"),"accept_p95_ms":$(num "$(k6v p95_ms)"),"complete_p95_s":$(num "$p95"),"api_cpu_avg":$(num "$api_cpu"),"gw_cpu_avg":$(num "$gw_cpu"),
 "log_lines_per_s":${lines_json:-null},"log_bytes_per_s":${bytes_json:-null},"loki_cpu_avg":$(num "$loki_cpu"),"loki_rss_max_mb":$(num "$loki_rss"),"alloy_cpu_avg":$(num "$alloy_cpu"),"alloy_rss_max_mb":$(num "$alloy_rss"),
 "l1_samples":$(num "$l1_n"),"l1_p50_ms":$(num "$l1_p50"),"l1_p95_ms":$(num "$l1_p95"),"l1_missing":$(num "$l1_miss"),
 "loss_pod":$(str "$loss_pod"),"file_lines":$(num "$file_n"),"loki_lines":$(num "$loki_n"),"lost_lines":$(num "$lost"),"dup_lines":$(num "$dup"),
 "err5m_max":$(num "$err_max"),"err5m_max_with_dlt":$(num "$err_max_all"),"surge_n":$P9C_N,"surge_cross_s":$cross,"surge_fired_s":$surge_fired,"expected_min_s":$exp_min,"expected_max_s":$exp_max,
 "api5xx_fired":$api5xx,"probe_codes":"$probes","inhibit_ok":$inhibit,"surge_sink_firing":$(num "$surge_sink"),
 "forbidden_lines":$(num "$forb"),"masked_account_lines":$(num "$m_acct"),"masked_code_lines":$(num "$m_code"),"masked_amount_lines":$(num "$m_amt")}
JSON
  echo "[$RUN] P9c 원자료 — $OUT_DIR/$RUN.p9c.json"
}
