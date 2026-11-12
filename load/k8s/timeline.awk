# load/k8s/timeline.sh 의 줄 → 실험 시간 지표 (run.sh 가 부른다). ta = 동작 시각(epoch ms, steady 는 0), node = 대상 워커(없으면 -)
# 출력 key=value. 초는 동작 시각 기준. 해당 없음은 null
$1 == "P" {
  t = $2 + 0
  if (!(t in seen)) { seen[t] = 1; ts[++n] = t }
  if ($3 == "api") {
    if ($6 == "True") apiReady[t]++
    if ($6 == "True" && $5 != node) apiOff[t]++
    if ($5 == node && $6 != "True") apiOnNodeNot[t]++
  }
  if ($3 == "bank-gateway") {
    if ($6 == "True") gwReady[t] = 1
    if (!($4 in r0)) r0[$4] = $7
    r1[$4] = $7
  }
}
$1 == "N" && $3 == node && $4 != "True" && ($2 + 0) >= ta && nodeDown == "" { nodeDown = $2 + 0 }
function sec(x) { return sprintf("%.1f", (x - ref) / 1000) }
function allnull() {
  print "api_ready_min=null"; print "api_recovered_s=null"; print "gw_not_ready_s=null"; print "gw_restarts=null"
  print "node_notready_s=null"; print "pod_notready_s=null"; print "replacement_ready_s=null"
}
END {
  if (n == 0) { allnull(); exit }
  ref = (ta + 0 == 0 ? ts[1] : ta + 0)   # steady(ta=0) 는 첫 표본 시각을 기준으로
  min = ""; lastBad = ""; gwNot = 0; podNot = ""; repl = ""
  for (i = 1; i <= n; i++) {
    t = ts[i]; if (t < ta) continue
    c = apiReady[t] + 0
    if (min == "" || c < min) min = c
    if (c < 2) lastBad = t
    if (!(t in gwReady) && i < n) gwNot += ts[i + 1] - t
    if (podNot == "" && apiOnNodeNot[t] > 0) podNot = t
  }
  rec = ""
  if (lastBad == "") rec = "0.0"
  else for (i = 1; i <= n; i++) if (ts[i] > lastBad && apiReady[ts[i]] + 0 >= 2) { rec = sec(ts[i]); break }
  if (node != "-") for (i = 1; i <= n; i++) if (ts[i] >= ta && apiOff[ts[i]] + 0 >= 2) { repl = sec(ts[i]); break }
  restarts = 0; for (p in r1) restarts += r1[p] - r0[p]
  print "api_ready_min=" (min == "" ? "null" : min)
  print "api_recovered_s=" (rec == "" ? "null" : rec)
  print "gw_not_ready_s=" sprintf("%.1f", gwNot / 1000)
  print "gw_restarts=" restarts
  print "node_notready_s=" (nodeDown == "" ? "null" : sec(nodeDown))
  print "pod_notready_s=" (node == "-" || podNot == "" ? "null" : sec(podNot))
  print "replacement_ready_s=" (repl == "" ? "null" : repl)
}
