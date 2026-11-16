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
    if ($5 == "-") gwPend[t] = 1   # 노드가 정해지지 않은 Pod = Pending (P8-3c: 필수 분산이 막은 것)
    if ($6 == "True" && $5 != "-") { gwRC[t]++; if (!((t, $5) in gwRN)) { gwRN[t, $5] = 1; gwRD[t]++ } }   # Ready 게이트웨이 수 · 그들이 있는 노드 수
    if ($6 == "True") gwReady[t] = 1
    if ($6 == "True" && $8 == "leader") gwLead[t] = 1
    if (!($4 in r0)) r0[$4] = $7
    r1[$4] = $7
  }
}
$1 == "N" && $3 == node && $4 != "True" && ($2 + 0) >= ta && nodeDown == "" { nodeDown = $2 + 0 }
$1 == "L" && $3 != "-" && ($2 + 0) >= ta {   # holder 가 빈 표본(-, 인계 중 · kubectl 일시 실패)은 세지 않는다 — 실제 holder 만
  if (lastHolder != "" && $3 != lastHolder) changes++
  lastHolder = $3
}
function sec(x) { return sprintf("%.1f", (x - ref) / 1000) }
function allnull() {
  print "api_ready_min=null"; print "api_recovered_s=null"; print "gw_not_ready_s=null"; print "gw_restarts=null"
  print "node_notready_s=null"; print "pod_notready_s=null"; print "replacement_ready_s=null"
  print "gw_leaderless_s=null"; print "leader_changes=null"
  print "gw_pending_s=null"; print "gw_colocated_s=null"; print "gw_nodes_end=null"
}
END {
  if (n == 0) { allnull(); exit }
  ref = (ta + 0 == 0 ? ts[1] : ta + 0)   # steady(ta=0) 는 첫 표본 시각을 기준으로
  min = ""; lastBad = ""; gwNot = 0; gwNoLead = 0; podNot = ""; repl = ""
  for (i = 1; i <= n; i++) {
    t = ts[i]; if (t < ta) continue
    c = apiReady[t] + 0
    if (min == "" || c < min) min = c
    if (c < 2) lastBad = t
    if (!(t in gwReady) && i < n) gwNot += ts[i + 1] - t
    if (!(t in gwLead) && i < n) gwNoLead += ts[i + 1] - t
    if ((t in gwPend) && i < n) gwPendMs += ts[i + 1] - t
    if (gwRC[t] + 0 >= 2 && gwRD[t] + 0 == 1 && i < n) gwColoMs += ts[i + 1] - t   # Ready 게이트웨이 2대 이상이 한 노드에만
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
  print "gw_leaderless_s=" sprintf("%.1f", gwNoLead / 1000)   # Ready 이면서 리더 라벨인 게이트웨이가 없던 시간 (선출 끈 H0 은 전 구간 — gw_not_ready_s 를 본다)
  print "leader_changes=" (changes + 0)
  print "gw_pending_s=" sprintf("%.1f", gwPendMs / 1000)     # 게이트웨이 Pod 가 노드 없이 기다린 시간 (동작 뒤)
  print "gw_colocated_s=" sprintf("%.1f", gwColoMs / 1000)   # Ready 게이트웨이가 모두 한 노드에 있던 시간 (동작 뒤)
  print "gw_nodes_end=" (gwRD[ts[n]] + 0)                    # 마지막 표본에서 Ready 게이트웨이가 있는 노드 수
}
