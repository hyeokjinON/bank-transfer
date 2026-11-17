# 시계열 하나("epoch초 값" 줄, 5초 간격) → 동작 뒤 처음 0 이 된 시각 · 다시 1 이상이 된 시각 · 그 사이 (load/k8s/p9a.sh 가 부른다)
# ta = 동작 시각(epoch 초), name = 출력 접두. 출력 key=value — 초는 동작 시각 기준, 없으면 null. 해상도 = 수집 간격 5초
# "다시 1" 은 연속 두 표본(10초)이 1 이상일 때 그 첫 표본 — 거부되는 세션의 아주 짧은 연결(단일 세션 모의 은행)을 복구로 읽지 않게
$1 + 0 >= ta && zero == "" && $2 != "NaN" && $2 + 0 == 0 { zero = $1 + 0; next }
zero != "" && back == "" {
  if ($2 != "NaN" && $2 + 0 >= 1) { if (cand == "") cand = $1 + 0; else back = cand }
  else cand = ""
}
END {
  printf "%s_zero_s=%s\n", name, (zero == "" ? "null" : sprintf("%.1f", zero - ta))
  printf "%s_back_s=%s\n", name, (back == "" ? "null" : sprintf("%.1f", back - ta))
  printf "%s_gap_s=%s\n", name, (zero == "" || back == "" ? "null" : sprintf("%.1f", back - zero))
}
