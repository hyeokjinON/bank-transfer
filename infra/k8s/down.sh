#!/usr/bin/env bash
# P8-3a 클러스터 삭제 (데이터 · PV 까지 — 실험 클러스터라 남길 것이 없다). 사용: bash infra/k8s/down.sh
set -euo pipefail
. "$(dirname "$0")/lib.sh"
kindc delete cluster --name "$CLUSTER_NAME"
rm -rf build/k8s
