// P4-2 시나리오 ① 핫스팟: 한 사용자(load-hot)에게 50 VU 가 쉬지 않고 1원 출금 — 한 행을 50명이 다툰다.
// 비관적 = 대기열, 낙관적 = 재시도 폭주, Redis = 왕복 + 대기가 드러난다. 실행: bash load/run.sh <전략> hotspot <회차>
import { options as common, handleSummary as summarize, transfer } from './common.js';

export const options = common;

export function handleSummary(data) {
  return summarize(data);
}

export default function () {
  transfer('load-hot');
}
