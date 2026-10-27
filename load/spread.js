// P4-2 시나리오 ② 분산: 500명(load-u001 ~ load-u500) 중 요청마다 무작위 — 경합이 거의 없어 방식 자체의 고정 비용이 드러난다.
// 실행: bash load/run.sh <전략> spread <회차>
import { options as common, handleSummary as summarize, transfer } from './common.js';

export const options = common;

export function handleSummary(data) {
  return summarize(data);
}

export default function () {
  const n = Math.floor(Math.random() * 500) + 1;
  transfer(`load-u${String(n).padStart(3, '0')}`);
}
