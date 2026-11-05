// P6-2 웹 1원인증 측정 (설계 §6): 열린 모델 — 초당 RATE 건이 일정하게 도착한다 (constant-arrival-rate). 응답이 느려도 도착률은 그대로다.
// 시나리오 하나(워밍업 + 측정)로 돌리고, 요청마다 경과 시간으로 phase 태그를 붙여 측정 구간만 모은다 — 시나리오를 둘로 나누면 VU 풀이 둘이라 메모리가 두 배다.
// 환경변수는 load/verification/run.sh 가 넘긴다: RUN, BASE_URL, RATE, WARMUP_S(10), MEASURE_S(30), MAX_VUS(2000)
import http from 'k6/http';
import exec from 'k6/execution';

const BASE_URL = __ENV.BASE_URL;
const RUN = __ENV.RUN;
const RATE = Number(__ENV.RATE);
const WARMUP_S = Number(__ENV.WARMUP_S || 10);
const MEASURE_S = Number(__ENV.MEASURE_S || 30);
const MAX_VUS = Number(__ENV.MAX_VUS || 2000);
// 모의 은행 기본 계좌 — 예금주가 맞아 모두 201 이 된다. 셋에 나눠 한 계좌의 입금 기록이 너무 길어지지 않게
const ACCOUNTS = [['1002123456789', '홍길동'], ['3333012345678', '김카뱅'], ['1101234567890', '이페이']];
const STATUSES = ['201', '400', '404', '422', '500', '502', '503', '504', '0'];   // 0 = k6 시간 초과 · 연결 오류

function thresholds() {
  // 판정용이 아니다: 하위 지표를 요약에 남기게 하는 장치 (load/common.js 와 같다)
  const t = {
    'http_req_duration{phase:measure,status:201}': ['max>=0'],
    'http_reqs{phase:measure}': ['count>=0'],
    'http_reqs{status:201}': ['count>=0'],   // 워밍업 포함 201 수 = DB 행 수와 맞춰 본다
    'http_reqs{status:0}': ['count>=0'],     // 워밍업 포함 시간 초과 수 (스펙과 다른 점 5)
  };
  for (const s of STATUSES) t[`http_reqs{phase:measure,status:${s}}`] = ['count>=0'];
  return t;
}

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
  scenarios: {
    open: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: `${WARMUP_S + MEASURE_S}s`,
      preAllocatedVUs: Math.min(MAX_VUS, Math.max(50, RATE)),
      maxVUs: MAX_VUS,
      gracefulStop: '15s',   // 끝난 뒤 진행 중 요청을 기다린다 (k6 시간 초과 10초보다 길게) — 201 수와 DB 행 수를 맞춰 보기 때문
    },
  },
  thresholds: thresholds(),
};

export default function () {
  const i = exec.scenario.iterationInTest;
  const user = `p62-u${String((i % 100) + 1).padStart(3, '0')}`;
  const [accountNo, holderName] = ACCOUNTS[i % ACCOUNTS.length];
  const phase = exec.instance.currentTestRunDuration < WARMUP_S * 1000 ? 'warmup' : 'measure';
  http.post(`${BASE_URL}/api/v1/account-verifications`, JSON.stringify({ bankCode: '090', accountNo, holderName }), {
    headers: { 'Content-Type': 'application/json', 'X-User-Id': user },
    timeout: '10s',
    tags: { phase },
  });
}

function values(data, metric, tags) {
  const want = tags.slice().sort().join(',');
  for (const name of Object.keys(data.metrics)) {
    if (!name.startsWith(`${metric}{`)) continue;
    const got = name.slice(metric.length + 1, -1).split(',').map((s) => s.trim()).sort().join(',');
    if (got === want) return data.metrics[name].values;
  }
  return null;
}

function count(data, tags) {
  const v = values(data, 'http_reqs', tags);
  return v ? v.count : 0;
}

export function handleSummary(data) {
  const ok = values(data, 'http_req_duration', ['phase:measure', 'status:201']) || {};
  const by = {};
  for (const s of STATUSES) by[s] = count(data, ['phase:measure', `status:${s}`]);
  const result = {
    rps_201: by['201'] / MEASURE_S,
    p50_201: ok.med, p95_201: ok['p(95)'], p99_201: ok['p(99)'],
    status_201: by['201'],
    status_4xx: by['400'] + by['404'] + by['422'],
    status_5xx: by['500'] + by['502'] + by['503'] + by['504'],
    timeouts: by['0'],
    dropped_iterations: data.metrics.dropped_iterations ? data.metrics.dropped_iterations.values.count : 0,
    vus_max: data.metrics.vus_max ? data.metrics.vus_max.values.max : 0,
    created_all: count(data, ['status:201']),
    timeouts_all: count(data, ['status:0']),
  };
  const f = (x) => (x === undefined ? '-' : x.toFixed(1));
  return {
    stdout: `[${RUN}] 201 ${f(result.rps_201)}/s · p50 ${f(result.p50_201)} · p95 ${f(result.p95_201)} · p99 ${f(result.p99_201)} ms · ` +
      `시간초과 ${result.timeouts} · dropped ${result.dropped_iterations} · VU 최대 ${result.vus_max}\n`,
    [`/out/p6-2/${RUN}.k6.json`]: JSON.stringify(result, null, 2),
  };
}
