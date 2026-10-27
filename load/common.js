// P4-2 부하 측정 공통: 시나리오(워밍업 → 측정), 출금 요청, 측정 구간만 모은 요약.
// 환경변수는 load/run.sh 가 넘긴다: RUN, STRATEGY, SCENARIO, WARMUP(10s), MEASURE(60s — 초 단위만), VUS(50), BASE_URL
import http from 'k6/http';
import exec from 'k6/execution';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8080';
const RUN = __ENV.RUN || 'local';
const WARMUP = __ENV.WARMUP || '10s';
const MEASURE = __ENV.MEASURE || '60s';
const VUS = Number(__ENV.VUS || 50);
const STATUSES = ['202', '409', '422', '503'];
// 모의 은행에 없는 계좌 — load/seed.sql 이 부하용 사용자마다 VERIFIED 로 넣어 둔다. 금액 1원 · 잔고 10억이라 측정 중 422 가 나지 않는다
const BODY = JSON.stringify({ bankCode: '090', accountNo: '5599000000001', amount: 1, memo: 'load' });

function thresholds() {
  // 판정용이 아니다: 측정 시나리오(scenario:measure)의 하위 지표를 요약에 남기게 하는 장치라 항상 통과하는 조건을 건다
  const t = {
    'http_req_duration{scenario:measure}': ['max>=0'],
    'http_reqs{scenario:measure}': ['count>=0'],
    // 워밍업 포함 202 수 = DB 의 출금 행 수 (run.sh 가 맞춰 본다)
    'http_reqs{status:202}': ['count>=0'],
  };
  for (const s of STATUSES) t[`http_reqs{scenario:measure,status:${s}}`] = ['count>=0'];
  return t;
}

export const options = {
  discardResponseBodies: true, // 상태 코드만 본다 — 본문을 읽는 비용을 k6 쪽에서 뺀다
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
  scenarios: {
    // JIT · 커넥션 풀 데우기. 집계하지 않는다. 진행 중인 요청은 끝까지 기다린다(5초) — 202 수와 DB 출금 행 수를 맞춰 보기 때문
    warmup: { executor: 'constant-vus', vus: VUS, duration: WARMUP, gracefulStop: '5s' },
    measure: { executor: 'constant-vus', vus: VUS, duration: MEASURE, startTime: WARMUP },
  },
  thresholds: thresholds(),
};

/** 닫힌 모델: VU 하나가 요청 → 응답 대기 → 바로 다음 요청. 멱등키는 요청마다 새로 (64자 이내) — 같은 키가 겹치면 멱등 응답(200)이 섞인다. */
export function transfer(userId) {
  const key = `${RUN}-${exec.scenario.name.charAt(0)}-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`;
  return http.post(`${BASE_URL}/api/v1/transfers`, BODY, {
    headers: { 'Content-Type': 'application/json', 'X-User-Id': userId, 'Idempotency-Key': key },
  });
}

function values(data, metric, tags) {
  // 하위 지표 이름의 태그 순서에 기대지 않는다: {scenario:measure,status:202} 와 {status:202,scenario:measure} 를 같게 본다
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

function fmt(ms) {
  return ms === undefined ? '-' : ms.toFixed(1);
}

export function handleSummary(data) {
  const seconds = parseInt(MEASURE, 10);
  const total = count(data, ['scenario:measure']);
  const d = values(data, 'http_req_duration', ['scenario:measure']) || {};
  const by = {};
  for (const s of STATUSES) by[s] = count(data, ['scenario:measure', `status:${s}`]);
  const result = {
    run: RUN,
    strategy: __ENV.STRATEGY || '',
    scenario: __ENV.SCENARIO || '',
    vus: VUS,
    warmup: WARMUP,
    measure: MEASURE,
    rps: total / seconds,
    p50: d.med,
    p95: d['p(95)'],
    p99: d['p(99)'],
    avg: d.avg,
    max: d.max,
    total,
    status_202: by['202'],
    status_409: by['409'],
    status_422: by['422'],
    status_503: by['503'],
    other: total - STATUSES.reduce((sum, s) => sum + by[s], 0),
    accepted_all: count(data, ['status:202']),
  };
  const line = `[${RUN}] ${result.rps.toFixed(1)} req/s · p50 ${fmt(result.p50)} · p95 ${fmt(result.p95)} · p99 ${fmt(result.p99)} ms · ` +
    `202=${result.status_202} 409=${result.status_409} 422=${result.status_422} 503=${result.status_503} 그 외=${result.other}\n`;
  return {
    stdout: line,
    [`/out/${RUN}.json`]: JSON.stringify(result, null, 2),
    [`/out/${RUN}.full.json`]: JSON.stringify(data, null, 2),
  };
}
