// P8-2 E3 · E4: nginx(:80) 를 거쳐 출금을 일정한 속도로 (열린 모델 — api 한 대가 내려가도 요청은 계속 온다).
// 사용자 load-p82-u001 ~ u020 (load/compose/seed.sql). 실패(5xx · 연결 실패)는 같은 멱등키로 1초 뒤 최대 3번 다시 보낸다 —
// 클라이언트가 결과를 모를 때 할 일이고, 서버는 같은 키를 한 번만 접수한다 (ADR-0004). 그래서 "최종 접수된 키 수 = 출금 행 수" 여야 한다.
// 환경변수는 load/compose/run.sh 가 넘긴다: BASE_URL(http://nginx:80) RUN RATE(초당, 20) DURATION(60s)
// P8-3a 도 같은 파일을 쓴다 (load/k8s/run.sh — BASE_URL 은 kind 제어 노드의 NodePort).
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

const BASE_URL = __ENV.BASE_URL || 'http://nginx:80';
const RUN = __ENV.RUN || 'local';
const RATE = Number(__ENV.RATE || 20);
const DURATION = __ENV.DURATION || '60s';
const OUT_SUBDIR = __ENV.OUT_SUBDIR || 'p8-2';   // 결과 폴더 — P8-3a(load/k8s/run.sh)는 p8-3a
const BODY = JSON.stringify({ bankCode: '090', accountNo: '1002123456789', amount: 1, memo: 'p82' });

const first202 = new Counter('first_202');        // 첫 시도에 접수
const first5xx = new Counter('first_5xx');        // 첫 시도 5xx (nginx 502 포함)
const first502 = new Counter('first_502');        // 첫 시도 502 — 보낸 뒤 api 가 끊겼다 (nginx 는 다시 보내지 않는다)
const firstNet = new Counter('first_neterr');     // 첫 시도 연결 실패 · 시간 초과 (상태 0)
const retried = new Counter('retried_requests');  // 같은 키로 다시 보낸 요청 수
const accepted = new Counter('accepted_keys');    // 최종 202 · 200 을 받은 키 수 = DB 출금 행 수여야 한다
const gaveUp = new Counter('gave_up_keys');       // 다시 보내도 실패한 키 (DB 에 있을 수도 있다 — run.sh 가 출금 행과 비교한다)

export const options = {
  discardResponseBodies: true,
  scenarios: {
    steady: { executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: DURATION, preAllocatedVUs: 40, maxVUs: 200 },
  },
};

export default function () {
  const it = exec.scenario.iterationInTest;
  const user = `load-p82-u${String((it % 20) + 1).padStart(3, '0')}`;
  const key = `p82-${RUN}-${it}`;   // 64자 이내 ASCII
  const params = { headers: { 'Content-Type': 'application/json', 'X-User-Id': user, 'Idempotency-Key': key }, timeout: '15s' };

  let res = http.post(`${BASE_URL}/api/v1/transfers`, BODY, params);
  if (res.status === 202) first202.add(1);
  if (res.status >= 500) first5xx.add(1);
  if (res.status === 502) first502.add(1);
  if (res.status === 0) firstNet.add(1);

  for (let i = 0; i < 3 && res.status !== 202 && res.status !== 200; i++) {
    if (res.status !== 0 && res.status < 500) break;   // 4xx 는 다시 보내도 같다
    retried.add(1);
    sleep(1);
    res = http.post(`${BASE_URL}/api/v1/transfers`, BODY, params);
  }
  if (res.status === 202 || res.status === 200) accepted.add(1);
  else gaveUp.add(1);
}

export function handleSummary(data) {
  const count = (m) => (data.metrics[m] ? data.metrics[m].values.count : 0);
  const out = {
    run: RUN, rate: RATE, duration: DURATION,
    iterations: count('iterations'), first_202: count('first_202'), first_5xx: count('first_5xx'), first_502: count('first_502'),
    first_neterr: count('first_neterr'), retried_requests: count('retried_requests'), accepted_keys: count('accepted_keys'), gave_up_keys: count('gave_up_keys'),
    p95_ms: data.metrics.http_req_duration.values['p(95)'], max_ms: data.metrics.http_req_duration.values.max,
  };
  return { stdout: `${JSON.stringify(out)}\n`, [`/out/${OUT_SUBDIR}/${RUN}.k6.json`]: JSON.stringify(out, null, 2) };
}
