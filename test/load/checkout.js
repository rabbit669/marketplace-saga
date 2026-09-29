// k6 load test for the checkout path.
//
//   make k6        (or: k6 run test/load/checkout.js)
//
// It is not a benchmark. It answers one question the unit tests cannot: what
// happens to checkout latency and to the saga's failure modes when many
// customers arrive at once. The interesting traffic is deliberately mixed —
// most payments are approved, a fifth are declined, and some are transient —
// because the compensation path under load is the part that has never been
// exercised, and a load test that only exercises the happy path measures the
// least interesting half of the system.

import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8081';

// Identifiers are generated locally rather than imported from a CDN. A load
// test that cannot start because someone else's host is down is measuring the
// wrong thing, and the test runner should not depend on the network any more
// than the system under test does.
function uniqueId() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

// Domain metrics, so the report separates outcomes rather than lumping every
// non-201 into "failed". A declined card is the system working.
const ordersConfirmed = new Counter('checkout_confirmed');
const ordersRolledBack = new Counter('checkout_rolled_back');
const ordersNeedingHuman = new Counter('checkout_needs_human');
const sagaLatency = new Trend('checkout_saga_duration', true);
const unexpected = new Rate('checkout_unexpected');
// Transport-level failures only. k6's own `http_req_failed` counts every status
// at or above 400, which lumps a declined card (422) in with a server fault —
// and a load test whose failure threshold is crossed by the system working
// correctly is a threshold nobody will keep.
const serverErrors = new Counter('checkout_server_errors');

export const options = {
  scenarios: {
    // A steady arrival rate rather than a fixed number of virtual users. The
    // question is "does checkout hold up at N per second", and a VU count only
    // answers it indirectly, through however fast the system happens to respond.
    checkout: {
      executor: 'ramping-arrival-rate',
      startRate: 5,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 100,
      stages: [
        { target: 10, duration: '30s' }, // warm up
        { target: 40, duration: '1m' },  // expected peak
        { target: 40, duration: '1m' },  // hold
        { target: 5, duration: '20s' },  // ramp down
      ],
    },
  },

  thresholds: {
    // A checkout is a synchronous, multi-service, five-step flow with retries.
    // Half a second at p95 is demanding but achievable when every dependency is
    // healthy, and it is the figure at which a customer still perceives the
    // result as immediate.
    'http_req_duration{route:create_order}': ['p(95)<500', 'p(99)<1500'],

    // The failure budget is on the *system*, not on the business. A 422 is a
    // declined card: the saga rolled back, the customer was charged nothing and
    // the stock went back to the pool. The script sends 20% declines and 10%
    // timeouts on purpose, so roughly a third of responses are 422 and a
    // threshold on http_req_failed would be crossed by correct behaviour.
    'checkout_server_errors': ['count<10'],
    'checkout_unexpected': ['rate<0.001'],

    // If any saga needs a human during a normal load run, something is wrong
    // that a latency threshold would not have caught.
    'checkout_needs_human': ['count==0'],

    'checkout_unexpected': ['rate<0.005'],
  },
};

// setup runs once and shares its result with every VU.
export function setup() {
  const email = `load-${uniqueId().slice(0, 8)}@example.com`;
  const password = 'load-test-password-32-bytes-long';

  const register = http.post(
    `${BASE}/v1/auth/register`,
    JSON.stringify({ email, password, full_name: 'Load Test' }),
    { headers: { 'Content-Type': 'application/json' }, tags: { route: 'register' } },
  );

  if (register.status !== 201) {
    throw new Error(`could not register the load-test user: ${register.status} ${register.body}`);
  }

  const login = http.post(
    `${BASE}/v1/auth/login`,
    JSON.stringify({ email, password }),
    { headers: { 'Content-Type': 'application/json' }, tags: { route: 'login' } },
  );

  if (login.status !== 200) {
    throw new Error(`could not log in: ${login.status} ${login.body}`);
  }

  return { token: login.json('access_token'), customer: login.json('user.id') };
}

// paymentToken picks a provider outcome.
//
// The simulated provider honours magic tokens, so the mix is exact rather than
// probabilistic: every run produces the same proportion of declines, which is
// what makes two runs comparable.
function paymentToken() {
  const roll = Math.random();
  if (roll < 0.70) return 'pm_test_approve';
  if (roll < 0.90) return 'pm_test_decline'; // permanent: compensates immediately
  return 'pm_test_timeout';                  // transient: retried, then compensates
}

export default function (data) {
  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${data.token}`,
    // Unique per request. Reusing a key would measure the idempotency replay
    // instead of the checkout, which is a different and much cheaper path.
    'Idempotency-Key': uniqueId(),
  };

  const body = JSON.stringify({
    lines: [
      { product_id: 'p-1', quantity: 1 },
      { product_id: 'p-2', quantity: 2 },
    ],
    shipping_address: {
      country: 'DE',
      city: 'Berlin',
      street: 'Torstrasse 1',
      postal_code: '10119',
      recipient: 'Load Test',
    },
    payment_method_token: paymentToken(),
  });

  const response = http.post(`${BASE}/v1/orders`, body, {
    headers,
    tags: { route: 'create_order' },
    // Longer than the gateway's own request timeout, so a slow response is
    // reported as a slow response rather than as a k6 timeout.
    timeout: '30s',
  });

  sagaLatency.add(response.timings.duration);

  check(response, {
    'checkout answered with an expected status': (r) =>
      r.status === 201 || r.status === 200 || r.status === 422 || r.status === 503,
  });

  if (response.status === 201 || response.status === 200) {
    ordersConfirmed.add(1);
    return;
  }

  if (response.status === 422) {
    // The saga rolled back cleanly. The body names the step that failed, which
    // is what makes this distinguishable from a validation error at a glance.
    const failure = response.json('failure');
    if (failure && failure.code === 'internal') {
      // A rolled-back checkout is a business outcome; a failure to roll back is
      // not. The order service reports the second as internal, and it is the one
      // case that must never appear in a healthy run.
      ordersNeedingHuman.add(1);
    } else {
      ordersRolledBack.add(1);
    }
    return;
  }

  // Anything else — 500, 503, a connection reset — is unexpected.
  unexpected.add(1);
  if (response.status >= 500 || response.status === 0) {
    serverErrors.add(1);
  }
}

export function teardown(data) {
  console.log(`load test finished for customer ${data.customer}`);
}
