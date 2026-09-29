#!/usr/bin/env bash
#
# Demonstrates the checkout saga, including the part that matters: what happens
# when the payment is declined after stock has already been reserved.
#
# Run it with the stack up:  make demo
#
# The script drives the real API over HTTP. It does not poke at the database and
# it does not use a test harness, because the claim being made is that the
# rollback works in the deployed system, not that it works in a test.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Read the host port the stack actually published. Defaulting to 8080 here would
# silently hit whatever else is on that port and produce a confusing failure.
if [ -f .env ]; then
  # shellcheck disable=SC1091
  set -a; . ./.env; set +a
fi
PORT="${GATEWAY_HOST_PORT:-8080}"
BASE="http://localhost:${PORT}"

bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
dim()   { printf '\033[2m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
rule()  { printf '\033[2m%s\033[0m\n' "────────────────────────────────────────────────────────────────────"; }

# ---------------------------------------------------------------- preflight

# The gateway requires a token on every route, so an unauthenticated request
# answers 401 — which is exactly what proves it is up and enforcing auth. Only a
# failure to connect at all means "not running". The liveness probe lives on the
# service's internal port and is deliberately not published.
PREFLIGHT="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "${BASE}/v1/orders/preflight" 2>/dev/null || true)"
case "${PREFLIGHT}" in
  000|"")
    red "The gateway is not answering on ${BASE}."
    echo
    echo "Start the stack first:   make up"
    echo "Check its status:        make ps"
    exit 1
    ;;
  401) : ;;  # expected: up, and auth is enforced
  *)
    red "Something answered on ${BASE} with HTTP ${PREFLIGHT}, but it was not the gateway."
    echo
    echo "Another process is probably using port ${PORT}. Set GATEWAY_HOST_PORT in .env"
    echo "to a free port and run 'make up' again."
    exit 1
    ;;
esac

# A real token, minted with the same code path the auth service uses.
# The prebuilt binary is preferred: `go run` compiles on every invocation, which
# is slow enough to be noticeable in a demo, and it requires a Go toolchain on
# PATH for something that is not a Go task.
if [ -x ./bin/devtoken ]; then
  TOKEN="$(./bin/devtoken -customer cust-demo 2>/dev/null)"
else
  TOKEN="$(cd services/gateway && go run ./cmd/devtoken -customer cust-demo 2>/dev/null)"
fi
if [ -z "$TOKEN" ]; then
  red "Could not mint a development token."
  echo
  echo "Build the helper first:  make devtoken"
  echo "And check that JWT_SECRET is set in .env (at least 32 characters)."
  exit 1
fi

post_order() {
  local token_label="$1" payment_token="$2" idem_key="$3"
  curl -sS --max-time 30 \
    -X POST "${BASE}/v1/orders" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${idem_key}" \
    -d @- <<JSON
{
  "lines": [
    {"product_id": "p-1", "quantity": 1},
    {"product_id": "p-2", "quantity": 2}
  ],
  "shipping_address": {
    "country": "DE", "city": "Berlin", "street": "Torstrasse 1",
    "postal_code": "10119", "recipient": "A. Customer"
  },
  "payment_method_token": "${payment_token}"
}
JSON
}

order_id_of() { printf '%s' "$1" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p'; }
status_of()   { printf '%s' "$1" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p'; }

show_saga() {
  local order_id="$1"
  curl -sS --max-time 10 "${BASE}/v1/orders/${order_id}/saga" \
    -H "Authorization: Bearer ${TOKEN}" \
  | sed -e 's/,"/,\n  "/g' -e 's/^  "steps"/\n  "steps"/' \
  | head -40
}

# ------------------------------------------------------------------- demo

rule
bold "CHECKOUT SAGA DEMO"
dim  "gateway: ${BASE}"
rule
echo
dim "The saga has five steps, and only the first two can be undone:"
echo "  1. reserve_stock       compensatable  (release)"
echo "  2. authorize_payment   compensatable  (void)"
echo "  3. capture_payment     THE PIVOT      (no compensation)"
echo "  4. commit_stock        irreversible, idempotent"
echo "  5. confirm_order       local, idempotent"
echo

# --- Scenario 1: the happy path -------------------------------------------
rule
bold "1. APPROVED PAYMENT — the happy path"
rule
echo

RESULT="$(post_order "approve" "pm_test_approve" "demo-approve-$(date +%s)")"
ORDER_ID="$(order_id_of "$RESULT")"

echo "order status : $(status_of "$RESULT")"
echo "order id     : ${ORDER_ID}"
echo

# --- Scenario 2: the declined payment -------------------------------------
rule
bold "2. DECLINED PAYMENT — stock is reserved, then the card is refused"
rule
echo

dim "This is the scenario the whole project exists for. Stock gets reserved"
dim "in step 1 and committed to the inventory database. Step 2 is then refused."
dim "There is no transaction spanning those two databases, so the only way to"
dim "undo step 1 is to run a new operation that reverses it."
echo

RESULT="$(post_order "decline" "pm_test_decline" "demo-decline-$(date +%s)")"
ORDER_ID="$(order_id_of "$RESULT")"

echo "order status : $(status_of "$RESULT")"
echo "order id     : ${ORDER_ID}"
echo
bold "saga state:"
show_saga "$ORDER_ID"
echo
green "reserve_stock was COMPENSATED — the hold was released."
green "authorize_payment FAILED — no money moved."
green "capture_payment, commit_stock and confirm_order never ran."
echo
dim "Look at the same order in Jaeger: http://localhost:16686"
echo

# --- Scenario 3: idempotency ----------------------------------------------
rule
bold "3. RETRY WITH THE SAME IDEMPOTENCY KEY — no second order"
rule
echo

KEY="demo-idem-$(date +%s)"
FIRST="$(post_order "first" "pm_test_approve" "$KEY")"
SECOND="$(post_order "second" "pm_test_approve" "$KEY")"

echo "first  call → order $(order_id_of "$FIRST")  status $(status_of "$FIRST")"
echo "second call → order $(order_id_of "$SECOND")  status $(status_of "$SECOND")"
echo

if [ "$(order_id_of "$FIRST")" = "$(order_id_of "$SECOND")" ]; then
  green "Same order id: the retry replayed the original instead of creating a second one."
else
  red "Different order ids: the idempotency key did NOT prevent a duplicate."
fi
echo

rule
dim "Jaeger    http://localhost:16686"
dim "Grafana   http://localhost:3000"
dim "The saga state for any order:  curl -H \"Authorization: Bearer \$TOKEN\" ${BASE}/v1/orders/<id>/saga"
rule
