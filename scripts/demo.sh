#!/usr/bin/env bash

set -euo pipefail

API_BASE_URL="${RUNQ_API_BASE_URL:-http://localhost:8080}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_command curl
require_command python3

echo "Submitting demo jobs to ${API_BASE_URL}"

success_response="$(curl -sS -X POST "${API_BASE_URL}/v1/jobs" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "demo-success",
    "queue": "default",
    "kind": "http",
    "payload": {
      "url": "https://example.internal/demo-success",
      "method": "POST"
    }
  }')"

failure_response="$(curl -sS -X POST "${API_BASE_URL}/v1/jobs" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "demo-failure",
    "queue": "default",
    "kind": "http",
    "payload": {
      "url": "https://example.internal/demo-fail",
      "method": "POST",
      "simulate_failure": true
    }
  }')"

echo "Success job:"
echo "${success_response}"
echo
echo "Failure job:"
echo "${failure_response}"
echo

success_run_id="$(printf '%s' "${success_response}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["run_id"])')"
failure_run_id="$(printf '%s' "${failure_response}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["run_id"])')"

echo "Waiting for worker and scheduler activity..."
sleep 8

echo
echo "Run detail for success path (${success_run_id}):"
curl -sS "${API_BASE_URL}/v1/runs/${success_run_id}"
echo
echo
echo "Run detail for retry/failure path (${failure_run_id}):"
curl -sS "${API_BASE_URL}/v1/runs/${failure_run_id}"
echo
echo
echo "Recent runs:"
curl -sS "${API_BASE_URL}/v1/runs"
echo
