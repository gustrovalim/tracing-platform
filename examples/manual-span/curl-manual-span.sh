#!/usr/bin/env bash
# Sends one hand-made span to Jaeger over OTLP HTTP.
# Fills the placeholders in manual-span.json with fresh values on every run.
set -euo pipefail

cd "$(dirname "$0")"

# macOS date has no %N, so take seconds and append nine zeros to get nanoseconds.
now_s=$(date +%s)
START_NS="${now_s}000000000"
END_NS="$((now_s + 1))000000000"   # span lasts 1 second

TRACE_ID=$(openssl rand -hex 16)   # 16 bytes = 32 hex characters
SPAN_ID=$(openssl rand -hex 8)     # 8 bytes  = 16 hex characters

echo "traceId=${TRACE_ID} spanId=${SPAN_ID}"

sed -e "s/@@TRACE_ID@@/${TRACE_ID}/" \
    -e "s/@@SPAN_ID@@/${SPAN_ID}/" \
    -e "s/@@START_NS@@/${START_NS}/" \
    -e "s/@@END_NS@@/${END_NS}/" \
    manual-span.json |
  curl -sS -X POST http://localhost:4318/v1/traces \
    -H "Content-Type: application/json" \
    -d @- \
    -w "\nHTTP %{http_code}\n"
¬