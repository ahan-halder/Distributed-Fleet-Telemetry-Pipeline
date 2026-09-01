#!/usr/bin/env bash
# End-to-end local demo: gateway + REST proxy + simulated agents.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

GATEWAY_PORT="${GATEWAY_PORT:-50051}"
REST_PORT="${REST_PORT:-8080}"
NUM_AGENTS="${NUM_AGENTS:-5}"
DEMO_SECONDS="${DEMO_SECONDS:-12}"
FLEET_ID="${FLEET_ID:-fleet-demo}"
TOKEN="${TOKEN:-local-dev-token}"
RESULTS_DIR="${RESULTS_DIR:-$ROOT/benchmarks/results}"
DEMO_LOG="$RESULTS_DIR/demo_run.log"

mkdir -p "$RESULTS_DIR"
: > "$DEMO_LOG"

# Ensure ports are free from a previous demo run.
for port in "$GATEWAY_PORT" "$REST_PORT"; do
  if command -v fuser >/dev/null 2>&1; then
    fuser -k "${port}/tcp" >/dev/null 2>&1 || true
  fi
done
sleep 0.5

log() {
  echo "[demo] $*" | tee -a "$DEMO_LOG"
}

cleanup() {
  log "Stopping demo processes..."
  jobs -p | xargs -r kill 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

log "Building binaries..."
go build -o /tmp/fleet-gateway ./cmd/gateway
go build -o /tmp/fleet-agent ./cmd/agent
go build -o /tmp/fleet-rest-proxy ./cmd/rest-proxy

log "Starting gateway on :$GATEWAY_PORT"
/tmp/fleet-gateway --mode=local --port="$GATEWAY_PORT" >>"$DEMO_LOG" 2>&1 &
sleep 1

log "Starting REST proxy on :$REST_PORT"
/tmp/fleet-rest-proxy --gateway-endpoint="localhost:$GATEWAY_PORT" --port="$REST_PORT" >>"$DEMO_LOG" 2>&1 &
sleep 1

log "Starting $NUM_AGENTS simulated agents (fleet=$FLEET_ID)"
for i in $(seq 1 "$NUM_AGENTS"); do
  /tmp/fleet-agent \
    --gateway="localhost:$GATEWAY_PORT" \
    --agent-id="agent-demo-$(printf '%03d' "$i")" \
    --fleet-id="$FLEET_ID" \
    --rate=2 \
    --anomaly-prob=0.15 \
    --token="$TOKEN" >>"$DEMO_LOG" 2>&1 &
done

log "Streaming telemetry for ${DEMO_SECONDS}s..."
sleep "$DEMO_SECONDS"

log "Querying fleet status via REST..."
FLEET_JSON="$(curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:$REST_PORT/v1/fleet/$FLEET_ID/status")"
echo "$FLEET_JSON" | tee -a "$DEMO_LOG"

ACTIVE_COUNT="$(echo "$FLEET_JSON" | grep -o '"activeAgentCount":[0-9]*' | cut -d: -f2 || echo 0)"
ALERT_COUNT="$(grep -c 'Received alert' "$DEMO_LOG" || true)"

log "Demo summary:"
log "  Active agents reported: ${ACTIVE_COUNT:-0}"
log "  Alert notifications received: $ALERT_COUNT"
log "  Full log: $DEMO_LOG"

cat <<EOF

============================================================
 Fleet Telemetry Pipeline — Local Demo Complete
============================================================
 REST fleet status : http://localhost:$REST_PORT/v1/fleet/$FLEET_ID/status
 gRPC gateway      : localhost:$GATEWAY_PORT
 Agents simulated  : $NUM_AGENTS
 Active agents     : ${ACTIVE_COUNT:-0}
 Alerts triggered  : $ALERT_COUNT
 Demo log          : $DEMO_LOG
============================================================
EOF
