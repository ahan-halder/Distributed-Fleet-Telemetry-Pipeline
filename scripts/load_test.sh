#!/bin/bash
# Simple script to run multiple agents concurrently to test load

NUM_AGENTS=${1:-10}
GATEWAY_URL=${2:-"localhost:50051"}

echo "Starting $NUM_AGENTS agents connecting to $GATEWAY_URL"

for i in $(seq 1 $NUM_AGENTS); do
  ./agent -gateway="$GATEWAY_URL" -agent-id="agent-load-$i" -fleet-id="fleet-load-test" &
  echo "Started agent $i"
done

echo "All agents started. Press Ctrl+C to stop."
wait
