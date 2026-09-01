-- wrk Lua script for REST baseline comparison against grpc-gateway.
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"
wrk.headers["Authorization"] = "Bearer local-dev-token"
wrk.body = [[{
  "fleet_id": "fleet-load-test",
  "last_n_rows": 10
}]]
