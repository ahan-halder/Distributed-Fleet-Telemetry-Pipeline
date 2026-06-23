package bigtable

import (
	"fmt"
	"math"
)

const (
	TableName = "telemetry_frames"

	// Column Families
	ColumnFamilySys  = "sys"
	ColumnFamilyNet  = "net"
	ColumnFamilyMeta = "meta"

	// Sys Columns
	ColSysCPU      = "cpu"
	ColSysMemUsed  = "mem_used"
	ColSysMemTotal = "mem_total"
	ColSysDiskRead = "disk_r"
	ColSysDiskWrite = "disk_w"

	// Net Columns
	ColNetRX       = "rx"
	ColNetTX       = "tx"
	ColNetTCPRetx  = "tcp_retx"
	ColNetConn     = "conn"

	// Meta Columns
	ColMetaFleetID        = "fleet_id"
	ColMetaLabels         = "labels"
	ColMetaIdempotencyKey = "idempotency_key"
)

// GenerateRowKey creates an inverted timestamp row key for fast reverse-chronological scans.
// Format: {agent_id}#{inverted_unix_ts_ms}
func GenerateRowKey(agentID string, timestampMillis int64) string {
	inverted := math.MaxInt64 - timestampMillis
	return fmt.Sprintf("%s#%d", agentID, inverted)
}
