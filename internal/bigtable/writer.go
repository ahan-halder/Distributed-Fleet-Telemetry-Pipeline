package bigtable

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"time"

	"cloud.google.com/go/bigtable"
	telemetryv1 "github.com/ahan-halder/fleet-telemetry/gen/go/fleet/telemetry/v1"
)

// float64ToBytes converts a float64 to big-endian bytes for Bigtable storage.
func float64ToBytes(f float64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(f))
	return buf[:]
}

// uint64ToBytes converts a uint64 to big-endian bytes.
func uint64ToBytes(u uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], u)
	return buf[:]
}

// WriteMetricFrame writes a MetricFrame to Bigtable.
func (c *Client) WriteMetricFrame(ctx context.Context, frame *telemetryv1.MetricFrame) error {
	ts := frame.CollectedAt.AsTime().UnixMilli()
	rowKey := GenerateRowKey(frame.AgentId, ts)

	mut := bigtable.NewMutation()
	btTime := bigtable.Time(frame.CollectedAt.AsTime())

	// Meta
	mut.Set(ColumnFamilyMeta, ColMetaFleetID, btTime, []byte(frame.FleetId))
	mut.Set(ColumnFamilyMeta, ColMetaIdempotencyKey, btTime, []byte(frame.IdempotencyKey))
	if len(frame.Labels) > 0 {
		b, _ := json.Marshal(frame.Labels)
		mut.Set(ColumnFamilyMeta, ColMetaLabels, btTime, b)
	}

	// System
	if frame.System != nil {
		mut.Set(ColumnFamilySys, ColSysCPU, btTime, float64ToBytes(frame.System.CpuUtilizationPct))
		mut.Set(ColumnFamilySys, ColSysMemUsed, btTime, uint64ToBytes(frame.System.MemoryUsedBytes))
		mut.Set(ColumnFamilySys, ColSysMemTotal, btTime, uint64ToBytes(frame.System.MemoryTotalBytes))
		mut.Set(ColumnFamilySys, ColSysDiskRead, btTime, float64ToBytes(frame.System.DiskIoReadMbps))
		mut.Set(ColumnFamilySys, ColSysDiskWrite, btTime, float64ToBytes(frame.System.DiskIoWriteMbps))
	}

	// Network
	if frame.Network != nil {
		mut.Set(ColumnFamilyNet, ColNetRX, btTime, float64ToBytes(frame.Network.RxMbps))
		mut.Set(ColumnFamilyNet, ColNetTX, btTime, float64ToBytes(frame.Network.TxMbps))
		mut.Set(ColumnFamilyNet, ColNetTCPRetx, btTime, uint64ToBytes(frame.Network.TcpRetransmitCount))
		mut.Set(ColumnFamilyNet, ColNetConn, btTime, uint64ToBytes(frame.Network.ConnectionCount))
	}

	tbl := c.client.Open(TableName)
	
	start := time.Now()
	err := tbl.Apply(ctx, rowKey, mut)
	duration := time.Since(start).Seconds()
	
	// We could log metrics here using observability, but let's assume it's handled.
	_ = duration

	return err
}
