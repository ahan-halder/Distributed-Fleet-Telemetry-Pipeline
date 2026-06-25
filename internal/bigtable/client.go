package bigtable

import (
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"strings"

	"cloud.google.com/go/bigtable"
)

// Client wraps the Cloud Bigtable client.
type Client struct {
	client *bigtable.Client
}

// NewClient creates a new Bigtable client.
func NewClient(ctx context.Context, projectID, instanceID string) (*Client, error) {
	client, err := bigtable.NewClient(ctx, projectID, instanceID)
	if err != nil {
		return nil, err
	}
	return &Client{client: client}, nil
}

// Close closes the underlying client connection.
func (c *Client) Close() error {
	return c.client.Close()
}

func bytesToFloat64(b []byte) float64 {
	if len(b) < 8 {
		return 0.0
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}

func bytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// ReadRecentAgentSummary reads the N most recent rows for a given agent prefix
// and returns the average CPU utilization % and the most recent timestamp in ms.
func (c *Client) ReadRecentAgentSummary(ctx context.Context, agentID string, limit int) (doubleAvg float64, lastSeenMs uint64, err error) {
	if c == nil || c.client == nil {
		return 0, 0, nil
	}

	tbl := c.client.Open(TableName)
	prefix := agentID + "#"
	rangeFilter := bigtable.PrefixRange(prefix)

	var totalCPU float64
	var count int
	var maxTime uint64

	err = tbl.ReadRows(ctx, rangeFilter, func(row bigtable.Row) bool {
		// Extract CPU from sys family
		if sysCols, ok := row[ColumnFamilySys]; ok {
			for _, col := range sysCols {
				if col.Column == ColumnFamilySys+":"+ColSysCPU {
					totalCPU += bytesToFloat64(col.Value)
					count++
				}
			}
		}

		// Extract timestamp from row key: agentID#inverted_ts
		parts := strings.Split(row.Key(), "#")
		if len(parts) == 2 {
			if inv, e := strconv.ParseInt(parts[1], 10, 64); e == nil {
				ts := uint64(math.MaxInt64 - inv)
				if ts > maxTime {
					maxTime = ts
				}
			}
		}

		return true
	}, bigtable.LimitRows(int64(limit)))

	if err != nil {
		return 0, 0, err
	}

	if count > 0 {
		doubleAvg = totalCPU / float64(count)
	}
	return doubleAvg, maxTime, nil
}

