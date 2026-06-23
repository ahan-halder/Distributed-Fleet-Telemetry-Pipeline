package bigtable

import (
	"context"

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
