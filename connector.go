package flink

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/http"
)

type ConnConfig struct {
	GatewayURL string
	Client     *http.Client
	APIVersion string
	Properties map[string]string
}

func WithDefaults() *ConnConfig {
	return &ConnConfig{
		GatewayURL: "http://localhost:8081",
		Client:     http.DefaultClient,
		APIVersion: "v3",
		Properties: make(map[string]string),
	}
}

type ConnOption func(c *ConnConfig)

func WithGatewayURL(gatewayUrl string) ConnOption {
	return func(c *ConnConfig) {
		c.GatewayURL = gatewayUrl
	}
}

func WithClient(client *http.Client) ConnOption {
	return func(c *ConnConfig) {
		c.Client = client
	}
}

func WithAPIVersion(apiVersion string) ConnOption {
	return func(c *ConnConfig) {
		c.APIVersion = apiVersion
	}
}

func WithProperties(properties map[string]string) ConnOption {
	return func(c *ConnConfig) {
		if c.Properties == nil {
			c.Properties = make(map[string]string)
		}
		for k, v := range properties {
			c.Properties[k] = v
		}
	}
}

type connector struct {
	client *Client
	cfg    *ConnConfig
}

// NewConnector creates a connection that can be used with `sql.OpenDB()`.
// This is an easier way to set up the DB instead of having to construct a DSN string.
func NewConnector(options ...ConnOption) (driver.Connector, error) {
	cfg := WithDefaults()
	for _, opt := range options {
		opt(cfg)
	}
	flinkClient, err := NewClient(cfg.GatewayURL, cfg.Client, cfg.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("error while creating flink client: %w", err)
	}
	return &connector{client: flinkClient, cfg: cfg}, nil
}

// Connect returns a new Conn bound to this Connector's client.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	mergedProps, err := mergeProperties(c.cfg.GatewayURL, c.cfg.Properties)
	if err != nil {
		return nil, fmt.Errorf("flink: failed to merge properties: %w", err)
	}
	handle, err := c.client.OpenSession(ctx, &OpenSessionRequest{
		Properties: mergedProps,
	})
	if err != nil {
		return nil, fmt.Errorf("flink: failed to open session: %w", err)
	}
	return &flinkConn{client: c.client, sessionHandle: handle}, nil
}

func (c *connector) Driver() driver.Driver {
	return &sqlDiver{}
}
