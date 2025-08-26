package flink

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"
)

type flinkConn struct {
	client        GatewayClient
	sessionHandle string
}

func (c *flinkConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *flinkConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &flinkStmt{conn: c, query: query, ctx: ctx}, nil
}

// Close closes the gateway session associated with this connection.
func (c *flinkConn) Close() error {
	if c.sessionHandle != "" {
		_ = c.client.CloseSession(context.Background(), c.sessionHandle)
		c.sessionHandle = ""
	}
	return nil
}

func (c *flinkConn) Begin() (driver.Tx, error) {
	tx, err := c.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// BeginTx is not supported by Flink SQL Gateway.
func (c *flinkConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, driver.ErrSkip
}

func (c *flinkConn) Ping(ctx context.Context) error {
	err := c.client.Heartbeat(ctx, c.sessionHandle)
	if err != nil {
		return driver.ErrBadConn
	}

	return err
}

func (c *flinkConn) closeOperation(ctx context.Context, operationHandle string) {
	if operationHandle != "" {
		c.client.CloseOperation(ctx, c.sessionHandle, operationHandle)
	}
}

func (c *flinkConn) cancelOperation(ctx context.Context, operationHandle string) {
	if operationHandle != "" {
		c.client.CancelOperation(ctx, c.sessionHandle, operationHandle)
	}
}

// ExecContext submits a statement and ignores result rows.
func (c *flinkConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// todo: handle args
	opHandle, err := c.client.ExecuteStatement(ctx, c.sessionHandle, &ExecuteStatementRequest{
		Statement: query,
	})
	defer c.closeOperation(ctx, opHandle)
	if err != nil {
		return nil, fmt.Errorf("flinksql: ExecContext failed to submit statement: %w", err)
	}
	_, err = c.fetchUntilResultsReady(ctx, opHandle, 1000*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("flinksql: ExecContext failed: %w", err)
	}
	return driver.RowsAffected(0), nil
}

func (c *flinkConn) fetchUntilResultsReady(ctx context.Context, opHandle string, pollInterval time.Duration) (*FetchResultsResponseBody, error) {
	for {
		res, err := c.client.FetchResults(ctx, c.sessionHandle, opHandle, "0", "json")
		if err != nil {
			return nil, fmt.Errorf("flinksql: FetchResults failed: %w", err)
		}

		// Retry fetching until the query is ready, honoring context cancellation
		if res.ResultType == ResultTypeNotReady {
			select {
			case <-ctx.Done():
				c.closeOperation(ctx, opHandle)
				return nil, ctx.Err()
			case <-time.After(pollInterval):
				// keep polling at a fixed interval
			}
			continue
		}

		return res, nil
	}
}

// QueryContext submits a statement and fetches its result rows.
func (c *flinkConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// todo: handle args
	opHandle, err := c.client.ExecuteStatement(ctx, c.sessionHandle, &ExecuteStatementRequest{
		Statement: query,
	})
	if err != nil {
		return nil, fmt.Errorf("flinksql: QueryContext failed to submit statement: %w", err)
	}
	res, err := c.fetchUntilResultsReady(ctx, opHandle, 1000*time.Millisecond)
	if err != nil {
		c.cancelOperation(ctx, opHandle)
		return nil, fmt.Errorf("flinksql: FetchResults failed: %w", err)
	}
	if !res.IsQueryResult && res.ResultKind != ResultKindSuccessWithContent {
		c.closeOperation(ctx, opHandle)
		return nil, fmt.Errorf("flinksql: Statement [%s] is not a query", query)
	}
	return newRows(ctx, c, opHandle, res.Results.Data, res.Results.Columns, res.NextToken())
}
