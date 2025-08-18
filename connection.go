package flink

import (
	"context"
	"database/sql/driver"
	"fmt"
	"iter"
	"time"
)

type flinkConn struct {
	client        *Client
	sessionHandle string
}

func (c *flinkConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *flinkConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &flinkStmt{conn: c, query: query}, nil
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

// ExecContext submits a statement and ignores result rows.
func (c *flinkConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	_, err := c.client.ExecuteStatement(ctx, c.sessionHandle, &ExecuteStatementRequest{
		Statement: query,
	})
	if err != nil {
		return nil, fmt.Errorf("flinksql: ExecContext failed to submit statement: %w", err)
	}
	return driver.RowsAffected(0), nil
}

// QueryContext submits a statement and fetches its result rows.
func (c *flinkConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	opHandle, err := c.client.ExecuteStatement(ctx, c.sessionHandle, &ExecuteStatementRequest{
		Statement: query,
	})
	if err != nil {
		return nil, fmt.Errorf("flinksql: QueryContext failed to submit statement: %w", err)
	}
	for {
		res, err := c.client.FetchResults(ctx, c.sessionHandle, opHandle, "0", "json")
		if err != nil {
			return nil, fmt.Errorf("flinksql: FetchResults failed: %w", err)
		}

		// Retry fetching until the query is ready
		if res.ResultType == ResultTypeNotReady {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		if !res.IsQueryResult && res.ResultKind != ResultKindSuccessWithContent {
			return nil, fmt.Errorf("flinksql: Statement [%s] is not a query", query)
		}
		return &Rows{
			columns:  &res.Results.Columns,
			iterator: rowDataInfoIterator(c.sessionHandle, opHandle, c.client, &res.Results.Data, res.NextToken()),
		}, nil

	}
}

func rowDataInfoIterator(sessionHandle, operationHandle string, client *Client, initialResults *[]RowData, nextToken string) iter.Seq[RowData] {
	results := initialResults
	token := nextToken
	pos := 0

	return func(yield func(RowData) bool) {
		for {
			if pos < len(*results) {
				row := (*results)[pos]
				pos++
				if !yield(row) {
					return
				}
				continue
			}
			response, err := client.FetchResults(context.Background(), sessionHandle, operationHandle, token, "")
			if err != nil {
				fmt.Printf("flinksql: error fetching results: %v\n", err)
				return
			}
			if response.ResultType == ResultTypeEOS {
				return
			}
			results = &response.Results.Data
			token = response.NextToken()
			pos = 0
			time.Sleep(1000 * time.Millisecond)
		}
	}
}
