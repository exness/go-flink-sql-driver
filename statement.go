package flink

import (
	"context"
	"database/sql/driver"
)

type flinkStmt struct {
	conn  *flinkConn
	query string
	ctx   context.Context
}

func (s *flinkStmt) Close() error {
	return nil
}

func (s *flinkStmt) NumInput() int {
	// todo
	return -1
}

func (s *flinkStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *flinkStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

func (s *flinkStmt) Exec(args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(s.ctx, named)
}

func (s *flinkStmt) Query(args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.QueryContext(s.ctx, named)
}
