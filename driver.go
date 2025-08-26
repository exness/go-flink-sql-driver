package flink

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("flink", &sqlDiver{})
}

type sqlDiver struct{}

func (d *sqlDiver) OpenConnector(dsn string) (driver.Connector, error) {
	connector, err := NewConnector(WithGatewayURL(dsn))
	if err != nil {
		return nil, err
	}
	return connector, nil
}

func (d *sqlDiver) Open(dsn string) (driver.Conn, error) {
	conn, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return conn.Connect(context.Background())
}
