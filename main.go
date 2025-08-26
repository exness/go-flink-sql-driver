package flink

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type Ticks struct {
	headerCRC0   int
	contentType0 int
	Header       Header `json:"header"`
	Body         Body   `json:"body"`
}

type Header struct {
	HeaderCRC       int16 `json:"header_crc"`
	ContentType     int32 `json:"content_type"`
	ContentEncoding int32 `json:"content_encoding"`
	Length          int64 `json:"length"`
	DataCRC         int32 `json:"data_crc"`
}

func (h *Header) Scan(src any) error {
	return json.Unmarshal(src.([]byte), h)
}

type Body struct {
	Header     BodyHeader `json:"header"`
	SourceType string     `json:"source_type"`
	Feeders    []string   `json:"feeders"`
	Ticks      []Tick     `json:"ticks"`
}

func (b *Body) Scan(src any) error {
	return json.Unmarshal(src.([]byte), b)
}

type BodyHeader struct {
	SessionStartSystemTime int64  `json:"session_start_system_time"`
	SessionStartSteadyTime int64  `json:"session_start_steady_time"`
	MtTime                 int64  `json:"mt_time"`
	SystemTime             int64  `json:"system_time"`
	SteadyTime             int64  `json:"steady_time"`
	StateCounter           int32  `json:"state_counter"`
	ServerName             string `json:"server_name"`
	SeqNum                 int64  `json:"seq_num"`
}

type Price struct {
	Value float64 `json:"value"`
}

type Tick struct {
	MtTimeDelta     int64  `json:"mt_time_delta"`
	SystemTimeDelta int64  `json:"system_time_delta"`
	SteadyTimeDelta int64  `json:"steady_time_delta"`
	StateCounter    int32  `json:"state_counter"`
	Bid             Price  `json:"bid"`
	Ask             Price  `json:"ask"`
	Symbol          string `json:"symbol"`
	FeederIndex     int32  `json:"feeder_index"`
	SeqNum          int64  `json:"seq_num"`
}

func main() {
	conn, err := sql.Open("flink", "https://api-gateway.flink.test.env:443")

	if err != nil {
		panic(err)
	}
	defer conn.Close()

	createTable := `
		create table dds_ticks
with (
     'connector'='kafka-exness',
      'topic-pattern'='b2c.mt5.ticks.events.reals.*',
      'properties.bootstrap.servers'='di-kafka-01.test.env:9092,di-kafka-02.test.env:9092,di-kafka-03.test.env:9092',
      'properties.group.id'='sdp.flink.sql-gateway',
      'format'='protobuf-dds',
      'protobuf-dds.content-types-list'='1',
      'protobuf-dds.ignore-parse-errors'='false',
      'properties.ssl.truststore.password' = 'txEAb6QXRDNh',
      'properties.ssl.keystore.password'   = 'FqxnYzwGRTaGjRNArAiDxEmUKcntLu6O',
      'properties.ssl.truststore.type' = 'PKCS12',
      'properties.security.protocol' = 'SSL',
      'properties.ssl.keystore.type' = 'PKCS12',
      'scan.startup.mode' = 'latest-offset',
      'properties.bootstrap.servers' = 'di-kafka-01.test.env:9092,di-kafka-02.test.env:9092,di-kafka-03.test.env:9092',
      'properties.ssl.keystore.location' = '/data/gateway-flink-cluster-kafka-params/di-kafka-rke-test-env.user.p12',
      'properties.ssl.truststore.location' = '/data/gateway-flink-cluster-kafka-params/di-kafka-rke-test-env.ca.p12'
    ) like protobuf.dds.Ticks;
	`

	_, err = conn.ExecContext(context.Background(), createTable)
	if err != nil {
		panic(err)
	}

	rows, err := conn.QueryContext(context.Background(), "select t.header.header_crc, t.header.content_type, * from dds_ticks t limit 10")
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	// Iterate over rows and print
	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Columns: %v\n", cols)

	numRows := 0
	for rows.Next() {
		var ticks Ticks

		err = rows.Scan(&ticks.headerCRC0, &ticks.contentType0, &ticks.Header, &ticks.Body)
		if err != nil {
			panic(err)
		}

		numRows += 1

		fmt.Printf("Ticks: %+v", ticks)
		fmt.Println()
		fmt.Println("numRows:", numRows)

		if numRows >= 10 {
			break
		}

	}
	if err := rows.Err(); err != nil {
		panic(err)
	}

}
