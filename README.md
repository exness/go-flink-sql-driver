todo
# go-flink-sql-gateway

A lightweight Go driver for the **Apache Flink SQL Gateway**.

## Highlights
- Idiomatic `database/sql` integration (`sql.OpenDB(connector)`).
- Simple programmatic configuration via `NewConnector(WithGatewayURL(...), WithProperties(...))`.
- Careful type mapping for Flink → Go types, including **nullable** handling (`sql.Null*`).
- Ready for streaming queries (context‑driven cancellation/timeouts).

---

## Quick start

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"
    "time"
)

func main() {
    endpoint := "http://localhost:8083" // Flink SQL Gateway HTTP endpoint

    // Create connector and open *sql.DB
    connector, _ := NewConnector(
        WithGatewayURL(endpoint),
        WithProperties(map[string]string{
            // example: run in streaming mode
            "execution.runtime-mode": "STREAMING",
        }),
    )
    db := sql.OpenDB(connector)
    defer db.Close()

    // A minimal sanity check: SELECT 1 must be int64
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    rows, err := db.QueryContext(ctx, "SELECT 1")
    if err != nil { log.Fatal(err) }
    defer rows.Close()

    var got1 [][]any
    for rows.Next() {
        var v any
        if err := rows.Scan(&v); err != nil { log.Fatal(err) }
        got1 = append(got1, []any{v})
        break
    }
    if len(got1) == 0 { log.Fatal("no rows") }
    if _, ok := got1[0][0].(int64); !ok {
        log.Fatalf("expected int64, got %T", got1[0][0])
    }
    fmt.Println("SELECT 1 OK (int64)")
}
```
> Tip: For binary/complex types (e.g., `BINARY`, `VARBINARY`, `ROW`, `MAP`), values arrive as `[]byte`. Decode as needed for your app.


## Type mapping (summary)

Non‑nullable columns map to concrete Go types; nullable columns map to `sql.Null*` wrappers:

| Flink Type              | Go (NOT NULL) | Go (NULLABLE)   |
|-------------------------|---------------|-----------------|
| TINYINT / SMALLINT / INT| int64         | sql.NullInt64   |
| BIGINT / INTERVAL       | int64         | sql.NullInt64   |
| FLOAT                   | float64       | sql.NullFloat64 |
| DOUBLE                  | float64       | sql.NullFloat64 |
| BOOLEAN                 | bool          | sql.NullBool    |
| CHAR / VARCHAR / STRING | string        | sql.NullString  |
| DATE / TIME / TIMESTAMP | time.Time     | sql.NullTime    |
| BINARY / VARBINARY      | []byte        | []byte          |
| ROW / MAP / ARRAY       | []byte        | []byte          |

---

## Streaming behavior
Use contexts to bound your streaming queries. Cancellation or deadline expiration will gracefully end the stream (the driver closes the operation and the gateway cleans up).

---

## Development & tests
- Requires a reachable Flink SQL Gateway; tests spin up a local cluster with Testcontainers.

---

## License
MIT (see LICENSE)