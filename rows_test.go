package flink

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"io"
	"iter"
	"reflect"
	"strings"
	"testing"
	"time"
)

// mockClient implements the minimal method used by Rows.Close.

// gatewayMock implements GatewayClient for testing;
type gatewayMock struct{ cancelCalled bool }

func (m *gatewayMock) GetInfo(ctx context.Context) (*InfoResponse, error)   { return nil, nil }
func (m *gatewayMock) GetAPIVersions(ctx context.Context) ([]string, error) { return nil, nil }
func (m *gatewayMock) OpenSession(ctx context.Context, reqBody *OpenSessionRequest) (string, error) {
	return "", nil
}
func (m *gatewayMock) CloseSession(ctx context.Context, sessionHandle string) error { return nil }
func (m *gatewayMock) GetSessionConfig(ctx context.Context, sessionHandle string) (map[string]string, error) {
	return nil, nil
}
func (m *gatewayMock) CompleteStatement(ctx context.Context, sessionHandle string, reqBody *CompleteStatementRequest) ([]string, error) {
	return nil, nil
}
func (m *gatewayMock) ConfigureSession(ctx context.Context, sessionHandle string, reqBody *ConfigureSessionRequest) error {
	return nil
}
func (m *gatewayMock) Heartbeat(ctx context.Context, sessionHandle string) error { return nil }
func (m *gatewayMock) RefreshMaterializedTable(ctx context.Context, sessionHandle, identifier string, reqBody *RefreshMaterializedTableRequest) (string, error) {
	return "", nil
}
func (m *gatewayMock) CancelOperation(ctx context.Context, sessionHandle, operationHandle string) (string, error) {
	m.cancelCalled = true
	return "", nil
}
func (m *gatewayMock) CloseOperation(ctx context.Context, sessionHandle, operationHandle string) (string, error) {
	return "", nil
}
func (m *gatewayMock) GetOperationStatus(ctx context.Context, sessionHandle, operationHandle string) (string, error) {
	return "", nil
}
func (m *gatewayMock) ExecuteStatement(ctx context.Context, sessionHandle string, reqBody *ExecuteStatementRequest) (string, error) {
	return "", nil
}
func (m *gatewayMock) FetchResults(ctx context.Context, sessionHandle, operationHandle, token string, rowFormat string) (*FetchResultsResponseBody, error) {
	return nil, nil
}

func assertRawBytes(t *testing.T, dest []driver.Value, idxs ...int) {
	t.Helper()
	for _, idx := range idxs {
		if _, ok := dest[idx].([]byte); !ok {
			t.Fatalf("index %d expected []byte, got %T", idx, dest[idx])
		}
	}
}

// assertTypeAndValue checks that v has the same concrete type as want (via reflection)
// and that the value equals want.
func assertTypeAndValue[T comparable](t *testing.T, v any, want T, idx int, label string) {
	t.Helper()
	if reflect.TypeOf(v) != reflect.TypeOf(want) {
		t.Fatalf("index %d (%s) expected type %T, got %T", idx, label, want, v)
	}
	got, ok := v.(T)
	if !ok || got != want {
		t.Fatalf("index %d (%s) expected value %v, got %v", idx, label, want, v)
	}
}

func jm(s string) json.RawMessage { return []byte(s) }

func pint(v int) *int { return &v }

// makeCols builds the ColumnInfo list with the given nullability flag.
func makeCols(nullable bool) []ColumnInfo {
	return []ColumnInfo{
		{Name: "tiny", LogicalType: LogicalType{Type: "TINYINT", Nullable: nullable}},
		{Name: "small", LogicalType: LogicalType{Type: "SMALLINT", Nullable: nullable}},
		{Name: "int", LogicalType: LogicalType{Type: "INTEGER", Nullable: nullable}},
		{Name: "big", LogicalType: LogicalType{Type: "BIGINT", Nullable: nullable}},
		{Name: "interval_int", LogicalType: LogicalType{Type: "INTERVAL", Nullable: nullable}},
		{Name: "float", LogicalType: LogicalType{Type: "FLOAT", Nullable: nullable}},
		{Name: "double", LogicalType: LogicalType{Type: "DOUBLE", Nullable: nullable}},
		{Name: "bool", LogicalType: LogicalType{Type: "BOOLEAN", Nullable: nullable}},
		{Name: "char", LogicalType: LogicalType{Type: "CHAR", Nullable: nullable}},
		{Name: "varchar", LogicalType: LogicalType{Type: "VARCHAR", Nullable: nullable}},
		{Name: "decimal", LogicalType: LogicalType{Type: "DECIMAL", Nullable: nullable, Precision: pint(10), Scale: pint(2)}},
		{Name: "date", LogicalType: LogicalType{Type: "DATE", Nullable: nullable}},
		{Name: "time0", LogicalType: LogicalType{Type: "TIME", Nullable: nullable, Precision: pint(0)}},
		{Name: "time3", LogicalType: LogicalType{Type: "TIME", Nullable: nullable, Precision: pint(3)}},
		{Name: "ts", LogicalType: LogicalType{Type: "TIMESTAMP", Nullable: nullable, Precision: pint(6)}},
		{Name: "ts_ltz", LogicalType: LogicalType{Type: "TIMESTAMP_LTZ", Nullable: nullable, Precision: pint(6)}},
		{Name: "ts_wtz", LogicalType: LogicalType{Type: "TIMESTAMP_WITH_TIME_ZONE", Nullable: nullable, Precision: pint(6)}},
		{Name: "ym", LogicalType: LogicalType{Type: "INTERVAL_YEAR_MONTH", Nullable: nullable}},
		{Name: "dt", LogicalType: LogicalType{Type: "INTERVAL_DAY_TIME", Nullable: nullable}},
		{Name: "arr", LogicalType: LogicalType{Type: "ARRAY", Nullable: nullable}},
		{Name: "map", LogicalType: LogicalType{Type: "MAP", Nullable: nullable}},
		{Name: "row", LogicalType: LogicalType{Type: "ROW", Nullable: nullable}},
		{Name: "multiset", LogicalType: LogicalType{Type: "MULTISET", Nullable: nullable}},
	}
}

func TestRowsNext_AllTypesNotNull(t *testing.T) {
	ctx := context.Background()
	cols := makeCols(false)
	// One row with JSON literals for each field in the same order as cols
	row := makeNonNullRow()

	var yielded1 bool
	iterator := iter.Seq[RowData](func(yield func(RowData) bool) {
		if yielded1 {
			return
		}
		yielded1 = true
		yield(row)
	})

	r := &Rows{
		conn:            nil,
		operationHandle: "",
		ctx:             ctx,
		iterator:        iterator,
		columns:         cols,
	}

	dest := make([]driver.Value, len(cols))
	if err := r.Next(dest); err != nil {
		t.Fatalf("Next() unexpected error: %v", err)
	}

	// Validate integer-like (all numbers decoded as int64 in non-nullable path)
	assertTypeAndValue(t, dest[0], int64(127), 0, "tiny")
	assertTypeAndValue(t, dest[1], int64(32767), 1, "small")
	assertTypeAndValue(t, dest[2], int64(2147483647), 2, "int")
	assertTypeAndValue(t, dest[3], int64(9223372036854775807), 3, "big")
	assertTypeAndValue(t, dest[4], int64(42), 4, "interval_int")
	assertTypeAndValue(t, dest[17], int64(12), 17, "interval_year_month")
	assertTypeAndValue(t, dest[18], int64(3456), 18, "interval_day_time")

	// Floats decoded as float64 in non-nullable path
	assertTypeAndValue(t, dest[5], float64(3.14), 5, "float")
	assertTypeAndValue(t, dest[6], float64(2.718281828), 6, "double")

	// Bool
	assertTypeAndValue(t, dest[7], true, 7, "bool")

	// Strings
	assertTypeAndValue(t, dest[8], "A", 8, "char")
	assertTypeAndValue(t, dest[9], "hello", 9, "varchar")

	// Decimal as string
	assertTypeAndValue(t, dest[10], "12345.67", 10, "decimal")

	// Date/time/timestamps
	mustParse := func(layout, s string) time.Time {
		tm, err := time.Parse(layout, s)
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		return tm
	}
	assertTypeAndValue(t, dest[11], mustParse("2006-01-02", "2025-08-22"), 11, "date")
	assertTypeAndValue(t, dest[12], mustParse("15:04:05", "13:14:15"), 12, "time0")
	assertTypeAndValue(t, dest[13], mustParse("15:04:05.000", "13:14:15.123"), 13, "time3")
	assertTypeAndValue(t, dest[14], mustParse("2006-01-02T15:04:05.000000", "2025-08-22T13:14:15.123456"), 14, "ts")
	assertTypeAndValue(t, dest[15], mustParse("2006-01-02T15:04:05.000000", "2025-08-22T13:14:15.123456"), 15, "ts_ltz")

	// TIMESTAMP_WITH_TIME_ZONE returns raw bytes (JSON string), ensure bytes match
	if b, ok := dest[16].([]byte); !ok || string(b) != "\"2025-08-22T13:14:15.123456Z\"" {
		t.Fatalf("ts_wtz raw mismatch: %v (%T)", dest[16], dest[16])
	}

	// Complex types are returned as raw bytes
	assertRawBytes(t, dest, 19, 20, 21, 22)

	// Next() should now return io.EOF
	if err := r.Next(dest); err != io.EOF {
		t.Fatalf("expected io.EOF after draining rows, got %v", err)
	}
}

// makeNonNullRow returns a RowData with all non-null values matching makeCols order.
func makeNonNullRow() RowData {
	return RowData{Fields: []json.RawMessage{
		jm("127"),
		jm("32767"),
		jm("2147483647"),
		jm("9223372036854775807"),
		jm("42"),
		jm("3.14"),
		jm("2.718281828"),
		jm("true"),
		jm("\"A\""),
		jm("\"hello\""),
		jm("12345.67"),
		jm("\"2025-08-22\""),
		jm("\"13:14:15\""),
		jm("\"13:14:15.123\""),
		jm("\"2025-08-22T13:14:15.123456\""),
		jm("\"2025-08-22T13:14:15.123456\""),
		jm("\"2025-08-22T13:14:15.123456Z\""),
		jm("12"),
		jm("3456"),
		jm("[1,2,3]"),
		jm("{\"k\":1}"),
		jm("{\"a\":1}"),
		jm("[1,1,2]"),
	}}
}

// makeAllNullRow returns a RowData with all fields set to JSON null.
func makeAllNullRow() RowData {
	fields := make([]json.RawMessage, 23)
	for i := range fields {
		fields[i] = jm("null")
	}
	return RowData{Fields: fields}
}

func TestRowsNext_AllTypes_Nullable(t *testing.T) {
	t.Run("non-null values with Nullable true", func(t *testing.T) {
		ctx := context.Background()
		cols := makeCols(true)
		row := makeNonNullRow()
		var yielded2 bool
		iterator := iter.Seq[RowData](func(yield func(RowData) bool) {
			if yielded2 {
				return
			}
			yielded2 = true
			yield(row)
		})
		r := &Rows{ctx: ctx, iterator: iterator, columns: cols}
		dest := make([]driver.Value, len(cols))
		if err := r.Next(dest); err != nil {
			t.Fatalf("Next error: %v", err)
		}

		// integer-like with correct widths and values (via reflection)
		assertTypeAndValue(t, dest[0], sql.NullInt64{Int64: 127, Valid: true}, 0, "tiny")
		assertTypeAndValue(t, dest[1], sql.NullInt64{Int64: 32767, Valid: true}, 1, "small")
		assertTypeAndValue(t, dest[2], sql.NullInt64{Int64: 2147483647, Valid: true}, 2, "int")
		assertTypeAndValue(t, dest[3], sql.NullInt64{Int64: 9223372036854775807, Valid: true}, 3, "big")
		assertTypeAndValue(t, dest[4], sql.NullInt64{Int64: 42, Valid: true}, 4, "interval_int")
		assertTypeAndValue(t, dest[17], sql.NullInt64{Int64: 12, Valid: true}, 17, "interval_year_month")
		assertTypeAndValue(t, dest[18], sql.NullInt64{Int64: 3456, Valid: true}, 18, "interval_day_time")
		// floats (float -> NullFloat64, double -> NullFloat64)
		assertTypeAndValue(t, dest[5], sql.NullFloat64{Float64: 3.14, Valid: true}, 5, "float")
		assertTypeAndValue(t, dest[6], sql.NullFloat64{Float64: 2.718281828, Valid: true}, 6, "double")
		// bool
		assertTypeAndValue(t, dest[7], sql.NullBool{Bool: true, Valid: true}, 7, "bool")
		// strings
		assertTypeAndValue(t, dest[8], sql.NullString{String: "A", Valid: true}, 8, "char")
		assertTypeAndValue(t, dest[9], sql.NullString{String: "hello", Valid: true}, 9, "varchar")
		// decimal as string
		assertTypeAndValue(t, dest[10], sql.NullString{String: "12345.67", Valid: true}, 10, "decimal")

		// Date/time/timestamps
		mustParse := func(layout, s string) time.Time {
			tm, err := time.Parse(layout, s)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			return tm
		}
		assertTypeAndValue(t, dest[11], sql.NullTime{Time: mustParse("2006-01-02", "2025-08-22"), Valid: true}, 11, "date")
		assertTypeAndValue(t, dest[12], sql.NullTime{Time: mustParse("15:04:05", "13:14:15"), Valid: true}, 12, "time0")
		assertTypeAndValue(t, dest[13], sql.NullTime{Time: mustParse("15:04:05.000", "13:14:15.123"), Valid: true}, 13, "time3")
		assertTypeAndValue(t, dest[14], sql.NullTime{Time: mustParse("2006-01-02T15:04:05.000000", "2025-08-22T13:14:15.123456"), Valid: true}, 14, "ts")
		assertTypeAndValue(t, dest[15], sql.NullTime{Time: mustParse("2006-01-02T15:04:05.000000", "2025-08-22T13:14:15.123456"), Valid: true}, 15, "ts_ltz")

		// TIMESTAMP_WITH_TIME_ZONE returns raw bytes (JSON string), ensure bytes match
		if b, ok := dest[16].([]byte); !ok || string(b) != "\"2025-08-22T13:14:15.123456Z\"" {
			t.Fatalf("ts_wtz raw mismatch: %v (%T)", dest[16], dest[16])
		}

		// Complex types are returned as raw bytes
		assertRawBytes(t, dest, 19, 20, 21, 22)

		if err := r.Next(dest); err != io.EOF {
			t.Fatalf("expected io.EOF after draining rows, got %v", err)
		}
	})

	t.Run("all null values with Nullable true", func(t *testing.T) {
		ctx := context.Background()
		cols := makeCols(true)
		row := makeAllNullRow()
		var yielded3 bool
		iterator := iter.Seq[RowData](func(yield func(RowData) bool) {
			if yielded3 {
				return
			}
			yielded3 = true
			yield(row)
		})
		r := &Rows{ctx: ctx, iterator: iterator, columns: cols}
		dest := make([]driver.Value, len(cols))
		if err := r.Next(dest); err != nil {
			t.Fatalf("Next error: %v", err)
		}

		// For nullable columns, null values should be wrapped in sql.Null* with Valid=false.
		assertTypeAndValue(t, dest[0], sql.NullInt64{Valid: false}, 0, "tiny")
		assertTypeAndValue(t, dest[1], sql.NullInt64{Valid: false}, 1, "small")
		assertTypeAndValue(t, dest[2], sql.NullInt64{Valid: false}, 2, "int")
		assertTypeAndValue(t, dest[3], sql.NullInt64{Valid: false}, 3, "big")
		assertTypeAndValue(t, dest[4], sql.NullInt64{Valid: false}, 4, "interval_int")
		assertTypeAndValue(t, dest[5], sql.NullFloat64{Valid: false}, 5, "float")
		assertTypeAndValue(t, dest[6], sql.NullFloat64{Valid: false}, 6, "double")
		assertTypeAndValue(t, dest[7], sql.NullBool{Valid: false}, 7, "bool")
		assertTypeAndValue(t, dest[8], sql.NullString{Valid: false}, 8, "char")
		assertTypeAndValue(t, dest[9], sql.NullString{Valid: false}, 9, "varchar")
		assertTypeAndValue(t, dest[10], sql.NullString{Valid: false}, 10, "decimal")
		assertTypeAndValue(t, dest[11], sql.NullTime{Valid: false}, 11, "date")
		assertTypeAndValue(t, dest[12], sql.NullTime{Valid: false}, 12, "time0")
		assertTypeAndValue(t, dest[13], sql.NullTime{Valid: false}, 13, "time3")
		assertTypeAndValue(t, dest[14], sql.NullTime{Valid: false}, 14, "ts")
		assertTypeAndValue(t, dest[15], sql.NullTime{Valid: false}, 15, "ts_ltz")

		// TIMESTAMP_WITH_TIME_ZONE remains raw bytes even when null
		assertRawBytes(t, dest, 16)

		// Intervals as nullable ints
		assertTypeAndValue(t, dest[17], sql.NullInt64{Valid: false}, 17, "interval_year_month")
		assertTypeAndValue(t, dest[18], sql.NullInt64{Valid: false}, 18, "interval_day_time")

		// Complex types remain raw bytes (carrying the literal "null")
		assertRawBytes(t, dest, 19, 20, 21, 22)

		if err := r.Next(dest); err != io.EOF {
			t.Fatalf("expected io.EOF after draining rows, got %v", err)
		}
	})
}

func TestRowsColumns_ReturnsNamesInOrder(t *testing.T) {
	cols := makeCols(false)
	r := &Rows{columns: cols}
	got := r.Columns()
	want := make([]string, len(cols))
	for i, c := range cols {
		want[i] = c.Name
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Columns() mismatch. got=%v want=%v", got, want)
	}
}

func TestRowsColumns_Empty(t *testing.T) {
	r := &Rows{columns: nil}
	got := r.Columns()
	if len(got) != 0 {
		t.Fatalf("Columns() expected empty slice, got=%v", got)
	}
}

func TestRowsColumnTypeDatabaseTypeName_FromMakeCols(t *testing.T) {
	cols := makeCols(false)
	r := &Rows{columns: cols}
	for i := range cols {
		got := r.ColumnTypeDatabaseTypeName(i)
		want := strings.ToUpper(cols[i].LogicalType.Type)
		if got != want {
			t.Fatalf("ColumnTypeDatabaseTypeName[%d] mismatch: got %q want %q", i, got, want)
		}
	}
}

func TestRowsColumnTypeDatabaseTypeName_Uppercases(t *testing.T) {
	cols := []ColumnInfo{
		{Name: "c1", LogicalType: LogicalType{Type: "varchar"}},
		{Name: "c2", LogicalType: LogicalType{Type: "integer"}},
		{Name: "c3", LogicalType: LogicalType{Type: "timestamp_ltz"}},
		{Name: "c4", LogicalType: LogicalType{Type: "timestamp_with_time_zone"}},
		{Name: "c5", LogicalType: LogicalType{Type: "string"}},
	}
	r := &Rows{columns: cols}
	wants := []string{"VARCHAR", "INTEGER", "TIMESTAMP_LTZ", "TIMESTAMP_WITH_TIME_ZONE", "STRING"}
	for i := range cols {
		got := r.ColumnTypeDatabaseTypeName(i)
		if got != wants[i] {
			t.Fatalf("uppercasing mismatch at %d: got %q want %q", i, got, wants[i])
		}
	}
}

func TestRowsColumnTypeNullable_FromMakeColsFalse(t *testing.T) {
	cols := makeCols(false)
	r := &Rows{columns: cols}
	for i := range cols {
		nullable, ok := r.RowsColumnTypeNullable(i)
		if !ok {
			t.Fatalf("RowsColumnTypeNullable[%d]: ok=false, expected true", i)
		}
		want := cols[i].LogicalType.Nullable
		if nullable != want {
			t.Fatalf("RowsColumnTypeNullable[%d]: got %v want %v", i, nullable, want)
		}
	}
}

func TestRowsColumnTypeNullable_FromMakeColsTrue(t *testing.T) {
	cols := makeCols(true)
	r := &Rows{columns: cols}
	for i := range cols {
		nullable, ok := r.RowsColumnTypeNullable(i)
		if !ok {
			t.Fatalf("RowsColumnTypeNullable[%d]: ok=false, expected true", i)
		}
		want := cols[i].LogicalType.Nullable
		if nullable != want {
			t.Fatalf("RowsColumnTypeNullable[%d]: got %v want %v", i, nullable, want)
		}
	}
}

func TestRowsClose_CallsCancelWhenOpen(t *testing.T) {
	m := &gatewayMock{}
	fc := &flinkConn{client: m, sessionHandle: "sess"}

	r := &Rows{
		conn:            fc,
		operationHandle: "op",
		ctx:             context.Background(),
		closed:          false,
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
	if !m.cancelCalled {
		t.Fatalf("expected CancelOperation to be called when closing open Rows")
	}

	// Idempotent on second call
	m.cancelCalled = false
	if err := r.Close(); err != nil {
		t.Fatalf("second Close() returned error: %v", err)
	}
	if m.cancelCalled {
		t.Fatalf("CancelOperation should not be called on second Close()")
	}
}

func TestRowsColumnTypeScanType_FromMakeColsFalse(t *testing.T) {
	cols := makeCols(false)
	r := &Rows{columns: cols}

	// Build expected scan types aligned with makeCols order
	exp := []reflect.Type{
		scanTypeInt64,   // TINYINT -> int8
		scanTypeInt64,   // SMALLINT -> int16
		scanTypeInt64,   // INTEGER -> int32
		scanTypeInt64,   // BIGINT -> int64
		scanTypeInt64,   // INTERVAL (int) -> int64
		scanTypeFloat64, // FLOAT -> float64
		scanTypeFloat64, // DOUBLE -> float64
		scanTypeBool,    // BOOLEAN -> bool
		scanTypeString,  // CHAR -> string
		scanTypeString,  // VARCHAR -> string
		scanTypeString,  // DECIMAL -> string (preserve precision)
		scanTypeTime,    // DATE -> time.Time
		scanTypeTime,    // TIME -> time.Time
		scanTypeTime,    // TIME(3) -> time.Time
		scanTypeTime,    // TIMESTAMP -> time.Time
		scanTypeTime,    // TIMESTAMP_LTZ -> time.Time
		scanTypeBytes,   // TIMESTAMP_WITH_TIME_ZONE -> []byte (raw JSON string)
		scanTypeInt64,   // INTERVAL_YEAR_MONTH -> int64
		scanTypeInt64,   // INTERVAL_DAY_TIME -> int64
		scanTypeBytes,   // ARRAY -> []byte
		scanTypeBytes,   // MAP -> []byte
		scanTypeBytes,   // ROW -> []byte
		scanTypeBytes,   // MULTISET -> []byte
	}

	for i := range cols {
		got := r.ColumnTypeScanType(i)
		if got != exp[i] {
			t.Fatalf("ColumnTypeScanType[%d] mismatch: got %v want %v", i, got, exp[i])
		}
	}
}

func TestRowsColumnTypeScanType_FromMakeColsTrue(t *testing.T) {
	cols := makeCols(true)
	r := &Rows{columns: cols}

	// Build expected scan types aligned with makeCols order for nullable columns
	exp := []reflect.Type{
		scanTypeNullInt,    // TINYINT (nullable)
		scanTypeNullInt,    // SMALLINT (nullable)
		scanTypeNullInt,    // INTEGER (nullable)
		scanTypeNullInt,    // BIGINT (nullable)
		scanTypeNullInt,    // INTERVAL (nullable)
		scanTypeNullFloat,  // FLOAT (nullable)
		scanTypeNullFloat,  // DOUBLE (nullable)
		scanTypeNullBool,   // BOOLEAN (nullable)
		scanTypeNullString, // CHAR (nullable)
		scanTypeNullString, // VARCHAR (nullable)
		scanTypeNullString, // DECIMAL (nullable)
		scanTypeNullTime,   // DATE (nullable)
		scanTypeNullTime,   // TIME (nullable)
		scanTypeNullTime,   // TIME(3) (nullable)
		scanTypeNullTime,   // TIMESTAMP (nullable)
		scanTypeNullTime,   // TIMESTAMP_LTZ (nullable)
		scanTypeBytes,      // TIMESTAMP_WITH_TIME_ZONE -> bytes (no nullable variant)
		scanTypeNullInt,    // INTERVAL_YEAR_MONTH (nullable)
		scanTypeNullInt,    // INTERVAL_DAY_TIME (nullable)
		scanTypeBytes,      // ARRAY -> bytes
		scanTypeBytes,      // MAP -> bytes
		scanTypeBytes,      // ROW -> bytes
		scanTypeBytes,      // MULTISET -> bytes
		scanTypeNullInt,    // is_null (INTEGER, nullable)
	}

	for i := range cols {
		got := r.ColumnTypeScanType(i)
		if got != exp[i] {
			t.Fatalf("ColumnTypeScanType[%d] (%s) mismatch (nullable=true): got %v want %v", i, cols[i].Name, got, exp[i])
		}
	}
}
