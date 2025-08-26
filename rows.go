package flink

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"reflect"
	"strings"
	"time"
)

type typeAlias string

const (
	tinyIntType           typeAlias = "TINYINT"
	smallIntType          typeAlias = "SMALLINT"
	integerType           typeAlias = "INTEGER"
	bigIntType            typeAlias = "BIGINT"
	intervalType          typeAlias = "INTERVAL"
	floatType             typeAlias = "FLOAT"
	doubleType            typeAlias = "DOUBLE"
	booleanType           typeAlias = "BOOLEAN"
	charType              typeAlias = "CHAR"
	varCharType           typeAlias = "VARCHAR"
	decimal               typeAlias = "DECIMAL"
	dateType              typeAlias = "DATE"
	timeType              typeAlias = "TIME"
	timestamp             typeAlias = "TIMESTAMP"
	timestampWTZType      typeAlias = "TIMESTAMP_WITH_TIME_ZONE"
	timestampLTZType      typeAlias = "TIMESTAMP_LTZ"
	intervalYearMonthType typeAlias = "INTERVAL_YEAR_MONTH"
	intervalDayTimeType   typeAlias = "INTERVAL_DAY_TIME"
	arrayType             typeAlias = "ARRAY"
	mapType               typeAlias = "MAP"
	rowType               typeAlias = "ROW"
	multisetType          typeAlias = "MULTISET"
)

func normalizeFlinkType(s string) typeAlias {
	switch strings.ToUpper(s) {
	case "TINYINT":
		return tinyIntType
	case "SMALLINT":
		return smallIntType
	case "INTEGER", "INT":
		return integerType
	case "BIGINT":
		return bigIntType
	case "INTERVAL":
		return intervalType
	case "FLOAT":
		return floatType
	case "DOUBLE":
		return doubleType
	case "BOOLEAN":
		return booleanType
	case "CHAR":
		return charType
	case "VARCHAR", "STRING":
		return varCharType
	case "DECIMAL", "NUMERIC":
		return decimal
	case "DATE":
		return dateType
	case "TIME", "TIME_WITHOUT_TIME_ZONE":
		return timeType
	case "TIMESTAMP", "TIMESTAMP_WITHOUT_TIME_ZONE":
		return timestamp
	case "TIMESTAMP_LTZ", "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
		return timestampLTZType
	case "TIMESTAMP_WITH_TIME_ZONE":
		return timestampWTZType
	case "INTERVAL_YEAR_MONTH":
		return intervalYearMonthType
	case "INTERVAL_DAY_TIME":
		return intervalDayTimeType
	case "ARRAY":
		return arrayType
	case "MAP":
		return mapType
	case "ROW":
		return rowType
	case "MULTISET":
		return multisetType
	default:
		return ""
	}
}

var (
	scanTypeInt64      = reflect.TypeOf(int64(0))
	scanTypeFloat64    = reflect.TypeOf(float64(0))
	scanTypeBool       = reflect.TypeOf(true)
	scanTypeString     = reflect.TypeOf("")
	scanTypeTime       = reflect.TypeOf(time.Time{})
	scanTypeBytes      = reflect.TypeOf([]byte{})
	scanTypeNullFloat  = reflect.TypeOf(sql.NullFloat64{})
	scanTypeNullInt    = reflect.TypeOf(sql.NullInt64{})
	scanTypeNullTime   = reflect.TypeOf(sql.NullTime{})
	scanTypeNullString = reflect.TypeOf(sql.NullString{})
	scanTypeNullBool   = reflect.TypeOf(sql.NullBool{})
)

type Rows struct {
	conn            *flinkConn
	operationHandle string
	ctx             context.Context
	iterator        iter.Seq[RowData]
	columns         []ColumnInfo
	closed          bool
}

func (r *Rows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, c := range r.columns {
		names[i] = c.Name
	}
	return names
}

func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	return strings.ToUpper((r.columns)[index].LogicalType.Type)
}

func (r *Rows) RowsColumnTypeNullable(index int) (nullable, ok bool) {
	return (r.columns)[index].LogicalType.Nullable, true
}

func (r *Rows) Close() error {
	if !r.closed {
		r.closed = true
		r.iterator = nil
		r.conn.client.CancelOperation(r.ctx, r.conn.sessionHandle, r.operationHandle)
	}
	return nil
}

func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	nullable := (r.columns)[index].LogicalType.Nullable
	t := normalizeFlinkType((r.columns)[index].LogicalType.Type)

	switch t {
	case tinyIntType, smallIntType, integerType, bigIntType, intervalType:
		if nullable {
			return scanTypeNullInt
		}
		return scanTypeInt64
	case floatType, doubleType:
		if nullable {
			return scanTypeNullFloat
		}
		return scanTypeFloat64
	case booleanType:
		if nullable {
			return scanTypeNullBool
		}
		return scanTypeBool
	case charType, varCharType:
		if nullable {
			return scanTypeNullString
		}
		return scanTypeString
	case decimal:
		if nullable {
			return scanTypeNullString
		}
		return scanTypeString
	case dateType, timeType, timestamp, timestampLTZType:
		if nullable {
			return scanTypeNullTime
		}
		return scanTypeTime
	case intervalYearMonthType, intervalDayTimeType:
		if nullable {
			return scanTypeNullInt
		}
		return scanTypeInt64
	default:
		return scanTypeBytes
	}
}

func (r *Rows) ColumnTypeLength(index int) (length int64, ok bool) {
	typeLen := (r.columns)[index].LogicalType.Length
	if typeLen == nil {
		return 0, false
	}
	return *typeLen, true
}

func (r *Rows) columnTypePrecision(index int) (length int, ok bool) {
	perc := (r.columns)[index].LogicalType.Precision
	if perc == nil {
		return 0, false
	}
	return *perc, true
}

func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	perc := (r.columns)[index].LogicalType.Precision
	sc := (r.columns)[index].LogicalType.Scale
	if perc == nil || sc == nil {
		return 0, 0, false
	}
	return int64(*perc), int64(*sc), true
}

func (r *Rows) decodeField(t typeAlias, nullable bool, raw []byte, colIdx int) (driver.Value, error) {
	isNull := bytes.Equal(raw, []byte("null"))

	switch t {
	case tinyIntType, smallIntType, integerType, bigIntType, intervalType:
		if isNull {
			if nullable {
				return sql.NullInt64{Valid: false}, nil
			}
			return nil, nil
		}
		var v int64
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("column %d: int decode failed: %w", colIdx, err)
		}
		if nullable {
			return sql.NullInt64{Int64: v, Valid: true}, nil
		}
		return v, nil

	case floatType, doubleType:
		if isNull {
			if nullable {
				return sql.NullFloat64{Valid: false}, nil
			}
			return nil, nil
		}
		var v float64
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("column %d: float decode failed: %w", colIdx, err)
		}
		if nullable {
			return sql.NullFloat64{Float64: v, Valid: true}, nil
		}
		return v, nil

	case booleanType:
		if isNull {
			if nullable {
				return sql.NullBool{Valid: false}, nil
			}
			return nil, nil
		}
		var v bool
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("column %d: bool decode failed: %w", colIdx, err)
		}
		if nullable {
			return sql.NullBool{Bool: v, Valid: true}, nil
		}
		return v, nil

	case charType, varCharType:
		if isNull {
			if nullable {
				return sql.NullString{Valid: false}, nil
			}
			return nil, nil
		}
		var v string
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("column %d: string decode failed: %w", colIdx, err)
		}
		if nullable {
			return sql.NullString{String: v, Valid: true}, nil
		}
		return v, nil

	case decimal:
		if isNull {
			if nullable {
				return sql.NullString{Valid: false}, nil
			}
			return nil, nil
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var num json.Number
		if err := dec.Decode(&num); err != nil {
			return nil, fmt.Errorf("column %d: decimal decode failed: %w", colIdx, err)
		}
		if nullable {
			return sql.NullString{String: num.String(), Valid: true}, nil
		}
		return num.String(), nil

	case dateType:
		if isNull {
			if nullable {
				return sql.NullTime{Valid: false}, nil
			}
			return nil, nil
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("column %d: date decode failed: %w", colIdx, err)
		}
		tval, err := time.Parse("2006-01-02", s)
		if err != nil {
			return nil, fmt.Errorf("column %d: date parse failed: %w", colIdx, err)
		}
		if nullable {
			return sql.NullTime{Time: tval, Valid: true}, nil
		}
		return tval, nil

	case timeType:
		if isNull {
			if nullable {
				return sql.NullTime{Valid: false}, nil
			}
			return nil, nil
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("column %d: time decode failed: %w", colIdx, err)
		}
		prec, ok := r.columnTypePrecision(colIdx)
		if !ok {
			prec = 0
		}
		layout := "15:04:05"
		if prec > 0 {
			layout = layout + "." + strings.Repeat("9", int(prec))
		}
		tval, err := time.Parse(layout, s)
		if err != nil {
			return nil, fmt.Errorf("column %d: time parse failed for %q with precision %d: %w", colIdx, s, prec, err)
		}
		if nullable {
			return sql.NullTime{Time: tval, Valid: true}, nil
		}
		return tval, nil

	case timestampWTZType:
		// Keep raw bytes for WTZ
		return raw, nil

	case timestamp, timestampLTZType:
		if isNull {
			if nullable {
				return sql.NullTime{Valid: false}, nil
			}
			return nil, nil
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("column %d: timestamp decode failed: %w", colIdx, err)
		}
		prec, ok := r.columnTypePrecision(colIdx)
		if !ok {
			prec = 6
		}
		base := "2006-01-02T15:04:05"
		if prec > 0 {
			base = base + "." + strings.Repeat("9", int(prec))
		}
		layout := base
		if strings.HasSuffix(s, "Z") {
			layout = base + "Z"
		}
		tval, err := time.Parse(layout, s)
		if err != nil {
			return nil, fmt.Errorf("column %d: timestamp parse failed for %q with precision %d using layout %q: %w", colIdx, s, prec, layout, err)
		}
		if nullable {
			return sql.NullTime{Time: tval, Valid: true}, nil
		}
		return tval, nil

	case intervalYearMonthType, intervalDayTimeType:
		if isNull {
			if nullable {
				return sql.NullInt64{Valid: false}, nil
			}
			return nil, nil
		}
		var n int64
		if err := json.Unmarshal(raw, &n); err != nil {
			return nil, fmt.Errorf("column %d: interval decode failed: %w", colIdx, err)
		}
		if nullable {
			return sql.NullInt64{Int64: n, Valid: true}, nil
		}
		return n, nil

	case arrayType, mapType, rowType, multisetType:
		return raw, nil
	default:
		return raw, nil
	}
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	var row RowData
	var ok bool

	r.iterator(func(rdata RowData) bool {
		row = rdata
		ok = true
		return false
	})

	// No more data
	if !ok {
		// r.closed = true
		return io.EOF
	}

	for i, raw := range row.Fields {
		t := normalizeFlinkType((r.columns)[i].LogicalType.Type)
		nullable := (r.columns)[i].LogicalType.Nullable
		val, err := r.decodeField(t, nullable, raw, i)
		if err != nil {
			return err
		}
		dest[i] = val
	}

	return nil
}

func newRows(ctx context.Context, conn *flinkConn, operationHandle string, initialResults []RowData, columns []ColumnInfo, nextToken string) (*Rows, error) {
	iterator := newResultsIterator(ctx, conn, operationHandle, initialResults, nextToken)
	return &Rows{
		conn:            conn,
		ctx:             ctx,
		operationHandle: operationHandle,
		iterator:        iterator,
		columns:         columns,
	}, nil
}

func newResultsIterator(ctx context.Context, conn *flinkConn, operationHandle string, initialResults []RowData, nextToken string) iter.Seq[RowData] {
	results := initialResults
	token := nextToken
	pos := 0
	client := conn.client
	sessionHandle := conn.sessionHandle

	return func(yield func(RowData) bool) {
		baseBackoff := 1 * time.Second
		backoff := baseBackoff
		for {
			if pos < len(results) {
				row := results[pos]
				pos++
				if !yield(row) {
					return
				}
				continue
			}
			// todo: fetch next results asynchronously
			response, err := client.FetchResults(ctx, sessionHandle, operationHandle, token, "")
			if err != nil {
				conn.cancelOperation(ctx, operationHandle)
				return
			}
			if response.ResultType == ResultTypeEOS {
				return
			}
			results = response.Results.Data
			token = response.NextToken()
			pos = 0

			if len(results) == 0 {
				select {
				case <-ctx.Done():
					conn.cancelOperation(ctx, operationHandle)
					return
				case <-time.After(backoff):
					if backoff < 8*time.Second {
						backoff *= 2
					}
				}
			} else {
				backoff = baseBackoff
			}
		}
	}
}
