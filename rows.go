package flink

import (
	"bytes"
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
	case "TIME":
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
	scanTypeInt8       = reflect.TypeOf(int8(0))
	scanTypeInt16      = reflect.TypeOf(int16(0))
	scanTypeInt32      = reflect.TypeOf(int32(0))
	scanTypeInt64      = reflect.TypeOf(int64(0))
	scanTypeFloat32    = reflect.TypeOf(float32(0))
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
	iterator iter.Seq[RowData]
	columns  *[]ColumnInfo
	closed   bool
}

func (r *Rows) Columns() []string {
	names := make([]string, len(*r.columns))
	for i, c := range *r.columns {
		names[i] = c.Name
	}
	return names
}

func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	return strings.ToUpper((*r.columns)[index].LogicalType.Type)
}

func (r *Rows) RowsColumnTypeNullable(index int) (nullable, ok bool) {
	return (*r.columns)[index].LogicalType.Nullable, true
}

func (r *Rows) Close() error {
	r.closed = true
	r.iterator = nil
	// todo: cancel the underlying operation handle
	return nil
}

func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	nullable := (*r.columns)[index].LogicalType.Nullable
	t := normalizeFlinkType((*r.columns)[index].LogicalType.Type)

	switch t {
	case tinyIntType:
		if nullable {
			return scanTypeNullInt
		}
		return scanTypeInt8
	case smallIntType:
		if nullable {
			return scanTypeNullInt
		}
		return scanTypeInt16
	case integerType:
		if nullable {
			return scanTypeNullInt
		}
		return scanTypeInt32
	case bigIntType, intervalType:
		if nullable {
			return scanTypeNullInt
		}
		return scanTypeInt64
	case floatType:
		if nullable {
			return scanTypeNullFloat
		}
		return scanTypeFloat32
	case doubleType:
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
	default:
		return scanTypeBytes
	}
}

func (r *Rows) ColumnTypeLength(index int) (length int64, ok bool) {
	typeLen := (*r.columns)[index].LogicalType.Length
	if typeLen == nil {
		return 0, false
	}
	return *typeLen, true
}

func (r *Rows) columnTypePrecision(index int) (length int, ok bool) {
	perc := (*r.columns)[index].LogicalType.Precision
	if perc == nil {
		return 0, false
	}
	return *perc, true
}

func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	perc := (*r.columns)[index].LogicalType.Precision
	sc := (*r.columns)[index].LogicalType.Scale
	if perc == nil || sc == nil {
		return 0, 0, false
	}
	return int64(*perc), int64(*sc), true
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.closed || r.iterator == nil {
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
		return io.EOF
	}

	for i, raw := range row.Fields {
		t := normalizeFlinkType((*r.columns)[i].LogicalType.Type)

		// handle null explicitly
		if strings.TrimSpace(string(raw)) == "null" {
			dest[i] = nil
			continue
		}

		switch t {
		case tinyIntType, smallIntType, integerType, bigIntType, intervalType:
			var v int64
			if err := json.Unmarshal(raw, &v); err != nil {
				return fmt.Errorf("column %d: int decode failed: %w", i, err)
			}
			dest[i] = v
		case floatType, doubleType:
			var v float64
			if err := json.Unmarshal(raw, &v); err != nil {
				return fmt.Errorf("column %d: float decode failed: %w", i, err)
			}
			dest[i] = v
		case booleanType:
			var v bool
			if err := json.Unmarshal(raw, &v); err != nil {
				return fmt.Errorf("column %d: bool decode failed: %w", i, err)
			}
			dest[i] = v
		case charType, varCharType:
			var v string
			if err := json.Unmarshal(raw, &v); err != nil {
				return fmt.Errorf("column %d: string decode failed: %w", i, err)
			}
			dest[i] = v
		case decimal:
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.UseNumber()
			var num json.Number
			if err := dec.Decode(&num); err != nil {
				return fmt.Errorf("column %d: decimal decode failed: %w", i, err)
			}
			// Preserve exact lexical representation to avoid precision loss
			dest[i] = num.String()
		case dateType:
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return fmt.Errorf("column %d: date decode failed: %w", i, err)
			}
			tval, err := time.Parse("2006-01-02", s)
			if err != nil {
				return fmt.Errorf("column %d: date parse failed: %w", i, err)
			}
			dest[i] = tval
		case timeType:
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return fmt.Errorf("column %d: time decode failed: %w", i, err)
			}
			prec, ok := r.columnTypePrecision(i)
			if !ok {
				prec = 0
			}
			layout := "15:04:05"
			if prec > 0 {
				layout = layout + "." + strings.Repeat("9", int(prec))
			}
			tval, err := time.Parse(layout, s)
			if err != nil {
				return fmt.Errorf("column %d: time parse failed for %q with precision %d: %w", i, s, prec, err)
			}
			dest[i] = tval
		case timestampWTZType:
			dest[i] = []byte(raw)
		case timestamp, timestampLTZType:
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return fmt.Errorf("column %d: timestamp decode failed: %w", i, err)
			}
			prec, ok := r.columnTypePrecision(i)
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
				return fmt.Errorf("column %d: timestamp parse failed for %q with precision %d using layout %q: %w", i, s, prec, layout, err)
			}
			dest[i] = tval
		case intervalYearMonthType, intervalDayTimeType:
			var n int64
			if err := json.Unmarshal(raw, &n); err != nil {
				return fmt.Errorf("column %d: interval decode failed: %w", i, err)
			}
			dest[i] = n
		case arrayType, mapType, rowType, multisetType:
			dest[i] = []byte(raw)
		default:
			dest[i] = []byte(raw)
		}
	}

	return nil
}
