package flink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	jobManager  testcontainers.Container
	taskManager testcontainers.Container
	sqlGateway  testcontainers.Container
	endpoint    string
	ctx         = context.Background()
)

func TestMain(m *testing.M) {
	net, err := network.New(ctx)
	if err != nil {
		fmt.Printf("failed to create network: %v\n", err)
		os.Exit(1)
	}
	defer net.Remove(ctx)

	// JobManager
	jobManager, err = testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:    "flink:1.20",
				Networks: []string{net.Name},
				Env: map[string]string{
					"FLINK_PROPERTIES": "jobmanager.rpc.address: jobmanager",
				},
				ConfigModifier: func(cfg *container.Config) {
					cfg.Hostname = "jobmanager"
				},
				ExposedPorts: []string{"8081/tcp"},
				Cmd:          []string{"jobmanager"},
				WaitingFor: wait.ForHTTP("/").
					WithPort("8081/tcp").
					WithStartupTimeout(2 * time.Minute),
			},
			Started: true,
		})
	if err != nil {
		fmt.Printf("failed to start JobManager: %v\n", err)
		os.Exit(1)
	}
	defer jobManager.Terminate(ctx)

	// TaskManager
	taskManager, err = testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:    "flink:1.20",
				Networks: []string{net.Name},
				Env: map[string]string{
					"FLINK_PROPERTIES": "jobmanager.rpc.address: jobmanager",
				},
				Cmd: []string{"taskmanager"},
				ConfigModifier: func(cfg *container.Config) {
					cfg.Hostname = "taskmanager"
				},
			},
			Started: true,
		})
	if err != nil {
		fmt.Printf("failed to start TaskManager: %v\n", err)
		os.Exit(1)
	}
	defer taskManager.Terminate(ctx)

	// SQL Gateway
	sqlGateway, err = testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:    "flink:1.20",
				Networks: []string{net.Name},
				Env: map[string]string{
					"FLINK_PROPERTIES": "jobmanager.rpc.address: jobmanager",
				},
				ExposedPorts: []string{"8083/tcp"},
				Cmd:          []string{"/opt/flink/bin/sql-gateway.sh", "start-foreground", "-Dsql-gateway.endpoint.rest.address=localhost", "-Drest.address=jobmanager"},
				WaitingFor: wait.ForHTTP("/info").
					WithPort("8083/tcp").
					WithStartupTimeout(2 * time.Minute),
			},
			Started: true,
		})
	if err != nil {
		fmt.Printf("failed to start sql-gateway: %v\n", err)
		os.Exit(1)
	}
	defer sqlGateway.Terminate(ctx)

	host, err := sqlGateway.Host(ctx)
	if err != nil {
		fmt.Printf("failed to get sqlGateway host: %v\n", err)
		os.Exit(1)
	}
	mappedPort, err := sqlGateway.MappedPort(ctx, "8083/tcp")
	if err != nil {
		fmt.Printf("failed to get sqlGateway mapped port: %v\n", err)
		os.Exit(1)
	}

	endpoint = fmt.Sprintf("http://%s:%s", host, mappedPort.Port())

	code := m.Run()

	// Terminate containers on exit
	jobManager.Terminate(ctx)
	taskManager.Terminate(ctx)
	sqlGateway.Terminate(ctx)
	net.Remove(ctx)

	os.Exit(code)
}

func TestFlinkConn_EndToEnd_SelectAllTypesNotNull(t *testing.T) {
	// 1) Create connector and open a connection (flinkConn)
	connector, _ := NewConnector(
		WithGatewayURL(endpoint),
		WithProperties(map[string]string{
			"execution.runtime-mode": "STREAMING",
		}),
	)
	db := sql.OpenDB(connector)

	defer func() {
		if cerr := db.Close(); cerr != nil {
			t.Logf("close db: %v", cerr)
		}
	}()

	createTable := `CREATE TABLE test_table (
		bool BOOLEAN NOT NULL, 
		num SMALLINT NOT NULL,
		dbl DOUBLE NOT NULL,
		bin BINARY NOT NULL,
		vbin VARBINARY NOT NULL,
		chr CHAR NOT NULL,
		vchr VARCHAR NOT NULL,
		tmstp TIMESTAMP_LTZ(3) NOT NULL,
		dte DATE NOT NULL,
		dcml DECIMAL NOT NULL ,
		tint TINYINT NOT NULL,
		sint SMALLINT NOT NULL,
		nint INTEGER NOT NULL,
		twotz TIME WITHOUT TIME ZONE NOT NULL,
		bgint BIGINT NOT NULL,
		flt FLOAT NOT NULL,
		tmstpwotz TIMESTAMP NOT NULL,
		row1 ROW<int1 INTEGER NOT NULL, int2 INTEGER, str1 STRING NOT NULL, nested_row_not_null ROW<str2 STRING, int4 INT NOT NULL> NOT NULL, nested_row_null ROW<int5 int not null>> NOT NULL,
		map1 MAP<STRING, INT> NOT NULL
		// TODO "array1 ARRAY<INT> NOT NULL
		// TODO ms "MULTISET<INT NOT NULL> NOT NULL
		) WITH (
		'connector' = 'datagen',
		'rows-per-second' = '5'
	)`

	result, err := db.ExecContext(ctx, createTable)
	if err != nil {
		t.Fatalf("ExecContext(create table) failed: %v", err)
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != 0 {
		t.Fatalf("ExecContext(create table) expected 0 rows affected, got %d", rowsAffected)
	}

	ctx3, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx3, "SELECT * from test_table")
	if err != nil {
		t.Fatalf("QueryContext(SELECT * from test_table) failed: %v", err)
	}

	// Validate types for at least the first row; the 3s context will cancel the stream gracefully.
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns() failed: %v", err)
	}

	// Build expected Go types per column (NOT NULL schema).
	typeOfBool := reflect.TypeOf(true)
	typeOfInt64 := reflect.TypeOf(int64(0))
	typeOfF64 := reflect.TypeOf(float64(0))
	typeOfBytes := reflect.TypeOf([]byte(nil))
	typeOfString := reflect.TypeOf("")
	typeOfTime := reflect.TypeOf(time.Time{})

	expected := map[string][]reflect.Type{
		"bool": {typeOfBool},
		"num":  {typeOfInt64}, // SMALLINT
		"dbl":  {typeOfF64},   // DOUBLE
		"bin":  {typeOfBytes}, // BINARY
		"vbin": {typeOfBytes}, // VARBINARY
		"chr":  {typeOfString},
		"vchr": {typeOfString},
		// Allow time-ish values to be time.Time or string/[]byte depending on driver formatting.
		"tmstp": {typeOfTime}, // TIMESTAMP_LTZ
		"dte":   {typeOfTime}, // DATE
		// Decimal: allow string or bytes; some gateways may surface float64.
		"dcml":      {typeOfString},
		"tint":      {typeOfInt64}, // TINYINT
		"sint":      {typeOfInt64}, // SMALLINT
		"nint":      {typeOfInt64}, // INTEGER
		"twotz":     {typeOfTime},  // TIME WITHOUT TZ
		"bgint":     {typeOfInt64}, // BIGINT
		"flt":       {typeOfF64},   // FLOAT
		"tmstpwotz": {typeOfTime},
		// Complex types may be surfaced as JSON-encoded []byte or string
		"row1": {typeOfBytes, typeOfString},
		"map1": {typeOfBytes, typeOfString},
	}

	for rows.Next() {
		// Scan current row
		dest := make([]any, len(cols))
		scanArgs := make([]any, len(cols))
		for i := range dest {
			scanArgs[i] = &dest[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		// Check each column's Go type against expectations
		for i, name := range cols {
			lname := strings.ToLower(strings.TrimSpace(name))
			allowed, ok := expected[lname]
			if !ok {
				// If we somehow don't have an expectation, just continue
				continue
			}
			got := reflect.TypeOf(dest[i])
			match := false
			for _, a := range allowed {
				if got == a {
					match = true
					break
				}
			}
			if !match {
				t.Fatalf("column %q: got Go type %v (value=%v), want one of %v", name, got, dest[i], allowed)
			}
		}
	}

	// Handle end-of-rows or timeout gracefully
	if err := rows.Err(); err != nil {
		// If the context timed out, that's expected; otherwise fail.
		if !errors.Is(ctx3.Err(), context.DeadlineExceeded) {
			t.Fatalf("rows.Err(): %v", err)
		}
	}

}

func TestFlinkConn_EndToEnd_SelectAllTypesNullable(t *testing.T) {
	// 1) Create connector and open a connection (flinkConn)
	connector, _ := NewConnector(
		WithGatewayURL(endpoint),
		WithProperties(map[string]string{
			"execution.runtime-mode": "STREAMING",
		}),
	)
	db := sql.OpenDB(connector)

	defer func() {
		if cerr := db.Close(); cerr != nil {
			t.Logf("close db: %v", cerr)
		}
	}()

	createTable := `CREATE TABLE test_table_nullable (
		bool BOOLEAN,
		num SMALLINT,
		dbl DOUBLE,
		bin BINARY,
		vbin VARBINARY,
		chr CHAR,
		vchr VARCHAR,
		tmstp TIMESTAMP_LTZ(3),
		dte DATE,
		dcml DECIMAL,
		tint TINYINT,
		sint SMALLINT,
		nint INTEGER,
		twotz TIME WITHOUT TIME ZONE,
		bgint BIGINT,
		flt FLOAT,
		tmstpwotz TIMESTAMP,
		row1 ROW<int1 INTEGER, int2 INTEGER, str1 STRING, nested_row_not_null ROW<str2 STRING, int4 INT>, nested_row_null ROW<int5 INT>>,
		map1 MAP<STRING, INT>
	) WITH (
		'connector' = 'datagen',
		'rows-per-second' = '5'
	)`

	result, err := db.ExecContext(ctx, createTable)
	if err != nil {
		t.Fatalf("ExecContext(create table) failed: %v", err)
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != 0 {
		t.Fatalf("ExecContext(create table) expected 0 rows affected, got %d", rowsAffected)
	}

	ctx3, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx3, "SELECT * from test_table_nullable")
	if err != nil {
		t.Fatalf("QueryContext(SELECT * from test_table_nullable) failed: %v", err)
	}

	// Validate types for at least the first row; the 3s context will cancel the stream gracefully.
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns() failed: %v", err)
	}

	// Build expected Go types per column (ALL NULLABLE schema).
	typeOfNullBool := reflect.TypeOf(sql.NullBool{})
	typeOfNullInt := reflect.TypeOf(sql.NullInt64{})
	typeOfNullFloat := reflect.TypeOf(sql.NullFloat64{})
	typeOfNullString := reflect.TypeOf(sql.NullString{})
	typeOfNullTime := reflect.TypeOf(sql.NullTime{})
	typeOfBytes := reflect.TypeOf([]byte(nil))
	typeOfString := reflect.TypeOf("")

	expected := map[string][]reflect.Type{
		"bool":      {typeOfNullBool},
		"num":       {typeOfNullInt},    // SMALLINT -> sql.NullInt64
		"dbl":       {typeOfNullFloat},  // DOUBLE -> sql.NullFloat64
		"bin":       {typeOfBytes},      // BINARY (nullable) -> []byte
		"vbin":      {typeOfBytes},      // VARBINARY (nullable) -> []byte
		"chr":       {typeOfNullString}, // CHAR -> sql.NullString
		"vchr":      {typeOfNullString}, // VARCHAR -> sql.NullString
		"tmstp":     {typeOfNullTime},   // TIMESTAMP_LTZ -> sql.NullTime
		"dte":       {typeOfNullTime},   // DATE -> sql.NullTime
		"dcml":      {typeOfNullString}, // DECIMAL -> sql.NullString
		"tint":      {typeOfNullInt},    // TINYINT -> sql.NullInt64
		"sint":      {typeOfNullInt},    // SMALLINT -> sql.NullInt64
		"nint":      {typeOfNullInt},    // INTEGER -> sql.NullInt64
		"twotz":     {typeOfNullTime},   // TIME WITHOUT TZ -> sql.NullTime
		"bgint":     {typeOfNullInt},    // BIGINT -> sql.NullInt64
		"flt":       {typeOfNullFloat},  // FLOAT -> sql.NullFloat64
		"tmstpwotz": {typeOfNullTime},   // TIMESTAMP -> sql.NullTime
		// Complex types may be surfaced as JSON-encoded []byte or string
		"row1": {typeOfBytes, typeOfString},
		"map1": {typeOfBytes, typeOfString},
	}

	for rows.Next() {
		// Scan current row
		dest := make([]any, len(cols))
		scanArgs := make([]any, len(cols))
		for i := range dest {
			scanArgs[i] = &dest[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		// Check each column's Go type against expectations
		for i, name := range cols {
			lname := strings.ToLower(strings.TrimSpace(name))
			allowed, ok := expected[lname]
			if !ok {
				// If we somehow don't have an expectation, just continue
				continue
			}
			got := reflect.TypeOf(dest[i])
			match := false
			for _, a := range allowed {
				if got == a {
					match = true
					break
				}
			}
			if !match {
				t.Fatalf("column %q: got Go type %v (value=%v), want one of %v", name, got, dest[i], allowed)
			}
		}
	}

	// Handle end-of-rows or timeout gracefully
	if err := rows.Err(); err != nil {
		// If the context timed out, that's expected; otherwise fail.
		if !errors.Is(ctx3.Err(), context.DeadlineExceeded) {
			t.Fatalf("rows.Err(): %v", err)
		}
	}
}

// readAllRows drains *sql.Rows and returns a copy of all values.
// It also ensures rows.Close() is called.
func readAllRows(rows *sql.Rows) ([][]any, error) {
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var out [][]any
	for rows.Next() {
		dest := make([]any, len(cols))
		scanArgs := make([]any, len(cols))
		for i := range dest {
			scanArgs[i] = &dest[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		row := make([]any, len(dest))
		copy(row, dest)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// timeOfDaySuffix extracts "HH:MM:SS" for a timestamp-ish value coming back from the driver.
// It supports time.Time, string (various layouts), and []byte (string-encoded).
func timeOfDaySuffix(v any) string {
	switch x := v.(type) {
	case time.Time:
		return x.Format("15:04:05")
	case string:
		s := strings.TrimSpace(x)
		if len(s) >= 8 {
			return s[len(s)-8:]
		}
		return s
	case []byte:
		s := strings.TrimSpace(string(x))
		if len(s) >= 8 {
			return s[len(s)-8:]
		}
		return s
	default:
		return fmt.Sprintf("%v", x)
	}
}

// awaitAtLeastOneFinishedJob polls "SHOW JOBS" until it finds any row whose status/state is FINISHED.
func awaitAtLeastOneFinishedJob(db *sql.DB, timeout time.Duration, interval time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		rows, err := db.QueryContext(ctx, "SHOW JOBS")
		if err != nil {
			time.Sleep(interval)
			continue
		}

		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			time.Sleep(interval)
			continue
		}

		all, err := readAllRows(rows)
		if err != nil {
			time.Sleep(interval)
			continue
		}
		if len(all) == 0 {
			time.Sleep(interval)
			continue
		}

		statusIdx := -1
		for i, c := range cols {
			s := strings.ToLower(strings.TrimSpace(c))
			if s == "status" || s == "state" {
				statusIdx = i
				break
			}
		}
		if statusIdx == -1 {
			if len(cols) >= 3 {
				statusIdx = 2
			} else if len(cols) >= 1 {
				statusIdx = len(cols) - 1
			}
		}

		for _, r := range all {
			if statusIdx >= 0 && statusIdx < len(r) {
				status := strings.ToUpper(strings.TrimSpace(fmt.Sprint(r[statusIdx])))
				if status == "FINISHED" {
					return nil
				}
			}
		}
		time.Sleep(interval)
	}
	return fmt.Errorf("no FINISHED job observed within %s", timeout)
}

func getJobIDs(db *sql.DB) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, "SHOW JOBS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	all, err := readAllRows(rows)
	if err != nil {
		return nil, err
	}

	fmt.Println("columns:", cols)
	fmt.Println("rows:", all)

	// Find a likely job id column (usually the first)
	idIdx := 0

	out := make(map[string]struct{})
	for _, r := range all {
		id := strings.TrimSpace(fmt.Sprint(r[idIdx]))
		out[id] = struct{}{}
	}
	fmt.Println("idIdx:", idIdx)
	fmt.Println("out: ", out)
	return out, nil
}

func awaitNewJobFinished(db *sql.DB, before map[string]struct{}, timeout time.Duration, interval time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		rows, err := db.QueryContext(ctx, "SHOW JOBS")
		if err != nil {
			time.Sleep(interval)
			continue
		}

		cols, err := rows.Columns()
		if err == nil {
			fmt.Println("columns:", cols)
		}

		colTypes, err := rows.ColumnTypes()
		if err == nil {
			var b strings.Builder
			fmt.Fprintln(&b, "col types:")
			for i, ct := range colTypes {
				if ct == nil {
					fmt.Fprintf(&b, "  [%d] <nil>\n", i)
					continue
				}
				name := ct.Name()
				dbt := ct.DatabaseTypeName()
				length, lengthOK := ct.Length()
				nullable, nullableOK := ct.Nullable()
				precision, scale, decOK := ct.DecimalSize()
				scanType := ct.ScanType()

				fmt.Fprintf(&b, "  [%d] Name=%q DBType=%q", i, name, dbt)
				if lengthOK {
					fmt.Fprintf(&b, " Length=%d", length)
				}
				if decOK {
					fmt.Fprintf(&b, " Precision=%d Scale=%d", precision, scale)
				}
				if nullableOK {
					fmt.Fprintf(&b, " Nullable=%t", nullable)
				}
				if scanType != nil {
					fmt.Fprintf(&b, " ScanType=%v", scanType)
				}
				fmt.Fprintln(&b)
			}
			fmt.Print(b.String())
		} else {
			fmt.Println("col types error:", err)
		}

		all, err := readAllRows(rows)
		if err != nil {
			time.Sleep(interval)
			continue
		}

		fmt.Println("All new jobs:", all)

		// Identify columns
		idIdx := 0
		statusIdx := 2

		for _, r := range all {
			var jid string
			switch v := r[idIdx].(type) {
			case sql.NullString:
				if v.Valid {
					jid = v.String
				}
			default:
				jid = strings.TrimSpace(fmt.Sprint(v))
			}

			var status string
			switch v := r[statusIdx].(type) {
			case sql.NullString:
				if v.Valid {
					status = v.String
				}
			default:
				status = strings.TrimSpace(fmt.Sprint(v))
			}

			status = strings.ToUpper(status)

			fmt.Println("jid:", jid, "status:", status)
			if status == "FINISHED" {
				return jid, nil
			}
		}

		time.Sleep(interval)
	}
	return "", fmt.Errorf("no new FINISHED job observed within %s", timeout)
}

func TestFlinkConn_EndToEnd_FilesystemInsertSelect(t *testing.T) {
	// Prepare connector and DB
	connector, _ := NewConnector(
		WithGatewayURL(endpoint),
		WithProperties(map[string]string{
			"execution.runtime-mode": "STREAMING",
		}),
	)
	db := sql.OpenDB(connector)
	defer func() {
		if cerr := db.Close(); cerr != nil {
			t.Logf("close db: %v", cerr)
		}
	}()

	dir := fmt.Sprintf("file:///tmp/test_table_%d", time.Now().UnixNano())

	// Ensure the target directory exists inside the containers and is writable by 'flink'
	localDir := strings.TrimPrefix(dir, "file://")
	ensureDir := func(c testcontainers.Container, name string) {
		exitCode, err, _ := c.Exec(ctx, []string{"sh", "-lc", fmt.Sprintf("mkdir -p %s && chmod 777 %s", localDir, localDir)})
		if err != nil || exitCode != 0 {
			t.Logf("warn: failed to ensure dir on %s: err=%v exit=%d", name, err, exitCode)
		} else {
			t.Logf("ensured %s exists on %s", localDir, name)
		}
	}
	ensureDir(jobManager, "jobmanager")
	ensureDir(taskManager, "taskmanager")
	ensureDir(sqlGateway, "sql-gateway")

	ddl := fmt.Sprintf(
		"CREATE TABLE test_table(\n"+
			"  id BIGINT,\n"+
			"  val INT,\n"+
			"  str STRING,\n"+
			"  timestamp1 TIMESTAMP(0),\n"+
			"  timestamp2 TIMESTAMP_LTZ(3),\n"+
			"  time_data TIME,\n"+
			"  date_data DATE\n"+
			") WITH (\n"+
			"  'connector'='filesystem',\n"+
			"  'format'='csv',\n"+
			"  'path'='%s'\n"+
			")",
		dir,
	)

	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	// Capture job IDs before INSERT
	beforeIDs, _ := getJobIDs(db)

	insertSQL := "INSERT INTO test_table VALUES " +
		"(1, 11, '111', TIMESTAMP '2021-04-15 23:18:36', TO_TIMESTAMP_LTZ(400000000000, 3), TIME '12:32:00', DATE '2023-11-02'), " +
		"(3, 33, '333', TIMESTAMP '2021-04-16 23:18:36', TO_TIMESTAMP_LTZ(500000000000, 3), TIME '13:32:00', DATE '2023-12-02'), " +
		"(2, 22, '222', TIMESTAMP '2021-04-17 23:18:36', TO_TIMESTAMP_LTZ(600000000000, 3), TIME '14:32:00', DATE '2023-01-02'), " +
		"(4, 44, '444', TIMESTAMP '2021-04-18 23:18:36', TO_TIMESTAMP_LTZ(700000000000, 3), TIME '15:32:00', DATE '2023-02-02')"

	if _, err := db.ExecContext(ctx, insertSQL); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Wait for the specific new INSERT job to finish
	if _, err := awaitNewJobFinished(db, beforeIDs, 2*time.Minute, 2*time.Second); err != nil {
		t.Fatalf("await insert job finished: %v", err)
	}

	// Now SELECT the data back and compare (order-insensitive)
	rows, err := db.QueryContext(ctx, "SELECT * FROM test_table")
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	data, err := readAllRows(rows)
	if err != nil {
		t.Fatalf("drain rows failed: %v", err)
	}

	fmt.Println("data:", data)

	// Normalize actual rows into comparable strings
	normalize := func(r []any) string {
		if len(r) != 7 {
			return fmt.Sprintf("len=%d row=%v", len(r), r)
		}

		// Helper extractors for nullable types
		getNullInt := func(v any) int64 {
			switch x := v.(type) {
			case sql.NullInt64:
				if x.Valid {
					return x.Int64
				}
			case int64:
				return x
			}
			return 0
		}
		getNullString := func(v any) string {
			switch x := v.(type) {
			case sql.NullString:
				if x.Valid {
					return x.String
				}
			case string:
				return x
			}
			return ""
		}
		getNullTime := func(v any) time.Time {
			switch x := v.(type) {
			case sql.NullTime:
				if x.Valid {
					return x.Time
				}
			case time.Time:
				return x
			case string:
				// best-effort parse common layouts
				if ts, err := time.Parse(time.RFC3339Nano, x); err == nil {
					return ts
				}
			}
			return time.Time{}
		}

		id := getNullInt(r[0])
		val := getNullInt(r[1])
		strv := getNullString(r[2])
		ts1 := getNullTime(r[3]).UTC().Format("2006-01-02 15:04:05")
		ts2 := getNullTime(r[4]).UTC().Format("2006-01-02 15:04:05.000")
		tOnly := getNullTime(r[5]).Format("15:04:05")
		dOnly := getNullTime(r[6]).Format("2006-01-02")

		return fmt.Sprintf("id=%d,val=%d,str=%s,t1=%s,t2=%s,time=%s,date=%s", id, val, strv, ts1, ts2, tOnly, dOnly)
	}

	gotSet := make(map[string]struct{})
	for _, r := range data {
		gotSet[normalize(r)] = struct{}{}
	}

	// Build expected set
	exp := []struct {
		id  int64
		val int64
		str string
		t1  string
		t2  time.Time
		tm  string
		dt  string
	}{
		{1, 11, "111", "2021-04-15 23:18:36", time.UnixMilli(400000000000).UTC(), "12:32:00", "2023-11-02"},
		{3, 33, "333", "2021-04-16 23:18:36", time.UnixMilli(500000000000).UTC(), "13:32:00", "2023-12-02"},
		{2, 22, "222", "2021-04-17 23:18:36", time.UnixMilli(600000000000).UTC(), "14:32:00", "2023-01-02"},
		{4, 44, "444", "2021-04-18 23:18:36", time.UnixMilli(700000000000).UTC(), "15:32:00", "2023-02-02"},
	}

	wantSet := make(map[string]struct{})
	for _, e := range exp {
		s := fmt.Sprintf(
			"id=%d,val=%d,str=%s,t1=%s,t2=%s,time=%s,date=%s",
			e.id, e.val, e.str, e.t1, e.t2.Format("2006-01-02 15:04:05.000"), e.tm, e.dt,
		)
		wantSet[s] = struct{}{}
	}

	if len(gotSet) != len(wantSet) {
		t.Fatalf("row count mismatch: got=%d want=%d\nGOT=%v\nWANT=%v", len(gotSet), len(wantSet), gotSet, wantSet)
	}

	for s := range wantSet {
		if _, ok := gotSet[s]; !ok {
			t.Fatalf("missing expected row: %s\nGotSet=%v", s, gotSet)
		}
	}
}
