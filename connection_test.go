package flink

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	jobManager    testcontainers.Container
	taskManager   testcontainers.Container
	sqlGateway    testcontainers.Container
	endpoint      string
	ctx           = context.Background()
	hostSharedDir string
)

var (
	typeOfBool   = reflect.TypeOf(true)
	typeOfInt64  = reflect.TypeOf(int64(0))
	typeOfF64    = reflect.TypeOf(float64(0))
	typeOfBytes  = reflect.TypeOf([]byte(nil))
	typeOfString = reflect.TypeOf("")
	typeOfTime   = reflect.TypeOf(time.Time{})

	typeOfNullBool   = reflect.TypeOf(sql.NullBool{})
	typeOfNullInt    = reflect.TypeOf(sql.NullInt64{})
	typeOfNullFloat  = reflect.TypeOf(sql.NullFloat64{})
	typeOfNullString = reflect.TypeOf(sql.NullString{})
	typeOfNullTime   = reflect.TypeOf(sql.NullTime{})
)

func TestMain(m *testing.M) {
	net, err := network.New(ctx)
	if err != nil {
		fmt.Printf("failed to create network: %v\n", err)
		os.Exit(1)
	}
	defer net.Remove(ctx)

	// Create a host temp dir and mount it into all containers so TaskManagers can write CSV files
	hostSharedDir, err = os.MkdirTemp("", "flink-e2e-*")
	if err != nil {
		fmt.Printf("failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	if err := os.Chmod(hostSharedDir, 0777); err != nil {
		fmt.Printf("failed to chmod temp dir: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Using shared host dir: %s\n", hostSharedDir)

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
				HostConfigModifier: func(hc *container.HostConfig) {
					hc.Binds = append(hc.Binds, fmt.Sprintf("%s:%s", hostSharedDir, "/shared"))
				},
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
				HostConfigModifier: func(hc *container.HostConfig) {
					hc.Binds = append(hc.Binds, fmt.Sprintf("%s:%s", hostSharedDir, "/shared"))
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
				HostConfigModifier: func(hc *container.HostConfig) {
					hc.Binds = append(hc.Binds, fmt.Sprintf("%s:%s", hostSharedDir, "/shared"))
				},
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

	_ = os.RemoveAll(hostSharedDir)
	os.Exit(code)
}

// openStreamingDB creates a DB with STREAMING runtime mode and returns a cleanup func.
func openStreamingDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	connector, _ := NewConnector(
		WithGatewayURL(endpoint),
		WithProperties(map[string]string{
			"execution.runtime-mode": "STREAMING",
		}),
	)
	db := sql.OpenDB(connector)
	cleanup := func() {
		if cerr := db.Close(); cerr != nil {
			t.Logf("close db: %v", cerr)
		}
	}
	return db, cleanup
}

func execDDL(t *testing.T, db *sql.DB, ddl string) {
	t.Helper()
	res, err := db.ExecContext(ctx, ddl)
	if err != nil {
		t.Fatalf("ExecContext(DDL) failed: %v", err)
	}
	if rows, _ := res.RowsAffected(); rows != 0 {
		t.Fatalf("ExecContext(DDL) expected 0 rows affected, got %d", rows)
	}
}

// assertTypesFromQuery runs a query with timeout and validates column Go types.
func assertTypesFromQuery(t *testing.T, db *sql.DB, query string, expected map[string][]reflect.Type, timeout time.Duration) {
	t.Helper()
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	rows, err := db.QueryContext(cctx, query)
	if err != nil {
		t.Fatalf("QueryContext(%s) failed: %v", query, err)
	}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns() failed: %v", err)
	}
	for rows.Next() {
		dest := make([]any, len(cols))
		scanArgs := make([]any, len(cols))
		for i := range dest {
			scanArgs[i] = &dest[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		for i, name := range cols {
			lname := strings.ToLower(strings.TrimSpace(name))
			allowed, ok := expected[lname]
			if !ok {
				continue
			}
			got := reflect.TypeOf(dest[i])
			okType := false
			for _, a := range allowed {
				if got == a {
					okType = true
					break
				}
			}
			if !okType {
				t.Fatalf("column %q: got Go type %v (value=%v), want one of %v", name, got, dest[i], allowed)
			}
		}
	}
	if err := rows.Err(); err != nil {
		if !errors.Is(cctx.Err(), context.DeadlineExceeded) { // deadline cancel is expected
			t.Fatalf("rows.Err(): %v", err)
		}
	}
}

// ---- Filesystem test helpers ----
func getNullInt(v any) int64 {
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
func getNullString(v any) string {
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
func getNullTime(v any) time.Time {
	switch x := v.(type) {
	case sql.NullTime:
		if x.Valid {
			return x.Time
		}
	case time.Time:
		return x
	case string:
		if ts, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return ts
		}
	}
	return time.Time{}
}

func normalizeFSRow(r []any) string {
	if len(r) != 7 {
		return fmt.Sprintf("len=%d row=%v", len(r), r)
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

func toSet(rows [][]any, normalize func([]any) string) map[string]struct{} {
	set := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		set[normalize(r)] = struct{}{}
	}
	return set
}

func assertSetEqual(t *testing.T, got, want map[string]struct{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count mismatch: got=%d want=%d\nGOT=%v\nWANT=%v", len(got), len(want), got, want)
	}
	for s := range want {
		if _, ok := got[s]; !ok {
			t.Fatalf("missing expected row: %s\nGotSet=%v", s, got)
		}
	}
}

// ---- Custom Scanner types for complex columns ----

type RowInfo struct {
	Score int    `json:"score"`
	Label string `json:"label"`
}

// Scan implements sql.Scanner to accept either JSON bytes or JSON string representing the ROW value.
func (r *RowInfo) Scan(src any) error {
	var b []byte
	switch v := src.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("RowInfo.Scan: unsupported src type %T", src)
	}
	// Some connectors may wrap inner quotes; trim whitespace first.
	b = []byte(strings.TrimSpace(string(b)))
	return json.Unmarshal(b, r)
}

// StringMap scans a JSON object into a map[string]string with stable comparison helpers.
// It implements sql.Scanner so database/sql can populate it directly.
type StringMap map[string]string

func (m *StringMap) Scan(src any) error {
	var b []byte
	switch v := src.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("StringMap.Scan: unsupported src type %T", src)
	}
	b = []byte(strings.TrimSpace(string(b)))
	var tmp map[string]string
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	*m = tmp
	return nil
}

func (m StringMap) Canonical() string {
	if m == nil {
		return "{}"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", k, m[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (r RowInfo) Canonical() string {
	return fmt.Sprintf("score=%d,label=%s", r.Score, r.Label)
}

func TestFlinkConn_EndToEnd_SelectAllTypesNotNull(t *testing.T) {
	// 1) Open DB
	db, cleanup := openStreamingDB(t)
	defer cleanup()

	// 2) Create source table
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
		) WITH (
		'connector' = 'datagen',
		'rows-per-second' = '5'
	)`
	execDDL(t, db, createTable)

	// 3) Expected Go types per column (NOT NULL schema)
	expected := map[string][]reflect.Type{
		"bool":      {typeOfBool},
		"num":       {typeOfInt64}, // SMALLINT
		"dbl":       {typeOfF64},   // DOUBLE
		"bin":       {typeOfBytes}, // BINARY
		"vbin":      {typeOfBytes}, // VARBINARY
		"chr":       {typeOfString},
		"vchr":      {typeOfString},
		"tmstp":     {typeOfTime}, // TIMESTAMP_LTZ
		"dte":       {typeOfTime}, // DATE
		"dcml":      {typeOfString},
		"tint":      {typeOfInt64}, // TINYINT
		"sint":      {typeOfInt64}, // SMALLINT
		"nint":      {typeOfInt64}, // INTEGER
		"twotz":     {typeOfTime},  // TIME WITHOUT TZ
		"bgint":     {typeOfInt64}, // BIGINT
		"flt":       {typeOfF64},   // FLOAT
		"tmstpwotz": {typeOfTime},
		"row1":      {typeOfBytes},
		"map1":      {typeOfBytes},
	}

	// 4) Validate
	assertTypesFromQuery(t, db, "SELECT * from test_table", expected, 3*time.Second)
}

func TestFlinkConn_EndToEnd_SelectAllTypesNullable(t *testing.T) {
	// 1) Open DB
	db, cleanup := openStreamingDB(t)
	defer cleanup()

	// 2) Create source table
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
	execDDL(t, db, createTable)

	// 3) Expected Go types per column (ALL NULLABLE schema)
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
		"row1":      {typeOfBytes},
		"map1":      {typeOfBytes},
	}

	// 4) Validate
	assertTypesFromQuery(t, db, "SELECT * from test_table_nullable", expected, 3*time.Second)
}

// readAllRows drains *sql.Rows and returns a copy of all values.
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

func awaitNewJobFinished(db *sql.DB, timeout time.Duration, interval time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		rows, err := db.QueryContext(ctx, "SHOW JOBS")
		if err != nil {
			time.Sleep(interval)
			continue
		}

		all, err := readAllRows(rows)
		if err != nil {
			time.Sleep(interval)
			continue
		}

		idIdx := 0
		statusIdx := 2

		for _, r := range all {
			jobIdNullable := r[idIdx].(sql.NullString)
			jobStatusNullable := r[statusIdx].(sql.NullString)
			var jobId string
			if jobIdNullable.Valid {
				jobId = jobIdNullable.String
			}
			var jobStatus string
			if jobStatusNullable.Valid {
				jobStatus = strings.ToUpper(jobStatusNullable.String)
			}
			if jobStatus == "FINISHED" {
				return jobId, nil
			}
		}
		time.Sleep(interval)
	}
	return "", fmt.Errorf("no new FINISHED job observed within %s", timeout)
}

func TestFlinkConn_EndToEnd_FilesystemInsertSelect(t *testing.T) {
	// 1) Open DB
	db, cleanup := openStreamingDB(t)
	defer cleanup()

	// 2) Create filesystem table writing under shared mount
	containerDir := fmt.Sprintf("/shared/test_table_%d", time.Now().UnixNano())
	dir := "file://" + containerDir
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
	execDDL(t, db, ddl)

	// 3) Insert rows
	insertSQL := "INSERT INTO test_table VALUES " +
		"(1, 11, '111', TIMESTAMP '2021-04-15 23:18:36', TO_TIMESTAMP_LTZ(400000000000, 3), TIME '12:32:00', DATE '2023-11-02'), " +
		"(3, 33, '333', TIMESTAMP '2021-04-16 23:18:36', TO_TIMESTAMP_LTZ(500000000000, 3), TIME '13:32:00', DATE '2023-12-02'), " +
		"(2, 22, '222', TIMESTAMP '2021-04-17 23:18:36', TO_TIMESTAMP_LTZ(600000000000, 3), TIME '14:32:00', DATE '2023-01-02'), " +
		"(4, 44, '444', TIMESTAMP '2021-04-18 23:18:36', TO_TIMESTAMP_LTZ(700000000000, 3), TIME '15:32:00', DATE '2023-02-02')"
	if _, err := db.ExecContext(ctx, insertSQL); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if _, err := awaitNewJobFinished(db, 2*time.Minute, 2*time.Second); err != nil {
		t.Fatalf("await insert job finished: %v", err)
	}

	// 4) Read back and compare as unordered set
	rows, err := db.QueryContext(ctx, "SELECT * FROM test_table")
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	data, err := readAllRows(rows)
	if err != nil {
		t.Fatalf("drain rows failed: %v", err)
	}

	gotSet := toSet(data, normalizeFSRow)

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

	assertSetEqual(t, gotSet, wantSet)
}

func TestFlinkConn_Filesystem_RowAndMap_ScanIntoStructAndMap(t *testing.T) {
	// 1) Open DB
	db, cleanup := openStreamingDB(t)
	defer cleanup()

	// 2) Create filesystem table with ROW and MAP columns
	containerDir := fmt.Sprintf("/shared/csv_complex_%d", time.Now().UnixNano())
	dir := "file://" + containerDir
	ddl := fmt.Sprintf(
		"CREATE TABLE csv_complex(\n"+
			"  id BIGINT NOT NULL,\n"+
			"  name STRING NOT NULL,\n"+
			"  info ROW<score INT, label STRING> NOT NULL,\n"+
			"  `attrs` MAP<STRING, STRING> NOT NULL"+
			") WITH (\n"+
			"  'connector'='filesystem',\n"+
			"  'format'='csv',\n"+
			"  'path'='%s'\n"+
			")",
		dir,
	)
	execDDL(t, db, ddl)

	// 3) Insert rows using ROW(...) and STR_TO_MAP constructors
	insertSQL := "INSERT INTO csv_complex VALUES " +
		"(1, 'alpha', ROW(10, 'A'), STR_TO_MAP('k1=1,k2=2'))," +
		"(2, 'beta',  ROW(20, 'B'), STR_TO_MAP('x=9'))"
	if _, err := db.ExecContext(ctx, insertSQL); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if _, err := awaitNewJobFinished(db, 2*time.Minute, 2*time.Second); err != nil {
		t.Fatalf("await insert job finished: %v", err)
	}

	// 4) Select and scan into struct and custom map
	rows, err := db.QueryContext(ctx, "SELECT id, name, info, attrs FROM csv_complex")
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	defer rows.Close()

	type rec struct {
		id    int64
		name  string
		info  RowInfo
		attrs StringMap
	}
	var got []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.id, &r.name, &r.info, &r.attrs); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}

	// Build canonical sets for comparison (order-insensitive)
	gotSet := make(map[string]struct{}, len(got))
	for _, r := range got {
		key := fmt.Sprintf("id=%d|name=%s|info={%s}|attrs=%s", r.id, r.name, r.info.Canonical(), r.attrs.Canonical())
		gotSet[key] = struct{}{}
	}

	want := []rec{
		{id: 1, name: "alpha", info: RowInfo{Score: 10, Label: "A"}, attrs: StringMap{"k1": "1", "k2": "2"}},
		{id: 2, name: "beta", info: RowInfo{Score: 20, Label: "B"}, attrs: StringMap{"x": "9"}},
	}
	wantSet := make(map[string]struct{}, len(want))
	for _, r := range want {
		key := fmt.Sprintf("id=%d|name=%s|info={%s}|attrs=%s", r.id, r.name, r.info.Canonical(), r.attrs.Canonical())
		wantSet[key] = struct{}{}
	}

	assertSetEqual(t, gotSet, wantSet)
}
