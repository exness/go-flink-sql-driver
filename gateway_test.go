package flink

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetInfo_Success(t *testing.T) {
	// Mock server returns a valid InfoResponse
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/v3/info") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"productName":"Flink SQL Gateway","version":"1.20.0"}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	info, err := client.GetInfo(context.Background())
	if err != nil {
		t.Fatalf("GetInfo failed: %v", err)
	}
	if info.ProductName != "Flink SQL Gateway" {
		t.Errorf("unexpected ProductName: %s", info.ProductName)
	}
	if info.Version != "1.20.0" {
		t.Errorf("unexpected Version: %s", info.Version)
	}
}

func TestGetInfo_HTTPError(t *testing.T) {
	// Mock server returns a non-200 status
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetInfo(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetInfo_InvalidJSON(t *testing.T) {
	// Mock server returns invalid JSON
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not-json`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetInfo(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetAPIVersions_Success(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/v3/api_versions") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"versions":["v1","v2","v3"]}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	versions, err := client.GetAPIVersions(context.Background())
	if err != nil {
		t.Fatalf("GetAPIVersions failed: %v", err)
	}
	expected := []string{"v1", "v2", "v3"}
	if len(versions) != len(expected) {
		t.Errorf("unexpected versions length: got %d, want %d", len(versions), len(expected))
	}
	for i, v := range expected {
		if versions[i] != v {
			t.Errorf("unexpected version at %d: got %s, want %s", i, versions[i], v)
		}
	}
}

func TestGetAPIVersions_HTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetAPIVersions(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetAPIVersions_InvalidJSON(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not-json`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetAPIVersions(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestOpenSession_Success(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/v3/sessions") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"sessionHandle":"abc123"}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	handle, err := client.OpenSession(context.Background(), &OpenSessionRequest{
		Properties:  map[string]string{"foo": "bar"},
		SessionName: "test-session",
	})
	if err != nil {
		t.Fatalf("OpenSession failed: %v", err)
	}
	if handle != "abc123" {
		t.Errorf("unexpected session handle: %s", handle)
	}
}

func TestOpenSession_HTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.OpenSession(context.Background(), &OpenSessionRequest{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestOpenSession_InvalidJSON(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not-json`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.OpenSession(context.Background(), &OpenSessionRequest{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestCloseSession_Success(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/v3/sessions/abc123") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.CloseSession(context.Background(), "abc123")
	if err != nil {
		t.Fatalf("CloseSession failed: %v", err)
	}
}

func TestCloseSession_HTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.CloseSession(context.Background(), "abc123")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestCloseSession_NotFound(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.CloseSession(context.Background(), "doesnotexist")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetSessionConfig_Success(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/v3/sessions/abc123") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"properties":{"foo":"bar","baz":"qux"}}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	props, err := client.GetSessionConfig(context.Background(), "abc123")
	if err != nil {
		t.Fatalf("GetSessionConfig failed: %v", err)
	}
	if props["foo"] != "bar" || props["baz"] != "qux" {
		t.Errorf("unexpected properties: %+v", props)
	}
}

func TestGetSessionConfig_HTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetSessionConfig(context.Background(), "abc123")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetSessionConfig_InvalidJSON(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not-json`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetSessionConfig(context.Background(), "abc123")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestConfigureSession_Success(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/v3/sessions/abc123/configure-session") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.ConfigureSession(context.Background(), "abc123", &ConfigureSessionRequest{
		Statement: "USE CATALOG my_catalog",
	})
	if err != nil {
		t.Fatalf("ConfigureSession failed: %v", err)
	}
}

func TestConfigureSession_HTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.ConfigureSession(context.Background(), "abc123", &ConfigureSessionRequest{
		Statement: "USE CATALOG my_catalog",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestConfigureSession_InvalidJSON(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not-json`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// The client ignores the response body for ConfigureSession, so this should not error.
	err = client.ConfigureSession(context.Background(), "abc123", &ConfigureSessionRequest{
		Statement: "USE CATALOG my_catalog",
	})
	if err != nil {
		t.Fatalf("ConfigureSession should not fail on invalid JSON body, got: %v", err)
	}
}

func TestHeartbeat_Success(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/v3/sessions/abc123/heartbeat") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Heartbeat(context.Background(), "abc123")
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}
}

func TestHeartbeat_HTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Heartbeat(context.Background(), "abc123")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestCancelOperation_Success(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/v3/sessions/abc123/operations/op456/cancel") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.CancelOperation(context.Background(), "abc123", "op456")
	if err != nil {
		t.Fatalf("CancelOperation failed: %v", err)
	}
}

func TestCancelOperation_HTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.CancelOperation(context.Background(), "abc123", "op456")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestCloseOperation_Success(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/v3/sessions/abc123/operations/op456") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.CloseOperation(context.Background(), "abc123", "op456")
	if err != nil {
		t.Fatalf("CloseOperation failed: %v", err)
	}
}

func TestCloseOperation_HTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.CloseOperation(context.Background(), "abc123", "op456")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetOperationStatus_Success(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/v3/sessions/abc123/operations/op456/status") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"COMPLETED"}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	status, err := client.GetOperationStatus(context.Background(), "abc123", "op456")
	if err != nil {
		t.Fatalf("GetOperationStatus failed: %v", err)
	}
	if status != "COMPLETED" {
		t.Errorf("unexpected status: %s", status)
	}
}

func TestGetOperationStatus_HTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetOperationStatus(context.Background(), "abc123", "op456")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetOperationStatus_InvalidJSON(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not-json`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.GetOperationStatus(context.Background(), "abc123", "op456")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestExecuteStatement_Success(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/v3/sessions/abc123/statements") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"operationHandle":"op789"}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	handle, err := client.ExecuteStatement(context.Background(), "abc123", &ExecuteStatementRequest{
		Statement: "SELECT 1",
	})
	if err != nil {
		t.Fatalf("ExecuteStatement failed: %v", err)
	}
	if handle != "op789" {
		t.Errorf("unexpected operation handle: %s", handle)
	}
}

func TestExecuteStatement_HTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.ExecuteStatement(context.Background(), "abc123", &ExecuteStatementRequest{
		Statement: "SELECT 1",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestExecuteStatement_InvalidJSON(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not-json`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.ExecuteStatement(context.Background(), "abc123", &ExecuteStatementRequest{
		Statement: "SELECT 1",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TODO: add  more tests with different structures of response
func TestFetchResults_Success(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/v3/sessions/abc123/operations/op456/result/0") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"jobID": "job-1",
			"nextResultUri": "/v3/sessions/abc123/operations/op456/result/1",
			"isQueryResult": true,
			"resultKind": "SUCCESS",
			"resultType": "PAYLOAD",
			"results": {
				"columns": [
					{"name":"col1","logicalType":{"type":"INT","nullable":false},"comment":""},
					{"name":"col2","logicalType":{"type":"STRING","nullable":true},"comment":""}
				],
				"rowFormat": "JSON",
				"data": [
					{"kind":"INSERT","fields":[1,"foo"]},
					{"kind":"INSERT","fields":[2,"bar"]}
				]
			}
		}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	resp, err := client.FetchResults(context.Background(), "abc123", "op456", "0", "json")
	if err != nil {
		t.Fatalf("FetchResults failed: %v", err)
	}
	if resp.JobID != "job-1" {
		t.Errorf("unexpected JobID: %s", resp.JobID)
	}
	if resp.NextToken() != "1" {
		t.Errorf("unexpected NextToken: %s", resp.NextToken())
	}
	if len(resp.Results.Data) != 2 {
		t.Errorf("unexpected data length: %d", len(resp.Results.Data))
	}
	//if resp.Results.Data[0].Fields[0].(float64) != 1 || resp.Results.Data[0].Fields[1].(string) != "foo" {
	//	t.Errorf("unexpected row 0 fields: %v", resp.Results.Data[0].Fields)
	//}
}

func TestFetchResults_HTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.FetchResults(context.Background(), "abc123", "op456", "0", "json")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestFetchResults_InvalidJSON(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not-json`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client(), "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.FetchResults(context.Background(), "abc123", "op456", "0", "json")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
