// Package flink provides a small Go wrapper around Apache Flink's SQL
// Gateway REST API. The client supports creating and closing sessions,
// executing statements, polling results, managing operations and
// retrieving cluster information.  It hides the details of URL
// construction and JSON marshaling/unmarshaling so that users can
// interact with the gateway using idiomatic Go functions.

package flink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type ResultType string
type ResultKind string

const (
	ResultTypeNotReady ResultType = "NOT_READY"
	ResultTypeEOS      ResultType = "EOS"
	ResultTypePayload  ResultType = "PAYLOAD"

	ResultKindSuccess            ResultKind = "SUCCESS"
	ResultKindSuccessWithContent ResultKind = "SUCCESS_WITH_CONTENT"
)

// GatewayClient is the minimal interface implemented by *Client.
// It allows consumers to depend on an interface for easier testing/mocking.
type GatewayClient interface {
	GetInfo(ctx context.Context) (*InfoResponse, error)
	GetAPIVersions(ctx context.Context) ([]string, error)
	OpenSession(ctx context.Context, reqBody *OpenSessionRequest) (string, error)
	CloseSession(ctx context.Context, sessionHandle string) error
	GetSessionConfig(ctx context.Context, sessionHandle string) (map[string]string, error)
	CompleteStatement(ctx context.Context, sessionHandle string, reqBody *CompleteStatementRequest) ([]string, error)
	ConfigureSession(ctx context.Context, sessionHandle string, reqBody *ConfigureSessionRequest) error
	Heartbeat(ctx context.Context, sessionHandle string) error
	RefreshMaterializedTable(ctx context.Context, sessionHandle, identifier string, reqBody *RefreshMaterializedTableRequest) (string, error)
	CancelOperation(ctx context.Context, sessionHandle, operationHandle string) (string, error)
	CloseOperation(ctx context.Context, sessionHandle, operationHandle string) (string, error)
	GetOperationStatus(ctx context.Context, sessionHandle, operationHandle string) (string, error)
	ExecuteStatement(ctx context.Context, sessionHandle string, reqBody *ExecuteStatementRequest) (string, error)
	FetchResults(ctx context.Context, sessionHandle, operationHandle, token string, rowFormat string) (*FetchResultsResponseBody, error)
}

// Client represents a SQL Gateway REST API client.  It holds the
// base URL for the Gateway and an underlying http.Client that is used
// to execute all requests.  Users may set custom timeouts or HTTP
// transport settings on the underlying client if necessary.
type Client struct {
	// BaseURL should be the root of the SQL Gateway REST API, e.g.
	// "http://localhost:8083".  Do not include a trailing slash; the
	// Client automatically appends endpoint paths.
	BaseURL *url.URL
	// HTTPClient is the http.Client used for sending requests.  If
	// nil, http.DefaultClient is used.  You can set custom timeouts
	// or transports on this client for better control.
	HTTPClient *http.Client
	// APIVersion controls which API version is used for requests.
	// Valid values are "v1", "v2" or "v3".  If empty, "v3" is used.
	APIVersion string
}

var _ GatewayClient = (*Client)(nil)

// NewClient constructs a new Client for the given baseURL.  The
// baseURL must be a valid URL string pointing at the SQL Gateway
// server (e.g. "http://localhost:8083").  Optionally, a custom
// http.Client can be supplied via the httpClient parameter; if nil,
// http.DefaultClient is used.  The APIVersion argument controls the
// version prefix ("v1", "v2", "v3").  If left empty, "v3" is used
// because v3 includes all features of prior versions.
func NewClient(baseURL string, httpClient *http.Client, apiVersion string) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}
	if apiVersion == "" {
		apiVersion = "v3"
	}
	return &Client{
		BaseURL:    u,
		HTTPClient: httpClient,
		APIVersion: apiVersion,
	}, nil
}

// buildEndpoint constructs an absolute URL for the given path segments.
// It joins the base URL, version prefix and any additional path
// components, ensuring that there is exactly one slash between each
// component.  Query parameters may be attached later.
func (c *Client) buildEndpoint(segments ...string) (string, error) {
	// Start with base URL and API version.
	u := *c.BaseURL
	version := c.APIVersion
	if version == "" {
		version = "v3"
	}
	all := append([]string{version}, segments...)
	u.Path = path.Join(u.Path, path.Join(all...))
	return u.String(), nil
}

// InfoResponse represents the response from GET /info.  It contains
// the product name and version of the connected cluster.
type InfoResponse struct {
	ProductName string `json:"productName"`
	Version     string `json:"version"`
}

// GetInfo fetches cluster metadata from the gateway using GET /info.
// It returns an InfoResponse or an error.  Context may be used to
// cancel the request.
func (c *Client) GetInfo(ctx context.Context) (*InfoResponse, error) {
	endpoint, err := c.buildEndpoint("info")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("info request failed: %s", resp.Status)
	}
	var info InfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}
	return &info, nil
}

// APIResponse holds the list of API versions supported by the server.
type APIResponse struct {
	Versions []string `json:"versions"`
}

// GetAPIVersions returns the list of supported REST API versions
// available on the server.  Clients can inspect this to decide which
// version to use.  The default version is usually the highest one.
func (c *Client) GetAPIVersions(ctx context.Context) ([]string, error) {
	endpoint, err := c.buildEndpoint("api_versions")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("api_versions request failed: %s", resp.Status)
	}
	var body APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body.Versions, nil
}

// OpenSessionRequest defines the payload for creating a new session.
// Properties may include arbitrary configuration entries; sessionName
// can be used to label the session on the server.
type OpenSessionRequest struct {
	Properties  map[string]string `json:"properties,omitempty"`
	SessionName string            `json:"sessionName,omitempty"`
}

// OpenSessionResponse contains the session handle returned from
// creating a session.
type OpenSessionResponse struct {
	SessionHandle string `json:"sessionHandle"`
}

// OpenSession opens a new SQL Gateway session with the provided
// properties and name.  It returns the session handle on success.  If
// properties is nil, the default gateway configuration is used.
func (c *Client) OpenSession(ctx context.Context, reqBody *OpenSessionRequest) (string, error) {
	endpoint, err := c.buildEndpoint("sessions")
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	if reqBody != nil {
		if err := json.NewEncoder(buf).Encode(reqBody); err != nil {
			return "", err
		}
	} else {
		buf.WriteString("{}")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("open session failed: %s", resp.Status)
	}
	var out OpenSessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.SessionHandle, nil
}

// CloseSession closes the given session handle.  It returns nil on
// success or an error if the server rejects the request.
func (c *Client) CloseSession(ctx context.Context, sessionHandle string) error {
	endpoint, err := c.buildEndpoint("sessions", sessionHandle)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("close session failed: %s", resp.Status)
	}
	return nil
}

// GetSessionConfig fetches the current configuration for a session.
// The returned map contains session-specific property overrides.  If
// the session does not exist, an error is returned.
func (c *Client) GetSessionConfig(ctx context.Context, sessionHandle string) (map[string]string, error) {
	endpoint, err := c.buildEndpoint("sessions", sessionHandle)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get session config failed: %s", resp.Status)
	}
	var result struct {
		Properties map[string]string `json:"properties"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Properties, nil
}

// CompleteStatementRequest defines the payload for the complete-statement
// endpoint, which returns auto-completion candidates.
type CompleteStatementRequest struct {
	Position  int    `json:"position"`
	Statement string `json:"statement"`
}

// CompleteStatementResponse holds the list of completion candidates.
type CompleteStatementResponse struct {
	Candidates []string `json:"candidates"`
}

// CompleteStatement returns SQL completion hints for the given
// statement at the specified cursor position.  It returns a slice of
// candidate completions or an error.
func (c *Client) CompleteStatement(ctx context.Context, sessionHandle string, reqBody *CompleteStatementRequest) ([]string, error) {
	endpoint, err := c.buildEndpoint("sessions", sessionHandle, "complete-statement")
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	if reqBody != nil {
		if err := json.NewEncoder(buf).Encode(reqBody); err != nil {
			return nil, err
		}
	} else {
		buf.WriteString("{}")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("complete statement failed: %s", resp.Status)
	}
	var out CompleteStatementResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Candidates, nil
}

// ConfigureSessionRequest defines a request to adjust session settings
// or perform catalog/database modifications.  It accepts an SQL
// statement (CREATE, DROP, ALTER, USE, ADD JAR, etc.) and optional
// timeout.  ExecutionTimeout is expressed in seconds; zero means
// server default.
type ConfigureSessionRequest struct {
	// ExecutionTimeout specifies a timeout in seconds for configuring
	// the session.  Use int64 to match the OpenAPI specification.
	ExecutionTimeout int64  `json:"executionTimeout,omitempty"`
	Statement        string `json:"statement"`
}

// ConfigureSession executes the given DDL or session configuration
// statement within the context of the provided session.  It returns
// nil on success.  Note that many configuration statements do not
// produce an OperationHandle; they take effect immediately.
func (c *Client) ConfigureSession(ctx context.Context, sessionHandle string, reqBody *ConfigureSessionRequest) error {
	endpoint, err := c.buildEndpoint("sessions", sessionHandle, "configure-session")
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	if reqBody != nil {
		if err := json.NewEncoder(buf).Encode(reqBody); err != nil {
			return err
		}
	} else {
		buf.WriteString("{}")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("configure session failed: %s", resp.Status)
	}
	return nil
}

// Heartbeat triggers a heartbeat for the specified session.  This
// method simply sends an empty POST body to keep the session alive.
func (c *Client) Heartbeat(ctx context.Context, sessionHandle string) error {
	endpoint, err := c.buildEndpoint("sessions", sessionHandle, "heartbeat")
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString("{}"))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("heartbeat failed: %s", resp.Status)
	}
	return nil
}

// RefreshMaterializedTableRequest defines parameters to refresh a
// materialized table.  DynamicOptions and ExecutionConfig allow
// adjusting job parameters; StaticPartitions define optional
// partitioning.  ScheduleTime can be used when scheduling periodic
// refreshes.  IsPeriodic and Periodic are synonyms; set one of them
// true to indicate that refresh should be scheduled repeatedly.
type RefreshMaterializedTableRequest struct {
	DynamicOptions   map[string]string `json:"dynamicOptions,omitempty"`
	ExecutionConfig  map[string]string `json:"executionConfig,omitempty"`
	IsPeriodic       bool              `json:"isPeriodic,omitempty"`
	Periodic         bool              `json:"periodic,omitempty"`
	ScheduleTime     string            `json:"scheduleTime,omitempty"`
	StaticPartitions map[string]string `json:"staticPartitions,omitempty"`
}

// RefreshMaterializedTableResponse contains the operation handle
// associated with the refresh job.  Clients should poll the
// operation’s status and results as with any other SQL operation.
type RefreshMaterializedTableResponse struct {
	OperationHandle string `json:"operationHandle"`
}

// RefreshMaterializedTable triggers a refresh of the named materialized
// table and returns the resulting operation handle.  The identifier
// must be the fully-qualified table identifier as a string (e.g.
// "my_catalog.my_db.my_table").  Optional request fields may
// influence execution.  See Flink documentation for supported
// configuration keys.
func (c *Client) RefreshMaterializedTable(ctx context.Context, sessionHandle, identifier string, reqBody *RefreshMaterializedTableRequest) (string, error) {
	endpoint, err := c.buildEndpoint("sessions", sessionHandle, "materialized-tables", identifier, "refresh")
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	if reqBody != nil {
		if err := json.NewEncoder(buf).Encode(reqBody); err != nil {
			return "", err
		}
	} else {
		buf.WriteString("{}")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("refresh materialized table failed: %s", resp.Status)
	}
	var out RefreshMaterializedTableResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.OperationHandle, nil
}

// CancelOperation cancels a running operation.  The server will try
// to terminate the job associated with the operation handle.
func (c *Client) CancelOperation(ctx context.Context, sessionHandle, operationHandle string) (string, error) {
	endpoint, err := c.buildEndpoint("sessions", sessionHandle, "operations", operationHandle, "cancel")
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString("{}"))
	if err != nil {
		return "nil", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return "nil", err
	}
	defer resp.Body.Close()
	var out OperationStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "nil", err
	}
	if resp.StatusCode != http.StatusOK {
		return "nil", fmt.Errorf("cancel operation failed: %s", resp.Status)
	}
	return out.Status, nil
}

// CloseOperation closes a finished operation.  This should be called
// after fetching all results to free server-side resources.
func (c *Client) CloseOperation(ctx context.Context, sessionHandle, operationHandle string) (string, error) {
	endpoint, err := c.buildEndpoint("sessions", sessionHandle, "operations", operationHandle)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out OperationStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("close operation failed: %s", resp.Status)
	}
	return out.Status, nil
}

// OperationStatusResponse captures the status of an operation, which
// may be "RUNNING", "COMPLETED", "CANCELLED", or another status
// defined by the server.
type OperationStatusResponse struct {
	Status string `json:"status"`
}

// FetchResultsResponseBody mirrors the OpenAPI schema for the
// response returned by fetching results from an operation.  It
// contains the job ID, the URI for fetching the next batch, a flag
// indicating whether the query returns results, the result kind and
// type, and a nested results structure with column metadata and
// payload.  See the Flink SQL Gateway REST API specification for
// details.
type FetchResultsResponseBody struct {
	JobID         string     `json:"jobID"`
	NextResultUri string     `json:"nextResultUri"`
	IsQueryResult bool       `json:"isQueryResult"`
	ResultKind    ResultKind `json:"resultKind"`
	ResultType    ResultType `json:"resultType"`
	Results       struct {
		Columns   []ColumnInfo `json:"columns"`
		RowFormat string       `json:"rowFormat"`
		Data      []RowData    `json:"data"`
	} `json:"results"`
}

type ColumnInfo struct {
	Name        string      `json:"name"`
	LogicalType LogicalType `json:"logicalType"`
	Comment     string      `json:"comment"`
}

type LogicalType struct {
	Type       string  `json:"type"`
	Nullable   bool    `json:"nullable"`
	Length     *int64  `json:"length"`
	Precision  *int    `json:"precision"`
	Scale      *int    `json:"scale"`
	Resolution *string `json:"resolution"`
}

func (lt *LogicalType) UnmarshalJSON(data []byte) error {
	type Alias LogicalType
	aux := Alias{
		Nullable: true,
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*lt = LogicalType(aux)
	return nil
}

type RowData struct {
	Kind   string            `json:"kind"`
	Fields []json.RawMessage `json:"fields"`
}

// GetOperationStatus retrieves the current status of an operation.
// Clients should poll this endpoint until they see a terminal state
// (COMPLETED, CANCELLED, etc.).
func (c *Client) GetOperationStatus(ctx context.Context, sessionHandle, operationHandle string) (string, error) {
	endpoint, err := c.buildEndpoint("sessions", sessionHandle, "operations", operationHandle, "status")
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("get operation status failed: %s", resp.Status)
	}
	var out OperationStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.Status, nil
}

// ExecuteStatementRequest represents the payload for executing a SQL
// statement.  ExecutionConfig allows passing dynamic configuration
// options; ExecutionTimeout expresses a timeout in seconds for
// statement execution.  Set ExecutionTimeout to zero to use the
// gateway’s default timeout.  Statement must contain the SQL text to
// execute.
type ExecuteStatementRequest struct {
	ExecutionConfig map[string]string `json:"executionConfig,omitempty"`
	// ExecutionTimeout specifies a timeout in seconds for the statement
	// execution.  Use int64 to match the OpenAPI specification.
	ExecutionTimeout int64  `json:"executionTimeout,omitempty"`
	Statement        string `json:"statement"`
}

// ExecuteStatementResponse contains the operation handle returned
// after submitting a statement.  Clients should use the handle to
// poll for results and status.
type ExecuteStatementResponse struct {
	OperationHandle string `json:"operationHandle"`
}

// ExecuteStatement submits a SQL statement for execution in the
// specified session.  It returns the operation handle on success.
// Statement execution is asynchronous; use GetOperationStatus and
// FetchResults to monitor and retrieve results.
func (c *Client) ExecuteStatement(ctx context.Context, sessionHandle string, reqBody *ExecuteStatementRequest) (string, error) {
	endpoint, err := c.buildEndpoint("sessions", sessionHandle, "statements")
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	if reqBody != nil {
		if err := json.NewEncoder(buf).Encode(reqBody); err != nil {
			return "", err
		}
	} else {
		buf.WriteString("{}")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("execute statement failed: %s", resp.Status)
	}
	var out ExecuteStatementResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.OperationHandle, nil
}

// FetchResults retrieves a batch of results for the given operation
// handle and token.  The token identifies which batch to fetch: 0
// indicates the first batch, while the `nextResultUri` returned from
// prior calls contains the token for subsequent batches.  The
// rowFormat parameter controls the serialization format of the
// response.  Valid values include "JSON" (default) and "PLAIN_TEXT".
// This function returns an arbitrary JSON structure (decoded into
// interface{}) representing the result payload.  The caller can cast
// or map the data into concrete Go types as needed.
func (c *Client) FetchResults(ctx context.Context, sessionHandle, operationHandle, token string, rowFormat string) (*FetchResultsResponseBody, error) {
	endpoint, err := c.buildEndpoint("sessions", sessionHandle, "operations", operationHandle, "result", token)
	if err != nil {
		return nil, err
	}
	// Append rowFormat as a query parameter if provided.
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	if rowFormat != "" {
		q.Set("rowFormat", rowFormat)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		var errPayload struct {
			Errors []string `json:"errors"`
		}
		if json.Unmarshal(bodyBytes, &errPayload) == nil && len(errPayload.Errors) > 0 {
			return nil, fmt.Errorf("fetch results failed: %s", strings.Join(errPayload.Errors, "; "))
		}
		return nil, fmt.Errorf("fetch results failed: %s", resp.Status)
	}

	var result FetchResultsResponseBody
	if err := json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NextToken extracts the token from the NextResultUri, or returns "" if not present.
func (r *FetchResultsResponseBody) NextToken() string {
	if r.NextResultUri == "" {
		return ""
	}
	u, err := url.Parse(r.NextResultUri)
	if err != nil {
		return ""
	}
	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(segments) == 0 {
		return ""
	}
	return segments[len(segments)-1]
}

// do executes an HTTP request using the client’s HTTPClient.  It
// ensures that a non-nil http.Client is available and applies a
// default timeout if none is provided.  In particular, if the
// caller’s HTTPClient is nil, http.DefaultClient is used.  This
// helper centralizes request execution and may be extended to add
// authentication headers or other custom behaviours in the future.
func (c *Client) do(req *http.Request) (*http.Response, error) {
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return client.Do(req)
}
