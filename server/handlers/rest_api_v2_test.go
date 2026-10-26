package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/go-chi/chi/v5"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

// setupRestAPIv2Handler creates a test handler with dependencies.
func setupRestAPIv2Handler(t *testing.T) (*RestAPIv2Handler, *chi.Mux) {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close DB: %v", err)
		}
	})

	connMgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(connMgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	executor := query.NewExecutor(connMgr, repo)
	stmtMgr := query.NewStatementManager(1 * time.Hour)

	handler := NewRestAPIv2Handler(executor, stmtMgr, repo)

	// Setup router
	r := chi.NewRouter()
	r.Route("/api/v2", func(r chi.Router) {
		r.Post("/statements", handler.SubmitStatement)
		r.Get("/statements/{handle}", handler.GetStatement)
		r.Post("/statements/{handle}/cancel", handler.CancelStatement)
	})

	return handler, r
}

func TestRestAPIv2Handler_SubmitStatement_Sync(t *testing.T) {
	_, router := setupRestAPIv2Handler(t)

	reqBody := types.SubmitStatementRequest{
		Statement: "SELECT 1 AS num",
		Database:  "TEST_DB",
		Schema:    "PUBLIC",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/statements", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d. Body: %s", http.StatusOK, rr.Code, rr.Body.String())
	}

	var resp types.StatementResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.StatementHandle == "" {
		t.Error("Expected statement handle to be set")
	}

	if resp.Code != types.ResponseCodeSuccess {
		t.Errorf("Expected code %s, got %s", types.ResponseCodeSuccess, resp.Code)
	}

	if resp.SQLState != types.SQLState00000 {
		t.Errorf("Expected SQLState %s, got %s", types.SQLState00000, resp.SQLState)
	}

	if resp.ResultSetMetaData == nil {
		t.Error("Expected ResultSetMetaData to be set")
	}

	if len(resp.Rows()) == 0 {
		t.Error("Expected data to be returned")
	}
}

func TestRestAPIv2Handler_SubmitStatement_WithBindings(t *testing.T) {
	_, router := setupRestAPIv2Handler(t)

	reqBody := types.SubmitStatementRequest{
		Statement: "SELECT :1 AS num, :2 AS name",
		Database:  "TEST_DB",
		Schema:    "PUBLIC",
		Bindings: map[string]*types.BindingValue{
			"1": {Type: "FIXED", Value: "42"},
			"2": {Type: "TEXT", Value: "hello"},
		},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/statements", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d. Body: %s", http.StatusOK, rr.Code, rr.Body.String())
	}

	var resp types.StatementResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.Code != types.ResponseCodeSuccess {
		t.Errorf("Expected code %s, got %s. Message: %s", types.ResponseCodeSuccess, resp.Code, resp.Message)
	}

	if len(resp.Rows()) == 0 {
		t.Error("Expected data to be returned")
		return
	}

	// Check that the values are correct
	if len(resp.Rows()[0]) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(resp.Rows()[0]))
	}
}

func TestRestAPIv2Handler_SubmitStatement_EmptyStatement(t *testing.T) {
	_, router := setupRestAPIv2Handler(t)

	reqBody := types.SubmitStatementRequest{
		Statement: "",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/statements", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	// Should return error
	var resp types.StatementResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.SQLState == types.SQLState00000 {
		t.Error("Expected error SQLState for empty statement")
	}
}

func TestRestAPIv2Handler_SubmitStatement_InvalidSQL(t *testing.T) {
	_, router := setupRestAPIv2Handler(t)

	reqBody := types.SubmitStatementRequest{
		Statement: "INVALID SQL STATEMENT",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/statements", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	var resp types.StatementResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Invalid SQL should fail
	if resp.SQLState == types.SQLState00000 {
		t.Error("Expected error SQLState for invalid SQL")
	}
}

func TestRestAPIv2Handler_GetStatement(t *testing.T) {
	handler, router := setupRestAPIv2Handler(t)

	// First, submit a statement
	reqBody := types.SubmitStatementRequest{
		Statement: "SELECT 1 AS num",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/statements", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	var submitResp types.StatementResponse
	json.Unmarshal(rr.Body.Bytes(), &submitResp)

	// Now get the statement
	req = httptest.NewRequest(http.MethodGet, "/api/v2/statements/"+submitResp.StatementHandle, nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rr = httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d. Body: %s", http.StatusOK, rr.Code, rr.Body.String())
	}

	var getResp types.StatementResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &getResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if getResp.StatementHandle != submitResp.StatementHandle {
		t.Errorf("Expected handle %s, got %s", submitResp.StatementHandle, getResp.StatementHandle)
	}

	_ = handler // Use handler to avoid unused warning
}

func TestRestAPIv2Handler_GetStatement_NotFound(t *testing.T) {
	_, router := setupRestAPIv2Handler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/statements/non-existing-handle", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, rr.Code)
	}
}

func TestRestAPIv2Handler_CancelStatement(t *testing.T) {
	handler, router := setupRestAPIv2Handler(t)

	// Create a statement directly in the manager (simulating a long-running query)
	stmt := handler.stmtMgr.CreateStatement("SELECT pg_sleep(100)", "TEST_DB", "PUBLIC", "")
	handler.stmtMgr.UpdateStatus(stmt.Handle, query.StatementStatusRunning)

	// Set a mock cancel function
	cancelled := false
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	handler.stmtMgr.SetCancelFunc(stmt.Handle, func() {
		cancelled = true
		cancelFunc()
	})
	_ = cancelCtx // Use cancelCtx to avoid unused warning

	// Cancel the statement
	req := httptest.NewRequest(http.MethodPost, "/api/v2/statements/"+stmt.Handle+"/cancel", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d. Body: %s", http.StatusOK, rr.Code, rr.Body.String())
	}

	if !cancelled {
		t.Error("Expected cancel function to be called")
	}

	// Verify statement status
	updatedStmt, _ := handler.stmtMgr.GetStatement(stmt.Handle)
	if updatedStmt.Status != query.StatementStatusCanceled {
		t.Errorf("Expected status %s, got %s", query.StatementStatusCanceled, updatedStmt.Status)
	}
}

func TestRestAPIv2Handler_CancelStatement_NotFound(t *testing.T) {
	_, router := setupRestAPIv2Handler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/statements/non-existing-handle/cancel", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, rr.Code)
	}
}

func TestRestAPIv2Handler_InvalidJSON(t *testing.T) {
	_, router := setupRestAPIv2Handler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/statements", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusBadRequest, rr.Code)
	}
}

func TestRestAPIv2Handler_SubmitStatement_ResultShapeMatchesSQLAPI(t *testing.T) {
	_, router := setupRestAPIv2Handler(t)

	submit := func(statement string) (int, map[string]interface{}) {
		t.Helper()
		body, _ := json.Marshal(types.SubmitStatementRequest{Statement: statement})
		req := httptest.NewRequest(http.MethodPost, "/api/v2/statements", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		var resp map[string]interface{}
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}
		return rr.Code, resp
	}

	code, resp := submit("SELECT 1 AS n, 'a' AS s, 1.5::DOUBLE AS f, true AS b, DATE '2024-01-15' AS d, " +
		"TIMESTAMP '2024-01-15 14:30:00' AS ts, NULL::VARCHAR AS v")
	if code != http.StatusOK {
		t.Fatalf("Expected status %d, got %d. Body: %v", http.StatusOK, code, resp)
	}

	meta := resp["resultSetMetaData"].(map[string]interface{})
	rowType := meta["rowType"].([]interface{})
	wantTypes := []string{"fixed", "text", "real", "boolean", "date", "timestamp_ntz", "text"}
	for i, want := range wantTypes {
		if got := rowType[i].(map[string]interface{})["type"]; got != want {
			t.Errorf("rowType[%d].type = %v, want %q", i, got, want)
		}
	}

	partitions := meta["partitionInfo"].([]interface{})
	if len(partitions) != 1 || partitions[0].(map[string]interface{})["rowCount"] != float64(1) {
		t.Errorf("partitionInfo = %v, want one partition with rowCount 1", partitions)
	}

	row := resp["data"].([]interface{})[0].([]interface{})
	wantCells := []interface{}{"1", "a", "1.5", strconv.FormatBool(true), "19737", "2024-01-15 14:30:00.000", nil}
	for i, want := range wantCells {
		if row[i] != want {
			t.Errorf("data[0][%d] = %#v, want %#v", i, row[i], want)
		}
	}

	if code, _ = submit("CREATE TABLE shape_t (id INTEGER)"); code != http.StatusOK {
		t.Fatalf("CREATE TABLE failed with status %d", code)
	}
	_, resp = submit("INSERT INTO shape_t VALUES (1), (2)")
	meta = resp["resultSetMetaData"].(map[string]interface{})
	rowType = meta["rowType"].([]interface{})
	if got := rowType[0].(map[string]interface{})["type"]; got != "fixed" {
		t.Errorf("DML rowType[0].type = %v, want %q", got, "fixed")
	}
	if meta["numRows"] != float64(1) {
		t.Errorf("DML numRows = %v, want 1", meta["numRows"])
	}
	partitions = meta["partitionInfo"].([]interface{})
	if len(partitions) != 1 || partitions[0].(map[string]interface{})["rowCount"] != float64(1) {
		t.Errorf("DML partitionInfo = %v, want one partition with rowCount 1", partitions)
	}
	if got := resp["data"].([]interface{})[0].([]interface{})[0]; got != "2" {
		t.Errorf("DML data[0][0] = %#v, want %q", got, "2")
	}
}

func TestRestAPIv2Handler_SubmitStatement_ExecutionErrorReturns422(t *testing.T) {
	_, router := setupRestAPIv2Handler(t)

	body, _ := json.Marshal(types.SubmitStatementRequest{Statement: "SELECT * FROM no_such_table"})
	req := httptest.NewRequest(http.MethodPost, "/api/v2/statements", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnprocessableEntity {
		t.Errorf("Expected status %d, got %d. Body: %s", http.StatusUnprocessableEntity, rr.Code, rr.Body.String())
	}

	var resp types.StatementResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.SQLState != types.SQLState42000 {
		t.Errorf("Expected SQLState %s, got %s", types.SQLState42000, resp.SQLState)
	}
	if resp.Message == "" {
		t.Error("Expected error message to be set")
	}
	if resp.StatementHandle == "" {
		t.Error("Expected statement handle to be set")
	}
}

func TestRestAPIv2Handler_SubmitStatement_EmptyResultHasDataArray(t *testing.T) {
	_, router := setupRestAPIv2Handler(t)

	submit := func(statement string) *httptest.ResponseRecorder {
		t.Helper()
		body, _ := json.Marshal(types.SubmitStatementRequest{Statement: statement})
		req := httptest.NewRequest(http.MethodPost, "/api/v2/statements", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		return rr
	}

	if rr := submit("CREATE TABLE empty_t (id INTEGER)"); rr.Code != http.StatusOK {
		t.Fatalf("CREATE TABLE failed with status %d", rr.Code)
	}

	rr := submit("SELECT id FROM empty_t")
	if rr.Code != http.StatusOK {
		t.Fatalf("Expected status %d, got %d. Body: %s", http.StatusOK, rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"data":[]`) {
		t.Errorf("Expected an empty data array in the body, got: %s", rr.Body.String())
	}

	var resp types.StatementResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if resp.Data == nil || len(resp.Rows()) != 0 {
		t.Errorf("Expected Data to be an empty slice, got %#v", resp.Data)
	}

	rr = submit("SELECT * FROM no_such_table")
	if strings.Contains(rr.Body.String(), `"data"`) {
		t.Errorf("Expected no data field in an error response, got: %s", rr.Body.String())
	}
}
