package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

	if resp.Data == nil || len(resp.Data) == 0 {
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

	if resp.Data == nil || len(resp.Data) == 0 {
		t.Error("Expected data to be returned")
		return
	}

	// Check that the values are correct
	if len(resp.Data[0]) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(resp.Data[0]))
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
