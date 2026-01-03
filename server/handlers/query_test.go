package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
	"github.com/nnnkkk7/snowflake-emulator/pkg/session"
	"github.com/nnnkkk7/snowflake-emulator/server/apierror"
	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

// setupTestQueryHandler creates a test query handler with dependencies.
func setupTestQueryHandler(t *testing.T) (*QueryHandler, *session.Manager, *metadata.Repository) {
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

	mgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	sessionMgr := session.NewManager(1 * time.Hour)
	executor := query.NewExecutor(mgr, repo)

	// Create test database and schema
	ctx := context.Background()
	database, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	schema, err := repo.CreateSchema(ctx, database.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Create test table
	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "VALUE", Type: "INTEGER"},
	}
	_, err = repo.CreateTable(ctx, schema.ID, "TEST_TABLE", columns, "")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	insertSQL := "INSERT INTO TEST_DB.PUBLIC_TEST_TABLE VALUES (1, 'Alice', 100), (2, 'Bob', 200)"
	_, err = executor.Execute(ctx, insertSQL)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	return NewQueryHandler(executor, sessionMgr), sessionMgr, repo
}

// TestQueryHandler_ExecuteQuery tests the query execution endpoint.
func TestQueryHandler_ExecuteQuery(t *testing.T) {
	handler, sessionMgr, _ := setupTestQueryHandler(t)
	ctx := context.Background()

	// Create a session for authentication
	sess, err := sessionMgr.CreateSession(ctx, "testuser", "TEST_DB", "PUBLIC")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	tests := []struct {
		name           string
		request        types.QueryRequest
		token          string
		expectedStatus int
		checkResponse  func(*testing.T, *types.QueryResponse)
	}{
		{
			name: "ValidSELECT",
			request: types.QueryRequest{
				SQLText: "SELECT * FROM TEST_DB.PUBLIC_TEST_TABLE ORDER BY ID",
			},
			token:          sess.Token,
			expectedStatus: http.StatusOK,
			checkResponse: func(t *testing.T, resp *types.QueryResponse) {
				if !resp.Success {
					t.Error("Expected success to be true")
				}
				if resp.Data == nil {
					t.Fatal("Expected data to be set")
				}
				if len(resp.Data.RowSet) != 2 {
					t.Errorf("Expected 2 rows, got %d", len(resp.Data.RowSet))
				}
				if len(resp.Data.RowType) != 3 {
					t.Errorf("Expected 3 columns, got %d", len(resp.Data.RowType))
				}
				if resp.Data.QueryID == "" {
					t.Error("Expected QueryID to be set")
				}
			},
		},
		{
			name: "QueryWithIFF",
			request: types.QueryRequest{
				SQLText: "SELECT NAME, IFF(VALUE > 150, 'High', 'Low') AS category FROM TEST_DB.PUBLIC_TEST_TABLE",
			},
			token:          sess.Token,
			expectedStatus: http.StatusOK,
			checkResponse: func(t *testing.T, resp *types.QueryResponse) {
				if !resp.Success {
					t.Error("Expected success to be true")
				}
				if resp.Data == nil {
					t.Fatal("Expected data to be set")
				}
				if len(resp.Data.RowSet) != 2 {
					t.Errorf("Expected 2 rows, got %d", len(resp.Data.RowSet))
				}
			},
		},
		{
			name: "InvalidSQL",
			request: types.QueryRequest{
				SQLText: "SELECT FROM TEST_DB.PUBLIC_TEST_TABLE",
			},
			token:          sess.Token,
			expectedStatus: http.StatusOK, // Snowflake returns 200 even for errors
			checkResponse: func(t *testing.T, resp *types.QueryResponse) {
				if resp.Success {
					t.Error("Expected success to be false")
				}
				// With AST parser's graceful degradation, invalid SQL may fail at execution (001007)
				// rather than compilation (001003)
				if resp.Code != apierror.CodeSQLCompilationError && resp.Code != apierror.CodeSQLExecutionError {
					t.Errorf("Expected code %s or %s, got %s", apierror.CodeSQLCompilationError, apierror.CodeSQLExecutionError, resp.Code)
				}
			},
		},
		{
			name: "MissingToken",
			request: types.QueryRequest{
				SQLText: "SELECT * FROM TEST_DB.PUBLIC_TEST_TABLE",
			},
			token:          "",
			expectedStatus: http.StatusOK, // Snowflake returns 200 even for errors
			checkResponse: func(t *testing.T, resp *types.QueryResponse) {
				if resp.Success {
					t.Error("Expected success to be false")
				}
			},
		},
		{
			name: "InvalidToken",
			request: types.QueryRequest{
				SQLText: "SELECT * FROM TEST_DB.PUBLIC_TEST_TABLE",
			},
			token:          "invalid-token-12345",
			expectedStatus: http.StatusOK, // Snowflake returns 200 even for errors
			checkResponse: func(t *testing.T, resp *types.QueryResponse) {
				if resp.Success {
					t.Error("Expected success to be false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request
			body, err := json.Marshal(tt.request)
			if err != nil {
				t.Fatalf("Failed to marshal request: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			if tt.token != "" {
				req.Header.Set("Authorization", "Snowflake Token=\""+tt.token+"\"")
			}

			// Record response
			rr := httptest.NewRecorder()

			// Handle request
			handler.ExecuteQuery(rr, req)

			// Check status code
			if rr.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rr.Code)
			}

			// Parse response
			var resp types.QueryResponse
			if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
				t.Fatalf("Failed to unmarshal response: %v", err)
			}

			// Check response
			if tt.checkResponse != nil {
				tt.checkResponse(t, &resp)
			}
		})
	}
}

// TestQueryHandler_ExecuteDML tests DML operations (INSERT, UPDATE, DELETE).
func TestQueryHandler_ExecuteDML(t *testing.T) {
	handler, sessionMgr, _ := setupTestQueryHandler(t)
	ctx := context.Background()

	// Create session
	sess, err := sessionMgr.CreateSession(ctx, "testuser", "TEST_DB", "PUBLIC")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	tests := []struct {
		name           string
		statement      string
		expectedStatus int
		checkResponse  func(*testing.T, *types.QueryResponse)
	}{
		{
			name:           "INSERT",
			statement:      "INSERT INTO TEST_DB.PUBLIC_TEST_TABLE VALUES (3, 'Charlie', 300)",
			expectedStatus: http.StatusOK,
			checkResponse: func(t *testing.T, resp *types.QueryResponse) {
				if !resp.Success {
					t.Error("Expected success to be true")
				}
				if resp.Data == nil {
					t.Fatal("Expected data to be set")
				}
				if resp.Data.Total != 1 {
					t.Errorf("Expected 1 row affected, got %d", resp.Data.Total)
				}
			},
		},
		{
			name:           "UPDATE",
			statement:      "UPDATE TEST_DB.PUBLIC_TEST_TABLE SET VALUE = 150 WHERE ID = 1",
			expectedStatus: http.StatusOK,
			checkResponse: func(t *testing.T, resp *types.QueryResponse) {
				if !resp.Success {
					t.Error("Expected success to be true")
				}
				if resp.Data == nil {
					t.Fatal("Expected data to be set")
				}
				if resp.Data.Total != 1 {
					t.Errorf("Expected 1 row affected, got %d", resp.Data.Total)
				}
			},
		},
		{
			name:           "DELETE",
			statement:      "DELETE FROM TEST_DB.PUBLIC_TEST_TABLE WHERE ID = 2",
			expectedStatus: http.StatusOK,
			checkResponse: func(t *testing.T, resp *types.QueryResponse) {
				if !resp.Success {
					t.Error("Expected success to be true")
				}
				if resp.Data == nil {
					t.Fatal("Expected data to be set")
				}
				if resp.Data.Total != 1 {
					t.Errorf("Expected 1 row affected, got %d", resp.Data.Total)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := types.QueryRequest{
				SQLText: tt.statement,
			}

			body, _ := json.Marshal(req)
			httpReq := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", bytes.NewReader(body))
			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("Authorization", "Snowflake Token=\""+sess.Token+"\"")

			rr := httptest.NewRecorder()
			handler.ExecuteQuery(rr, httpReq)

			if rr.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rr.Code)
			}

			var resp types.QueryResponse
			json.Unmarshal(rr.Body.Bytes(), &resp)

			if tt.checkResponse != nil {
				tt.checkResponse(t, &resp)
			}
		})
	}
}

// TestQueryHandler_ConcurrentQueries tests concurrent query execution.
func TestQueryHandler_ConcurrentQueries(t *testing.T) {
	handler, sessionMgr, _ := setupTestQueryHandler(t)
	ctx := context.Background()

	// Create session
	sess, err := sessionMgr.CreateSession(ctx, "testuser", "TEST_DB", "PUBLIC")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			req := types.QueryRequest{
				SQLText: "SELECT * FROM TEST_DB.PUBLIC_TEST_TABLE",
			}

			body, _ := json.Marshal(req)
			httpReq := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", bytes.NewReader(body))
			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("Authorization", "Snowflake Token=\""+sess.Token+"\"")

			rr := httptest.NewRecorder()
			handler.ExecuteQuery(rr, httpReq)

			if rr.Code != http.StatusOK {
				t.Errorf("Expected status OK, got %d", rr.Code)
				done <- false
				return
			}

			var resp types.QueryResponse
			if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
				t.Errorf("Failed to unmarshal: %v", err)
				done <- false
				return
			}

			if !resp.Success {
				t.Error("Expected success to be true")
				done <- false
				return
			}

			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestQueryHandler_QueryResultFormat tests the format of query results.
func TestQueryHandler_QueryResultFormat(t *testing.T) {
	handler, sessionMgr, _ := setupTestQueryHandler(t)
	ctx := context.Background()

	sess, err := sessionMgr.CreateSession(ctx, "testuser", "TEST_DB", "PUBLIC")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	req := types.QueryRequest{
		SQLText: "SELECT ID, NAME, VALUE FROM TEST_DB.PUBLIC_TEST_TABLE WHERE ID = 1",
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Snowflake Token=\""+sess.Token+"\"")

	rr := httptest.NewRecorder()
	handler.ExecuteQuery(rr, httpReq)

	var resp types.QueryResponse
	json.Unmarshal(rr.Body.Bytes(), &resp)

	// Verify structure
	if !resp.Success {
		t.Error("Expected success to be true")
	}

	if resp.Data == nil {
		t.Fatal("Expected data to be set")
	}

	// Verify query ID is set
	if resp.Data.QueryID == "" {
		t.Error("Expected QueryID to be set")
	}

	// Verify SQL state
	if resp.Data.SQLState != "00000" {
		t.Errorf("Expected SQLState 00000, got %s", resp.Data.SQLState)
	}

	// Verify rowType (column metadata)
	expectedColumns := []string{"ID", "NAME", "VALUE"}
	if len(resp.Data.RowType) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(resp.Data.RowType))
	}

	// Verify row data (rowset)
	if len(resp.Data.RowSet) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(resp.Data.RowSet))
	}

	row := resp.Data.RowSet[0]
	if len(row) != 3 {
		t.Errorf("Expected 3 values in row, got %d", len(row))
	}

	// Verify query result format
	if resp.Data.QueryResultFormat != "json" {
		t.Errorf("Expected queryResultFormat 'json', got %s", resp.Data.QueryResultFormat)
	}
}
