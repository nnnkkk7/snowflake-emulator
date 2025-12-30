// tests/e2e/rest_api_v2_test.go - REST API v2 compatibility tests
package e2e

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
	"github.com/nnnkkk7/snowflake-emulator/server/handlers"
	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

// setupRESTAPIV2Server creates an in-process server for REST API v2 testing.
func setupRESTAPIV2Server(t *testing.T) *httptest.Server {
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

	restHandler := handlers.NewRestAPIv2Handler(executor, stmtMgr, repo)

	r := chi.NewRouter()
	r.Route("/api/v2", func(r chi.Router) {
		r.Post("/statements", restHandler.SubmitStatement)
		r.Get("/statements/{handle}", restHandler.GetStatement)
		r.Post("/statements/{handle}/cancel", restHandler.CancelStatement)

		r.Get("/databases", restHandler.ListDatabases)
		r.Post("/databases", restHandler.CreateDatabase)
		r.Get("/databases/{database}", restHandler.GetDatabase)
		r.Delete("/databases/{database}", restHandler.DeleteDatabase)

		r.Get("/databases/{database}/schemas", restHandler.ListSchemas)
		r.Post("/databases/{database}/schemas", restHandler.CreateSchema)
		r.Get("/databases/{database}/schemas/{schema}", restHandler.GetSchema)
		r.Delete("/databases/{database}/schemas/{schema}", restHandler.DeleteSchema)

		r.Get("/databases/{database}/schemas/{schema}/tables", restHandler.ListTables)
		r.Get("/databases/{database}/schemas/{schema}/tables/{table}", restHandler.GetTable)
		r.Delete("/databases/{database}/schemas/{schema}/tables/{table}", restHandler.DeleteTable)

		// Warehouse endpoints
		r.Get("/warehouses", restHandler.ListWarehouses)
		r.Post("/warehouses", restHandler.CreateWarehouse)
		r.Get("/warehouses/{warehouse}", restHandler.GetWarehouse)
		r.Delete("/warehouses/{warehouse}", restHandler.DeleteWarehouse)
		r.Post("/warehouses/{warehouse}:resume", restHandler.ResumeWarehouse)
		r.Post("/warehouses/{warehouse}:suspend", restHandler.SuspendWarehouse)
	})

	server := httptest.NewServer(r)
	t.Cleanup(server.Close)

	return server
}

// TestRESTAPIV2_SubmitStatement tests the statement submission endpoint.
func TestRESTAPIV2_SubmitStatement(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	testCases := []struct {
		name      string
		statement string
		wantCode  string
	}{
		{
			name:      "SimpleSelect",
			statement: "SELECT 1 AS num",
			wantCode:  types.ResponseCodeSuccess,
		},
		{
			name:      "SelectWithFunction",
			statement: "SELECT IFF(1 = 1, 'yes', 'no') AS result",
			wantCode:  types.ResponseCodeSuccess,
		},
		{
			name:      "SelectMultipleColumns",
			statement: "SELECT 1 AS a, 2 AS b, 'hello' AS c",
			wantCode:  types.ResponseCodeSuccess,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reqBody := types.SubmitStatementRequest{
				Statement: tc.statement,
			}
			body, _ := json.Marshal(reqBody)

			resp, err := http.Post(server.URL+"/api/v2/statements", "application/json", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("Failed to submit statement: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("Expected status 200, got %d. Body: %s", resp.StatusCode, string(body))
			}

			var stmtResp types.StatementResponse
			if err := json.NewDecoder(resp.Body).Decode(&stmtResp); err != nil {
				t.Fatalf("Failed to decode response: %v", err)
			}

			if stmtResp.Code != tc.wantCode {
				t.Errorf("Expected code %s, got %s. Message: %s", tc.wantCode, stmtResp.Code, stmtResp.Message)
			}

			if stmtResp.StatementHandle == "" {
				t.Error("Expected statement handle to be set")
			}

			if stmtResp.ResultSetMetaData == nil {
				t.Error("Expected ResultSetMetaData to be set")
			}

			t.Logf("%s: OK (handle=%s, rows=%d)", tc.name, stmtResp.StatementHandle, stmtResp.ResultSetMetaData.NumRows)
		})
	}
}

// TestRESTAPIV2_GetStatement tests getting statement status/result.
func TestRESTAPIV2_GetStatement(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	// Submit a statement first
	reqBody := types.SubmitStatementRequest{
		Statement: "SELECT 42 AS answer",
	}
	body, _ := json.Marshal(reqBody)

	resp, err := http.Post(server.URL+"/api/v2/statements", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to submit statement: %v", err)
	}
	defer resp.Body.Close()

	var submitResp types.StatementResponse
	json.NewDecoder(resp.Body).Decode(&submitResp)

	// Get the statement
	resp, err = http.Get(server.URL + "/api/v2/statements/" + submitResp.StatementHandle)
	if err != nil {
		t.Fatalf("Failed to get statement: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	var getResp types.StatementResponse
	if err := json.NewDecoder(resp.Body).Decode(&getResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if getResp.StatementHandle != submitResp.StatementHandle {
		t.Errorf("Expected handle %s, got %s", submitResp.StatementHandle, getResp.StatementHandle)
	}

	if getResp.Code != types.ResponseCodeSuccess {
		t.Errorf("Expected code %s, got %s", types.ResponseCodeSuccess, getResp.Code)
	}

	t.Logf("GetStatement: OK (handle=%s)", getResp.StatementHandle)
}

// TestRESTAPIV2_DatabaseManagement tests database CRUD operations.
func TestRESTAPIV2_DatabaseManagement(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	// Create database
	t.Run("CreateDatabase", func(t *testing.T) {
		reqBody := types.DatabaseRequest{
			Name:    "TEST_DB",
			Comment: "Test database",
		}
		body, _ := json.Marshal(reqBody)

		resp, err := http.Post(server.URL+"/api/v2/databases", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 201, got %d. Body: %s", resp.StatusCode, string(body))
		}

		var dbResp types.DatabaseResponse
		json.NewDecoder(resp.Body).Decode(&dbResp)

		if dbResp.Name != "TEST_DB" {
			t.Errorf("Expected name 'TEST_DB', got %s", dbResp.Name)
		}

		t.Logf("CreateDatabase: OK (name=%s)", dbResp.Name)
	})

	// List databases
	t.Run("ListDatabases", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/v2/databases")
		if err != nil {
			t.Fatalf("Failed to list databases: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", resp.StatusCode)
		}

		var databases types.ListDatabasesResponse
		json.NewDecoder(resp.Body).Decode(&databases)

		if len(databases) == 0 {
			t.Error("Expected at least one database")
		}

		t.Logf("ListDatabases: OK (count=%d)", len(databases))
	})

	// Get database
	t.Run("GetDatabase", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/v2/databases/TEST_DB")
		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", resp.StatusCode)
		}

		var dbResp types.DatabaseResponse
		json.NewDecoder(resp.Body).Decode(&dbResp)

		if dbResp.Name != "TEST_DB" {
			t.Errorf("Expected name 'TEST_DB', got %s", dbResp.Name)
		}

		t.Logf("GetDatabase: OK (name=%s)", dbResp.Name)
	})
}

// TestRESTAPIV2_SchemaManagement tests schema CRUD operations.
func TestRESTAPIV2_SchemaManagement(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	// Create database first
	dbReq := types.DatabaseRequest{Name: "SCHEMA_TEST_DB"}
	body, _ := json.Marshal(dbReq)
	resp, _ := http.Post(server.URL+"/api/v2/databases", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	// Create schema
	t.Run("CreateSchema", func(t *testing.T) {
		reqBody := types.SchemaRequest{
			Name:    "TEST_SCHEMA",
			Comment: "Test schema",
		}
		body, _ := json.Marshal(reqBody)

		resp, err := http.Post(server.URL+"/api/v2/databases/SCHEMA_TEST_DB/schemas", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create schema: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 201, got %d. Body: %s", resp.StatusCode, string(body))
		}

		var schemaResp types.SchemaResponse
		json.NewDecoder(resp.Body).Decode(&schemaResp)

		if schemaResp.Name != "TEST_SCHEMA" {
			t.Errorf("Expected name 'TEST_SCHEMA', got %s", schemaResp.Name)
		}

		t.Logf("CreateSchema: OK (name=%s)", schemaResp.Name)
	})

	// List schemas
	t.Run("ListSchemas", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/v2/databases/SCHEMA_TEST_DB/schemas")
		if err != nil {
			t.Fatalf("Failed to list schemas: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", resp.StatusCode)
		}

		var schemas types.ListSchemasResponse
		json.NewDecoder(resp.Body).Decode(&schemas)

		// Should have at least the schema we created
		found := false
		for _, s := range schemas {
			if s.Name == "TEST_SCHEMA" {
				found = true
				break
			}
		}

		if !found {
			t.Error("Expected to find TEST_SCHEMA in list")
		}

		t.Logf("ListSchemas: OK (count=%d)", len(schemas))
	})

	// Get schema
	t.Run("GetSchema", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/v2/databases/SCHEMA_TEST_DB/schemas/TEST_SCHEMA")
		if err != nil {
			t.Fatalf("Failed to get schema: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", resp.StatusCode)
		}

		var schemaResp types.SchemaResponse
		json.NewDecoder(resp.Body).Decode(&schemaResp)

		if schemaResp.Name != "TEST_SCHEMA" {
			t.Errorf("Expected name 'TEST_SCHEMA', got %s", schemaResp.Name)
		}

		t.Logf("GetSchema: OK (name=%s)", schemaResp.Name)
	})
}

// TestRESTAPIV2_ErrorHandling tests error responses.
func TestRESTAPIV2_ErrorHandling(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	t.Run("StatementNotFound", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/v2/statements/nonexistent-handle")
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status 404, got %d", resp.StatusCode)
		}

		t.Log("StatementNotFound: OK (404)")
	})

	t.Run("DatabaseNotFound", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/v2/databases/NONEXISTENT_DB")
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status 404, got %d", resp.StatusCode)
		}

		t.Log("DatabaseNotFound: OK (404)")
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		resp, err := http.Post(server.URL+"/api/v2/statements", "application/json", bytes.NewReader([]byte("invalid json")))
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", resp.StatusCode)
		}

		t.Log("InvalidJSON: OK (400)")
	})

	t.Run("EmptyStatement", func(t *testing.T) {
		reqBody := types.SubmitStatementRequest{
			Statement: "",
		}
		body, _ := json.Marshal(reqBody)

		resp, err := http.Post(server.URL+"/api/v2/statements", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", resp.StatusCode)
		}

		t.Log("EmptyStatement: OK (400)")
	})
}

// TestRESTAPIV2_WarehouseManagement tests warehouse CRUD operations.
func TestRESTAPIV2_WarehouseManagement(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	// Create warehouse
	t.Run("CreateWarehouse", func(t *testing.T) {
		reqBody := types.WarehouseRequest{
			Name:    "TEST_WH",
			Size:    "SMALL",
			Comment: "Test warehouse",
		}
		body, _ := json.Marshal(reqBody)

		resp, err := http.Post(server.URL+"/api/v2/warehouses", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create warehouse: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 201, got %d. Body: %s", resp.StatusCode, string(body))
		}

		var whResp types.WarehouseResponse
		json.NewDecoder(resp.Body).Decode(&whResp)

		if whResp.Name != "TEST_WH" {
			t.Errorf("Expected name 'TEST_WH', got %s", whResp.Name)
		}

		if whResp.State != "SUSPENDED" {
			t.Errorf("Expected state 'SUSPENDED', got %s", whResp.State)
		}

		t.Logf("CreateWarehouse: OK (name=%s, state=%s)", whResp.Name, whResp.State)
	})

	// List warehouses
	t.Run("ListWarehouses", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/v2/warehouses")
		if err != nil {
			t.Fatalf("Failed to list warehouses: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", resp.StatusCode)
		}

		var warehouses types.ListWarehousesResponse
		json.NewDecoder(resp.Body).Decode(&warehouses)

		if len(warehouses) == 0 {
			t.Error("Expected at least one warehouse")
		}

		t.Logf("ListWarehouses: OK (count=%d)", len(warehouses))
	})

	// Get warehouse
	t.Run("GetWarehouse", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/v2/warehouses/TEST_WH")
		if err != nil {
			t.Fatalf("Failed to get warehouse: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", resp.StatusCode)
		}

		var whResp types.WarehouseResponse
		json.NewDecoder(resp.Body).Decode(&whResp)

		if whResp.Name != "TEST_WH" {
			t.Errorf("Expected name 'TEST_WH', got %s", whResp.Name)
		}

		t.Logf("GetWarehouse: OK (name=%s)", whResp.Name)
	})

	// Resume warehouse
	t.Run("ResumeWarehouse", func(t *testing.T) {
		resp, err := http.Post(server.URL+"/api/v2/warehouses/TEST_WH:resume", "application/json", nil)
		if err != nil {
			t.Fatalf("Failed to resume warehouse: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 200, got %d. Body: %s", resp.StatusCode, string(body))
		}

		var whResp types.WarehouseResponse
		json.NewDecoder(resp.Body).Decode(&whResp)

		if whResp.State != "ACTIVE" {
			t.Errorf("Expected state 'ACTIVE', got %s", whResp.State)
		}

		t.Logf("ResumeWarehouse: OK (state=%s)", whResp.State)
	})

	// Suspend warehouse
	t.Run("SuspendWarehouse", func(t *testing.T) {
		resp, err := http.Post(server.URL+"/api/v2/warehouses/TEST_WH:suspend", "application/json", nil)
		if err != nil {
			t.Fatalf("Failed to suspend warehouse: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 200, got %d. Body: %s", resp.StatusCode, string(body))
		}

		var whResp types.WarehouseResponse
		json.NewDecoder(resp.Body).Decode(&whResp)

		if whResp.State != "SUSPENDED" {
			t.Errorf("Expected state 'SUSPENDED', got %s", whResp.State)
		}

		t.Logf("SuspendWarehouse: OK (state=%s)", whResp.State)
	})

	// Delete warehouse
	t.Run("DeleteWarehouse", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodDelete, server.URL+"/api/v2/warehouses/TEST_WH", nil)
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to delete warehouse: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("Expected status 204, got %d", resp.StatusCode)
		}

		t.Log("DeleteWarehouse: OK (204)")

		// Verify warehouse is deleted
		resp, err = http.Get(server.URL + "/api/v2/warehouses/TEST_WH")
		if err != nil {
			t.Fatalf("Failed to verify deletion: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status 404 after deletion, got %d", resp.StatusCode)
		}
	})
}
