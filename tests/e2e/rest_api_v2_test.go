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
		r.Put("/databases/{database}", restHandler.AlterDatabase)
		r.Delete("/databases/{database}", restHandler.DeleteDatabase)

		r.Get("/databases/{database}/schemas", restHandler.ListSchemas)
		r.Post("/databases/{database}/schemas", restHandler.CreateSchema)
		r.Get("/databases/{database}/schemas/{schema}", restHandler.GetSchema)
		r.Delete("/databases/{database}/schemas/{schema}", restHandler.DeleteSchema)

		r.Get("/databases/{database}/schemas/{schema}/tables", restHandler.ListTables)
		r.Post("/databases/{database}/schemas/{schema}/tables", restHandler.CreateTable)
		r.Get("/databases/{database}/schemas/{schema}/tables/{table}", restHandler.GetTable)
		r.Put("/databases/{database}/schemas/{schema}/tables/{table}", restHandler.AlterTable)
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

// TestRESTAPIV2_SubmitStatement_DDL tests DDL execution via SubmitStatement.
func TestRESTAPIV2_SubmitStatement_DDL(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	// First create database and schema for table creation
	dbReq := types.DatabaseRequest{Name: "DDL_TEST_DB"}
	body, _ := json.Marshal(dbReq)
	resp, _ := http.Post(server.URL+"/api/v2/databases", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	schemaReq := types.SchemaRequest{Name: "DDL_TEST_SCHEMA"}
	body, _ = json.Marshal(schemaReq)
	resp, _ = http.Post(server.URL+"/api/v2/databases/DDL_TEST_DB/schemas", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	// Test CREATE TABLE via SubmitStatement
	// Use {DATABASE}.{SCHEMA}_{TABLE} naming convention per CLAUDE.md
	t.Run("CreateTableViaStatement", func(t *testing.T) {
		reqBody := types.SubmitStatementRequest{
			Statement: "CREATE TABLE DDL_TEST_DB.DDL_TEST_SCHEMA_TEST_TABLE (id INTEGER, name VARCHAR(100))",
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

		if stmtResp.Code != types.ResponseCodeSuccess {
			t.Errorf("Expected code %s, got %s. Message: %s", types.ResponseCodeSuccess, stmtResp.Code, stmtResp.Message)
		}

		t.Log("CreateTableViaStatement: OK")
	})

	// Test DROP TABLE via SubmitStatement
	t.Run("DropTableViaStatement", func(t *testing.T) {
		reqBody := types.SubmitStatementRequest{
			Statement: "DROP TABLE DDL_TEST_DB.DDL_TEST_SCHEMA_TEST_TABLE",
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

		if stmtResp.Code != types.ResponseCodeSuccess {
			t.Errorf("Expected code %s, got %s. Message: %s", types.ResponseCodeSuccess, stmtResp.Code, stmtResp.Message)
		}

		t.Log("DropTableViaStatement: OK")
	})
}

// TestRESTAPIV2_SubmitStatement_DML tests DML execution via SubmitStatement.
func TestRESTAPIV2_SubmitStatement_DML(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	// Setup: Create database, schema, and table
	dbReq := types.DatabaseRequest{Name: "DML_TEST_DB"}
	body, _ := json.Marshal(dbReq)
	resp, _ := http.Post(server.URL+"/api/v2/databases", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	schemaReq := types.SchemaRequest{Name: "DML_TEST_SCHEMA"}
	body, _ = json.Marshal(schemaReq)
	resp, _ = http.Post(server.URL+"/api/v2/databases/DML_TEST_DB/schemas", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	// Create table via statement using {DATABASE}.{SCHEMA}_{TABLE} naming convention
	createReq := types.SubmitStatementRequest{
		Statement: "CREATE TABLE DML_TEST_DB.DML_TEST_SCHEMA_USERS (id INTEGER, name VARCHAR(100))",
	}
	body, _ = json.Marshal(createReq)
	resp, _ = http.Post(server.URL+"/api/v2/statements", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	// Test INSERT via SubmitStatement
	t.Run("InsertViaStatement", func(t *testing.T) {
		reqBody := types.SubmitStatementRequest{
			Statement: "INSERT INTO DML_TEST_DB.DML_TEST_SCHEMA_USERS VALUES (1, 'Alice')",
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

		if stmtResp.Code != types.ResponseCodeSuccess {
			t.Errorf("Expected code %s, got %s. Message: %s", types.ResponseCodeSuccess, stmtResp.Code, stmtResp.Message)
		}

		t.Log("InsertViaStatement: OK")
	})

	// Test UPDATE via SubmitStatement
	t.Run("UpdateViaStatement", func(t *testing.T) {
		reqBody := types.SubmitStatementRequest{
			Statement: "UPDATE DML_TEST_DB.DML_TEST_SCHEMA_USERS SET name = 'Bob' WHERE id = 1",
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

		if stmtResp.Code != types.ResponseCodeSuccess {
			t.Errorf("Expected code %s, got %s. Message: %s", types.ResponseCodeSuccess, stmtResp.Code, stmtResp.Message)
		}

		t.Log("UpdateViaStatement: OK")
	})

	// Test DELETE via SubmitStatement
	t.Run("DeleteViaStatement", func(t *testing.T) {
		reqBody := types.SubmitStatementRequest{
			Statement: "DELETE FROM DML_TEST_DB.DML_TEST_SCHEMA_USERS WHERE id = 1",
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

		if stmtResp.Code != types.ResponseCodeSuccess {
			t.Errorf("Expected code %s, got %s. Message: %s", types.ResponseCodeSuccess, stmtResp.Code, stmtResp.Message)
		}

		t.Log("DeleteViaStatement: OK")
	})
}

// TestRESTAPIV2_DeleteDatabase tests database deletion (name → ID lookup fix).
func TestRESTAPIV2_DeleteDatabase(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	// Create database
	dbReq := types.DatabaseRequest{Name: "DELETE_TEST_DB"}
	body, _ := json.Marshal(dbReq)
	resp, err := http.Post(server.URL+"/api/v2/databases", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	resp.Body.Close()

	// Verify database exists
	resp, err = http.Get(server.URL + "/api/v2/databases/DELETE_TEST_DB")
	if err != nil {
		t.Fatalf("Failed to get database: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200 for get, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Delete database by name (this tests the name → ID lookup fix)
	req, _ := http.NewRequest(http.MethodDelete, server.URL+"/api/v2/databases/DELETE_TEST_DB", nil)
	client := &http.Client{}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("Failed to delete database: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 204, got %d. Body: %s", resp.StatusCode, string(body))
	}

	// Verify database is deleted
	resp, err = http.Get(server.URL + "/api/v2/databases/DELETE_TEST_DB")
	if err != nil {
		t.Fatalf("Failed to verify deletion: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404 after deletion, got %d", resp.StatusCode)
	}

	t.Log("DeleteDatabase: OK (name → ID lookup works)")
}

// TestRESTAPIV2_CreatedOnMilliseconds verifies createdOn is in milliseconds.
func TestRESTAPIV2_CreatedOnMilliseconds(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	beforeMs := time.Now().UnixMilli()

	// Submit a statement
	reqBody := types.SubmitStatementRequest{
		Statement: "SELECT 1 AS num",
	}
	body, _ := json.Marshal(reqBody)

	resp, err := http.Post(server.URL+"/api/v2/statements", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to submit statement: %v", err)
	}
	defer resp.Body.Close()

	afterMs := time.Now().UnixMilli()

	var stmtResp types.StatementResponse
	if err := json.NewDecoder(resp.Body).Decode(&stmtResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// Verify createdOn is in milliseconds range
	// If it were in seconds, it would be ~1700000000 (10 digits)
	// In milliseconds, it should be ~1700000000000 (13 digits)
	if stmtResp.CreatedOn < beforeMs || stmtResp.CreatedOn > afterMs {
		t.Errorf("createdOn %d is not in expected range [%d, %d] (should be milliseconds)", stmtResp.CreatedOn, beforeMs, afterMs)
	}

	// Additional check: ensure it's not in seconds (would be 1000x smaller)
	if stmtResp.CreatedOn < 1000000000000 {
		t.Errorf("createdOn %d appears to be in seconds, not milliseconds", stmtResp.CreatedOn)
	}

	t.Logf("CreatedOnMilliseconds: OK (createdOn=%d)", stmtResp.CreatedOn)
}

// TestRESTAPIV2_BindingValidation tests parameter binding validation.
func TestRESTAPIV2_BindingValidation(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	// Snowflake uses :1, :2, etc. for positional parameters
	t.Run("ValidDateBinding", func(t *testing.T) {
		reqBody := types.SubmitStatementRequest{
			Statement: "SELECT :1 AS dt",
			Bindings: map[string]*types.BindingValue{
				"1": {Type: "DATE", Value: "2024-01-15"},
			},
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
		json.NewDecoder(resp.Body).Decode(&stmtResp)

		if stmtResp.Code != types.ResponseCodeSuccess {
			t.Errorf("Expected success, got code %s: %s", stmtResp.Code, stmtResp.Message)
		}

		t.Log("ValidDateBinding: OK")
	})

	t.Run("InvalidDateBinding", func(t *testing.T) {
		reqBody := types.SubmitStatementRequest{
			Statement: "SELECT :1 AS dt",
			Bindings: map[string]*types.BindingValue{
				"1": {Type: "DATE", Value: "invalid-date"},
			},
		}
		body, _ := json.Marshal(reqBody)

		resp, err := http.Post(server.URL+"/api/v2/statements", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to submit statement: %v", err)
		}
		defer resp.Body.Close()

		// Should return an error response (either 400 or 200 with error code)
		var stmtResp types.StatementResponse
		json.NewDecoder(resp.Body).Decode(&stmtResp)

		// Binding validation errors should result in error code
		if stmtResp.Code == types.ResponseCodeSuccess {
			t.Errorf("Expected error code for invalid date, got success")
		}

		t.Logf("InvalidDateBinding: OK (rejected with code %s)", stmtResp.Code)
	})

	t.Run("ValidTimeBinding", func(t *testing.T) {
		reqBody := types.SubmitStatementRequest{
			Statement: "SELECT :1 AS tm",
			Bindings: map[string]*types.BindingValue{
				"1": {Type: "TIME", Value: "14:30:00"},
			},
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
		json.NewDecoder(resp.Body).Decode(&stmtResp)

		if stmtResp.Code != types.ResponseCodeSuccess {
			t.Errorf("Expected success, got code %s: %s", stmtResp.Code, stmtResp.Message)
		}

		t.Log("ValidTimeBinding: OK")
	})

	t.Run("ValidTimestampBinding", func(t *testing.T) {
		reqBody := types.SubmitStatementRequest{
			Statement: "SELECT :1 AS ts",
			Bindings: map[string]*types.BindingValue{
				"1": {Type: "TIMESTAMP", Value: "2024-01-15T14:30:00Z"},
			},
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
		json.NewDecoder(resp.Body).Decode(&stmtResp)

		if stmtResp.Code != types.ResponseCodeSuccess {
			t.Errorf("Expected success, got code %s: %s", stmtResp.Code, stmtResp.Message)
		}

		t.Log("ValidTimestampBinding: OK")
	})

	t.Run("SQLInjectionBlocked", func(t *testing.T) {
		reqBody := types.SubmitStatementRequest{
			Statement: "SELECT :1 AS dt",
			Bindings: map[string]*types.BindingValue{
				"1": {Type: "DATE", Value: "2024-01-15'; DROP TABLE users; --"},
			},
		}
		body, _ := json.Marshal(reqBody)

		resp, err := http.Post(server.URL+"/api/v2/statements", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to submit statement: %v", err)
		}
		defer resp.Body.Close()

		// Should be rejected due to invalid format
		var stmtResp types.StatementResponse
		json.NewDecoder(resp.Body).Decode(&stmtResp)

		// Binding validation errors should result in error code (not success)
		if stmtResp.Code == types.ResponseCodeSuccess {
			t.Errorf("Expected error code for SQL injection attempt, got success")
		}

		t.Logf("SQLInjectionBlocked: OK (injection attempt rejected with code %s)", stmtResp.Code)
	})
}

// TestRESTAPIV2_StatementStatusURL verifies statementStatusUrl is present.
func TestRESTAPIV2_StatementStatusURL(t *testing.T) {
	server := setupRESTAPIV2Server(t)

	reqBody := types.SubmitStatementRequest{
		Statement: "SELECT 1 AS num",
	}
	body, _ := json.Marshal(reqBody)

	resp, err := http.Post(server.URL+"/api/v2/statements", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to submit statement: %v", err)
	}
	defer resp.Body.Close()

	var stmtResp types.StatementResponse
	if err := json.NewDecoder(resp.Body).Decode(&stmtResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if stmtResp.StatementStatusURL == "" {
		t.Error("Expected statementStatusUrl to be set")
	}

	expectedPrefix := "/api/v2/statements/"
	if len(stmtResp.StatementStatusURL) < len(expectedPrefix) {
		t.Errorf("statementStatusUrl too short: %s", stmtResp.StatementStatusURL)
	} else if stmtResp.StatementStatusURL[:len(expectedPrefix)] != expectedPrefix {
		t.Errorf("Expected statementStatusUrl to start with %s, got %s", expectedPrefix, stmtResp.StatementStatusURL)
	}

	t.Logf("StatementStatusURL: OK (url=%s)", stmtResp.StatementStatusURL)
}
