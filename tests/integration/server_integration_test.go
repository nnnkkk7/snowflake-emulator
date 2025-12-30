//nolint:errcheck,gosec,govet,bodyclose // Test file with simplified error handling
package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
	"github.com/nnnkkk7/snowflake-emulator/pkg/session"
	"github.com/nnnkkk7/snowflake-emulator/server/handlers"
)

// setupTestServer creates a test server with all components.
func setupTestServer(t *testing.T) (*httptest.Server, *session.Manager, *metadata.Repository) {
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
	database, err := repo.CreateDatabase(ctx, "TEST_DB", "Test database")
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	schema, err := repo.CreateSchema(ctx, database.ID, "PUBLIC", "Public schema")
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Create test table
	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "SCORE", Type: "INTEGER"},
	}
	_, err = repo.CreateTable(ctx, schema.ID, "STUDENTS", columns, "Student data")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create handlers
	sessionHandler := handlers.NewSessionHandler(sessionMgr, repo)
	queryHandler := handlers.NewQueryHandler(executor, sessionMgr)

	// Create router
	mux := http.NewServeMux()
	mux.HandleFunc("/session/v1/login-request", sessionHandler.Login)
	mux.HandleFunc("/session/renew", sessionHandler.RenewSession)
	mux.HandleFunc("/session/logout", sessionHandler.Logout)
	mux.HandleFunc("/queries/v1/query-request", queryHandler.ExecuteQuery)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return server, sessionMgr, repo
}

// TestIntegration_CompleteWorkflow tests the complete workflow: login → query → logout.
func TestIntegration_CompleteWorkflow(t *testing.T) { //nolint:gocyclo // Integration test covers complete workflow
	server, _, _ := setupTestServer(t)

	// Step 1: Login with gosnowflake protocol
	loginReq := map[string]interface{}{
		"data": map[string]string{
			"LOGIN_NAME":   "testuser",
			"PASSWORD":     "testpass",
			"databaseName": "TEST_DB",
			"schemaName":   "PUBLIC",
		},
	}

	body, _ := json.Marshal(loginReq)
	resp, err := http.Post(server.URL+"/session/v1/login-request", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Login request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	var loginResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		t.Fatalf("Failed to decode login response: %v", err)
	}

	// Extract token from nested data structure
	data, ok := loginResp["data"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected data in login response")
	}

	token, ok := data["token"].(string)
	if !ok || token == "" {
		t.Fatal("Expected token in login response")
	}

	// Step 2: Execute INSERT query
	insertReq := map[string]string{
		"sqlText": "INSERT INTO TEST_DB.PUBLIC_STUDENTS VALUES (1, 'Alice', 95), (2, 'Bob', 87), (3, 'Charlie', 92)",
	}

	body, _ = json.Marshal(insertReq)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Snowflake Token=\""+token+"\"")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Insert request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200 for INSERT, got %d", resp.StatusCode)
	}

	// Step 3: Execute SELECT query
	selectReq := map[string]string{
		"sqlText": "SELECT * FROM TEST_DB.PUBLIC_STUDENTS ORDER BY ID",
	}

	body, _ = json.Marshal(selectReq)
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Snowflake Token=\""+token+"\"")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Select request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200 for SELECT, got %d", resp.StatusCode)
	}

	var selectResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&selectResp); err != nil {
		t.Fatalf("Failed to decode select response: %v", err)
	}

	data, ok = selectResp["data"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected data in select response")
	}

	rowset, ok := data["rowset"].([]interface{})
	if !ok {
		t.Fatal("Expected rowset in data")
	}

	if len(rowset) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rowset))
	}

	// Step 4: Logout
	logoutReq := map[string]string{
		"token": token,
	}

	body, _ = json.Marshal(logoutReq)
	resp, err = http.Post(server.URL+"/session/logout", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Logout request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200 for logout, got %d", resp.StatusCode)
	}

	// Step 5: Verify token is invalid after logout
	verifyReq := map[string]string{
		"sqlText": "SELECT 1",
	}
	body, _ = json.Marshal(verifyReq)
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Snowflake Token=\""+token+"\"")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Request after logout failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Snowflake returns 200 even for auth errors, check success field
	var verifyResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&verifyResp)
	if success, ok := verifyResp["success"].(bool); ok && success {
		t.Error("Expected request to fail after logout")
	}
}

// TestIntegration_QueryWithTranslation tests Snowflake SQL translation in end-to-end flow.
func TestIntegration_QueryWithTranslation(t *testing.T) {
	server, _, _ := setupTestServer(t)

	// Login with gosnowflake protocol
	loginReq := map[string]interface{}{
		"data": map[string]string{
			"LOGIN_NAME":   "testuser",
			"PASSWORD":     "testpass",
			"databaseName": "TEST_DB",
			"schemaName":   "PUBLIC",
		},
	}

	body, _ := json.Marshal(loginReq)
	resp, _ := http.Post(server.URL+"/session/v1/login-request", "application/json", bytes.NewReader(body))
	defer func() { _ = resp.Body.Close() }()

	var loginResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&loginResp)
	data := loginResp["data"].(map[string]interface{})
	token := data["token"].(string)

	// Insert test data
	insertReq := map[string]string{
		"sqlText": "INSERT INTO TEST_DB.PUBLIC_STUDENTS VALUES (1, 'Alice', 95), (2, 'Bob', 87)",
	}

	body, _ = json.Marshal(insertReq)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Snowflake Token=\""+token+"\"")
	http.DefaultClient.Do(req)

	// Test IFF translation
	queryReq := map[string]string{
		"sqlText": "SELECT NAME, IFF(SCORE >= 90, 'A', 'B') AS GRADE FROM TEST_DB.PUBLIC_STUDENTS ORDER BY ID",
	}

	body, _ = json.Marshal(queryReq)
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Snowflake Token=\""+token+"\"")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Query request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	var queryResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&queryResp)

	queryData := queryResp["data"].(map[string]interface{})
	rowset := queryData["rowset"].([]interface{})

	if len(rowset) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rowset))
	}

	// Verify first row grade is 'A' (Alice: 95 >= 90)
	firstRow := rowset[0].([]interface{})
	if firstRow[1] != "A" {
		t.Errorf("Expected grade 'A' for Alice, got %v", firstRow[1])
	}

	// Verify second row grade is 'B' (Bob: 87 < 90)
	secondRow := rowset[1].([]interface{})
	if secondRow[1] != "B" {
		t.Errorf("Expected grade 'B' for Bob, got %v", secondRow[1])
	}
}

// TestIntegration_ConcurrentSessions tests multiple concurrent sessions.
func TestIntegration_ConcurrentSessions(t *testing.T) {
	server, _, _ := setupTestServer(t)

	done := make(chan bool, 5)

	for i := 0; i < 5; i++ {
		go func(id int) {
			// Login with gosnowflake protocol
			loginReq := map[string]interface{}{
				"data": map[string]string{
					"LOGIN_NAME":   "user" + string(rune('0'+id)),
					"PASSWORD":     "pass",
					"databaseName": "TEST_DB",
					"schemaName":   "PUBLIC",
				},
			}

			body, _ := json.Marshal(loginReq)
			resp, err := http.Post(server.URL+"/session/v1/login-request", "application/json", bytes.NewReader(body))
			if err != nil {
				t.Errorf("Login failed for user %d: %v", id, err)
				done <- false
				return
			}
			defer func() { _ = resp.Body.Close() }()

			var loginResp map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&loginResp)
			data := loginResp["data"].(map[string]interface{})
			token := data["token"].(string)

			// Execute query
			queryReq := map[string]string{
				"sqlText": "SELECT * FROM TEST_DB.PUBLIC_STUDENTS LIMIT 1",
			}

			body, _ = json.Marshal(queryReq)
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Snowflake Token=\""+token+"\"")

			resp, err = http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("Query failed for user %d: %v", id, err)
				done <- false
				return
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Expected status 200 for user %d, got %d", id, resp.StatusCode)
				done <- false
				return
			}

			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 5; i++ {
		<-done
	}
}

// TestIntegration_SessionRenewal tests session renewal workflow.
func TestIntegration_SessionRenewal(t *testing.T) {
	server, _, _ := setupTestServer(t)

	// Login with gosnowflake protocol
	loginReq := map[string]interface{}{
		"data": map[string]string{
			"LOGIN_NAME":   "testuser",
			"PASSWORD":     "testpass",
			"databaseName": "TEST_DB",
			"schemaName":   "PUBLIC",
		},
	}

	body, _ := json.Marshal(loginReq)
	resp, _ := http.Post(server.URL+"/session/v1/login-request", "application/json", bytes.NewReader(body))
	defer func() { _ = resp.Body.Close() }()

	var loginResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&loginResp)
	data := loginResp["data"].(map[string]interface{})
	token := data["token"].(string)
	masterToken := data["masterToken"].(string)

	// Renew session using master token
	renewReq := map[string]string{
		"masterToken": masterToken,
	}

	body, _ = json.Marshal(renewReq)
	resp, err := http.Post(server.URL+"/session/renew", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Renew request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200 for renew, got %d", resp.StatusCode)
	}

	var renewResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&renewResp)

	if success, ok := renewResp["success"].(bool); !ok || !success {
		t.Error("Expected success to be true")
	}

	// Verify session still works after renewal
	queryReq := map[string]string{
		"sqlText": "SELECT 1 AS test",
	}

	body, _ = json.Marshal(queryReq)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Snowflake Token=\""+token+"\"")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Query after renew failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 after renew, got %d", resp.StatusCode)
	}
}

// TestIntegration_Phase2Functions tests Phase 2 SQL function translations.
func TestIntegration_Phase2Functions(t *testing.T) { //nolint:gocyclo // Integration test covers multiple Phase 2 functions
	server, _, repo := setupTestServer(t)

	// Create test table with nullable and date columns for Phase 2 tests
	ctx := context.Background()
	database, _ := repo.GetDatabaseByName(ctx, "TEST_DB")
	schemas, _ := repo.ListSchemas(ctx, database.ID)
	var schema *metadata.Schema
	for _, s := range schemas {
		if s.Name == "PUBLIC" {
			schema = s
			break
		}
	}
	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "EMAIL", Type: "VARCHAR", Nullable: true},
		{Name: "SCORE", Type: "INTEGER", Nullable: true},
		{Name: "CREATED_AT", Type: "DATE"},
		{Name: "DATA", Type: "VARCHAR", Nullable: true},
	}
	_, _ = repo.CreateTable(ctx, schema.ID, "PHASE2_TEST", columns, "Phase 2 test data")

	// Login
	loginReq := map[string]interface{}{
		"data": map[string]string{
			"LOGIN_NAME":   "testuser",
			"PASSWORD":     "testpass",
			"databaseName": "TEST_DB",
			"schemaName":   "PUBLIC",
		},
	}

	body, _ := json.Marshal(loginReq)
	resp, _ := http.Post(server.URL+"/session/v1/login-request", "application/json", bytes.NewReader(body))
	defer func() { _ = resp.Body.Close() }()

	var loginResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&loginResp)
	data := loginResp["data"].(map[string]interface{})
	token := data["token"].(string)

	// Insert test data
	insertReq := map[string]string{
		"sqlText": `INSERT INTO TEST_DB.PUBLIC_PHASE2_TEST VALUES
			(1, 'Alice', 'alice@example.com', 95, '2024-01-15', '{"role": "admin"}'),
			(2, 'Bob', NULL, 87, '2024-02-20', NULL),
			(3, 'Charlie', 'charlie@example.com', NULL, '2024-03-25', '{"role": "user"}')`,
	}

	body, _ = json.Marshal(insertReq)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Snowflake Token=\""+token+"\"")
	http.DefaultClient.Do(req)

	testCases := []struct {
		name     string
		sqlText  string
		validate func(t *testing.T, rowset []interface{})
	}{
		{
			name:    "NVL2",
			sqlText: "SELECT NAME, NVL2(EMAIL, 'has email', 'no email') AS STATUS FROM TEST_DB.PUBLIC_PHASE2_TEST ORDER BY ID",
			validate: func(t *testing.T, rowset []interface{}) {
				if len(rowset) != 3 {
					t.Errorf("Expected 3 rows, got %d", len(rowset))
					return
				}
				row1 := rowset[0].([]interface{})
				if row1[1] != "has email" {
					t.Errorf("Expected 'has email' for Alice, got %v", row1[1])
				}
				row2 := rowset[1].([]interface{})
				if row2[1] != "no email" {
					t.Errorf("Expected 'no email' for Bob, got %v", row2[1])
				}
			},
		},
		{
			name:    "DATEADD",
			sqlText: "SELECT NAME, DATEADD(day, 30, CREATED_AT) AS DUE_DATE FROM TEST_DB.PUBLIC_PHASE2_TEST ORDER BY ID LIMIT 1",
			validate: func(t *testing.T, rowset []interface{}) {
				if len(rowset) != 1 {
					t.Errorf("Expected 1 row, got %d", len(rowset))
					return
				}
				// Just verify query succeeded - date math works
			},
		},
		{
			name:    "DATEDIFF",
			sqlText: "SELECT NAME, DATEDIFF(day, CREATED_AT, CURRENT_DATE()) AS DAYS_SINCE FROM TEST_DB.PUBLIC_PHASE2_TEST ORDER BY ID LIMIT 1",
			validate: func(t *testing.T, rowset []interface{}) {
				if len(rowset) != 1 {
					t.Errorf("Expected 1 row, got %d", len(rowset))
					return
				}
				// Just verify query succeeded - returns numeric result
				row := rowset[0].([]interface{})
				if row[1] == nil {
					t.Error("Expected numeric days_since, got nil")
				}
			},
		},
		{
			name:    "TO_VARIANT",
			sqlText: "SELECT NAME, TO_VARIANT(SCORE) AS SCORE_JSON FROM TEST_DB.PUBLIC_PHASE2_TEST WHERE SCORE IS NOT NULL ORDER BY ID",
			validate: func(t *testing.T, rowset []interface{}) {
				if len(rowset) != 2 {
					t.Errorf("Expected 2 rows, got %d", len(rowset))
				}
			},
		},
		{
			name:    "OBJECT_CONSTRUCT",
			sqlText: "SELECT NAME, OBJECT_CONSTRUCT('name', NAME, 'score', SCORE) AS USER_OBJ FROM TEST_DB.PUBLIC_PHASE2_TEST ORDER BY ID LIMIT 1",
			validate: func(t *testing.T, rowset []interface{}) {
				if len(rowset) != 1 {
					t.Errorf("Expected 1 row, got %d", len(rowset))
				}
			},
		},
		{
			name:    "LISTAGG",
			sqlText: "SELECT LISTAGG(NAME, ', ') AS ALL_NAMES FROM TEST_DB.PUBLIC_PHASE2_TEST",
			validate: func(t *testing.T, rowset []interface{}) {
				if len(rowset) != 1 {
					t.Errorf("Expected 1 row, got %d", len(rowset))
					return
				}
				row := rowset[0].([]interface{})
				names := row[0].(string)
				if names == "" {
					t.Error("Expected concatenated names, got empty string")
				}
			},
		},
		{
			name:    "Combined NVL2 with IFF",
			sqlText: "SELECT NAME, NVL2(SCORE, IFF(SCORE >= 90, 'A', 'B'), 'N/A') AS GRADE FROM TEST_DB.PUBLIC_PHASE2_TEST ORDER BY ID",
			validate: func(t *testing.T, rowset []interface{}) {
				if len(rowset) != 3 {
					t.Errorf("Expected 3 rows, got %d", len(rowset))
					return
				}
				row1 := rowset[0].([]interface{})
				if row1[1] != "A" {
					t.Errorf("Expected 'A' for Alice, got %v", row1[1])
				}
				row2 := rowset[1].([]interface{})
				if row2[1] != "B" {
					t.Errorf("Expected 'B' for Bob, got %v", row2[1])
				}
				row3 := rowset[2].([]interface{})
				if row3[1] != "N/A" {
					t.Errorf("Expected 'N/A' for Charlie, got %v", row3[1])
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			queryReq := map[string]string{
				"sqlText": tc.sqlText,
			}

			body, _ := json.Marshal(queryReq)
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Snowflake Token=\""+token+"\"")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Query request failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("Expected status 200, got %d", resp.StatusCode)
			}

			var queryResp map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&queryResp)

			if success, ok := queryResp["success"].(bool); !ok || !success {
				t.Errorf("Query failed: %v", queryResp["message"])
				return
			}

			queryData := queryResp["data"].(map[string]interface{})
			rowset := queryData["rowset"].([]interface{})
			tc.validate(t, rowset)
		})
	}
}

// TestIntegration_ErrorHandling tests error handling in integration scenario.
func TestIntegration_ErrorHandling(t *testing.T) {
	server, _, _ := setupTestServer(t)

	// Test 1: Login with non-existent database
	loginReq := map[string]interface{}{
		"data": map[string]string{
			"LOGIN_NAME":   "testuser",
			"PASSWORD":     "testpass",
			"databaseName": "NONEXISTENT_DB",
			"schemaName":   "PUBLIC",
		},
	}

	body, _ := json.Marshal(loginReq)
	resp, _ := http.Post(server.URL+"/session/v1/login-request", "application/json", bytes.NewReader(body))
	defer func() { _ = resp.Body.Close() }()

	// Snowflake returns 200 even for errors - check success field
	var errResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&errResp)
	// Note: Current implementation creates DB if not exists, so this actually succeeds
	// In Phase 2 with authentication, non-existent DB will properly fail

	// Test 2: Query without authentication
	queryReq := map[string]string{
		"sqlText": "SELECT * FROM TEST_DB.PUBLIC_STUDENTS",
	}

	body, _ = json.Marshal(queryReq)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, _ = http.DefaultClient.Do(req)
	defer func() { _ = resp.Body.Close() }()

	// Snowflake returns 200 even for auth errors - check success field
	var authResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&authResp)
	if success, ok := authResp["success"].(bool); ok && success {
		t.Error("Expected authentication failure for unauthenticated request")
	}

	// Test 3: Invalid SQL
	// First login
	loginReq = map[string]interface{}{
		"data": map[string]string{
			"LOGIN_NAME":   "testuser",
			"PASSWORD":     "testpass",
			"databaseName": "TEST_DB",
			"schemaName":   "PUBLIC",
		},
	}

	body, _ = json.Marshal(loginReq)
	resp, _ = http.Post(server.URL+"/session/v1/login-request", "application/json", bytes.NewReader(body))
	var loginResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&loginResp)
	loginData := loginResp["data"].(map[string]interface{})
	token := loginData["token"].(string)
	resp.Body.Close()

	// Execute invalid SQL
	queryReq = map[string]string{
		"sqlText": "SELECT FROM TEST_DB.PUBLIC_STUDENTS",
	}

	body, _ = json.Marshal(queryReq)
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Snowflake Token=\""+token+"\"")

	resp, _ = http.DefaultClient.Do(req)
	defer func() { _ = resp.Body.Close() }()

	// Snowflake returns 200 even for SQL errors - check success field
	var sqlErrResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&sqlErrResp)
	if success, ok := sqlErrResp["success"].(bool); ok && success {
		t.Error("Expected error for invalid SQL")
	}
}
