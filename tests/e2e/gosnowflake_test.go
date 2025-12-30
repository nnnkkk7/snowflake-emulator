// tests/e2e/gosnowflake_test.go - gosnowflake driver compatibility tests
//
// These tests verify that the emulator is compatible with the official
// gosnowflake driver. Tests MUST PASS for compatibility to be guaranteed.
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
	"github.com/nnnkkk7/snowflake-emulator/pkg/session"
	"github.com/nnnkkk7/snowflake-emulator/server/handlers"
	_ "github.com/snowflakedb/gosnowflake"
)

// capturedLoginRequest stores the last login request body for debugging
var capturedLoginRequest []byte

// capturedQueryRequest stores the last query request body for debugging
var capturedQueryRequest []byte

// setupTestEmulator creates an in-process emulator server for testing.
func setupTestEmulator(t *testing.T) *httptest.Server {
	t.Helper()

	// Reset captured requests for each test
	capturedLoginRequest = nil
	capturedQueryRequest = nil

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

	sessionMgr := session.NewManager(1 * time.Hour)
	executor := query.NewExecutor(connMgr, repo)

	sessionHandler := handlers.NewSessionHandler(sessionMgr, repo)
	queryHandler := handlers.NewQueryHandler(executor, sessionMgr)

	r := chi.NewRouter()

	// gosnowflake sends POST /session?delete=true to close session
	// The "delete=true" is a query parameter, not HTTP method
	r.Post("/session", sessionHandler.CloseSession)

	// Debug wrapper for login to capture what gosnowflake sends
	r.Post("/session/v1/login-request", func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		capturedLoginRequest = body
		req.Body = io.NopCloser(bytes.NewReader(body))
		sessionHandler.Login(w, req)
	})

	r.Post("/session/token-request", sessionHandler.TokenRequest)
	r.Post("/session/heartbeat", sessionHandler.Heartbeat)
	r.Post("/session/renew", sessionHandler.RenewSession)
	r.Post("/session/logout", sessionHandler.Logout)

	// Telemetry endpoint - accept and ignore
	r.Post("/telemetry/send", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	})

	// Debug wrapper for query to capture what gosnowflake sends
	r.Post("/queries/v1/query-request", func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		capturedQueryRequest = body
		req.Body = io.NopCloser(bytes.NewReader(body))
		queryHandler.ExecuteQuery(w, req)
	})

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := httptest.NewServer(r)
	t.Cleanup(server.Close)

	return server
}

// logCapturedRequests outputs captured request bodies for debugging on failure.
func logCapturedRequests(t *testing.T) {
	t.Helper()
	if len(capturedLoginRequest) > 0 {
		t.Logf("Captured LOGIN request:\n%s", string(capturedLoginRequest))
	}
	if len(capturedQueryRequest) > 0 {
		t.Logf("Captured QUERY request:\n%s", string(capturedQueryRequest))
	}
}

// TestGosnowflake_BasicConnection tests basic gosnowflake driver connection.
// This test MUST PASS for gosnowflake compatibility.
func TestGosnowflake_BasicConnection(t *testing.T) {
	server := setupTestEmulator(t)
	hostPort := server.URL[7:] // Remove "http://"

	dsn := fmt.Sprintf("testuser:testpass@%s/TEST_DB/PUBLIC?account=testaccount&protocol=http&loginTimeout=5", hostPort)
	t.Logf("DSN: %s", dsn)

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test 1: Ping MUST succeed
	if err := db.PingContext(ctx); err != nil {
		logCapturedRequests(t)
		t.Fatalf("Ping failed (gosnowflake compatibility broken): %v", err)
	}
	t.Log("Ping: OK")

	// Test 2: Simple SELECT MUST succeed
	rows, err := db.QueryContext(ctx, "SELECT 1 AS test")
	if err != nil {
		logCapturedRequests(t)
		t.Fatalf("SELECT 1 failed (gosnowflake compatibility broken): %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected 1 row, got 0")
	}

	var val int
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if val != 1 {
		t.Fatalf("Expected 1, got %d", val)
	}
	t.Log("SELECT 1: OK")
}

// TestGosnowflake_FunctionTranslations tests SQL function translations via gosnowflake.
// All functions MUST work for gosnowflake compatibility.
func TestGosnowflake_FunctionTranslations(t *testing.T) {
	server := setupTestEmulator(t)
	hostPort := server.URL[7:]

	dsn := fmt.Sprintf("testuser:testpass@%s/TEST_DB/PUBLIC?account=testaccount&protocol=http&loginTimeout=5", hostPort)

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Verify connection works before testing functions
	if err := db.PingContext(ctx); err != nil {
		logCapturedRequests(t)
		t.Fatalf("Connection failed: %v", err)
	}

	// Create test table for aggregate function tests
	_, err = db.ExecContext(ctx, `CREATE TABLE test_names (id INTEGER, name VARCHAR)`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO test_names VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	testCases := []struct {
		name     string
		sql      string
		validate func(t *testing.T, result any)
	}{
		{
			name: "IFF_true",
			sql:  "SELECT IFF(1 = 1, 'yes', 'no')",
			validate: func(t *testing.T, result any) {
				if result != "yes" {
					t.Errorf("Expected 'yes', got %v", result)
				}
			},
		},
		{
			name: "IFF_false",
			sql:  "SELECT IFF(1 = 2, 'yes', 'no')",
			validate: func(t *testing.T, result any) {
				if result != "no" {
					t.Errorf("Expected 'no', got %v", result)
				}
			},
		},
		{
			name: "NVL_null",
			sql:  "SELECT NVL(NULL, 'default')",
			validate: func(t *testing.T, result any) {
				if result != "default" {
					t.Errorf("Expected 'default', got %v", result)
				}
			},
		},
		{
			name: "NVL_not_null",
			sql:  "SELECT NVL('value', 'default')",
			validate: func(t *testing.T, result any) {
				if result != "value" {
					t.Errorf("Expected 'value', got %v", result)
				}
			},
		},
		{
			name: "NVL2_not_null",
			sql:  "SELECT NVL2('value', 'has value', 'no value')",
			validate: func(t *testing.T, result any) {
				if result != "has value" {
					t.Errorf("Expected 'has value', got %v", result)
				}
			},
		},
		{
			name: "NVL2_null",
			sql:  "SELECT NVL2(NULL, 'has value', 'no value')",
			validate: func(t *testing.T, result any) {
				if result != "no value" {
					t.Errorf("Expected 'no value', got %v", result)
				}
			},
		},
		{
			name: "DATEADD",
			sql:  "SELECT DATEADD(day, 7, '2024-01-01')",
			validate: func(t *testing.T, result any) {
				// Result should be a time.Time representing 2024-01-08
				if result == nil {
					t.Fatal("Expected date result, got nil")
				}
				switch v := result.(type) {
				case time.Time:
					expected := time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)
					if !v.Equal(expected) {
						t.Errorf("Expected %v, got %v", expected, v)
					}
				case string:
					// Rowset converts dates to strings, verify it contains 2024-01-08
					if !strings.Contains(v, "2024-01-08") {
						t.Errorf("Expected date string containing '2024-01-08', got %q", v)
					}
				default:
					t.Fatalf("Expected time.Time or string, got %T (%v)", result, result)
				}
			},
		},
		{
			name: "DATEDIFF",
			sql:  "SELECT DATEDIFF(day, '2024-01-01', '2024-01-10')",
			validate: func(t *testing.T, result any) {
				// Result should be exactly 9 days
				// Note: rowset values are converted to strings, so we may receive "9" as string
				if result == nil {
					t.Fatal("Expected result, got nil")
				}
				var diff int64
				switch v := result.(type) {
				case int64:
					diff = v
				case int32:
					diff = int64(v)
				case int:
					diff = int64(v)
				case float64:
					diff = int64(v)
				case string:
					// Parse string value
					if v == "9" {
						diff = 9
					} else {
						t.Fatalf("Expected '9', got string %q", v)
					}
				default:
					t.Fatalf("Expected numeric or string type, got %T (%v)", result, result)
				}
				if diff != 9 {
					t.Errorf("Expected 9 days, got %d", diff)
				}
			},
		},
		{
			name: "LISTAGG",
			sql:  "SELECT LISTAGG(name, ', ') FROM test_names",
			validate: func(t *testing.T, result any) {
				// Result should contain all names separated by comma
				if result == nil {
					t.Fatal("Expected string result, got nil")
				}
				strVal, ok := result.(string)
				if !ok {
					t.Fatalf("Expected string, got %T", result)
				}
				// Check that all names are present (order may vary)
				if !containsAll(strVal, []string{"Alice", "Bob", "Charlie"}) {
					t.Errorf("Expected all names in result, got %q", strVal)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capturedQueryRequest = nil

			rows, err := db.QueryContext(ctx, tc.sql)
			if err != nil {
				logCapturedRequests(t)
				t.Fatalf("%s query failed: %v", tc.name, err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatalf("%s: expected 1 row, got 0", tc.name)
			}

			var result any
			if err := rows.Scan(&result); err != nil {
				t.Fatalf("%s scan failed: %v", tc.name, err)
			}

			tc.validate(t, result)
			t.Logf("%s: OK (result=%v)", tc.name, result)
		})
	}
}

// containsAll checks if str contains all substrings.
func containsAll(str string, substrings []string) bool {
	for _, sub := range substrings {
		if !strings.Contains(str, sub) {
			return false
		}
	}
	return true
}

// TestHTTPAPI_DirectConnection tests HTTP API directly without gosnowflake.
// This is a basic sanity check for the HTTP layer.
func TestHTTPAPI_DirectConnection(t *testing.T) {
	server := setupTestEmulator(t)

	resp, err := http.Get(server.URL + "/health")
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	t.Log("Health check: OK")
}

// TestHTTPAPI_SessionClose tests POST /session?delete=true endpoint directly.
func TestHTTPAPI_SessionClose(t *testing.T) {
	server := setupTestEmulator(t)

	// Create a POST request to /session?delete=true (gosnowflake uses POST, not DELETE)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/session?delete=true", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Snowflake Token=\"test-token\"")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST /session?delete=true failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	t.Logf("POST /session?delete=true response: status=%d, body=%s", resp.StatusCode, string(body))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	t.Log("POST /session?delete=true: OK")
}
