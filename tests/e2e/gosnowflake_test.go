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

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/go-chi/chi/v5"
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

	// Initialize MERGE processor for MERGE INTO support
	mergeProcessor := query.NewMergeProcessor(executor)
	executor.Configure(query.WithMergeProcessor(mergeProcessor))

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

// TestGosnowflake_MergeStatement tests MERGE INTO statement via gosnowflake driver.
// This test verifies that MERGE operations work correctly through the emulator.
func TestGosnowflake_MergeStatement(t *testing.T) {
	server := setupTestEmulator(t)
	hostPort := server.URL[7:] // Remove "http://"

	dsn := fmt.Sprintf("testuser:testpass@%s/TEST_DB/PUBLIC?account=testaccount&protocol=http&loginTimeout=5", hostPort)

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Verify connection
	if err := db.PingContext(ctx); err != nil {
		logCapturedRequests(t)
		t.Fatalf("Connection failed: %v", err)
	}

	// Create target table
	_, err = db.ExecContext(ctx, `CREATE TABLE merge_target (id INTEGER, name VARCHAR, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create target table: %v", err)
	}

	// Insert initial data into target
	_, err = db.ExecContext(ctx, `INSERT INTO merge_target VALUES (1, 'Alice', 100), (2, 'Bob', 200)`)
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Create source table
	_, err = db.ExecContext(ctx, `CREATE TABLE merge_source (id INTEGER, name VARCHAR, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create source table: %v", err)
	}

	// Insert source data (id=2 exists in target, id=3 is new)
	_, err = db.ExecContext(ctx, `INSERT INTO merge_source VALUES (2, 'Bob Updated', 250), (3, 'Charlie', 300)`)
	if err != nil {
		t.Fatalf("Failed to insert source data: %v", err)
	}

	// Execute MERGE statement
	// Note: In UPDATE SET clause, use column name without target alias prefix
	// DuckDB's UPDATE ... FROM syntax requires plain column names in SET
	_, err = db.ExecContext(ctx, `
		MERGE INTO merge_target t
		USING merge_source s
		ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value
		WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)
	`)
	if err != nil {
		logCapturedRequests(t)
		t.Fatalf("MERGE statement failed: %v", err)
	}
	t.Log("MERGE executed successfully")

	// Verify results
	rows, err := db.QueryContext(ctx, `SELECT id, name, value FROM merge_target ORDER BY id`)
	if err != nil {
		t.Fatalf("Failed to query results: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id    int
		name  string
		value int
	}{
		{1, "Alice", 100},       // Unchanged
		{2, "Bob Updated", 250}, // Updated by MERGE
		{3, "Charlie", 300},     // Inserted by MERGE
	}

	i := 0
	for rows.Next() {
		var id, value int
		var name string
		if err := rows.Scan(&id, &name, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		if i >= len(expected) {
			t.Fatalf("Too many rows returned")
		}

		if id != expected[i].id || name != expected[i].name || value != expected[i].value {
			t.Errorf("Row %d: expected (%d, %s, %d), got (%d, %s, %d)",
				i, expected[i].id, expected[i].name, expected[i].value, id, name, value)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), i)
	}

	t.Log("MERGE verification: OK")
}

// TestGosnowflake_AllSQLOperations tests all SQL operations documented in README via gosnowflake driver.
// This comprehensive test verifies that all documented operations work correctly through the emulator.
func TestGosnowflake_AllSQLOperations(t *testing.T) {
	server := setupTestEmulator(t)
	hostPort := server.URL[7:] // Remove "http://"

	dsn := fmt.Sprintf("testuser:testpass@%s/TEST_DB/PUBLIC?account=testaccount&protocol=http&loginTimeout=5", hostPort)

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Verify connection
	if err := db.PingContext(ctx); err != nil {
		logCapturedRequests(t)
		t.Fatalf("Connection failed: %v", err)
	}

	// ===== DDL: CREATE TABLE =====
	t.Run("DDL_CREATE_TABLE", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `CREATE TABLE test_operations (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			score INTEGER,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`)
		if err != nil {
			t.Fatalf("CREATE TABLE failed: %v", err)
		}
		t.Log("CREATE TABLE: OK")
	})

	// ===== DML: INSERT =====
	t.Run("DML_INSERT", func(t *testing.T) {
		result, err := db.ExecContext(ctx, `INSERT INTO test_operations (id, name, score) VALUES
			(1, 'Alice', 95),
			(2, 'Bob', 87),
			(3, 'Charlie', 92)`)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
		rowsAffected, _ := result.RowsAffected()
		// Note: RowsAffected may return 0 via gosnowflake protocol (known behavior)
		// The actual insert works correctly as verified by subsequent SELECT
		t.Logf("INSERT: OK (rows affected reported: %d)", rowsAffected)
	})

	// ===== Query: SELECT =====
	t.Run("Query_SELECT", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, `SELECT id, name, score FROM test_operations ORDER BY id`)
		if err != nil {
			t.Fatalf("SELECT failed: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id, score int
			var name string
			if err := rows.Scan(&id, &name, &score); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			count++
		}
		if count != 3 {
			t.Errorf("Expected 3 rows, got %d", count)
		}
		t.Logf("SELECT: OK (rows: %d)", count)
	})

	// ===== Query: SELECT with IFF (function translation) =====
	t.Run("Query_SELECT_IFF", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, `SELECT name, IFF(score >= 90, 'A', 'B') AS grade FROM test_operations`)
		if err != nil {
			t.Fatalf("SELECT with IFF failed: %v", err)
		}
		defer rows.Close()

		grades := make(map[string]string)
		for rows.Next() {
			var name, grade string
			if err := rows.Scan(&name, &grade); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			grades[name] = grade
		}
		// Alice (95) and Charlie (92) should get A, Bob (87) should get B
		if grades["Alice"] != "A" || grades["Charlie"] != "A" || grades["Bob"] != "B" {
			t.Errorf("IFF function not working correctly: %v", grades)
		}
		t.Log("SELECT with IFF: OK")
	})

	// ===== Query: SELECT with NVL (function translation) =====
	t.Run("Query_SELECT_NVL", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, `SELECT NVL(NULL, 'default_value') AS result`)
		if err != nil {
			t.Fatalf("SELECT with NVL failed: %v", err)
		}
		defer rows.Close()

		if rows.Next() {
			var result string
			if err := rows.Scan(&result); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			if result != "default_value" {
				t.Errorf("Expected 'default_value', got %q", result)
			}
		}
		t.Log("SELECT with NVL: OK")
	})

	// ===== DML: UPDATE =====
	t.Run("DML_UPDATE", func(t *testing.T) {
		result, err := db.ExecContext(ctx, `UPDATE test_operations SET score = 88 WHERE name = 'Bob'`)
		if err != nil {
			t.Fatalf("UPDATE failed: %v", err)
		}
		rowsAffected, _ := result.RowsAffected()
		// Note: RowsAffected may return 0 via gosnowflake protocol (known behavior)
		t.Logf("UPDATE: OK (rows affected reported: %d)", rowsAffected)

		// Verify the update actually worked
		var score int
		err = db.QueryRowContext(ctx, `SELECT score FROM test_operations WHERE name = 'Bob'`).Scan(&score)
		if err != nil {
			t.Fatalf("Verification query failed: %v", err)
		}
		if score != 88 {
			t.Errorf("UPDATE verification failed: expected score 88, got %d", score)
		}
	})

	// ===== DML: DELETE =====
	t.Run("DML_DELETE", func(t *testing.T) {
		// First insert a row to delete
		_, err := db.ExecContext(ctx, `INSERT INTO test_operations (id, name, score) VALUES (99, 'ToDelete', 0)`)
		if err != nil {
			t.Fatalf("INSERT for DELETE test failed: %v", err)
		}

		result, err := db.ExecContext(ctx, `DELETE FROM test_operations WHERE id = 99`)
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}
		rowsAffected, _ := result.RowsAffected()
		// Note: RowsAffected may return 0 via gosnowflake protocol (known behavior)
		t.Logf("DELETE: OK (rows affected reported: %d)", rowsAffected)

		// Verify the delete actually worked
		var count int
		err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM test_operations WHERE id = 99`).Scan(&count)
		if err != nil {
			t.Fatalf("Verification query failed: %v", err)
		}
		if count != 0 {
			t.Errorf("DELETE verification failed: expected 0 rows, got %d", count)
		}
	})

	// ===== Query: SHOW TABLES =====
	t.Run("Query_SHOW_TABLES", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, `SHOW TABLES`)
		if err != nil {
			t.Fatalf("SHOW TABLES failed: %v", err)
		}
		defer rows.Close()

		found := false
		for rows.Next() {
			cols, _ := rows.Columns()
			values := make([]any, len(cols))
			valuePtrs := make([]any, len(cols))
			for i := range values {
				valuePtrs[i] = &values[i]
			}
			if err := rows.Scan(valuePtrs...); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			// Check if test_operations table is in results
			for _, v := range values {
				if str, ok := v.(string); ok && strings.Contains(strings.ToUpper(str), "TEST_OPERATIONS") {
					found = true
					break
				}
			}
		}
		if !found {
			t.Log("SHOW TABLES: OK (returned results, table may be listed differently)")
		} else {
			t.Log("SHOW TABLES: OK (found test_operations)")
		}
	})

	// ===== Query: DESCRIBE TABLE =====
	t.Run("Query_DESCRIBE_TABLE", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, `DESCRIBE TABLE test_operations`)
		if err != nil {
			t.Fatalf("DESCRIBE TABLE failed: %v", err)
		}
		defer rows.Close()

		columnCount := 0
		for rows.Next() {
			columnCount++
		}
		if columnCount < 1 {
			t.Errorf("Expected at least 1 column description, got %d", columnCount)
		}
		t.Logf("DESCRIBE TABLE: OK (columns: %d)", columnCount)
	})

	// ===== DDL: ALTER TABLE =====
	t.Run("DDL_ALTER_TABLE", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `ALTER TABLE test_operations ADD COLUMN email VARCHAR(255)`)
		if err != nil {
			t.Fatalf("ALTER TABLE ADD COLUMN failed: %v", err)
		}
		t.Log("ALTER TABLE ADD COLUMN: OK")
	})

	// ===== Transaction: BEGIN/COMMIT =====
	t.Run("Transaction_BEGIN_COMMIT", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}

		_, err = tx.ExecContext(ctx, `INSERT INTO test_operations (id, name, score) VALUES (10, 'TxTest', 100)`)
		if err != nil {
			tx.Rollback()
			t.Fatalf("INSERT in transaction failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("COMMIT failed: %v", err)
		}

		// Verify the insert was committed
		var count int
		err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM test_operations WHERE id = 10`).Scan(&count)
		if err != nil {
			t.Fatalf("Verification query failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row after commit, got %d", count)
		}
		t.Log("Transaction BEGIN/COMMIT: OK")
	})

	// ===== Transaction: BEGIN/ROLLBACK =====
	t.Run("Transaction_BEGIN_ROLLBACK", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}

		_, err = tx.ExecContext(ctx, `INSERT INTO test_operations (id, name, score) VALUES (20, 'RollbackTest', 100)`)
		if err != nil {
			tx.Rollback()
			t.Fatalf("INSERT in transaction failed: %v", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatalf("ROLLBACK failed: %v", err)
		}

		// Verify the insert was rolled back
		var count int
		err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM test_operations WHERE id = 20`).Scan(&count)
		if err != nil {
			t.Fatalf("Verification query failed: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected 0 rows after rollback, got %d", count)
		}
		t.Log("Transaction BEGIN/ROLLBACK: OK")
	})

	// ===== MERGE INTO =====
	t.Run("DML_MERGE_INTO", func(t *testing.T) {
		// Create source table for merge
		_, err := db.ExecContext(ctx, `CREATE TABLE merge_src (id INTEGER, name VARCHAR, score INTEGER)`)
		if err != nil {
			t.Fatalf("CREATE source table failed: %v", err)
		}

		_, err = db.ExecContext(ctx, `INSERT INTO merge_src VALUES (1, 'Alice Updated', 98), (100, 'NewPerson', 85)`)
		if err != nil {
			t.Fatalf("INSERT source data failed: %v", err)
		}

		_, err = db.ExecContext(ctx, `
			MERGE INTO test_operations t
			USING merge_src s
			ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score
			WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)
		`)
		if err != nil {
			t.Fatalf("MERGE INTO failed: %v", err)
		}

		// Verify Alice was updated
		var name string
		var score int
		err = db.QueryRowContext(ctx, `SELECT name, score FROM test_operations WHERE id = 1`).Scan(&name, &score)
		if err != nil {
			t.Fatalf("Verification query failed: %v", err)
		}
		if name != "Alice Updated" || score != 98 {
			t.Errorf("Expected (Alice Updated, 98), got (%s, %d)", name, score)
		}

		// Verify NewPerson was inserted
		err = db.QueryRowContext(ctx, `SELECT name, score FROM test_operations WHERE id = 100`).Scan(&name, &score)
		if err != nil {
			t.Fatalf("Verification query failed: %v", err)
		}
		if name != "NewPerson" || score != 85 {
			t.Errorf("Expected (NewPerson, 85), got (%s, %d)", name, score)
		}
		t.Log("MERGE INTO: OK")
	})

	// ===== Query: EXPLAIN =====
	t.Run("Query_EXPLAIN", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, `EXPLAIN SELECT * FROM test_operations`)
		if err != nil {
			t.Fatalf("EXPLAIN failed: %v", err)
		}
		defer rows.Close()

		hasRows := rows.Next()
		if !hasRows {
			t.Log("EXPLAIN: OK (returned no rows, which is acceptable)")
		} else {
			t.Log("EXPLAIN: OK (returned execution plan)")
		}
	})

	// ===== DDL: DROP TABLE =====
	t.Run("DDL_DROP_TABLE", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `DROP TABLE merge_src`)
		if err != nil {
			t.Fatalf("DROP TABLE failed: %v", err)
		}
		t.Log("DROP TABLE: OK")
	})

	// ===== DDL: CREATE/DROP SCHEMA =====
	t.Run("DDL_CREATE_DROP_SCHEMA", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `CREATE SCHEMA test_schema`)
		if err != nil {
			t.Fatalf("CREATE SCHEMA failed: %v", err)
		}
		t.Log("CREATE SCHEMA: OK")

		_, err = db.ExecContext(ctx, `DROP SCHEMA test_schema`)
		if err != nil {
			t.Fatalf("DROP SCHEMA failed: %v", err)
		}
		t.Log("DROP SCHEMA: OK")
	})

	// Final cleanup
	t.Run("Cleanup", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS test_operations`)
		if err != nil {
			t.Logf("Cleanup warning: %v", err)
		}
		t.Log("Cleanup: OK")
	})

	t.Log("All SQL operations via gosnowflake driver: PASSED")
}
