package query

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/go-cmp/cmp"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

// setupTestExecutor creates a test executor with in-memory DuckDB.
func setupTestExecutor(t *testing.T) (*Executor, *metadata.Repository) {
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

	executor := NewExecutor(mgr, repo)
	return executor, repo
}

// TestExecutor_ExecuteQuery tests basic query execution.
func TestExecutor_ExecuteQuery(t *testing.T) {
	executor, repo := setupTestExecutor(t)
	ctx := context.Background()

	// Setup: Create database, schema, and table
	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "AGE", Type: "INTEGER"},
	}
	_, err = repo.CreateTable(ctx, schema.ID, "USERS", columns, "")
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Insert test data
	insertSQL := "INSERT INTO TEST_DB.PUBLIC_USERS VALUES (1, 'Alice', 30), (2, 'Bob', 25)"
	_, err = executor.Execute(ctx, insertSQL)
	if err != nil {
		t.Fatalf("Insert error = %v", err)
	}

	// Test simple SELECT
	selectSQL := "SELECT * FROM TEST_DB.PUBLIC_USERS ORDER BY ID"
	result, err := executor.Query(ctx, selectSQL)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	// Verify first row
	if len(result.Rows) > 0 {
		firstRow := result.Rows[0]
		if len(firstRow) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(firstRow))
		}
	}
}

// TestExecutor_ExecuteWithTranslation tests query execution with Snowflake SQL translation.
func TestExecutor_ExecuteWithTranslation(t *testing.T) { //nolint:gocyclo // Test covers multiple execution cases
	executor, repo := setupTestExecutor(t)
	ctx := context.Background()

	// Setup database and table
	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "AGE", Type: "INTEGER"},
		{Name: "EMAIL", Type: "VARCHAR", Nullable: true},
	}
	_, err = repo.CreateTable(ctx, schema.ID, "USERS", columns, "")
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Insert test data
	insertSQL := "INSERT INTO TEST_DB.PUBLIC_USERS VALUES (1, 'Alice', 30, 'alice@example.com'), (2, 'Bob', 17, 'bob@example.com')"
	_, err = executor.Execute(ctx, insertSQL)
	if err != nil {
		t.Fatalf("Insert error = %v", err)
	}

	tests := []struct {
		name          string
		sql           string
		expectedRows  int
		expectedCols  int
		checkFirstRow func(*testing.T, []interface{})
	}{
		{
			name:         "IFFTranslation",
			sql:          "SELECT NAME, IFF(AGE >= 18, 'adult', 'minor') AS category FROM TEST_DB.PUBLIC_USERS ORDER BY ID",
			expectedRows: 2,
			expectedCols: 2,
			checkFirstRow: func(t *testing.T, row []interface{}) {
				if row[0] != "Alice" {
					t.Errorf("Expected name 'Alice', got %v", row[0])
				}
				if row[1] != "adult" {
					t.Errorf("Expected category 'adult', got %v", row[1])
				}
			},
		},
		{
			name:         "NVLTranslation",
			sql:          "SELECT NAME, NVL(EMAIL, 'no-email') AS email FROM TEST_DB.PUBLIC_USERS WHERE ID = 2",
			expectedRows: 1,
			expectedCols: 2,
			checkFirstRow: func(t *testing.T, row []interface{}) {
				if row[0] != "Bob" {
					t.Errorf("Expected name 'Bob', got %v", row[0])
				}
				// NVL should return the actual email since it's not NULL
				if row[1] != "bob@example.com" {
					t.Errorf("Expected email 'bob@example.com', got %v", row[1])
				}
			},
		},
		{
			name:         "CONCATTranslation",
			sql:          "SELECT CONCAT(NAME, ' is ', NAME) AS display FROM TEST_DB.PUBLIC_USERS WHERE ID = 1",
			expectedRows: 1,
			expectedCols: 1,
			checkFirstRow: func(t *testing.T, row []interface{}) {
				expected := "Alice is Alice"
				if row[0] != expected {
					t.Errorf("Expected '%s', got %v", expected, row[0])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Query(ctx, tt.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}

			if len(result.Rows) != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, len(result.Rows))
			}

			if len(result.Rows) > 0 && len(result.Rows[0]) != tt.expectedCols {
				t.Errorf("Expected %d columns, got %d", tt.expectedCols, len(result.Rows[0]))
			}

			if tt.checkFirstRow != nil && len(result.Rows) > 0 {
				tt.checkFirstRow(t, result.Rows[0])
			}
		})
	}
}

// TestExecutor_DDLOperations tests DDL statement execution (CREATE, DROP).
func TestExecutor_DDLOperations(t *testing.T) {
	executor, repo := setupTestExecutor(t)
	ctx := context.Background()

	// Create database
	db, err := repo.CreateDatabase(ctx, "DDL_TEST", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	_ = schema // Suppress unused warning

	// Create table via executor
	createTableSQL := `CREATE TABLE DDL_TEST.PUBLIC_EMPLOYEES (
		ID INTEGER PRIMARY KEY,
		NAME VARCHAR NOT NULL,
		SALARY DOUBLE
	)`

	_, err = executor.Execute(ctx, createTableSQL)
	if err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}

	// Note: We don't verify metadata here because full SQL parsing
	// for CREATE TABLE is not yet implemented. The table should exist
	// in DuckDB though, which we verify by inserting and querying data.

	// Insert and query data
	insertSQL := "INSERT INTO DDL_TEST.PUBLIC_EMPLOYEES VALUES (1, 'John', 50000.0)"
	_, err = executor.Execute(ctx, insertSQL)
	if err != nil {
		t.Fatalf("INSERT error = %v", err)
	}

	selectSQL := "SELECT NAME, SALARY FROM DDL_TEST.PUBLIC_EMPLOYEES WHERE ID = 1"
	result, err := executor.Query(ctx, selectSQL)
	if err != nil {
		t.Fatalf("SELECT error = %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

// TestExecutor_ErrorHandling tests error cases.
func TestExecutor_ErrorHandling(t *testing.T) {
	executor, _ := setupTestExecutor(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "InvalidSQL",
			sql:     "SELECT FROM",
			wantErr: true,
		},
		{
			name:    "NonExistentTable",
			sql:     "SELECT * FROM NONEXISTENT.TABLE",
			wantErr: true,
		},
		{
			name:    "EmptySQL",
			sql:     "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := executor.Query(ctx, tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestExecutor_GetColumnInfo tests column metadata retrieval.
func TestExecutor_GetColumnInfo(t *testing.T) {
	executor, repo := setupTestExecutor(t)
	ctx := context.Background()

	// Setup
	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "SALARY", Type: "DOUBLE"},
	}
	_, err = repo.CreateTable(ctx, schema.ID, "EMPLOYEES", columns, "")
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Query and check column info
	selectSQL := "SELECT * FROM TEST_DB.PUBLIC_EMPLOYEES LIMIT 0"
	result, err := executor.Query(ctx, selectSQL)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	expectedColumns := []string{"ID", "NAME", "SALARY"}
	if diff := cmp.Diff(expectedColumns, result.Columns); diff != "" {
		t.Errorf("Column names mismatch (-want +got):\n%s", diff)
	}
}

// TestExecutor_QueryWithBindings tests query execution with parameter bindings.
func TestExecutor_QueryWithBindings(t *testing.T) {
	executor, _ := setupTestExecutor(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		sql          string
		bindings     map[string]*BindingValue
		expectedRows int
		checkValue   func(t *testing.T, rows [][]interface{})
	}{
		{
			name: "IntegerBinding",
			sql:  "SELECT :1 AS num",
			bindings: map[string]*BindingValue{
				"1": {Type: "FIXED", Value: "42"},
			},
			expectedRows: 1,
			checkValue: func(t *testing.T, rows [][]interface{}) {
				if rows[0][0] != int64(42) && rows[0][0] != int32(42) {
					t.Errorf("Expected 42, got %v (type: %T)", rows[0][0], rows[0][0])
				}
			},
		},
		{
			name: "TextBinding",
			sql:  "SELECT :1 AS name",
			bindings: map[string]*BindingValue{
				"1": {Type: "TEXT", Value: "Hello World"},
			},
			expectedRows: 1,
			checkValue: func(t *testing.T, rows [][]interface{}) {
				if rows[0][0] != "Hello World" {
					t.Errorf("Expected 'Hello World', got %v", rows[0][0])
				}
			},
		},
		{
			name: "MultipleBindings",
			sql:  "SELECT :1 AS a, :2 AS b, :3 AS c",
			bindings: map[string]*BindingValue{
				"1": {Type: "FIXED", Value: "1"},
				"2": {Type: "TEXT", Value: "test"},
				"3": {Type: "REAL", Value: "3.14"},
			},
			expectedRows: 1,
			checkValue: func(t *testing.T, rows [][]interface{}) {
				if len(rows[0]) != 3 {
					t.Errorf("Expected 3 columns, got %d", len(rows[0]))
				}
			},
		},
		{
			name: "BooleanBindingTrue",
			sql:  "SELECT :1 AS flag",
			bindings: map[string]*BindingValue{
				"1": {Type: "BOOLEAN", Value: "true"},
			},
			expectedRows: 1,
			checkValue: func(t *testing.T, rows [][]interface{}) {
				if rows[0][0] != true {
					t.Errorf("Expected true, got %v", rows[0][0])
				}
			},
		},
		{
			name: "BooleanBindingFalse",
			sql:  "SELECT :1 AS flag",
			bindings: map[string]*BindingValue{
				"1": {Type: "BOOLEAN", Value: "false"},
			},
			expectedRows: 1,
			checkValue: func(t *testing.T, rows [][]interface{}) {
				if rows[0][0] != false {
					t.Errorf("Expected false, got %v", rows[0][0])
				}
			},
		},
		{
			name: "TextWithSpecialChars",
			sql:  "SELECT :1 AS text",
			bindings: map[string]*BindingValue{
				"1": {Type: "TEXT", Value: "hello-world_123"},
			},
			expectedRows: 1,
			checkValue: func(t *testing.T, rows [][]interface{}) {
				if rows[0][0] != "hello-world_123" {
					t.Errorf("Expected 'hello-world_123', got %v", rows[0][0])
				}
			},
		},
		{
			name:         "NoBindings",
			sql:          "SELECT 1 AS num",
			bindings:     nil,
			expectedRows: 1,
		},
		{
			name:         "EmptyBindings",
			sql:          "SELECT 1 AS num",
			bindings:     map[string]*BindingValue{},
			expectedRows: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.QueryWithBindings(ctx, tt.sql, tt.bindings)
			if err != nil {
				t.Fatalf("QueryWithBindings() error = %v", err)
			}

			if len(result.Rows) != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, len(result.Rows))
			}

			if tt.checkValue != nil && len(result.Rows) > 0 {
				tt.checkValue(t, result.Rows)
			}
		})
	}
}

// TestFormatBindingValue tests the formatBindingValue helper function.
func TestFormatBindingValue(t *testing.T) {
	tests := []struct {
		name     string
		binding  *BindingValue
		expected string
		wantErr  bool
	}{
		{
			name:     "NilBinding",
			binding:  nil,
			expected: "NULL",
		},
		{
			name:     "TextValue",
			binding:  &BindingValue{Type: "TEXT", Value: "hello"},
			expected: "'hello'",
		},
		{
			name:     "TextWithQuotes",
			binding:  &BindingValue{Type: "TEXT", Value: "it's"},
			expected: "'it''s'",
		},
		{
			name:     "IntegerValue",
			binding:  &BindingValue{Type: "FIXED", Value: "123"},
			expected: "123",
		},
		{
			name:     "RealValue",
			binding:  &BindingValue{Type: "REAL", Value: "3.14"},
			expected: "3.14",
		},
		{
			name:     "BooleanTrue",
			binding:  &BindingValue{Type: "BOOLEAN", Value: "true"},
			expected: "TRUE",
		},
		{
			name:     "BooleanFalse",
			binding:  &BindingValue{Type: "BOOLEAN", Value: "false"},
			expected: "FALSE",
		},
		{
			name:     "DateValue",
			binding:  &BindingValue{Type: "DATE", Value: "2024-01-15"},
			expected: "DATE '2024-01-15'",
		},
		{
			name:     "TimestampValue",
			binding:  &BindingValue{Type: "TIMESTAMP", Value: "2024-01-15 10:30:00"},
			expected: "TIMESTAMP '2024-01-15 10:30:00'",
		},
		{
			name:     "NullType",
			binding:  &BindingValue{Type: "NULL", Value: ""},
			expected: "NULL",
		},
		{
			name:    "InvalidInteger",
			binding: &BindingValue{Type: "FIXED", Value: "not a number"},
			wantErr: true,
		},
		{
			name:    "InvalidReal",
			binding: &BindingValue{Type: "REAL", Value: "not a float"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := formatBindingValue(tt.binding)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// TestExecutor_TransactionStatements tests transaction control statement execution.
func TestExecutor_TransactionStatements(t *testing.T) {
	executor, _ := setupTestExecutor(t)
	ctx := context.Background()

	// Test BEGIN statements
	t.Run("BEGIN", func(t *testing.T) {
		result, err := executor.Execute(ctx, "BEGIN")
		if err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}
		if result == nil {
			t.Error("Expected result, got nil")
		}
		// Rollback to clean up
		_, _ = executor.Execute(ctx, "ROLLBACK")
	})

	t.Run("BEGIN_TRANSACTION", func(t *testing.T) {
		result, err := executor.Execute(ctx, "BEGIN TRANSACTION")
		if err != nil {
			t.Fatalf("BEGIN TRANSACTION failed: %v", err)
		}
		if result == nil {
			t.Error("Expected result, got nil")
		}
		// Rollback to clean up
		_, _ = executor.Execute(ctx, "ROLLBACK")
	})

	t.Run("START_TRANSACTION", func(t *testing.T) {
		result, err := executor.Execute(ctx, "START TRANSACTION")
		if err != nil {
			t.Fatalf("START TRANSACTION failed: %v", err)
		}
		if result == nil {
			t.Error("Expected result, got nil")
		}
		// Rollback to clean up
		_, _ = executor.Execute(ctx, "ROLLBACK")
	})

	// Test COMMIT - requires active transaction
	t.Run("COMMIT", func(t *testing.T) {
		// First start a transaction
		_, err := executor.Execute(ctx, "BEGIN")
		if err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}

		result, err := executor.Execute(ctx, "COMMIT")
		if err != nil {
			t.Fatalf("COMMIT failed: %v", err)
		}
		if result == nil {
			t.Error("Expected result, got nil")
		}
	})

	// Test ROLLBACK - requires active transaction
	t.Run("ROLLBACK", func(t *testing.T) {
		// First start a transaction
		_, err := executor.Execute(ctx, "BEGIN")
		if err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}

		result, err := executor.Execute(ctx, "ROLLBACK")
		if err != nil {
			t.Fatalf("ROLLBACK failed: %v", err)
		}
		if result == nil {
			t.Error("Expected result, got nil")
		}
	})

	// Test full transaction workflow with data
	t.Run("FullTransactionWorkflow", func(t *testing.T) {
		// Create a test table
		_, err := executor.Execute(ctx, "CREATE TABLE IF NOT EXISTS tx_test (id INTEGER)")
		if err != nil {
			t.Fatalf("CREATE TABLE failed: %v", err)
		}

		// Begin transaction
		_, err = executor.Execute(ctx, "BEGIN")
		if err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}

		// Insert data
		_, err = executor.Execute(ctx, "INSERT INTO tx_test VALUES (1)")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}

		// Commit
		_, err = executor.Execute(ctx, "COMMIT")
		if err != nil {
			t.Fatalf("COMMIT failed: %v", err)
		}

		// Verify data was committed
		result, err := executor.Query(ctx, "SELECT * FROM tx_test WHERE id = 1")
		if err != nil {
			t.Fatalf("SELECT failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row after commit, got %d", len(result.Rows))
		}

		// Clean up
		_, _ = executor.Execute(ctx, "DROP TABLE tx_test")
	})
}

// TestTransactionClassifier tests transaction statement classification.
func TestTransactionClassifier(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		isTransaction bool
		isBegin       bool
		isCommit      bool
		isRollback    bool
	}{
		{"BEGIN", "BEGIN", true, true, false, false},
		{"BEGIN_TRANSACTION", "BEGIN TRANSACTION", true, true, false, false},
		{"START_TRANSACTION", "START TRANSACTION", true, true, false, false},
		{"COMMIT", "COMMIT", true, false, true, false},
		{"ROLLBACK", "ROLLBACK", true, false, false, true},
		{"SELECT", "SELECT 1", false, false, false, false},
		{"INSERT", "INSERT INTO t VALUES (1)", false, false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransaction(tt.sql); got != tt.isTransaction {
				t.Errorf("IsTransaction(%q) = %v, want %v", tt.sql, got, tt.isTransaction)
			}
			if got := IsBegin(tt.sql); got != tt.isBegin {
				t.Errorf("IsBegin(%q) = %v, want %v", tt.sql, got, tt.isBegin)
			}
			if got := IsCommit(tt.sql); got != tt.isCommit {
				t.Errorf("IsCommit(%q) = %v, want %v", tt.sql, got, tt.isCommit)
			}
			if got := IsRollback(tt.sql); got != tt.isRollback {
				t.Errorf("IsRollback(%q) = %v, want %v", tt.sql, got, tt.isRollback)
			}
		})
	}
}
