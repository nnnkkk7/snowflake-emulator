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
