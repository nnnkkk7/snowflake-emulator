package contentdata

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

// setupTestRepository creates a test repository with in-memory DuckDB.
func setupTestRepository(t *testing.T) *Repository {
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
	metaRepo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create metadata repository: %v", err)
	}

	contentRepo := NewRepository(mgr, metaRepo)

	// Create test database and schema
	ctx := context.Background()
	database, err := metaRepo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	_, err = metaRepo.CreateSchema(ctx, database.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	return contentRepo
}

// TestRepository_CreateTable tests table creation in DuckDB.
func TestRepository_CreateTable(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true, Nullable: false},
		{Name: "NAME", Type: "VARCHAR", Nullable: true},
		{Name: "VALUE", Type: "NUMBER", Nullable: true},
	}

	// Create table
	err := repo.CreateTable(ctx, "TEST_DB", "PUBLIC", "TEST_TABLE", columns)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Verify table exists by querying DuckDB information schema
	query := "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'PUBLIC_TEST_TABLE'"
	rows, err := repo.mgr.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to query table existence: %v", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		rows.Scan(&count)
	}

	if count != 1 {
		t.Errorf("Expected 1 table, got %d", count)
	}
}

// TestRepository_DropTable tests table dropping in DuckDB.
func TestRepository_DropTable(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
	}

	// Create table first
	err := repo.CreateTable(ctx, "TEST_DB", "PUBLIC", "TEST_TABLE", columns)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Drop table
	err = repo.DropTable(ctx, "TEST_DB", "PUBLIC", "TEST_TABLE")
	if err != nil {
		t.Fatalf("DropTable() error = %v", err)
	}

	// Verify table doesn't exist
	query := "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'PUBLIC_TEST_TABLE'"
	rows, err := repo.mgr.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to query table existence: %v", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		rows.Scan(&count)
	}

	if count != 0 {
		t.Errorf("Expected 0 tables after drop, got %d", count)
	}
}

// TestRepository_InsertData tests data insertion.
func TestRepository_InsertData(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "VALUE", Type: "INTEGER"},
	}

	// Create table
	err := repo.CreateTable(ctx, "TEST_DB", "PUBLIC", "TEST_TABLE", columns)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Insert data
	insertColumns := []string{"ID", "NAME", "VALUE"}
	values := [][]interface{}{
		{1, "Alice", 100},
		{2, "Bob", 200},
		{3, "Charlie", 300},
	}

	rowsAffected, err := repo.InsertData(ctx, "TEST_DB", "PUBLIC", "TEST_TABLE", insertColumns, values)
	if err != nil {
		t.Fatalf("InsertData() error = %v", err)
	}

	if rowsAffected != 3 {
		t.Errorf("Expected 3 rows affected, got %d", rowsAffected)
	}

	// Verify data was inserted
	query := "SELECT COUNT(*) FROM TEST_DB.PUBLIC_TEST_TABLE"
	rows, err := repo.mgr.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to query row count: %v", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		rows.Scan(&count)
	}

	if count != 3 {
		t.Errorf("Expected 3 rows in table, got %d", count)
	}
}

// TestRepository_ExecuteQuery tests SELECT query execution.
func TestRepository_ExecuteQuery(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	// Create and populate table
	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
	}

	err := repo.CreateTable(ctx, "TEST_DB", "PUBLIC", "TEST_TABLE", columns)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	values := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}
	_, err = repo.InsertData(ctx, "TEST_DB", "PUBLIC", "TEST_TABLE", []string{"ID", "NAME"}, values)
	if err != nil {
		t.Fatalf("InsertData() error = %v", err)
	}

	// Execute query
	query := "SELECT * FROM TEST_DB.PUBLIC_TEST_TABLE ORDER BY ID"
	rows, err := repo.ExecuteQuery(ctx, query)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	defer rows.Close()

	// Count rows
	rowCount := 0
	for rows.Next() {
		rowCount++
	}

	if rowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", rowCount)
	}
}

// TestRepository_ExecuteDML tests INSERT/UPDATE/DELETE execution.
func TestRepository_ExecuteDML(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	// Create table
	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "VALUE", Type: "INTEGER"},
	}

	err := repo.CreateTable(ctx, "TEST_DB", "PUBLIC", "TEST_TABLE", columns)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Test INSERT
	insertSQL := "INSERT INTO TEST_DB.PUBLIC_TEST_TABLE VALUES (1, 100)"
	rowsAffected, err := repo.ExecuteDML(ctx, insertSQL)
	if err != nil {
		t.Fatalf("ExecuteDML(INSERT) error = %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row inserted, got %d", rowsAffected)
	}

	// Test UPDATE
	updateSQL := "UPDATE TEST_DB.PUBLIC_TEST_TABLE SET VALUE = 200 WHERE ID = 1"
	rowsAffected, err = repo.ExecuteDML(ctx, updateSQL)
	if err != nil {
		t.Fatalf("ExecuteDML(UPDATE) error = %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row updated, got %d", rowsAffected)
	}

	// Test DELETE
	deleteSQL := "DELETE FROM TEST_DB.PUBLIC_TEST_TABLE WHERE ID = 1"
	rowsAffected, err = repo.ExecuteDML(ctx, deleteSQL)
	if err != nil {
		t.Fatalf("ExecuteDML(DELETE) error = %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row deleted, got %d", rowsAffected)
	}
}

// TestSnowflakeToDuckDBType tests type mapping.
func TestSnowflakeToDuckDBType(t *testing.T) {
	tests := []struct {
		snowflakeType string
		expectedDuck  string
	}{
		{"NUMBER", "DECIMAL"},
		{"INTEGER", "INTEGER"},
		{"VARCHAR", "VARCHAR"},
		{"TEXT", "VARCHAR"},
		{"BOOLEAN", "BOOLEAN"},
		{"DATE", "DATE"},
		{"TIMESTAMP", "TIMESTAMP"},
		{"TIMESTAMP_NTZ", "TIMESTAMP"},
		{"FLOAT", "DOUBLE"},
		{"VARIANT", "VARCHAR"},
		{"UNKNOWN_TYPE", "VARCHAR"}, // Default
	}

	for _, tt := range tests {
		t.Run(tt.snowflakeType, func(t *testing.T) {
			result := snowflakeToDuckDBType(tt.snowflakeType)
			if result != tt.expectedDuck {
				t.Errorf("snowflakeToDuckDBType(%s) = %s, want %s",
					tt.snowflakeType, result, tt.expectedDuck)
			}
		})
	}
}

// TestRepository_ResolveTableName tests table name resolution.
func TestRepository_ResolveTableName(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	// Resolve table name
	fqtn, err := repo.resolveTableName(ctx, "TEST_DB", "PUBLIC", "TEST_TABLE")
	if err != nil {
		t.Fatalf("resolveTableName() error = %v", err)
	}

	expected := "TEST_DB.PUBLIC_TEST_TABLE"
	if fqtn != expected {
		t.Errorf("Expected %s, got %s", expected, fqtn)
	}
}

// TestRepository_ResolveTableName_NonExistentDatabase tests error handling.
func TestRepository_ResolveTableName_NonExistentDatabase(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	_, err := repo.resolveTableName(ctx, "NONEXISTENT_DB", "PUBLIC", "TEST_TABLE")
	if err == nil {
		t.Error("Expected error for non-existent database")
	}
}
