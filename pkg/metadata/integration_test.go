package metadata

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
)

// TestIntegration_CompleteWorkflow tests the complete metadata + DuckDB workflow.
// This integration test verifies:
// 1. Database creation
// 2. Schema creation
// 3. Table creation with columns
// 4. Data insertion and retrieval
// 5. Cleanup (drop table, schema, database)
func TestIntegration_CompleteWorkflow(t *testing.T) { //nolint:gocyclo // Integration test covers complete workflow
	// Setup: Create in-memory DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := connection.NewManager(db)
	repo, err := NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	ctx := context.Background()

	// Step 1: Create database
	database, err := repo.CreateDatabase(ctx, "TEST_DB", "Test database for integration")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	if database.Name != "TEST_DB" {
		t.Errorf("Database name = %s, want TEST_DB", database.Name)
	}

	// Step 2: Create schema
	schema, err := repo.CreateSchema(ctx, database.ID, "PUBLIC", "Public schema")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	if schema.Name != "PUBLIC" {
		t.Errorf("Schema name = %s, want PUBLIC", schema.Name)
	}
	if schema.DatabaseID != database.ID {
		t.Errorf("Schema.DatabaseID = %s, want %s", schema.DatabaseID, database.ID)
	}

	// Step 3: Create table with columns
	columns := []ColumnDef{
		{Name: "ID", Type: "INTEGER", Nullable: false, PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR", Nullable: false},
		{Name: "AGE", Type: "INTEGER", Nullable: true},
		{Name: "EMAIL", Type: "VARCHAR", Nullable: true},
	}

	table, err := repo.CreateTable(ctx, schema.ID, "USERS", columns, "Users table")
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if table.Name != "USERS" {
		t.Errorf("Table name = %s, want USERS", table.Name)
	}
	if table.SchemaID != schema.ID {
		t.Errorf("Table.SchemaID = %s, want %s", table.SchemaID, schema.ID)
	}

	// Step 4: Insert data into the table
	insertSQL := "INSERT INTO TEST_DB.PUBLIC_USERS (ID, NAME, AGE, EMAIL) VALUES (?, ?, ?, ?)"
	testData := []struct {
		ID    int
		Name  string
		Age   int
		Email string
	}{
		{1, "Alice", 30, "alice@example.com"},
		{2, "Bob", 25, "bob@example.com"},
		{3, "Charlie", 35, "charlie@example.com"},
	}

	for _, data := range testData {
		if _, err := mgr.Exec(ctx, insertSQL, data.ID, data.Name, data.Age, data.Email); err != nil {
			t.Fatalf("Insert data error = %v", err)
		}
	}

	// Step 5: Query the data
	selectSQL := "SELECT ID, NAME, AGE, EMAIL FROM TEST_DB.PUBLIC_USERS ORDER BY ID"
	rows, err := mgr.Query(ctx, selectSQL)
	if err != nil {
		t.Fatalf("Query error = %v", err)
	}
	defer func() { _ = rows.Close() }()

	var results []struct {
		ID    int
		Name  string
		Age   int
		Email string
	}

	for rows.Next() {
		var id, age int
		var name, email string
		if err := rows.Scan(&id, &name, &age, &email); err != nil {
			t.Fatalf("Scan error = %v", err)
		}
		results = append(results, struct {
			ID    int
			Name  string
			Age   int
			Email string
		}{id, name, age, email})
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Rows iteration error = %v", err)
	}

	// Verify data
	if len(results) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(results))
	}

	if diff := cmp.Diff(testData, results); diff != "" {
		t.Errorf("Query results mismatch (-want +got):\n%s", diff)
	}

	// Step 6: Verify metadata retrieval
	retrievedTable, err := repo.GetTable(ctx, table.ID)
	if err != nil {
		t.Fatalf("GetTable() error = %v", err)
	}
	if retrievedTable.Name != "USERS" {
		t.Errorf("Retrieved table name = %s, want USERS", retrievedTable.Name)
	}

	// Step 7: List tables in schema
	tables, err := repo.ListTables(ctx, schema.ID)
	if err != nil {
		t.Fatalf("ListTables() error = %v", err)
	}
	if len(tables) != 1 {
		t.Errorf("Expected 1 table in schema, got %d", len(tables))
	}

	// Step 8: Cleanup - Drop table
	if err := repo.DropTable(ctx, table.ID); err != nil {
		t.Errorf("DropTable() error = %v", err)
	}

	// Verify table is dropped
	_, err = repo.GetTable(ctx, table.ID)
	if err == nil {
		t.Error("Expected error for dropped table, got nil")
	}

	// Step 9: Drop schema
	if err := repo.DropSchema(ctx, schema.ID); err != nil {
		t.Errorf("DropSchema() error = %v", err)
	}

	// Verify schema is dropped
	_, err = repo.GetSchema(ctx, schema.ID)
	if err == nil {
		t.Error("Expected error for dropped schema, got nil")
	}

	// Step 10: Drop database
	if err := repo.DropDatabase(ctx, database.ID); err != nil {
		t.Errorf("DropDatabase() error = %v", err)
	}

	// Verify database is dropped
	_, err = repo.GetDatabase(ctx, database.ID)
	if err == nil {
		t.Error("Expected error for dropped database, got nil")
	}
}

// TestIntegration_MultipleSchemas tests multiple schemas in a single database.
func TestIntegration_MultipleSchemas(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := connection.NewManager(db)
	repo, err := NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	ctx := context.Background()

	// Create database
	database, err := repo.CreateDatabase(ctx, "MULTI_SCHEMA_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	// Create multiple schemas
	schemaNames := []string{"PUBLIC", "STAGING", "ANALYTICS"}

	for _, name := range schemaNames {
		_, err := repo.CreateSchema(ctx, database.ID, name, "")
		if err != nil {
			t.Fatalf("CreateSchema(%s) error = %v", name, err)
		}
	}

	// List all schemas
	schemas, err := repo.ListSchemas(ctx, database.ID)
	if err != nil {
		t.Fatalf("ListSchemas() error = %v", err)
	}

	if len(schemas) != 3 {
		t.Errorf("Expected 3 schemas, got %d", len(schemas))
	}

	// Verify each schema exists
	for _, expectedName := range schemaNames {
		found := false
		for _, schema := range schemas {
			if schema.Name == expectedName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Schema %s not found in list", expectedName)
		}
	}

	// Cleanup
	if err := repo.DropDatabase(ctx, database.ID); err != nil {
		t.Errorf("DropDatabase() error = %v", err)
	}
}

// TestIntegration_MultipleTables tests multiple tables in a single schema.
func TestIntegration_MultipleTables(t *testing.T) { //nolint:gocyclo // Integration test
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := connection.NewManager(db)
	repo, err := NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	ctx := context.Background()

	// Create database and schema
	database, err := repo.CreateDatabase(ctx, "MULTI_TABLE_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, database.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	// Create multiple tables
	tableDefinitions := []struct {
		name    string
		columns []ColumnDef
	}{
		{
			name: "USERS",
			columns: []ColumnDef{
				{Name: "ID", Type: "INTEGER", PrimaryKey: true},
				{Name: "NAME", Type: "VARCHAR"},
			},
		},
		{
			name: "ORDERS",
			columns: []ColumnDef{
				{Name: "ID", Type: "INTEGER", PrimaryKey: true},
				{Name: "USER_ID", Type: "INTEGER"},
				{Name: "AMOUNT", Type: "DOUBLE"},
			},
		},
		{
			name: "PRODUCTS",
			columns: []ColumnDef{
				{Name: "ID", Type: "INTEGER", PrimaryKey: true},
				{Name: "NAME", Type: "VARCHAR"},
				{Name: "PRICE", Type: "DOUBLE"},
			},
		},
	}

	for _, tableDef := range tableDefinitions {
		_, err := repo.CreateTable(ctx, schema.ID, tableDef.name, tableDef.columns, "")
		if err != nil {
			t.Fatalf("CreateTable(%s) error = %v", tableDef.name, err)
		}
	}

	// List all tables
	tables, err := repo.ListTables(ctx, schema.ID)
	if err != nil {
		t.Fatalf("ListTables() error = %v", err)
	}

	if len(tables) != 3 {
		t.Errorf("Expected 3 tables, got %d", len(tables))
	}

	// Verify each table exists
	for _, tableDef := range tableDefinitions {
		found := false
		for _, table := range tables {
			if table.Name == tableDef.name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Table %s not found in list", tableDef.name)
		}
	}

	// Test cross-table insert and query (referential integrity simulation)
	// Insert user
	insertUserSQL := "INSERT INTO MULTI_TABLE_DB.PUBLIC_USERS (ID, NAME) VALUES (?, ?)"
	if _, err := mgr.Exec(ctx, insertUserSQL, 1, "Alice"); err != nil {
		t.Fatalf("Insert user error = %v", err)
	}

	// Insert order for that user
	insertOrderSQL := "INSERT INTO MULTI_TABLE_DB.PUBLIC_ORDERS (ID, USER_ID, AMOUNT) VALUES (?, ?, ?)"
	if _, err := mgr.Exec(ctx, insertOrderSQL, 1, 1, 99.99); err != nil {
		t.Fatalf("Insert order error = %v", err)
	}

	// Query with JOIN
	joinSQL := `
		SELECT u.NAME, o.AMOUNT
		FROM MULTI_TABLE_DB.PUBLIC_USERS u
		JOIN MULTI_TABLE_DB.PUBLIC_ORDERS o ON u.ID = o.USER_ID
	`
	rows, err := mgr.Query(ctx, joinSQL)
	if err != nil {
		t.Fatalf("Join query error = %v", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		t.Fatal("Expected at least one row from join query")
	}

	var name string
	var amount float64
	if err := rows.Scan(&name, &amount); err != nil {
		t.Fatalf("Scan error = %v", err)
	}

	if name != "Alice" {
		t.Errorf("Expected name Alice, got %s", name)
	}
	if amount != 99.99 {
		t.Errorf("Expected amount 99.99, got %f", amount)
	}

	// Cleanup
	if err := repo.DropDatabase(ctx, database.ID); err != nil {
		t.Errorf("DropDatabase() error = %v", err)
	}
}

// TestIntegration_DatabaseIsolation tests that data is properly isolated between databases.
func TestIntegration_DatabaseIsolation(t *testing.T) { //nolint:gocyclo // Integration test
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := connection.NewManager(db)
	repo, err := NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	ctx := context.Background()

	// Create two databases
	db1, err := repo.CreateDatabase(ctx, "DB1", "")
	if err != nil {
		t.Fatalf("CreateDatabase(DB1) error = %v", err)
	}

	db2, err := repo.CreateDatabase(ctx, "DB2", "")
	if err != nil {
		t.Fatalf("CreateDatabase(DB2) error = %v", err)
	}

	// Create schemas with same name in both databases
	schema1, err := repo.CreateSchema(ctx, db1.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema(DB1.PUBLIC) error = %v", err)
	}

	schema2, err := repo.CreateSchema(ctx, db2.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema(DB2.PUBLIC) error = %v", err)
	}

	// Create tables with same name in both schemas
	columns := []ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "VALUE", Type: "VARCHAR"},
	}

	table1, err := repo.CreateTable(ctx, schema1.ID, "DATA", columns, "")
	if err != nil {
		t.Fatalf("CreateTable(DB1.PUBLIC.DATA) error = %v", err)
	}

	table2, err := repo.CreateTable(ctx, schema2.ID, "DATA", columns, "")
	if err != nil {
		t.Fatalf("CreateTable(DB2.PUBLIC.DATA) error = %v", err)
	}

	// Insert different data into each table
	if _, err := mgr.Exec(ctx, "INSERT INTO DB1.PUBLIC_DATA (ID, VALUE) VALUES (?, ?)", 1, "DB1_DATA"); err != nil {
		t.Fatalf("Insert DB1 data error = %v", err)
	}

	if _, err := mgr.Exec(ctx, "INSERT INTO DB2.PUBLIC_DATA (ID, VALUE) VALUES (?, ?)", 1, "DB2_DATA"); err != nil {
		t.Fatalf("Insert DB2 data error = %v", err)
	}

	// Query DB1 table
	row1 := mgr.DB().QueryRowContext(ctx, "SELECT VALUE FROM DB1.PUBLIC_DATA WHERE ID = 1")
	var value1 string
	if err := row1.Scan(&value1); err != nil {
		t.Fatalf("Query DB1 error = %v", err)
	}
	if value1 != "DB1_DATA" {
		t.Errorf("DB1 value = %s, want DB1_DATA", value1)
	}

	// Query DB2 table
	row2 := mgr.DB().QueryRowContext(ctx, "SELECT VALUE FROM DB2.PUBLIC_DATA WHERE ID = 1")
	var value2 string
	if err := row2.Scan(&value2); err != nil {
		t.Fatalf("Query DB2 error = %v", err)
	}
	if value2 != "DB2_DATA" {
		t.Errorf("DB2 value = %s, want DB2_DATA", value2)
	}

	// Cleanup
	_ = table1
	_ = table2
	if err := repo.DropDatabase(ctx, db1.ID); err != nil {
		t.Errorf("DropDatabase(DB1) error = %v", err)
	}
	if err := repo.DropDatabase(ctx, db2.ID); err != nil {
		t.Errorf("DropDatabase(DB2) error = %v", err)
	}
}
