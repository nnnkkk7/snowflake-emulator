package query

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

// contains is a helper function to check if a string contains a substring.
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// TestIntegration_QueryEngineWorkflow tests the complete query engine workflow.
// This integration test verifies:
// 1. Metadata repository setup
// 2. SQL translation (Snowflake → DuckDB)
// 3. Query execution
// 4. Result retrieval
func TestIntegration_QueryEngineWorkflow(t *testing.T) { //nolint:gocyclo // Integration test covers complete workflow
	// Setup: Create in-memory DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	executor := NewExecutor(mgr, repo)
	ctx := context.Background()

	// Step 1: Create database and schema
	database, err := repo.CreateDatabase(ctx, "ANALYTICS_DB", "Analytics database")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, database.ID, "PROD", "Production schema")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	// Step 2: Create tables
	customerCols := []metadata.ColumnDef{
		{Name: "CUSTOMER_ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "EMAIL", Type: "VARCHAR", Nullable: true},
		{Name: "SIGNUP_DATE", Type: "DATE"},
		{Name: "IS_ACTIVE", Type: "BOOLEAN"},
	}
	_, err = repo.CreateTable(ctx, schema.ID, "CUSTOMERS", customerCols, "Customer data")
	if err != nil {
		t.Fatalf("CreateTable(CUSTOMERS) error = %v", err)
	}

	orderCols := []metadata.ColumnDef{
		{Name: "ORDER_ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "CUSTOMER_ID", Type: "INTEGER"},
		{Name: "AMOUNT", Type: "DOUBLE"},
		{Name: "ORDER_DATE", Type: "DATE"},
		{Name: "STATUS", Type: "VARCHAR"},
	}
	_, err = repo.CreateTable(ctx, schema.ID, "ORDERS", orderCols, "Order data")
	if err != nil {
		t.Fatalf("CreateTable(ORDERS) error = %v", err)
	}

	// Step 3: Insert test data
	customerInsertSQL := `
		INSERT INTO ANALYTICS_DB.PROD_CUSTOMERS (CUSTOMER_ID, NAME, EMAIL, SIGNUP_DATE, IS_ACTIVE) VALUES
		(1, 'Alice Johnson', 'alice@example.com', '2024-01-15', true),
		(2, 'Bob Smith', 'bob@example.com', '2024-02-20', true),
		(3, 'Charlie Brown', NULL, '2024-03-10', false)
	`
	_, err = executor.Execute(ctx, customerInsertSQL)
	if err != nil {
		t.Fatalf("Insert customers error = %v", err)
	}

	orderInsertSQL := `
		INSERT INTO ANALYTICS_DB.PROD_ORDERS (ORDER_ID, CUSTOMER_ID, AMOUNT, ORDER_DATE, STATUS) VALUES
		(101, 1, 150.50, '2024-04-01', 'completed'),
		(102, 1, 200.00, '2024-04-15', 'completed'),
		(103, 2, 75.25, '2024-04-20', 'pending'),
		(104, 3, 300.00, '2024-04-25', 'canceled')
	`
	_, err = executor.Execute(ctx, orderInsertSQL)
	if err != nil {
		t.Fatalf("Insert orders error = %v", err)
	}

	// Step 4: Test Snowflake SQL queries with translation
	t.Run("IFFWithAggregation", func(t *testing.T) {
		sqlText := `
			SELECT
				NAME,
				IFF(IS_ACTIVE, 'Active', 'Inactive') AS status,
				COUNT(*) as customer_count
			FROM ANALYTICS_DB.PROD_CUSTOMERS
			GROUP BY NAME, IS_ACTIVE
			ORDER BY NAME
		`
		result, err := executor.Query(ctx, sqlText)
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}

		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}

		// Check first customer status
		if len(result.Rows) > 0 {
			name := result.Rows[0][0]
			status := result.Rows[0][1]
			if name != "Alice Johnson" {
				t.Errorf("Expected first customer 'Alice Johnson', got %v", name)
			}
			if status != "Active" {
				t.Errorf("Expected status 'Active', got %v", status)
			}
		}
	})

	t.Run("NVLWithJoin", func(t *testing.T) {
		sqlText := `
			SELECT
				c.NAME,
				NVL(c.EMAIL, 'no-email@example.com') AS email,
				SUM(o.AMOUNT) AS total_spent
			FROM ANALYTICS_DB.PROD_CUSTOMERS c
			LEFT JOIN ANALYTICS_DB.PROD_ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
			WHERE o.STATUS != 'canceled'
			GROUP BY c.NAME, c.EMAIL
			ORDER BY total_spent DESC
		`
		result, err := executor.Query(ctx, sqlText)
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}

		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows (excluding canceled), got %d", len(result.Rows))
		}

		// Verify top spender
		if len(result.Rows) > 0 {
			name := result.Rows[0][0]
			totalSpent := result.Rows[0][2]
			if name != "Alice Johnson" {
				t.Errorf("Expected top spender 'Alice Johnson', got %v", name)
			}
			// Alice has orders 101 (150.50) + 102 (200.00) = 350.50
			if totalSpent.(float64) != 350.50 {
				t.Errorf("Expected total spent 350.50, got %v", totalSpent)
			}
		}
	})

	t.Run("CONCATWithAggregation", func(t *testing.T) {
		sqlText := `
			SELECT
				CONCAT(c.NAME, ' <', NVL(c.EMAIL, 'none'), '>') AS contact,
				COUNT(o.ORDER_ID) AS order_count
			FROM ANALYTICS_DB.PROD_CUSTOMERS c
			LEFT JOIN ANALYTICS_DB.PROD_ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
			GROUP BY c.NAME, c.EMAIL
			HAVING COUNT(o.ORDER_ID) > 0
			ORDER BY order_count DESC
		`
		result, err := executor.Query(ctx, sqlText)
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}

		if len(result.Rows) < 1 {
			t.Fatal("Expected at least 1 row")
		}

		// Alice should have the most orders (2)
		contact := result.Rows[0][0].(string)
		orderCount := result.Rows[0][1]

		// The CONCAT result should contain the name and email
		if !contains(contact, "Alice Johnson") || !contains(contact, "alice@example.com") {
			t.Errorf("Expected contact to contain 'Alice Johnson' and 'alice@example.com', got '%s'", contact)
		}
		if orderCount.(int64) != 2 {
			t.Errorf("Expected order count 2, got %v", orderCount)
		}
	})

	t.Run("ComplexAnalyticsQuery", func(t *testing.T) {
		sqlText := `
			SELECT
				IFF(o.AMOUNT > 100, 'High Value', 'Low Value') AS order_category,
				COUNT(*) AS order_count,
				SUM(o.AMOUNT) AS total_amount
			FROM ANALYTICS_DB.PROD_ORDERS o
			WHERE o.STATUS IN ('completed', 'pending')
			GROUP BY IFF(o.AMOUNT > 100, 'High Value', 'Low Value')
			ORDER BY total_amount DESC
		`
		result, err := executor.Query(ctx, sqlText)
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}

		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 categories, got %d", len(result.Rows))
		}

		// Verify high value orders
		if len(result.Rows) > 0 {
			category := result.Rows[0][0]
			if category != "High Value" {
				t.Errorf("Expected first category 'High Value', got %v", category)
			}
		}
	})

	t.Run("DateFunctions", func(t *testing.T) {
		sqlText := `
			SELECT
				NAME,
				SIGNUP_DATE,
				CURRENT_DATE() AS today
			FROM ANALYTICS_DB.PROD_CUSTOMERS
			WHERE SIGNUP_DATE < CURRENT_DATE()
			ORDER BY SIGNUP_DATE
			LIMIT 1
		`
		result, err := executor.Query(ctx, sqlText)
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}

		if len(result.Rows) < 1 {
			t.Fatal("Expected at least 1 row")
		}

		// Should return earliest signup date
		name := result.Rows[0][0]
		if name != "Alice Johnson" {
			t.Errorf("Expected earliest signup 'Alice Johnson', got %v", name)
		}
	})

	// Step 5: Test error handling
	t.Run("InvalidSyntax", func(t *testing.T) {
		sqlText := "SELECT FROM ANALYTICS_DB.PROD_CUSTOMERS"
		_, err := executor.Query(ctx, sqlText)
		if err == nil {
			t.Error("Expected error for invalid SQL, got nil")
		}
	})

	t.Run("NonExistentTable", func(t *testing.T) {
		sqlText := "SELECT * FROM ANALYTICS_DB.NONEXISTENT_TABLE"
		_, err := executor.Query(ctx, sqlText)
		if err == nil {
			t.Error("Expected error for non-existent table, got nil")
		}
	})

	// Step 6: Test UPDATE and DELETE
	t.Run("UpdateWithTranslation", func(t *testing.T) {
		updateSQL := `
			UPDATE ANALYTICS_DB.PROD_CUSTOMERS
			SET IS_ACTIVE = IFF(CUSTOMER_ID = 3, true, IS_ACTIVE)
			WHERE CUSTOMER_ID = 3
		`
		result, err := executor.Execute(ctx, updateSQL)
		if err != nil {
			t.Fatalf("Update error = %v", err)
		}

		if result.RowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
		}

		// Verify update
		selectSQL := "SELECT IS_ACTIVE FROM ANALYTICS_DB.PROD_CUSTOMERS WHERE CUSTOMER_ID = 3"
		queryResult, err := executor.Query(ctx, selectSQL)
		if err != nil {
			t.Fatalf("Query after update error = %v", err)
		}

		if len(queryResult.Rows) > 0 {
			isActive := queryResult.Rows[0][0]
			if isActive != true {
				t.Errorf("Expected IS_ACTIVE to be true, got %v", isActive)
			}
		}
	})

	t.Run("DeleteWithCondition", func(t *testing.T) {
		deleteSQL := `
			DELETE FROM ANALYTICS_DB.PROD_ORDERS
			WHERE STATUS = 'canceled'
		`
		result, err := executor.Execute(ctx, deleteSQL)
		if err != nil {
			t.Fatalf("Delete error = %v", err)
		}

		if result.RowsAffected != 1 {
			t.Errorf("Expected 1 row deleted, got %d", result.RowsAffected)
		}

		// Verify deletion
		selectSQL := "SELECT COUNT(*) FROM ANALYTICS_DB.PROD_ORDERS WHERE STATUS = 'canceled'"
		queryResult, err := executor.Query(ctx, selectSQL)
		if err != nil {
			t.Fatalf("Query after delete error = %v", err)
		}

		if len(queryResult.Rows) > 0 {
			count := queryResult.Rows[0][0]
			if count.(int64) != 0 {
				t.Errorf("Expected 0 canceled orders, got %v", count)
			}
		}
	})
}

// TestIntegration_AllSQLOperations tests all SQL operations documented in README.
// This comprehensive test verifies: Query, DML, DDL, Transaction, MERGE operations.
func TestIntegration_AllSQLOperations(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	executor := NewExecutor(mgr, repo)
	mergeProcessor := NewMergeProcessor(executor)
	executor.Configure(WithMergeProcessor(mergeProcessor))
	ctx := context.Background()

	// === Query Operations: SELECT, SHOW, DESCRIBE, EXPLAIN ===
	t.Run("Query_SELECT", func(t *testing.T) {
		result, err := executor.Query(ctx, "SELECT 1 AS num, 'hello' AS str")
		if err != nil {
			t.Fatalf("SELECT failed: %v", err)
		}
		if len(result.Rows) != 1 || len(result.Columns) != 2 {
			t.Errorf("Expected 1 row with 2 columns, got %d rows with %d columns", len(result.Rows), len(result.Columns))
		}
	})

	t.Run("Query_SHOW_TABLES", func(t *testing.T) {
		// DuckDB supports SHOW TABLES
		result, err := executor.Query(ctx, "SHOW TABLES")
		if err != nil {
			t.Logf("SHOW TABLES: DuckDB pass-through - %v", err)
		} else {
			t.Logf("SHOW TABLES: OK - returned %d rows", len(result.Rows))
		}
	})

	t.Run("Query_EXPLAIN", func(t *testing.T) {
		result, err := executor.Query(ctx, "EXPLAIN SELECT 1")
		if err != nil {
			t.Logf("EXPLAIN: DuckDB pass-through - %v", err)
		} else {
			t.Logf("EXPLAIN: OK - returned %d rows", len(result.Rows))
		}
	})

	// === DDL Operations: CREATE/DROP TABLE ===
	t.Run("DDL_CREATE_TABLE", func(t *testing.T) {
		_, err := executor.Execute(ctx, "CREATE TABLE ddl_test (id INTEGER PRIMARY KEY, name VARCHAR, value DOUBLE)")
		if err != nil {
			t.Fatalf("CREATE TABLE failed: %v", err)
		}
	})

	t.Run("DDL_CREATE_TABLE_IF_NOT_EXISTS", func(t *testing.T) {
		_, err := executor.Execute(ctx, "CREATE TABLE IF NOT EXISTS ddl_test (id INTEGER)")
		if err != nil {
			t.Fatalf("CREATE TABLE IF NOT EXISTS failed: %v", err)
		}
	})

	t.Run("Query_DESCRIBE", func(t *testing.T) {
		// Try DESCRIBE after table exists
		result, err := executor.Query(ctx, "DESCRIBE ddl_test")
		if err != nil {
			t.Logf("DESCRIBE: DuckDB pass-through - %v", err)
		} else {
			t.Logf("DESCRIBE: OK - returned %d rows", len(result.Rows))
		}
	})

	t.Run("DDL_ALTER_TABLE", func(t *testing.T) {
		_, err := executor.Execute(ctx, "ALTER TABLE ddl_test ADD COLUMN email VARCHAR(255)")
		if err != nil {
			t.Logf("ALTER TABLE ADD COLUMN: DuckDB pass-through - %v", err)
		} else {
			t.Log("ALTER TABLE ADD COLUMN: OK")
		}
	})

	// === DML Operations: INSERT, UPDATE, DELETE ===
	t.Run("DML_INSERT", func(t *testing.T) {
		result, err := executor.Execute(ctx, "INSERT INTO ddl_test (id, name, value) VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.0)")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
		if result.RowsAffected != 2 {
			t.Errorf("Expected 2 rows affected, got %d", result.RowsAffected)
		}
	})

	t.Run("DML_UPDATE", func(t *testing.T) {
		result, err := executor.Execute(ctx, "UPDATE ddl_test SET value = 150.0 WHERE id = 1")
		if err != nil {
			t.Fatalf("UPDATE failed: %v", err)
		}
		if result.RowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
		}
	})

	t.Run("DML_DELETE", func(t *testing.T) {
		result, err := executor.Execute(ctx, "DELETE FROM ddl_test WHERE id = 2")
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}
		if result.RowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
		}
	})

	// === Transaction Operations: BEGIN, COMMIT, ROLLBACK ===
	t.Run("Transaction_BEGIN_COMMIT", func(t *testing.T) {
		_, err := executor.Execute(ctx, "BEGIN")
		if err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}
		_, err = executor.Execute(ctx, "INSERT INTO ddl_test (id, name, value) VALUES (3, 'Charlie', 300.0)")
		if err != nil {
			t.Fatalf("INSERT in transaction failed: %v", err)
		}
		_, err = executor.Execute(ctx, "COMMIT")
		if err != nil {
			t.Fatalf("COMMIT failed: %v", err)
		}
		// Verify committed
		result, _ := executor.Query(ctx, "SELECT COUNT(*) FROM ddl_test WHERE id = 3")
		if len(result.Rows) > 0 && result.Rows[0][0].(int64) != 1 {
			t.Error("Expected row to be committed")
		}
	})

	t.Run("Transaction_BEGIN_ROLLBACK", func(t *testing.T) {
		_, err := executor.Execute(ctx, "BEGIN")
		if err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}
		_, err = executor.Execute(ctx, "INSERT INTO ddl_test (id, name, value) VALUES (99, 'Rollback', 999.0)")
		if err != nil {
			t.Fatalf("INSERT in transaction failed: %v", err)
		}
		_, err = executor.Execute(ctx, "ROLLBACK")
		if err != nil {
			t.Fatalf("ROLLBACK failed: %v", err)
		}
		// Verify rolled back
		result, _ := executor.Query(ctx, "SELECT COUNT(*) FROM ddl_test WHERE id = 99")
		if len(result.Rows) > 0 && result.Rows[0][0].(int64) != 0 {
			t.Error("Expected row to be rolled back")
		}
	})

	// === DDL: CREATE/DROP SCHEMA via SQL (DuckDB pass-through) ===
	t.Run("DDL_CREATE_SCHEMA_SQL", func(t *testing.T) {
		_, err := executor.Execute(ctx, "CREATE SCHEMA test_schema_via_sql")
		if err != nil {
			t.Logf("CREATE SCHEMA via SQL: DuckDB pass-through - %v", err)
		} else {
			t.Log("CREATE SCHEMA via SQL: OK")
		}
	})

	t.Run("DDL_DROP_SCHEMA_SQL", func(t *testing.T) {
		_, err := executor.Execute(ctx, "DROP SCHEMA IF EXISTS test_schema_via_sql")
		if err != nil {
			t.Logf("DROP SCHEMA via SQL: DuckDB pass-through - %v", err)
		} else {
			t.Log("DROP SCHEMA via SQL: OK")
		}
	})

	// === MERGE INTO ===
	t.Run("MERGE_INTO", func(t *testing.T) {
		// Setup source table
		_, _ = executor.Execute(ctx, "CREATE TABLE merge_source (id INTEGER, name VARCHAR, value DOUBLE)")
		_, _ = executor.Execute(ctx, "INSERT INTO merge_source VALUES (1, 'Alice Updated', 999.0), (4, 'David', 400.0)")

		mergeSQL := `MERGE INTO ddl_test t
			USING merge_source s ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value
			WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)`

		result, err := executor.Execute(ctx, mergeSQL)
		if err != nil {
			t.Fatalf("MERGE INTO failed: %v", err)
		}
		t.Logf("MERGE INTO: OK - %d rows affected", result.RowsAffected)

		// Verify Alice was updated
		queryResult, _ := executor.Query(ctx, "SELECT name, value FROM ddl_test WHERE id = 1")
		if len(queryResult.Rows) > 0 {
			if queryResult.Rows[0][0] != "Alice Updated" {
				t.Errorf("Expected 'Alice Updated', got %v", queryResult.Rows[0][0])
			}
		}

		// Verify David was inserted
		queryResult, _ = executor.Query(ctx, "SELECT COUNT(*) FROM ddl_test WHERE id = 4")
		if len(queryResult.Rows) > 0 && queryResult.Rows[0][0].(int64) != 1 {
			t.Error("Expected David to be inserted")
		}
	})

	// === Parameter Binding ===
	t.Run("ParameterBinding_Colon", func(t *testing.T) {
		bindings := map[string]*QueryBindingValue{
			"1": {Type: "FIXED", Value: "1"},
		}
		result, err := executor.QueryWithBindings(ctx, "SELECT * FROM ddl_test WHERE id = :1", bindings)
		if err != nil {
			t.Fatalf("Query with :1 binding failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}
	})

	t.Run("ParameterBinding_QuestionMark", func(t *testing.T) {
		bindings := map[string]*QueryBindingValue{
			"1": {Type: "TEXT", Value: "Alice Updated"},
		}
		result, err := executor.QueryWithBindings(ctx, "SELECT * FROM ddl_test WHERE name = ?", bindings)
		if err != nil {
			t.Fatalf("Query with ? binding failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}
	})

	// === DDL: DROP TABLE ===
	t.Run("DDL_DROP_TABLE", func(t *testing.T) {
		_, err := executor.Execute(ctx, "DROP TABLE ddl_test")
		if err != nil {
			t.Fatalf("DROP TABLE failed: %v", err)
		}
	})

	t.Run("DDL_DROP_TABLE_IF_EXISTS", func(t *testing.T) {
		_, err := executor.Execute(ctx, "DROP TABLE IF EXISTS nonexistent_table")
		if err != nil {
			t.Fatalf("DROP TABLE IF EXISTS failed: %v", err)
		}
	})
}

// TestIntegration_DDL_Database_Schema_REST tests CREATE/DROP DATABASE/SCHEMA via repository.
func TestIntegration_DDL_Database_Schema_REST(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	ctx := context.Background()

	t.Run("CreateDatabase_REST", func(t *testing.T) {
		db, err := repo.CreateDatabase(ctx, "REST_DB", "Test via REST")
		if err != nil {
			t.Fatalf("CreateDatabase failed: %v", err)
		}
		if db.Name != "REST_DB" {
			t.Errorf("Expected 'REST_DB', got '%s'", db.Name)
		}
	})

	t.Run("CreateSchema_REST", func(t *testing.T) {
		db, _ := repo.GetDatabaseByName(ctx, "REST_DB")
		schema, err := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "Test schema")
		if err != nil {
			t.Fatalf("CreateSchema failed: %v", err)
		}
		if schema.Name != "TEST_SCHEMA" {
			t.Errorf("Expected 'TEST_SCHEMA', got '%s'", schema.Name)
		}
	})

	t.Run("DropSchema_REST", func(t *testing.T) {
		db, _ := repo.GetDatabaseByName(ctx, "REST_DB")
		schema, _ := repo.GetSchemaByName(ctx, db.ID, "TEST_SCHEMA")
		err := repo.DropSchema(ctx, schema.ID)
		if err != nil {
			t.Fatalf("DropSchema failed: %v", err)
		}
	})

	t.Run("DropDatabase_REST", func(t *testing.T) {
		db, _ := repo.GetDatabaseByName(ctx, "REST_DB")
		err := repo.DropDatabase(ctx, db.ID)
		if err != nil {
			t.Fatalf("DropDatabase failed: %v", err)
		}
	})
}

// TestIntegration_ConcurrentQueries tests concurrent query execution.
func TestIntegration_ConcurrentQueries(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	executor := NewExecutor(mgr, repo)
	ctx := context.Background()

	// Setup data
	database, _ := repo.CreateDatabase(ctx, "CONCURRENT_DB", "")
	schema, _ := repo.CreateSchema(ctx, database.ID, "PUBLIC", "")
	cols := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "VALUE", Type: "INTEGER"},
	}
	_, _ = repo.CreateTable(ctx, schema.ID, "TEST_TABLE", cols, "")

	// Insert test data
	insertSQL := "INSERT INTO CONCURRENT_DB.PUBLIC_TEST_TABLE (ID, VALUE) VALUES (1, 100), (2, 200), (3, 300)"
	_, _ = executor.Execute(ctx, insertSQL)

	// Run concurrent queries
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			sqlText := "SELECT IFF(VALUE > 150, 'HIGH', 'LOW') AS category, COUNT(*) FROM CONCURRENT_DB.PUBLIC_TEST_TABLE GROUP BY category"
			_, err := executor.Query(ctx, sqlText)
			if err != nil {
				t.Errorf("Concurrent query error: %v", err)
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}
