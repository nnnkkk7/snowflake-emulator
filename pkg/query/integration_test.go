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
func TestIntegration_QueryEngineWorkflow(t *testing.T) {
	// Setup: Create in-memory DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer db.Close()

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
		(104, 3, 300.00, '2024-04-25', 'cancelled')
	`
	_, err = executor.Execute(ctx, orderInsertSQL)
	if err != nil {
		t.Fatalf("Insert orders error = %v", err)
	}

	// Step 4: Test Snowflake SQL queries with translation
	t.Run("IFFWithAggregation", func(t *testing.T) {
		sql := `
			SELECT
				NAME,
				IFF(IS_ACTIVE, 'Active', 'Inactive') AS status,
				COUNT(*) as customer_count
			FROM ANALYTICS_DB.PROD_CUSTOMERS
			GROUP BY NAME, IS_ACTIVE
			ORDER BY NAME
		`
		result, err := executor.Query(ctx, sql)
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
		sql := `
			SELECT
				c.NAME,
				NVL(c.EMAIL, 'no-email@example.com') AS email,
				SUM(o.AMOUNT) AS total_spent
			FROM ANALYTICS_DB.PROD_CUSTOMERS c
			LEFT JOIN ANALYTICS_DB.PROD_ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
			WHERE o.STATUS != 'cancelled'
			GROUP BY c.NAME, c.EMAIL
			ORDER BY total_spent DESC
		`
		result, err := executor.Query(ctx, sql)
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}

		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows (excluding cancelled), got %d", len(result.Rows))
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
		sql := `
			SELECT
				CONCAT(c.NAME, ' <', NVL(c.EMAIL, 'none'), '>') AS contact,
				COUNT(o.ORDER_ID) AS order_count
			FROM ANALYTICS_DB.PROD_CUSTOMERS c
			LEFT JOIN ANALYTICS_DB.PROD_ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
			GROUP BY c.NAME, c.EMAIL
			HAVING COUNT(o.ORDER_ID) > 0
			ORDER BY order_count DESC
		`
		result, err := executor.Query(ctx, sql)
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
		sql := `
			SELECT
				IFF(o.AMOUNT > 100, 'High Value', 'Low Value') AS order_category,
				COUNT(*) AS order_count,
				SUM(o.AMOUNT) AS total_amount
			FROM ANALYTICS_DB.PROD_ORDERS o
			WHERE o.STATUS IN ('completed', 'pending')
			GROUP BY IFF(o.AMOUNT > 100, 'High Value', 'Low Value')
			ORDER BY total_amount DESC
		`
		result, err := executor.Query(ctx, sql)
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
		sql := `
			SELECT
				NAME,
				SIGNUP_DATE,
				CURRENT_DATE() AS today
			FROM ANALYTICS_DB.PROD_CUSTOMERS
			WHERE SIGNUP_DATE < CURRENT_DATE()
			ORDER BY SIGNUP_DATE
			LIMIT 1
		`
		result, err := executor.Query(ctx, sql)
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
		sql := "SELECT FROM ANALYTICS_DB.PROD_CUSTOMERS"
		_, err := executor.Query(ctx, sql)
		if err == nil {
			t.Error("Expected error for invalid SQL, got nil")
		}
	})

	t.Run("NonExistentTable", func(t *testing.T) {
		sql := "SELECT * FROM ANALYTICS_DB.NONEXISTENT_TABLE"
		_, err := executor.Query(ctx, sql)
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
			WHERE STATUS = 'cancelled'
		`
		result, err := executor.Execute(ctx, deleteSQL)
		if err != nil {
			t.Fatalf("Delete error = %v", err)
		}

		if result.RowsAffected != 1 {
			t.Errorf("Expected 1 row deleted, got %d", result.RowsAffected)
		}

		// Verify deletion
		selectSQL := "SELECT COUNT(*) FROM ANALYTICS_DB.PROD_ORDERS WHERE STATUS = 'cancelled'"
		queryResult, err := executor.Query(ctx, selectSQL)
		if err != nil {
			t.Fatalf("Query after delete error = %v", err)
		}

		if len(queryResult.Rows) > 0 {
			count := queryResult.Rows[0][0]
			if count.(int64) != 0 {
				t.Errorf("Expected 0 cancelled orders, got %v", count)
			}
		}
	})
}

// TestIntegration_ConcurrentQueries tests concurrent query execution.
func TestIntegration_ConcurrentQueries(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer db.Close()

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
	repo.CreateTable(ctx, schema.ID, "TEST_TABLE", cols, "")

	// Insert test data
	insertSQL := "INSERT INTO CONCURRENT_DB.PUBLIC_TEST_TABLE (ID, VALUE) VALUES (1, 100), (2, 200), (3, 300)"
	executor.Execute(ctx, insertSQL)

	// Run concurrent queries
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			sql := "SELECT IFF(VALUE > 150, 'HIGH', 'LOW') AS category, COUNT(*) FROM CONCURRENT_DB.PUBLIC_TEST_TABLE GROUP BY category"
			_, err := executor.Query(ctx, sql)
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
