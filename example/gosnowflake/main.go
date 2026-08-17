// Example: Using Snowflake Emulator with gosnowflake driver
//
// This example demonstrates how to use the Snowflake Emulator with the official
// gosnowflake driver. The emulator must be running before executing this example.
//
// Start the emulator:
//
//	go run ./cmd/server
//
// Then run this example:
//
//	go run ./example/gosnowflake
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/snowflakedb/gosnowflake"
)

func main() {
	// Get emulator host from environment or use default
	host := os.Getenv("SNOWFLAKE_HOST")
	if host == "" {
		host = "localhost:8080"
	}

	// Connect to the local emulator
	// DSN format: user:pass@host:port/database/schema?account=name&protocol=http
	dsn := fmt.Sprintf("testuser:testpass@%s/TEST_DB/PUBLIC?account=testaccount&protocol=http", host)

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Failed to ping: %v", err)
	}
	fmt.Println("Connected to Snowflake Emulator!")

	// Create a sample table using Snowflake-native types
	// The emulator translates these to DuckDB equivalents automatically
	fmt.Println("\n=== Creating table ===")
	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id NUMBER,
			name TEXT,
			email STRING,
			score NUMBER,
			created_at TIMESTAMP_NTZ
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Println("Table 'users' created")

	// Insert sample data
	fmt.Println("\n=== Inserting data ===")
	_, err = db.ExecContext(ctx, `
		INSERT INTO users VALUES
		(1, 'Alice', 'alice@example.com', 95, '2024-01-15'),
		(2, 'Bob', NULL, 87, '2024-02-20'),
		(3, 'Charlie', 'charlie@example.com', NULL, '2024-03-25')
	`)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	fmt.Println("3 rows inserted")

	// Query with Snowflake-specific functions
	fmt.Println("\n=== Snowflake SQL Function Examples ===")

	// Example 1: IFF function
	fmt.Println("\n1. IFF (conditional expression):")
	rows, err := db.QueryContext(ctx, `
		SELECT name, score, IFF(score >= 90, 'A', 'B') AS grade
		FROM users
		WHERE score IS NOT NULL
		ORDER BY id
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printRows(rows)

	// Example 2: NVL function
	fmt.Println("\n2. NVL (null value substitution):")
	rows, err = db.QueryContext(ctx, `
		SELECT name, NVL(email, 'no-email@example.com') AS email
		FROM users
		ORDER BY id
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printRows(rows)

	// Example 3: NVL2 function
	fmt.Println("\n3. NVL2 (null conditional):")
	rows, err = db.QueryContext(ctx, `
		SELECT name, NVL2(email, 'Has Email', 'No Email') AS email_status
		FROM users
		ORDER BY id
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printRows(rows)

	// Example 4: DATEADD function
	fmt.Println("\n4. DATEADD (date arithmetic):")
	rows, err = db.QueryContext(ctx, `
		SELECT name, created_at, DATEADD(day, 30, created_at) AS due_date
		FROM users
		ORDER BY id
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printRows(rows)

	// Example 5: DATEDIFF function
	fmt.Println("\n5. DATEDIFF (date difference):")
	rows, err = db.QueryContext(ctx, `
		SELECT name, created_at, DATEDIFF(day, created_at, '2024-12-31') AS days_until_year_end
		FROM users
		ORDER BY id
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printRows(rows)

	// Example 6: LISTAGG function
	fmt.Println("\n6. LISTAGG (string aggregation):")
	rows, err = db.QueryContext(ctx, `
		SELECT LISTAGG(name, ', ') AS all_users
		FROM users
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printRows(rows)

	// Example 7: Combined functions
	fmt.Println("\n7. Combined functions (NVL2 + IFF):")
	rows, err = db.QueryContext(ctx, `
		SELECT
			name,
			NVL2(score, IFF(score >= 90, 'A', 'B'), 'N/A') AS grade
		FROM users
		ORDER BY id
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printRows(rows)

	// Example 8: INFORMATION_SCHEMA query
	fmt.Println("\n8. INFORMATION_SCHEMA (system table query):")
	rows, err = db.QueryContext(ctx, `
		SELECT TABLE_NAME, TABLE_TYPE
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = 'main'
		ORDER BY TABLE_NAME
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printRows(rows)

	// Clean up
	fmt.Println("\n=== Cleanup ===")
	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS users")
	if err != nil {
		log.Printf("Warning: Failed to drop table: %v", err)
	} else {
		fmt.Println("Table 'users' dropped")
	}

	fmt.Println("\nExample completed successfully!")
}

// printRows prints all rows from a result set
func printRows(rows *sql.Rows) {
	defer rows.Close()

	cols, _ := rows.Columns()
	fmt.Printf("  Columns: %v\n", cols)

	values := make([]any, len(cols))
	valuePtrs := make([]any, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	rowNum := 0
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}
		rowNum++
		fmt.Printf("  Row %d: %v\n", rowNum, values)
	}
}
