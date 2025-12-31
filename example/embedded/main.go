// Example: Using Snowflake Emulator as an Embedded Library
//
// This example demonstrates how to use the Snowflake Emulator components
// directly in your application without starting an HTTP server. This is
// useful for unit tests and scenarios where you want in-process testing.
//
// Run this example:
//
//	go run ./example/embedded
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
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

func main() {
	fmt.Println("=== Snowflake Emulator Embedded Example ===\n")

	// Create an in-memory DuckDB instance
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer db.Close()

	// Initialize emulator components
	connMgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(connMgr)
	if err != nil {
		log.Fatalf("Failed to create repository: %v", err)
	}

	sessionMgr := session.NewManager(1 * time.Hour)
	executor := query.NewExecutor(connMgr, repo)

	// Create HTTP handlers
	sessionHandler := handlers.NewSessionHandler(sessionMgr, repo)
	queryHandler := handlers.NewQueryHandler(executor, sessionMgr)

	// Set up router
	r := chi.NewRouter()
	r.Post("/session/v1/login-request", sessionHandler.Login)
	r.Post("/session/token-request", sessionHandler.TokenRequest)
	r.Post("/session/heartbeat", sessionHandler.Heartbeat)
	r.Post("/session", sessionHandler.CloseSession)
	r.Post("/queries/v1/query-request", queryHandler.ExecuteQuery)
	r.Post("/telemetry/send", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	})

	// Start in-process test server
	server := httptest.NewServer(r)
	defer server.Close()

	fmt.Printf("Embedded emulator running at: %s\n\n", server.URL)

	// Connect using gosnowflake driver
	hostPort := server.URL[7:] // Remove "http://"
	dsn := fmt.Sprintf("testuser:testpass@%s/TEST_DB/PUBLIC?account=testaccount&protocol=http", hostPort)

	snowflakeDB, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("Failed to open Snowflake connection: %v", err)
	}
	defer snowflakeDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test connection
	if err := snowflakeDB.PingContext(ctx); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("1. Connection established successfully!")

	// Create test table
	fmt.Println("\n2. Creating table 'employees'...")
	_, err = snowflakeDB.ExecContext(ctx, `
		CREATE TABLE employees (
			id INTEGER,
			name VARCHAR,
			department VARCHAR,
			salary DECIMAL(10,2),
			hire_date DATE
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	fmt.Println("\n3. Inserting test data...")
	_, err = snowflakeDB.ExecContext(ctx, `
		INSERT INTO employees VALUES
		(1, 'Alice Johnson', 'Engineering', 95000.00, '2022-01-15'),
		(2, 'Bob Smith', 'Engineering', 85000.00, '2022-03-20'),
		(3, 'Charlie Brown', 'Sales', 75000.00, '2021-06-10'),
		(4, 'Diana Ross', 'Marketing', 80000.00, '2023-02-01'),
		(5, 'Eve Wilson', 'Engineering', 105000.00, '2020-11-30')
	`)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	fmt.Println("   Inserted 5 employees")

	// Demonstrate Snowflake SQL functions
	fmt.Println("\n4. Executing Snowflake SQL queries...")

	// Query 1: IFF for salary tier classification
	fmt.Println("\n   Query 1: Salary tier classification (IFF)")
	rows, err := snowflakeDB.QueryContext(ctx, `
		SELECT
			name,
			salary,
			IFF(salary >= 90000, 'Senior', IFF(salary >= 80000, 'Mid', 'Junior')) AS level
		FROM employees
		ORDER BY salary DESC
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResults(rows)

	// Query 2: Department summary with LISTAGG
	fmt.Println("\n   Query 2: Department summary (LISTAGG)")
	rows, err = snowflakeDB.QueryContext(ctx, `
		SELECT
			department,
			COUNT(*) AS headcount,
			LISTAGG(name, ', ') AS members
		FROM employees
		GROUP BY department
		ORDER BY headcount DESC
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResults(rows)

	// Query 3: DATEADD for anniversary calculation
	fmt.Println("\n   Query 3: Work anniversary dates (DATEADD)")
	rows, err = snowflakeDB.QueryContext(ctx, `
		SELECT
			name,
			hire_date,
			DATEADD(year, 1, hire_date) AS first_anniversary
		FROM employees
		ORDER BY hire_date
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResults(rows)

	// Query 4: DATEDIFF for tenure calculation
	fmt.Println("\n   Query 4: Employee tenure in days (DATEDIFF)")
	rows, err = snowflakeDB.QueryContext(ctx, `
		SELECT
			name,
			hire_date,
			DATEDIFF(day, hire_date, '2024-12-31') AS days_employed
		FROM employees
		ORDER BY days_employed DESC
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResults(rows)

	// Query 5: NVL2 with conditional logic
	fmt.Println("\n   Query 5: Bonus eligibility (combined functions)")
	rows, err = snowflakeDB.QueryContext(ctx, `
		SELECT
			name,
			salary,
			IFF(salary >= 90000, salary * 0.15, salary * 0.10) AS bonus,
			IFF(salary >= 90000, 'Tier 1', 'Tier 2') AS bonus_tier
		FROM employees
		ORDER BY bonus DESC
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResults(rows)

	// Clean up
	fmt.Println("\n5. Cleanup...")
	_, err = snowflakeDB.ExecContext(ctx, "DROP TABLE employees")
	if err != nil {
		log.Printf("Warning: Failed to drop table: %v", err)
	} else {
		fmt.Println("   Table 'employees' dropped")
	}

	fmt.Println("\n=== Example completed successfully! ===")
	fmt.Println("\nThis example ran entirely in-process without an external server.")
	fmt.Println("This approach is ideal for unit tests and CI/CD pipelines.")
}

func printResults(rows *sql.Rows) {
	defer rows.Close()

	cols, _ := rows.Columns()
	fmt.Printf("   %-20s", "")
	for _, col := range cols {
		fmt.Printf("%-20s", col)
	}
	fmt.Println()
	fmt.Printf("   %-20s", "")
	for range cols {
		fmt.Printf("%-20s", "--------------------")
	}
	fmt.Println()

	values := make([]any, len(cols))
	valuePtrs := make([]any, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}
		fmt.Printf("   %-20s", "")
		for _, v := range values {
			switch val := v.(type) {
			case nil:
				fmt.Printf("%-20s", "NULL")
			case []byte:
				fmt.Printf("%-20s", string(val))
			default:
				fmt.Printf("%-20v", val)
			}
		}
		fmt.Println()
	}
}
