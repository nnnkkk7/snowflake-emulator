// sample/main.go - SQL Translation Verification
//
// This sample demonstrates Snowflake SQL to DuckDB SQL translation.
// It directly uses the translator and DuckDB, NOT the gosnowflake driver.
//
// For gosnowflake driver compatibility testing, see tests/e2e/gosnowflake_test.go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
)

func main() {
	// Open in-memory DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer db.Close()

	mgr := connection.NewManager(db)
	translator := query.NewTranslator()
	ctx := context.Background()

	// Create test table
	_, err = mgr.Exec(ctx, `
		CREATE TABLE test_users (
			id INTEGER,
			name VARCHAR,
			email VARCHAR,
			score INTEGER,
			created_at DATE,
			data VARCHAR
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = mgr.Exec(ctx, `
		INSERT INTO test_users VALUES
		(1, 'Alice', 'alice@example.com', 95, '2024-01-15', '{"role": "admin"}'),
		(2, 'Bob', NULL, 87, '2024-02-20', NULL),
		(3, 'Charlie', 'charlie@example.com', NULL, '2024-03-25', '{"role": "user"}')
	`)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	fmt.Println("=== Snowflake SQL Translation Verification ===")
	fmt.Println("This verifies Snowflake SQL -> DuckDB SQL translation.")
	fmt.Println("For gosnowflake driver tests, run: go test ./tests/e2e/...")
	fmt.Println()

	// Test cases for SQL translation
	testCases := []struct {
		name         string
		snowflakeSQL string
	}{
		{
			name:         "IFF",
			snowflakeSQL: "SELECT name, IFF(score >= 90, 'A', 'B') AS grade FROM test_users ORDER BY id",
		},
		{
			name:         "NVL",
			snowflakeSQL: "SELECT name, NVL(email, 'no-email@example.com') AS email FROM test_users ORDER BY id",
		},
		{
			name:         "NVL2",
			snowflakeSQL: "SELECT name, NVL2(email, 'has email', 'no email') AS email_status FROM test_users ORDER BY id",
		},
		{
			name:         "DATEADD",
			snowflakeSQL: "SELECT name, DATEADD(day, 30, created_at) AS due_date FROM test_users ORDER BY id",
		},
		{
			name:         "DATEDIFF",
			snowflakeSQL: "SELECT name, DATEDIFF(day, created_at, CURRENT_DATE()) AS days_since FROM test_users ORDER BY id",
		},
		{
			name:         "TO_VARIANT",
			snowflakeSQL: "SELECT name, TO_VARIANT(score) AS score_json FROM test_users WHERE score IS NOT NULL ORDER BY id",
		},
		{
			name:         "PARSE_JSON",
			snowflakeSQL: "SELECT name, PARSE_JSON(data) AS data_json FROM test_users WHERE data IS NOT NULL ORDER BY id",
		},
		{
			name:         "OBJECT_CONSTRUCT",
			snowflakeSQL: "SELECT name, OBJECT_CONSTRUCT('name', name, 'score', score) AS user_obj FROM test_users ORDER BY id",
		},
		{
			name:         "LISTAGG",
			snowflakeSQL: "SELECT LISTAGG(name, ', ') AS all_names FROM test_users",
		},
		{
			name:         "Combined (NVL2 + IFF)",
			snowflakeSQL: "SELECT name, NVL2(score, IFF(score >= 90, 'A', 'B'), 'N/A') AS grade FROM test_users ORDER BY id",
		},
	}

	passed := 0
	failed := 0

	for _, tc := range testCases {
		fmt.Printf("--- %s ---\n", tc.name)
		fmt.Printf("Snowflake: %s\n", tc.snowflakeSQL)

		// Translate SQL
		duckdbSQL, err := translator.Translate(tc.snowflakeSQL)
		if err != nil {
			fmt.Printf("FAIL: Translation error: %v\n\n", err)
			failed++
			continue
		}
		fmt.Printf("DuckDB:    %s\n", duckdbSQL)

		// Execute translated SQL
		rows, err := mgr.Query(ctx, duckdbSQL)
		if err != nil {
			fmt.Printf("FAIL: Execution error: %v\n\n", err)
			failed++
			continue
		}

		// Get column names and count rows
		cols, _ := rows.Columns()
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		rowCount := 0
		for rows.Next() {
			_ = rows.Scan(valuePtrs...)
			rowCount++
		}
		rows.Close()

		fmt.Printf("OK: %d rows returned\n\n", rowCount)
		passed++
	}

	fmt.Println("=== Summary ===")
	fmt.Printf("Passed: %d/%d\n", passed, passed+failed)
	if failed > 0 {
		fmt.Printf("Failed: %d\n", failed)
	}
}
