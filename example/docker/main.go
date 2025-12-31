// Example: Using Snowflake Emulator with Docker
//
// This example demonstrates how to use the Snowflake Emulator running in Docker.
// It connects to the emulator container and executes SQL queries.
//
// Prerequisites: Docker must be installed and running.
//
// Start the emulator with Docker:
//
//	docker compose up -d
//
// Then run this example:
//
//	go run ./example/docker
//
// Or run everything with the provided script:
//
//	./example/docker/run.sh
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

var baseURL = getBaseURL()

func getBaseURL() string {
	host := os.Getenv("SNOWFLAKE_HOST")
	if host == "" {
		host = "localhost:8080"
	}
	return fmt.Sprintf("http://%s", host)
}

// StatementRequest represents a SQL statement submission request
type StatementRequest struct {
	Statement string `json:"statement"`
	Database  string `json:"database,omitempty"`
	Schema    string `json:"schema,omitempty"`
}

// StatementResponse represents the response from statement submission
type StatementResponse struct {
	ResultSetMetaData ResultSetMetaData `json:"resultSetMetaData"`
	Data              [][]any           `json:"data"`
	Code              string            `json:"code"`
	Message           string            `json:"message"`
	StatementHandle   string            `json:"statementHandle"`
}

// ResultSetMetaData contains metadata about the result set
type ResultSetMetaData struct {
	NumRows int            `json:"numRows"`
	RowType []RowTypeField `json:"rowType"`
}

// RowTypeField describes a column in the result set
type RowTypeField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func main() {
	fmt.Println("=== Snowflake Emulator Docker Example ===")
	fmt.Printf("Connecting to emulator at: %s\n\n", baseURL)

	// Step 1: Wait for emulator to be ready
	fmt.Println("1. Checking emulator health...")
	if err := waitForHealth(30 * time.Second); err != nil {
		log.Fatalf("Emulator not ready: %v", err)
	}
	fmt.Println("   Emulator is healthy!")

	// Step 2: Create database using REST API
	fmt.Println("\n2. Creating database 'DOCKER_TEST_DB'...")
	if err := createDatabase("DOCKER_TEST_DB"); err != nil {
		log.Printf("   Warning: %v (may already exist)", err)
	} else {
		fmt.Println("   Database created successfully")
	}

	// Step 3: Execute SQL statements
	fmt.Println("\n3. Executing SQL statements...")

	// Create table
	fmt.Println("\n   Creating table 'docker_test'...")
	resp, err := executeStatement(`
		CREATE TABLE IF NOT EXISTS docker_test (
			id INTEGER,
			name VARCHAR,
			value DECIMAL(10,2),
			created_at DATE
		)
	`, "DOCKER_TEST_DB", "PUBLIC")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Printf("   Statement handle: %s\n", resp.StatementHandle)

	// Insert data
	fmt.Println("\n   Inserting sample data...")
	_, err = executeStatement(`
		INSERT INTO docker_test VALUES
		(1, 'Docker Test 1', 100.50, '2024-01-15'),
		(2, 'Docker Test 2', 200.75, '2024-02-20'),
		(3, 'Docker Test 3', 300.25, '2024-03-25')
	`, "DOCKER_TEST_DB", "PUBLIC")
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	fmt.Println("   3 rows inserted")

	// Step 4: Query with Snowflake functions
	fmt.Println("\n4. Querying with Snowflake SQL functions...")

	// IFF function
	fmt.Println("\n   IFF function (value classification):")
	resp, err = executeStatement(`
		SELECT
			name,
			value,
			IFF(value > 150, 'High', 'Low') AS category
		FROM docker_test
		ORDER BY value DESC
	`, "DOCKER_TEST_DB", "PUBLIC")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResponse(resp)

	// DATEADD function
	fmt.Println("\n   DATEADD function (date arithmetic):")
	resp, err = executeStatement(`
		SELECT
			name,
			created_at,
			DATEADD(day, 30, created_at) AS expiry_date
		FROM docker_test
		ORDER BY id
	`, "DOCKER_TEST_DB", "PUBLIC")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResponse(resp)

	// LISTAGG function
	fmt.Println("\n   LISTAGG function (string aggregation):")
	resp, err = executeStatement(`
		SELECT LISTAGG(name, ' | ') AS all_names
		FROM docker_test
	`, "DOCKER_TEST_DB", "PUBLIC")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResponse(resp)

	// Step 5: Cleanup
	fmt.Println("\n5. Cleanup...")
	_, _ = executeStatement("DROP TABLE IF EXISTS docker_test", "DOCKER_TEST_DB", "PUBLIC")
	fmt.Println("   Table 'docker_test' dropped")

	fmt.Println("\n=== Docker example completed successfully! ===")
	fmt.Println("\nThe emulator is running in Docker and responding to queries.")
	fmt.Println("Stop the container with: docker compose down")
}

func waitForHealth(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for emulator health check")
}

func executeStatement(sql, database, schema string) (*StatementResponse, error) {
	req := StatementRequest{
		Statement: sql,
		Database:  database,
		Schema:    schema,
	}

	body, _ := json.Marshal(req)
	resp, err := http.Post(baseURL+"/api/v2/statements", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	var result StatementResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w, body: %s", err, string(respBody))
	}

	if resp.StatusCode >= 400 {
		return &result, fmt.Errorf("statement failed: %s", result.Message)
	}

	return &result, nil
}

func createDatabase(name string) error {
	req := map[string]string{"name": name}
	body, _ := json.Marshal(req)

	resp, err := http.Post(baseURL+"/api/v2/databases", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func printResponse(resp *StatementResponse) {
	if resp.ResultSetMetaData.NumRows == 0 {
		fmt.Println("   (no rows)")
		return
	}

	// Print column headers
	cols := make([]string, len(resp.ResultSetMetaData.RowType))
	for i, col := range resp.ResultSetMetaData.RowType {
		cols[i] = col.Name
	}
	fmt.Printf("   Columns: %v\n", cols)

	// Print rows
	for i, row := range resp.Data {
		fmt.Printf("   Row %d: %v\n", i+1, row)
	}
}
