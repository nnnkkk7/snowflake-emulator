// Example: Using Snowflake Emulator REST API v2
//
// This example demonstrates how to use the Snowflake Emulator via REST API v2.
// This is useful for languages that don't have a native Snowflake driver.
//
// Start the emulator:
//
//	go run ./cmd/server
//
// Then run this example:
//
//	go run ./example/restapi
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

var baseURL = getBaseURL()

func getBaseURL() string {
	host := os.Getenv("SNOWFLAKE_HOST")
	if host == "" {
		host = "localhost:8080"
	}
	return fmt.Sprintf("http://%s/api/v2", host)
}

// StatementRequest represents a SQL statement submission request
type StatementRequest struct {
	Statement string            `json:"statement"`
	Database  string            `json:"database,omitempty"`
	Schema    string            `json:"schema,omitempty"`
	Warehouse string            `json:"warehouse,omitempty"`
	Bindings  map[string]Binding `json:"bindings,omitempty"`
}

// Binding represents a parameter binding
type Binding struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// StatementResponse represents the response from statement submission
type StatementResponse struct {
	ResultSetMetaData ResultSetMetaData `json:"resultSetMetaData"`
	Data              [][]any           `json:"data"`
	Code              string            `json:"code"`
	SQLState          string            `json:"sqlState"`
	StatementHandle   string            `json:"statementHandle"`
	Message           string            `json:"message"`
	CreatedOn         int64             `json:"createdOn"`
}

// ResultSetMetaData contains metadata about the result set
type ResultSetMetaData struct {
	NumRows int           `json:"numRows"`
	Format  string        `json:"format"`
	RowType []RowTypeField `json:"rowType"`
}

// RowTypeField describes a column in the result set
type RowTypeField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// DatabaseRequest represents a database creation request
type DatabaseRequest struct {
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
}

// WarehouseRequest represents a warehouse creation request
type WarehouseRequest struct {
	Name        string `json:"name"`
	Size        string `json:"size,omitempty"`
	AutoSuspend int    `json:"autoSuspend,omitempty"`
	AutoResume  bool   `json:"autoResume,omitempty"`
}

func main() {
	fmt.Println("=== Snowflake Emulator REST API v2 Example ===\n")

	// Example 1: Create a database
	fmt.Println("1. Creating database 'DEMO_DB'...")
	if err := createDatabase("DEMO_DB"); err != nil {
		log.Printf("Warning: %v (may already exist)", err)
	} else {
		fmt.Println("   Database created successfully")
	}

	// Example 2: Create a warehouse
	fmt.Println("\n2. Creating warehouse 'DEMO_WH'...")
	if err := createWarehouse("DEMO_WH"); err != nil {
		log.Printf("Warning: %v (may already exist)", err)
	} else {
		fmt.Println("   Warehouse created successfully")
	}

	// Example 3: Execute SQL statements
	fmt.Println("\n3. Executing SQL statements...")

	// Create table
	fmt.Println("\n   Creating table 'products'...")
	resp, err := executeStatement(`
		CREATE TABLE IF NOT EXISTS products (
			id INTEGER,
			name VARCHAR,
			price DECIMAL(10,2),
			category VARCHAR,
			in_stock BOOLEAN
		)
	`, "DEMO_DB", "PUBLIC")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Printf("   Statement handle: %s\n", resp.StatementHandle)

	// Insert data
	fmt.Println("\n   Inserting sample data...")
	_, err = executeStatement(`
		INSERT INTO products VALUES
		(1, 'Laptop', 999.99, 'Electronics', true),
		(2, 'Mouse', 29.99, 'Electronics', true),
		(3, 'Keyboard', 79.99, 'Electronics', false),
		(4, 'Monitor', 299.99, 'Electronics', true),
		(5, 'Desk Chair', 199.99, 'Furniture', true)
	`, "DEMO_DB", "PUBLIC")
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	fmt.Println("   5 rows inserted")

	// Example 4: Query with Snowflake functions
	fmt.Println("\n4. Querying with Snowflake SQL functions...")

	// IFF function
	fmt.Println("\n   IFF function (price classification):")
	resp, err = executeStatement(`
		SELECT
			name,
			price,
			IFF(price > 100, 'Premium', 'Budget') AS tier
		FROM products
		ORDER BY price DESC
	`, "DEMO_DB", "PUBLIC")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResponse(resp)

	// NVL2 function with aggregation
	fmt.Println("\n   Aggregation with IFF:")
	resp, err = executeStatement(`
		SELECT
			category,
			COUNT(*) AS total,
			SUM(IFF(in_stock, 1, 0)) AS in_stock_count
		FROM products
		GROUP BY category
	`, "DEMO_DB", "PUBLIC")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResponse(resp)

	// LISTAGG function
	fmt.Println("\n   LISTAGG function (product names by category):")
	resp, err = executeStatement(`
		SELECT
			category,
			LISTAGG(name, ', ') AS products
		FROM products
		GROUP BY category
	`, "DEMO_DB", "PUBLIC")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResponse(resp)

	// Example 5: Query with parameter bindings
	fmt.Println("\n5. Query with parameter bindings...")
	resp, err = executeStatementWithBindings(
		"SELECT * FROM products WHERE price > :1 AND in_stock = :2",
		"DEMO_DB", "PUBLIC",
		map[string]Binding{
			"1": {Type: "REAL", Value: "50"},
			"2": {Type: "BOOLEAN", Value: "true"},
		},
	)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printResponse(resp)

	// Example 6: List resources
	fmt.Println("\n6. Listing resources...")

	fmt.Println("\n   Databases:")
	listDatabases()

	fmt.Println("\n   Warehouses:")
	listWarehouses()

	// Cleanup
	fmt.Println("\n7. Cleanup...")
	_, _ = executeStatement("DROP TABLE IF EXISTS products", "DEMO_DB", "PUBLIC")
	fmt.Println("   Table 'products' dropped")

	fmt.Println("\n=== Example completed successfully! ===")
}

func executeStatement(sql, database, schema string) (*StatementResponse, error) {
	return executeStatementWithBindings(sql, database, schema, nil)
}

func executeStatementWithBindings(sql, database, schema string, bindings map[string]Binding) (*StatementResponse, error) {
	req := StatementRequest{
		Statement: sql,
		Database:  database,
		Schema:    schema,
		Bindings:  bindings,
	}

	body, _ := json.Marshal(req)
	resp, err := http.Post(baseURL+"/statements", "application/json", bytes.NewReader(body))
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
	req := DatabaseRequest{Name: name}
	body, _ := json.Marshal(req)

	resp, err := http.Post(baseURL+"/databases", "application/json", bytes.NewReader(body))
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

func createWarehouse(name string) error {
	req := WarehouseRequest{
		Name:        name,
		Size:        "X-SMALL",
		AutoSuspend: 300,
		AutoResume:  true,
	}
	body, _ := json.Marshal(req)

	resp, err := http.Post(baseURL+"/warehouses", "application/json", bytes.NewReader(body))
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

func listDatabases() {
	resp, err := http.Get(baseURL + "/databases")
	if err != nil {
		log.Printf("Failed to list databases: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]any
	json.Unmarshal(body, &result)

	if databases, ok := result["databases"].([]any); ok {
		for _, db := range databases {
			if dbMap, ok := db.(map[string]any); ok {
				fmt.Printf("   - %s\n", dbMap["name"])
			}
		}
	}
}

func listWarehouses() {
	resp, err := http.Get(baseURL + "/warehouses")
	if err != nil {
		log.Printf("Failed to list warehouses: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]any
	json.Unmarshal(body, &result)

	if warehouses, ok := result["warehouses"].([]any); ok {
		for _, wh := range warehouses {
			if whMap, ok := wh.(map[string]any); ok {
				fmt.Printf("   - %s (size: %s, state: %s)\n",
					whMap["name"], whMap["size"], whMap["state"])
			}
		}
	}
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
