package connection

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	_ "github.com/marcboeker/go-duckdb"
)

// setupTestDuckDB creates an in-memory DuckDB database for testing.
func setupTestDuckDB(t *testing.T) *sql.DB {
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

	return db
}

// TestManager_Query_Concurrent tests concurrent read queries.
// Based on DESIGN.md Section 15.3.1.
func TestManager_Query_Concurrent(t *testing.T) {
	db := setupTestDuckDB(t)
	mgr := NewManager(db)

	// Create a test table
	_, err := db.Exec("CREATE TABLE test_table (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec("INSERT INTO test_table VALUES (1, 100), (2, 200), (3, 300)")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	const goroutines = 10
	results := make(chan int, goroutines)
	errors := make(chan error, goroutines)

	// Execute concurrent queries
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			rows, err := mgr.Query(context.Background(), "SELECT value FROM test_table WHERE id = ?", id%3+1)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: query failed: %w", id, err)
				return
			}
			defer rows.Close()

			if !rows.Next() {
				errors <- fmt.Errorf("goroutine %d: no rows returned", id)
				return
			}

			var val int
			if err := rows.Scan(&val); err != nil {
				errors <- fmt.Errorf("goroutine %d: scan failed: %w", id, err)
				return
			}
			results <- val
			errors <- nil
		}(i)
	}

	// Collect results
	got := make([]int, 0, goroutines)
	for i := 0; i < goroutines; i++ {
		if err := <-errors; err != nil {
			t.Error(err)
		}
		select {
		case val := <-results:
			got = append(got, val)
		default:
			// Error occurred, already logged
		}
	}

	// Verify all goroutines completed without deadlock
	if len(got) != goroutines {
		t.Errorf("expected %d results, got %d", goroutines, len(got))
	}

	// Verify we got valid results (100, 200, or 300)
	for _, val := range got {
		if val != 100 && val != 200 && val != 300 {
			t.Errorf("unexpected value: %d", val)
		}
	}
}

// TestManager_Exec_Sequential tests sequential write operations.
// Based on DESIGN.md Section 15.3.1.
func TestManager_Exec_Sequential(t *testing.T) {
	db := setupTestDuckDB(t)
	mgr := NewManager(db)

	tests := []struct {
		name string
		sql  string
	}{
		{name: "Create", sql: "CREATE TABLE test (id INT)"},
		{name: "Insert1", sql: "INSERT INTO test VALUES (1)"},
		{name: "Insert2", sql: "INSERT INTO test VALUES (2)"},
		{name: "Drop", sql: "DROP TABLE test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mgr.Exec(context.Background(), tt.sql)
			if err != nil {
				t.Errorf("Exec() error = %v", err)
			}
		})
	}
}

// TestManager_ExecTx tests transaction execution.
func TestManager_ExecTx(t *testing.T) {
	db := setupTestDuckDB(t)
	mgr := NewManager(db)

	// Create table outside transaction
	_, err := mgr.Exec(context.Background(), "CREATE TABLE test (id INT, name VARCHAR)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tests := []struct {
		name    string
		fn      func(*sql.Tx) error
		wantErr bool
	}{
		{
			name: "SuccessfulTransaction",
			fn: func(tx *sql.Tx) error {
				_, err := tx.Exec("INSERT INTO test VALUES (1, 'Alice')")
				if err != nil {
					return err
				}
				_, err = tx.Exec("INSERT INTO test VALUES (2, 'Bob')")
				return err
			},
			wantErr: false,
		},
		{
			name: "FailedTransaction",
			fn: func(tx *sql.Tx) error {
				_, err := tx.Exec("INSERT INTO test VALUES (3, 'Charlie')")
				if err != nil {
					return err
				}
				// This should fail and cause rollback
				return fmt.Errorf("simulated error")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.ExecTx(context.Background(), tt.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecTx() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// Verify only successful transaction was committed
	rows, err := mgr.Query(context.Background(), "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("no rows returned")
	}

	var count int
	if err := rows.Scan(&count); err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	wantCount := 2 // Only Alice and Bob should be inserted
	if diff := cmp.Diff(wantCount, count); diff != "" {
		t.Errorf("count mismatch (-want +got):\n%s", diff)
	}
}

// TestManager_NewManager tests the NewManager constructor.
func TestManager_NewManager(t *testing.T) {
	db := setupTestDuckDB(t)
	mgr := NewManager(db)

	if mgr == nil {
		t.Error("NewManager() returned nil")
	}

	// Verify we can execute a simple query
	rows, err := mgr.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if rows != nil {
		rows.Close()
	}
}

// TestManager_QueryContext tests Query with context cancellation.
func TestManager_QueryContext(t *testing.T) {
	db := setupTestDuckDB(t)
	mgr := NewManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := mgr.Query(ctx, "SELECT 1")
	if err == nil {
		t.Error("expected error with cancelled context, got nil")
	}
}

// TestManager_ExecContext tests Exec with context cancellation.
func TestManager_ExecContext(t *testing.T) {
	db := setupTestDuckDB(t)
	mgr := NewManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := mgr.Exec(ctx, "CREATE TABLE test (id INT)")
	if err == nil {
		t.Error("expected error with cancelled context, got nil")
	}
}
