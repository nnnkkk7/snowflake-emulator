package metadata

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
)

// setupTestRepository creates a test repository with an in-memory DuckDB.
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
	repo, err := NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	return repo
}

// TestRepository_CreateDatabase tests database creation.
func TestRepository_CreateDatabase(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		dbName  string
		comment string
		wantErr bool
	}{
		{name: "ValidName", dbName: "TEST_DB", comment: "Test database", wantErr: false},
		{name: "WithUnderscore", dbName: "MY_TEST_DB", comment: "", wantErr: false},
		{name: "LowerCase", dbName: "test_db_lower", comment: "", wantErr: false},
		{name: "EmptyName", dbName: "", comment: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := repo.CreateDatabase(ctx, tt.dbName, tt.comment)

			if (err != nil) != tt.wantErr {
				t.Errorf("CreateDatabase() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Verify the database was created
				if db.ID == "" {
					t.Error("CreateDatabase() returned database with empty ID")
				}

				// Normalize name for comparison (Snowflake normalizes unquoted names to uppercase)
				wantName := strings.ToUpper(tt.dbName)
				if diff := cmp.Diff(wantName, db.Name); diff != "" {
					t.Errorf("Database name mismatch (-want +got):\n%s", diff)
				}

				if diff := cmp.Diff(tt.comment, db.Comment); diff != "" {
					t.Errorf("Database comment mismatch (-want +got):\n%s", diff)
				}

				// Verify we can retrieve the database
				gotDB, err := repo.GetDatabase(ctx, db.ID)
				if err != nil {
					t.Fatalf("GetDatabase() error = %v", err)
				}

				// Compare databases, ignoring CreatedAt which is set by DB
				opts := cmpopts.IgnoreFields(Database{}, "CreatedAt")
				if diff := cmp.Diff(db, gotDB, opts); diff != "" {
					t.Errorf("GetDatabase() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestRepository_CreateDatabase_Duplicate tests duplicate database creation.
func TestRepository_CreateDatabase_Duplicate(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	_, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("first CreateDatabase() error = %v", err)
	}

	_, err = repo.CreateDatabase(ctx, "TEST_DB", "")
	if err == nil {
		t.Error("expected error for duplicate database, got nil")
	}
}

// TestRepository_GetDatabase_NotFound tests getting a non-existent database.
func TestRepository_GetDatabase_NotFound(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	_, err := repo.GetDatabase(ctx, "nonexistent-id")
	if err == nil {
		t.Error("expected error for non-existent database, got nil")
	}
}

// TestRepository_ListDatabases tests listing all databases.
func TestRepository_ListDatabases(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	// Create multiple databases
	db1, err := repo.CreateDatabase(ctx, "DB_A", "Database A")
	if err != nil {
		t.Fatalf("CreateDatabase(DB_A) error = %v", err)
	}

	db2, err := repo.CreateDatabase(ctx, "DB_B", "Database B")
	if err != nil {
		t.Fatalf("CreateDatabase(DB_B) error = %v", err)
	}

	// List databases
	dbs, err := repo.ListDatabases(ctx)
	if err != nil {
		t.Fatalf("ListDatabases() error = %v", err)
	}

	if len(dbs) != 2 {
		t.Errorf("expected 2 databases, got %d", len(dbs))
	}

	// Verify databases are in the list
	opts := cmpopts.IgnoreFields(Database{}, "CreatedAt")
	found1 := false
	found2 := false
	for _, db := range dbs {
		if db.ID == db1.ID {
			found1 = true
			if diff := cmp.Diff(db1, db, opts); diff != "" {
				t.Errorf("DB_A mismatch (-want +got):\n%s", diff)
			}
		}
		if db.ID == db2.ID {
			found2 = true
			if diff := cmp.Diff(db2, db, opts); diff != "" {
				t.Errorf("DB_B mismatch (-want +got):\n%s", diff)
			}
		}
	}

	if !found1 {
		t.Error("DB_A not found in list")
	}
	if !found2 {
		t.Error("DB_B not found in list")
	}
}

// TestRepository_DropDatabase tests database deletion.
func TestRepository_DropDatabase(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	// Create a database
	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	// Drop the database
	err = repo.DropDatabase(ctx, db.ID)
	if err != nil {
		t.Errorf("DropDatabase() error = %v", err)
	}

	// Verify database is deleted
	_, err = repo.GetDatabase(ctx, db.ID)
	if err == nil {
		t.Error("expected error for deleted database, got nil")
	}
}

// TestRepository_DropDatabase_NotFound tests dropping a non-existent database.
func TestRepository_DropDatabase_NotFound(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	err := repo.DropDatabase(ctx, "nonexistent-id")
	if err == nil {
		t.Error("expected error for non-existent database, got nil")
	}
}

// TestRepository_GetDatabaseByName tests getting a database by name.
func TestRepository_GetDatabaseByName(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	// Create a database
	db, err := repo.CreateDatabase(ctx, "TEST_DB", "Test comment")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	tests := []struct {
		name    string
		dbName  string
		want    *Database
		wantErr bool
	}{
		{name: "ExactMatch", dbName: "TEST_DB", want: db, wantErr: false},
		{name: "NotFound", dbName: "NONEXISTENT", want: nil, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := repo.GetDatabaseByName(ctx, tt.dbName)

			if (err != nil) != tt.wantErr {
				t.Errorf("GetDatabaseByName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				opts := cmpopts.IgnoreFields(Database{}, "CreatedAt")
				if diff := cmp.Diff(tt.want, got, opts); diff != "" {
					t.Errorf("GetDatabaseByName() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestRepository_QueryHistory tests query history operations.
func TestRepository_QueryHistory(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	t.Run("RecordQueryStart", func(t *testing.T) {
		entry, err := repo.RecordQueryStart(ctx, "session-1", "query-1", "SELECT 1")
		if err != nil {
			t.Fatalf("RecordQueryStart() error = %v", err)
		}

		if entry.ID == "" {
			t.Error("expected non-empty ID")
		}
		if entry.SessionID != "session-1" {
			t.Errorf("expected SessionID 'session-1', got %s", entry.SessionID)
		}
		if entry.QueryID != "query-1" {
			t.Errorf("expected QueryID 'query-1', got %s", entry.QueryID)
		}
		if entry.SQLText != "SELECT 1" {
			t.Errorf("expected SQLText 'SELECT 1', got %s", entry.SQLText)
		}
		if entry.Status != "RUNNING" {
			t.Errorf("expected Status 'RUNNING', got %s", entry.Status)
		}
	})

	t.Run("RecordQuerySuccess", func(t *testing.T) {
		entry, err := repo.RecordQueryStart(ctx, "session-2", "query-2", "SELECT * FROM test")
		if err != nil {
			t.Fatalf("RecordQueryStart() error = %v", err)
		}

		err = repo.RecordQuerySuccess(ctx, entry.ID, 10, 150)
		if err != nil {
			t.Fatalf("RecordQuerySuccess() error = %v", err)
		}

		// Verify via GetQueryHistory
		history, err := repo.GetQueryHistory(ctx, 10)
		if err != nil {
			t.Fatalf("GetQueryHistory() error = %v", err)
		}

		var found *QueryHistoryEntry
		for _, e := range history {
			if e.ID == entry.ID {
				found = e
				break
			}
		}

		if found == nil {
			t.Fatal("entry not found in history")
		}
		if found.Status != "SUCCESS" {
			t.Errorf("expected Status 'SUCCESS', got %s", found.Status)
		}
		if found.RowsAffected != 10 {
			t.Errorf("expected RowsAffected 10, got %d", found.RowsAffected)
		}
		if found.ExecutionTimeMs != 150 {
			t.Errorf("expected ExecutionTimeMs 150, got %d", found.ExecutionTimeMs)
		}
		if found.CompletedAt == nil {
			t.Error("expected CompletedAt to be set")
		}
	})

	t.Run("RecordQueryFailure", func(t *testing.T) {
		entry, err := repo.RecordQueryStart(ctx, "session-3", "query-3", "SELECT * FROM nonexistent")
		if err != nil {
			t.Fatalf("RecordQueryStart() error = %v", err)
		}

		err = repo.RecordQueryFailure(ctx, entry.ID, "Table not found", 50)
		if err != nil {
			t.Fatalf("RecordQueryFailure() error = %v", err)
		}

		// Verify via GetQueryHistory
		history, err := repo.GetQueryHistory(ctx, 10)
		if err != nil {
			t.Fatalf("GetQueryHistory() error = %v", err)
		}

		var found *QueryHistoryEntry
		for _, e := range history {
			if e.ID == entry.ID {
				found = e
				break
			}
		}

		if found == nil {
			t.Fatal("entry not found in history")
		}
		if found.Status != "FAILED" {
			t.Errorf("expected Status 'FAILED', got %s", found.Status)
		}
		if found.ErrorMessage != "Table not found" {
			t.Errorf("expected ErrorMessage 'Table not found', got %s", found.ErrorMessage)
		}
		if found.ExecutionTimeMs != 50 {
			t.Errorf("expected ExecutionTimeMs 50, got %d", found.ExecutionTimeMs)
		}
	})
}

// TestRepository_GetQueryHistoryBySession tests session-specific query history.
func TestRepository_GetQueryHistoryBySession(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	// Create entries for different sessions
	_, err := repo.RecordQueryStart(ctx, "session-a", "query-1", "SELECT 1")
	if err != nil {
		t.Fatalf("RecordQueryStart() error = %v", err)
	}

	_, err = repo.RecordQueryStart(ctx, "session-a", "query-2", "SELECT 2")
	if err != nil {
		t.Fatalf("RecordQueryStart() error = %v", err)
	}

	_, err = repo.RecordQueryStart(ctx, "session-b", "query-3", "SELECT 3")
	if err != nil {
		t.Fatalf("RecordQueryStart() error = %v", err)
	}

	// Get history for session-a
	history, err := repo.GetQueryHistoryBySession(ctx, "session-a", 10)
	if err != nil {
		t.Fatalf("GetQueryHistoryBySession() error = %v", err)
	}

	if len(history) != 2 {
		t.Errorf("expected 2 entries for session-a, got %d", len(history))
	}

	for _, entry := range history {
		if entry.SessionID != "session-a" {
			t.Errorf("expected SessionID 'session-a', got %s", entry.SessionID)
		}
	}

	// Get history for session-b
	history, err = repo.GetQueryHistoryBySession(ctx, "session-b", 10)
	if err != nil {
		t.Fatalf("GetQueryHistoryBySession() error = %v", err)
	}

	if len(history) != 1 {
		t.Errorf("expected 1 entry for session-b, got %d", len(history))
	}
}

// TestRepository_ClearQueryHistory tests clearing old query history.
func TestRepository_ClearQueryHistory(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	// Create some entries
	_, err := repo.RecordQueryStart(ctx, "session-1", "query-1", "SELECT 1")
	if err != nil {
		t.Fatalf("RecordQueryStart() error = %v", err)
	}

	_, err = repo.RecordQueryStart(ctx, "session-1", "query-2", "SELECT 2")
	if err != nil {
		t.Fatalf("RecordQueryStart() error = %v", err)
	}

	// Wait a bit to ensure time difference
	time.Sleep(10 * time.Millisecond)

	// Clear entries older than now (should clear all)
	futureTime := time.Now().Add(time.Hour)
	deleted, err := repo.ClearQueryHistory(ctx, futureTime)
	if err != nil {
		t.Fatalf("ClearQueryHistory() error = %v", err)
	}

	if deleted != 2 {
		t.Errorf("expected 2 deleted entries, got %d", deleted)
	}

	// Verify history is empty
	history, err := repo.GetQueryHistory(ctx, 10)
	if err != nil {
		t.Fatalf("GetQueryHistory() error = %v", err)
	}

	if len(history) != 0 {
		t.Errorf("expected 0 entries after clear, got %d", len(history))
	}
}

// TestRepository_GetQueryHistory_Limit tests the limit parameter.
func TestRepository_GetQueryHistory_Limit(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	// Create 5 entries
	for i := 0; i < 5; i++ {
		_, err := repo.RecordQueryStart(ctx, "session-1", "query-"+string(rune('1'+i)), "SELECT "+string(rune('1'+i)))
		if err != nil {
			t.Fatalf("RecordQueryStart() error = %v", err)
		}
	}

	// Get with limit 3
	history, err := repo.GetQueryHistory(ctx, 3)
	if err != nil {
		t.Fatalf("GetQueryHistory() error = %v", err)
	}

	if len(history) != 3 {
		t.Errorf("expected 3 entries with limit, got %d", len(history))
	}

	// Get with default limit (0 means 100)
	history, err = repo.GetQueryHistory(ctx, 0)
	if err != nil {
		t.Fatalf("GetQueryHistory() error = %v", err)
	}

	if len(history) != 5 {
		t.Errorf("expected 5 entries with default limit, got %d", len(history))
	}
}
