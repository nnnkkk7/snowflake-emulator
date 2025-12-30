package metadata

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	_ "github.com/marcboeker/go-duckdb"
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
