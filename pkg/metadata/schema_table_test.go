package metadata

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// TestRepository_CreateSchema tests schema creation.
func TestRepository_CreateSchema(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	// Create a database first
	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	tests := []struct {
		name    string
		schema  string
		comment string
		wantErr bool
	}{
		{name: "ValidSchema", schema: "PUBLIC", comment: "Public schema", wantErr: false},
		{name: "CustomSchema", schema: "MY_SCHEMA", comment: "", wantErr: false},
		{name: "EmptyName", schema: "", comment: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := repo.CreateSchema(ctx, db.ID, tt.schema, tt.comment)

			if (err != nil) != tt.wantErr {
				t.Errorf("CreateSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				wantName := strings.ToUpper(tt.schema)
				if diff := cmp.Diff(wantName, schema.Name); diff != "" {
					t.Errorf("Schema name mismatch (-want +got):\n%s", diff)
				}

				if diff := cmp.Diff(db.ID, schema.DatabaseID); diff != "" {
					t.Errorf("DatabaseID mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestRepository_CreateTable tests table creation.
func TestRepository_CreateTable(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	// Setup: Create database and schema
	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	tests := []struct {
		name      string
		tableName string
		columns   []ColumnDef
		comment   string
		wantErr   bool
	}{
		{
			name:      "SimpleTable",
			tableName: "USERS",
			columns: []ColumnDef{
				{Name: "ID", Type: "INTEGER", Nullable: false, PrimaryKey: true},
				{Name: "NAME", Type: "VARCHAR", Nullable: false},
			},
			comment: "Users table",
			wantErr: false,
		},
		{
			name:      "EmptyColumns",
			tableName: "EMPTY_TABLE",
			columns:   []ColumnDef{},
			comment:   "",
			wantErr:   true,
		},
		{
			name:      "EmptyName",
			tableName: "",
			columns: []ColumnDef{
				{Name: "ID", Type: "INTEGER"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, err := repo.CreateTable(ctx, schema.ID, tt.tableName, tt.columns, tt.comment)

			if (err != nil) != tt.wantErr {
				t.Errorf("CreateTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				wantName := strings.ToUpper(tt.tableName)
				if diff := cmp.Diff(wantName, table.Name); diff != "" {
					t.Errorf("Table name mismatch (-want +got):\n%s", diff)
				}

				if diff := cmp.Diff(schema.ID, table.SchemaID); diff != "" {
					t.Errorf("SchemaID mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestRepository_GetSchema tests schema retrieval.
func TestRepository_GetSchema(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "Test schema")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	got, err := repo.GetSchema(ctx, schema.ID)
	if err != nil {
		t.Fatalf("GetSchema() error = %v", err)
	}

	opts := cmpopts.IgnoreFields(Schema{}, "CreatedAt")
	if diff := cmp.Diff(schema, got, opts); diff != "" {
		t.Errorf("GetSchema() mismatch (-want +got):\n%s", diff)
	}
}

// TestRepository_GetTable tests table retrieval.
func TestRepository_GetTable(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	columns := []ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
	}
	table, err := repo.CreateTable(ctx, schema.ID, "USERS", columns, "Users table")
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	got, err := repo.GetTable(ctx, table.ID)
	if err != nil {
		t.Fatalf("GetTable() error = %v", err)
	}

	opts := cmpopts.IgnoreFields(Table{}, "CreatedAt")
	if diff := cmp.Diff(table, got, opts); diff != "" {
		t.Errorf("GetTable() mismatch (-want +got):\n%s", diff)
	}
}

// TestRepository_ListSchemas tests listing schemas in a database.
func TestRepository_ListSchemas(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	// Create multiple schemas
	s1, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema(PUBLIC) error = %v", err)
	}

	s2, err := repo.CreateSchema(ctx, db.ID, "STAGING", "")
	if err != nil {
		t.Fatalf("CreateSchema(STAGING) error = %v", err)
	}

	schemas, err := repo.ListSchemas(ctx, db.ID)
	if err != nil {
		t.Fatalf("ListSchemas() error = %v", err)
	}

	if len(schemas) != 2 {
		t.Errorf("expected 2 schemas, got %d", len(schemas))
	}

	// Verify schemas are in the list
	opts := cmpopts.IgnoreFields(Schema{}, "CreatedAt")
	found1, found2 := false, false
	for _, s := range schemas {
		if s.ID == s1.ID {
			found1 = true
			if diff := cmp.Diff(s1, s, opts); diff != "" {
				t.Errorf("Schema PUBLIC mismatch (-want +got):\n%s", diff)
			}
		}
		if s.ID == s2.ID {
			found2 = true
			if diff := cmp.Diff(s2, s, opts); diff != "" {
				t.Errorf("Schema STAGING mismatch (-want +got):\n%s", diff)
			}
		}
	}

	if !found1 || !found2 {
		t.Error("not all schemas found in list")
	}
}

// TestRepository_ListTables tests listing tables in a schema.
func TestRepository_ListTables(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	// Create multiple tables
	cols := []ColumnDef{{Name: "ID", Type: "INTEGER"}}
	t1, err := repo.CreateTable(ctx, schema.ID, "USERS", cols, "")
	if err != nil {
		t.Fatalf("CreateTable(USERS) error = %v", err)
	}

	t2, err := repo.CreateTable(ctx, schema.ID, "ORDERS", cols, "")
	if err != nil {
		t.Fatalf("CreateTable(ORDERS) error = %v", err)
	}

	tables, err := repo.ListTables(ctx, schema.ID)
	if err != nil {
		t.Fatalf("ListTables() error = %v", err)
	}

	if len(tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(tables))
	}

	// Verify tables are in the list
	opts := cmpopts.IgnoreFields(Table{}, "CreatedAt")
	found1, found2 := false, false
	for _, tbl := range tables {
		if tbl.ID == t1.ID {
			found1 = true
			if diff := cmp.Diff(t1, tbl, opts); diff != "" {
				t.Errorf("Table USERS mismatch (-want +got):\n%s", diff)
			}
		}
		if tbl.ID == t2.ID {
			found2 = true
			if diff := cmp.Diff(t2, tbl, opts); diff != "" {
				t.Errorf("Table ORDERS mismatch (-want +got):\n%s", diff)
			}
		}
	}

	if !found1 || !found2 {
		t.Error("not all tables found in list")
	}
}

// TestRepository_DropSchema tests schema deletion.
func TestRepository_DropSchema(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	err = repo.DropSchema(ctx, schema.ID)
	if err != nil {
		t.Errorf("DropSchema() error = %v", err)
	}

	// Verify schema is deleted
	_, err = repo.GetSchema(ctx, schema.ID)
	if err == nil {
		t.Error("expected error for deleted schema, got nil")
	}
}

// TestRepository_DropTable tests table deletion.
func TestRepository_DropTable(t *testing.T) {
	repo := setupTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	schema, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	cols := []ColumnDef{{Name: "ID", Type: "INTEGER"}}
	table, err := repo.CreateTable(ctx, schema.ID, "USERS", cols, "")
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	err = repo.DropTable(ctx, table.ID)
	if err != nil {
		t.Errorf("DropTable() error = %v", err)
	}

	// Verify table is deleted
	_, err = repo.GetTable(ctx, table.ID)
	if err == nil {
		t.Error("expected error for deleted table, got nil")
	}
}
