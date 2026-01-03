package query

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/stage"
)

func setupCopyProcessorTest(t *testing.T) (*CopyProcessor, *stage.Manager, *metadata.Repository, string, func()) {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB: %v", err)
	}

	connMgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(connMgr)
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create repository: %v", err)
	}

	// Create temp directory for stages
	tempDir, err := os.MkdirTemp("", "copy_test_*")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	stageMgr := stage.NewManager(repo, tempDir)
	executor := NewExecutor(connMgr, repo)
	handler := NewCopyProcessor(stageMgr, repo, executor)

	cleanup := func() {
		os.RemoveAll(tempDir)
		db.Close()
	}

	return handler, stageMgr, repo, tempDir, cleanup
}

func TestCopyProcessor_ParseCopyStatement(t *testing.T) {
	handler, _, _, _, cleanup := setupCopyProcessorTest(t)
	defer cleanup()

	testCases := []struct {
		name    string
		sql     string
		want    *CopyStatement
		wantErr bool
	}{
		{
			name: "BasicCopy",
			sql:  "COPY INTO my_table FROM @my_stage",
			want: &CopyStatement{
				TargetTable: "MY_TABLE",
				StageName:   "MY_STAGE",
				FileFormat: FileFormatOptions{
					Type:            "CSV",
					FieldDelimiter:  ",",
					RecordDelimiter: "\n",
					SkipHeader:      0,
				},
				OnError: "ABORT",
			},
		},
		{
			name: "CopyWithPath",
			sql:  "COPY INTO my_table FROM @my_stage/data/files",
			want: &CopyStatement{
				TargetTable: "MY_TABLE",
				StageName:   "MY_STAGE",
				StagePath:   "data/files",
				FileFormat: FileFormatOptions{
					Type:            "CSV",
					FieldDelimiter:  ",",
					RecordDelimiter: "\n",
					SkipHeader:      0,
				},
				OnError: "ABORT",
			},
		},
		{
			name: "CopyWithFileFormat",
			sql:  "COPY INTO my_table FROM @my_stage FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 1)",
			want: &CopyStatement{
				TargetTable: "MY_TABLE",
				StageName:   "MY_STAGE",
				FileFormat: FileFormatOptions{
					Type:            "CSV",
					FieldDelimiter:  "|",
					RecordDelimiter: "\n",
					SkipHeader:      1,
				},
				OnError: "ABORT",
			},
		},
		{
			name: "CopyWithPattern",
			sql:  "COPY INTO my_table FROM @my_stage PATTERN = '*.csv'",
			want: &CopyStatement{
				TargetTable: "MY_TABLE",
				StageName:   "MY_STAGE",
				Pattern:     "*.csv",
				FileFormat: FileFormatOptions{
					Type:            "CSV",
					FieldDelimiter:  ",",
					RecordDelimiter: "\n",
					SkipHeader:      0,
				},
				OnError: "ABORT",
			},
		},
		{
			name: "CopyWithFullTableName",
			sql:  "COPY INTO my_db.my_schema.my_table FROM @my_stage",
			want: &CopyStatement{
				TargetDatabase: "MY_DB",
				TargetSchema:   "MY_SCHEMA",
				TargetTable:    "MY_TABLE",
				StageName:      "MY_STAGE",
				FileFormat: FileFormatOptions{
					Type:            "CSV",
					FieldDelimiter:  ",",
					RecordDelimiter: "\n",
					SkipHeader:      0,
				},
				OnError: "ABORT",
			},
		},
		{
			name: "CopyWithOnError",
			sql:  "COPY INTO my_table FROM @my_stage ON_ERROR = CONTINUE",
			want: &CopyStatement{
				TargetTable: "MY_TABLE",
				StageName:   "MY_STAGE",
				FileFormat: FileFormatOptions{
					Type:            "CSV",
					FieldDelimiter:  ",",
					RecordDelimiter: "\n",
					SkipHeader:      0,
				},
				OnError: "CONTINUE",
			},
		},
		{
			name: "CopyWithPurge",
			sql:  "COPY INTO my_table FROM @my_stage PURGE = TRUE",
			want: &CopyStatement{
				TargetTable: "MY_TABLE",
				StageName:   "MY_STAGE",
				FileFormat: FileFormatOptions{
					Type:            "CSV",
					FieldDelimiter:  ",",
					RecordDelimiter: "\n",
					SkipHeader:      0,
				},
				OnError:    "ABORT",
				PurgeFiles: true,
			},
		},
		{
			name: "CopyJSON",
			sql:  "COPY INTO my_table FROM @my_stage FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE)",
			want: &CopyStatement{
				TargetTable: "MY_TABLE",
				StageName:   "MY_STAGE",
				FileFormat: FileFormatOptions{
					Type:            "JSON",
					FieldDelimiter:  ",",
					RecordDelimiter: "\n",
					SkipHeader:      0,
					StripOuterArray: true,
				},
				OnError: "ABORT",
			},
		},
		{
			name:    "InvalidSyntax",
			sql:     "COPY FROM somewhere",
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := handler.ParseCopyStatement(tc.sql)

			if tc.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Compare key fields
			if got.TargetTable != tc.want.TargetTable {
				t.Errorf("TargetTable: got %s, want %s", got.TargetTable, tc.want.TargetTable)
			}
			if got.TargetSchema != tc.want.TargetSchema {
				t.Errorf("TargetSchema: got %s, want %s", got.TargetSchema, tc.want.TargetSchema)
			}
			if got.TargetDatabase != tc.want.TargetDatabase {
				t.Errorf("TargetDatabase: got %s, want %s", got.TargetDatabase, tc.want.TargetDatabase)
			}
			if got.StageName != tc.want.StageName {
				t.Errorf("StageName: got %s, want %s", got.StageName, tc.want.StageName)
			}
			if got.StagePath != tc.want.StagePath {
				t.Errorf("StagePath: got %s, want %s", got.StagePath, tc.want.StagePath)
			}
			if got.Pattern != tc.want.Pattern {
				t.Errorf("Pattern: got %s, want %s", got.Pattern, tc.want.Pattern)
			}
			if got.OnError != tc.want.OnError {
				t.Errorf("OnError: got %s, want %s", got.OnError, tc.want.OnError)
			}
			if got.PurgeFiles != tc.want.PurgeFiles {
				t.Errorf("PurgeFiles: got %v, want %v", got.PurgeFiles, tc.want.PurgeFiles)
			}
			if diff := cmp.Diff(tc.want.FileFormat, got.FileFormat); diff != "" {
				t.Errorf("FileFormat mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCopyProcessor_ExecuteCopyCSV(t *testing.T) {
	handler, stageMgr, repo, _, cleanup := setupCopyProcessorTest(t)
	defer cleanup()

	ctx := context.Background()

	// Setup: Create database, schema, stage, and table
	db, _ := repo.CreateDatabase(ctx, "COPY_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	_, _ = stageMgr.CreateStage(ctx, schema.ID, "DATA_STAGE", "INTERNAL", "", "")

	// Create target table (DuckDB uses DATABASE.SCHEMA_TABLE format)
	_, err := handler.executor.Execute(ctx, "CREATE TABLE COPY_DB.PUBLIC_LOAD_TABLE (id INTEGER, name VARCHAR, value DOUBLE)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Put CSV file in stage
	csvData := `1,Alice,10.5
2,Bob,20.0
3,Charlie,30.5`
	err = stageMgr.PutFile(ctx, schema.ID, "DATA_STAGE", "data.csv", bytes.NewReader([]byte(csvData)))
	if err != nil {
		t.Fatalf("Failed to put file: %v", err)
	}

	// Execute COPY INTO - use proper database/schema/table structure
	stmt := &CopyStatement{
		TargetTable:    "LOAD_TABLE",
		TargetSchema:   "PUBLIC",
		TargetDatabase: "COPY_DB",
		StageName:      "DATA_STAGE",
		FileFormat: FileFormatOptions{
			Type:           "CSV",
			FieldDelimiter: ",",
		},
		OnError: "ABORT",
	}

	result, err := handler.ExecuteCopyInto(ctx, stmt, schema.ID)
	if err != nil {
		t.Fatalf("ExecuteCopyInto failed: %v", err)
	}

	if result.RowsLoaded != 3 {
		t.Errorf("Expected 3 rows loaded, got %d", result.RowsLoaded)
	}

	if result.FilesLoaded != 1 {
		t.Errorf("Expected 1 file loaded, got %d", result.FilesLoaded)
	}

	// Verify data was loaded
	queryResult, err := handler.executor.Query(ctx, "SELECT * FROM COPY_DB.PUBLIC_LOAD_TABLE ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(queryResult.Rows) != 3 {
		t.Errorf("Expected 3 rows in table, got %d", len(queryResult.Rows))
	}
}

func TestCopyProcessor_ExecuteCopyCSVWithSkipHeader(t *testing.T) {
	handler, stageMgr, repo, _, cleanup := setupCopyProcessorTest(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "HEADER_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	_, _ = stageMgr.CreateStage(ctx, schema.ID, "HEADER_STAGE", "INTERNAL", "", "")

	_, _ = handler.executor.Execute(ctx, "CREATE TABLE HEADER_DB.PUBLIC_HEADER_TABLE (id INTEGER, name VARCHAR)")

	// CSV with header
	csvData := `id,name
1,Alice
2,Bob`
	_ = stageMgr.PutFile(ctx, schema.ID, "HEADER_STAGE", "with_header.csv", bytes.NewReader([]byte(csvData)))

	stmt := &CopyStatement{
		TargetTable:    "HEADER_TABLE",
		TargetSchema:   "PUBLIC",
		TargetDatabase: "HEADER_DB",
		StageName:      "HEADER_STAGE",
		FileFormat: FileFormatOptions{
			Type:           "CSV",
			FieldDelimiter: ",",
			SkipHeader:     1,
		},
		OnError: "ABORT",
	}

	result, err := handler.ExecuteCopyInto(ctx, stmt, schema.ID)
	if err != nil {
		t.Fatalf("ExecuteCopyInto failed: %v", err)
	}

	if result.RowsLoaded != 2 {
		t.Errorf("Expected 2 rows loaded (header skipped), got %d", result.RowsLoaded)
	}
}

func TestCopyProcessor_ExecuteCopyJSON(t *testing.T) {
	handler, stageMgr, repo, _, cleanup := setupCopyProcessorTest(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "JSON_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	_, _ = stageMgr.CreateStage(ctx, schema.ID, "JSON_STAGE", "INTERNAL", "", "")

	_, _ = handler.executor.Execute(ctx, "CREATE TABLE JSON_DB.PUBLIC_JSON_TABLE (data VARCHAR)")

	// JSON array
	jsonData := `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`
	_ = stageMgr.PutFile(ctx, schema.ID, "JSON_STAGE", "data.json", bytes.NewReader([]byte(jsonData)))

	stmt := &CopyStatement{
		TargetTable:    "JSON_TABLE",
		TargetSchema:   "PUBLIC",
		TargetDatabase: "JSON_DB",
		StageName:      "JSON_STAGE",
		FileFormat: FileFormatOptions{
			Type:            "JSON",
			StripOuterArray: true,
		},
		OnError: "ABORT",
	}

	result, err := handler.ExecuteCopyInto(ctx, stmt, schema.ID)
	if err != nil {
		t.Fatalf("ExecuteCopyInto failed: %v", err)
	}

	if result.RowsLoaded != 2 {
		t.Errorf("Expected 2 rows loaded, got %d", result.RowsLoaded)
	}

	// Verify data
	queryResult, err := handler.executor.Query(ctx, "SELECT * FROM JSON_DB.PUBLIC_JSON_TABLE")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(queryResult.Rows) != 2 {
		t.Errorf("Expected 2 rows in table, got %d", len(queryResult.Rows))
	}
}

func TestCopyProcessor_ExecuteCopyWithPurge(t *testing.T) {
	handler, stageMgr, repo, _, cleanup := setupCopyProcessorTest(t)
	defer cleanup()

	ctx := context.Background()

	db, err := repo.CreateDatabase(ctx, "PURGE_DB", "")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	schema, err := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	_, err = stageMgr.CreateStage(ctx, schema.ID, "PURGE_STAGE", "INTERNAL", "", "")
	if err != nil {
		t.Fatalf("Failed to create stage: %v", err)
	}

	// Use DATABASE.SCHEMA_TABLE format for DuckDB
	_, err = handler.executor.Execute(ctx, "CREATE TABLE PURGE_DB.PUBLIC_PURGE_DATA (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_ = stageMgr.PutFile(ctx, schema.ID, "PURGE_STAGE", "purge.csv", bytes.NewReader([]byte("1\n2\n3")))

	// Verify file exists
	files, _ := stageMgr.ListFiles(ctx, schema.ID, "PURGE_STAGE", "")
	if len(files) != 1 {
		t.Fatalf("Expected 1 file before copy, got %d", len(files))
	}

	stmt := &CopyStatement{
		TargetTable:    "PURGE_DATA",
		TargetSchema:   "PUBLIC",
		TargetDatabase: "PURGE_DB",
		StageName:      "PURGE_STAGE",
		FileFormat: FileFormatOptions{
			Type:           "CSV",
			FieldDelimiter: ",",
		},
		OnError:    "ABORT",
		PurgeFiles: true,
	}

	_, err = handler.ExecuteCopyInto(ctx, stmt, schema.ID)
	if err != nil {
		t.Fatalf("ExecuteCopyInto failed: %v", err)
	}

	// Verify file was purged
	files, err = stageMgr.ListFiles(ctx, schema.ID, "PURGE_STAGE", "")
	if err != nil {
		t.Fatalf("Failed to list files: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("Expected 0 files after purge, got %d", len(files))
	}
}

func TestCopyProcessor_ExecuteCopyNoFiles(t *testing.T) {
	handler, stageMgr, repo, _, cleanup := setupCopyProcessorTest(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "TEST_DB5", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")
	_, _ = stageMgr.CreateStage(ctx, schema.ID, "EMPTY_STAGE", "INTERNAL", "", "")

	stmt := &CopyStatement{
		TargetTable: "SOME_TABLE",
		StageName:   "EMPTY_STAGE",
		FileFormat: FileFormatOptions{
			Type: "CSV",
		},
		OnError: "ABORT",
	}

	result, err := handler.ExecuteCopyInto(ctx, stmt, schema.ID)
	if err != nil {
		t.Fatalf("ExecuteCopyInto should succeed with empty stage: %v", err)
	}

	if result.RowsLoaded != 0 {
		t.Errorf("Expected 0 rows loaded from empty stage, got %d", result.RowsLoaded)
	}

	if result.FilesLoaded != 0 {
		t.Errorf("Expected 0 files loaded from empty stage, got %d", result.FilesLoaded)
	}
}
