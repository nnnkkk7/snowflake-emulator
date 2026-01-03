package stage

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

func setupTestManager(t *testing.T) (*Manager, *metadata.Repository, string, func()) {
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
	tempDir, err := os.MkdirTemp("", "stage_test_*")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	mgr := NewManager(repo, tempDir)

	cleanup := func() {
		os.RemoveAll(tempDir)
		db.Close()
	}

	return mgr, repo, tempDir, cleanup
}

func TestManager_CreateStage(t *testing.T) {
	mgr, repo, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a database and schema first
	db, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	schema, err := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	testCases := []struct {
		name      string
		stageName string
		stageType string
		url       string
		comment   string
		wantErr   bool
	}{
		{
			name:      "InternalStage",
			stageName: "MY_STAGE",
			stageType: "INTERNAL",
			comment:   "Test stage",
			wantErr:   false,
		},
		{
			name:      "DefaultStageType",
			stageName: "DEFAULT_STAGE",
			stageType: "",
			comment:   "Default type stage",
			wantErr:   false,
		},
		{
			name:      "EmptyName",
			stageName: "",
			stageType: "INTERNAL",
			wantErr:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stage, err := mgr.CreateStage(ctx, schema.ID, tc.stageName, tc.stageType, tc.url, tc.comment)

			if tc.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if stage.Name != tc.stageName && tc.stageName != "" {
				t.Errorf("Expected name %s, got %s", tc.stageName, stage.Name)
			}

			// Verify stage was created in repository
			_, err = repo.GetStageByName(ctx, schema.ID, tc.stageName)
			if err != nil {
				t.Errorf("Stage not found in repository: %v", err)
			}
		})
	}
}

func TestManager_PutAndGetFile(t *testing.T) {
	mgr, repo, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create database, schema, and stage
	db, _ := repo.CreateDatabase(ctx, "TEST_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")
	_, err := mgr.CreateStage(ctx, schema.ID, "FILE_STAGE", "INTERNAL", "", "")
	if err != nil {
		t.Fatalf("Failed to create stage: %v", err)
	}

	// Test PUT file
	testContent := []byte("Hello, Stage!")
	err = mgr.PutFile(ctx, schema.ID, "FILE_STAGE", "test.txt", bytes.NewReader(testContent))
	if err != nil {
		t.Fatalf("PutFile failed: %v", err)
	}

	// Test GET file
	reader, err := mgr.GetFile(ctx, schema.ID, "FILE_STAGE", "test.txt")
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	if diff := cmp.Diff(testContent, content); diff != "" {
		t.Errorf("Content mismatch (-want +got):\n%s", diff)
	}
}

func TestManager_PutFileNestedPath(t *testing.T) {
	mgr, repo, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "TEST_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")
	_, _ = mgr.CreateStage(ctx, schema.ID, "NESTED_STAGE", "INTERNAL", "", "")

	// Test PUT file with nested path
	testContent := []byte("Nested content")
	err := mgr.PutFile(ctx, schema.ID, "NESTED_STAGE", "subdir/file.txt", bytes.NewReader(testContent))
	if err != nil {
		t.Fatalf("PutFile with nested path failed: %v", err)
	}

	// Verify file was created
	reader, err := mgr.GetFile(ctx, schema.ID, "NESTED_STAGE", "subdir/file.txt")
	if err != nil {
		t.Fatalf("GetFile with nested path failed: %v", err)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	if diff := cmp.Diff(testContent, content); diff != "" {
		t.Errorf("Content mismatch (-want +got):\n%s", diff)
	}
}

func TestManager_PutFileInvalidPath(t *testing.T) {
	mgr, repo, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "TEST_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")
	_, _ = mgr.CreateStage(ctx, schema.ID, "SECURE_STAGE", "INTERNAL", "", "")

	testCases := []struct {
		name     string
		fileName string
	}{
		{"DirectoryTraversal", "../../../etc/passwd"},
		{"AbsolutePath", "/etc/passwd"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := mgr.PutFile(ctx, schema.ID, "SECURE_STAGE", tc.fileName, bytes.NewReader([]byte("test")))
			if err == nil {
				t.Error("Expected error for invalid path")
			}
		})
	}
}

func TestManager_ListFiles(t *testing.T) {
	mgr, repo, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "TEST_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")
	_, _ = mgr.CreateStage(ctx, schema.ID, "LIST_STAGE", "INTERNAL", "", "")

	// Add some files
	files := []string{"file1.csv", "file2.csv", "data.json", "subdir/nested.txt"}
	for _, f := range files {
		_ = mgr.PutFile(ctx, schema.ID, "LIST_STAGE", f, bytes.NewReader([]byte("content")))
	}

	// List all files
	listed, err := mgr.ListFiles(ctx, schema.ID, "LIST_STAGE", "")
	if err != nil {
		t.Fatalf("ListFiles failed: %v", err)
	}

	if len(listed) != len(files) {
		t.Errorf("Expected %d files, got %d", len(files), len(listed))
	}

	// List with pattern filter
	csvFiles, err := mgr.ListFiles(ctx, schema.ID, "LIST_STAGE", "*.csv")
	if err != nil {
		t.Fatalf("ListFiles with pattern failed: %v", err)
	}

	if len(csvFiles) != 2 {
		t.Errorf("Expected 2 CSV files, got %d", len(csvFiles))
	}
}

func TestManager_RemoveFile(t *testing.T) {
	mgr, repo, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "TEST_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")
	_, _ = mgr.CreateStage(ctx, schema.ID, "REMOVE_STAGE", "INTERNAL", "", "")

	// Add a file
	_ = mgr.PutFile(ctx, schema.ID, "REMOVE_STAGE", "to_delete.txt", bytes.NewReader([]byte("delete me")))

	// Verify file exists
	files, _ := mgr.ListFiles(ctx, schema.ID, "REMOVE_STAGE", "")
	if len(files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(files))
	}

	// Remove file
	err := mgr.RemoveFile(ctx, schema.ID, "REMOVE_STAGE", "to_delete.txt")
	if err != nil {
		t.Fatalf("RemoveFile failed: %v", err)
	}

	// Verify file is gone
	files, _ = mgr.ListFiles(ctx, schema.ID, "REMOVE_STAGE", "")
	if len(files) != 0 {
		t.Errorf("Expected 0 files after removal, got %d", len(files))
	}
}

func TestManager_DropStage(t *testing.T) {
	mgr, repo, tempDir, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "TEST_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")
	_, _ = mgr.CreateStage(ctx, schema.ID, "DROP_STAGE", "INTERNAL", "", "")

	// Add a file
	_ = mgr.PutFile(ctx, schema.ID, "DROP_STAGE", "file.txt", bytes.NewReader([]byte("content")))

	// Verify stage directory exists
	stageDir := filepath.Join(tempDir, schema.ID, "DROP_STAGE")
	if _, err := os.Stat(stageDir); os.IsNotExist(err) {
		t.Fatal("Stage directory should exist before drop")
	}

	// Drop the stage
	err := mgr.DropStage(ctx, schema.ID, "DROP_STAGE")
	if err != nil {
		t.Fatalf("DropStage failed: %v", err)
	}

	// Verify stage directory is removed
	if _, err := os.Stat(stageDir); !os.IsNotExist(err) {
		t.Error("Stage directory should be removed after drop")
	}

	// Verify stage metadata is removed
	_, err = repo.GetStageByName(ctx, schema.ID, "DROP_STAGE")
	if err == nil {
		t.Error("Stage should not exist in repository after drop")
	}
}

func TestManager_GetStage(t *testing.T) {
	mgr, repo, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "TEST_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")
	created, _ := mgr.CreateStage(ctx, schema.ID, "GET_STAGE", "INTERNAL", "", "Test comment")

	// Get the stage
	stage, err := mgr.GetStage(ctx, schema.ID, "GET_STAGE")
	if err != nil {
		t.Fatalf("GetStage failed: %v", err)
	}

	if diff := cmp.Diff(created, stage, cmpopts.IgnoreFields(metadata.Stage{}, "CreatedAt")); diff != "" {
		t.Errorf("Stage mismatch (-want +got):\n%s", diff)
	}
}

func TestManager_ListStages(t *testing.T) {
	mgr, repo, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "TEST_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")

	// Create multiple stages
	stageNames := []string{"STAGE_A", "STAGE_B", "STAGE_C"}
	for _, name := range stageNames {
		_, err := mgr.CreateStage(ctx, schema.ID, name, "INTERNAL", "", "")
		if err != nil {
			t.Fatalf("Failed to create stage %s: %v", name, err)
		}
	}

	// List stages
	stages, err := mgr.ListStages(ctx, schema.ID)
	if err != nil {
		t.Fatalf("ListStages failed: %v", err)
	}

	if len(stages) != len(stageNames) {
		t.Errorf("Expected %d stages, got %d", len(stageNames), len(stages))
	}
}

func TestManager_GetFileNotFound(t *testing.T) {
	mgr, repo, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "TEST_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")
	_, _ = mgr.CreateStage(ctx, schema.ID, "EMPTY_STAGE", "INTERNAL", "", "")

	// Try to get non-existent file
	_, err := mgr.GetFile(ctx, schema.ID, "EMPTY_STAGE", "nonexistent.txt")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

func TestManager_RemoveFileNotFound(t *testing.T) {
	mgr, repo, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "TEST_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "TEST_SCHEMA", "")
	_, _ = mgr.CreateStage(ctx, schema.ID, "REMOVE_NF_STAGE", "INTERNAL", "", "")

	// Try to remove non-existent file
	err := mgr.RemoveFile(ctx, schema.ID, "REMOVE_NF_STAGE", "nonexistent.txt")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}
