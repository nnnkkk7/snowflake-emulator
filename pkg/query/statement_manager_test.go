package query

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nnnkkk7/snowflake-emulator/server/apierror"
)

func TestStatementManager_CreateStatement(t *testing.T) {
	sm := NewStatementManager(1 * time.Hour)

	stmt := sm.CreateStatement("SELECT 1", "TEST_DB", "PUBLIC", "TEST_WH")

	if stmt.Handle == "" {
		t.Error("Expected handle to be set")
	}
	if stmt.Status != StatementStatusPending {
		t.Errorf("Expected status %s, got %s", StatementStatusPending, stmt.Status)
	}
	if stmt.SQLText != "SELECT 1" {
		t.Errorf("Expected SQL 'SELECT 1', got %s", stmt.SQLText)
	}
	if stmt.Database != "TEST_DB" {
		t.Errorf("Expected database 'TEST_DB', got %s", stmt.Database)
	}
	if stmt.Schema != "PUBLIC" {
		t.Errorf("Expected schema 'PUBLIC', got %s", stmt.Schema)
	}
	if stmt.Warehouse != "TEST_WH" {
		t.Errorf("Expected warehouse 'TEST_WH', got %s", stmt.Warehouse)
	}
	if stmt.CreatedOn.IsZero() {
		t.Error("Expected CreatedOn to be set")
	}
	if stmt.CompletedOn != nil {
		t.Error("Expected CompletedOn to be nil")
	}
}

func TestStatementManager_GetStatement(t *testing.T) {
	sm := NewStatementManager(1 * time.Hour)

	// Create a statement
	created := sm.CreateStatement("SELECT 1", "TEST_DB", "PUBLIC", "")

	// Get existing statement
	stmt, ok := sm.GetStatement(created.Handle)
	if !ok {
		t.Fatal("Expected to find statement")
	}
	if stmt.Handle != created.Handle {
		t.Errorf("Expected handle %s, got %s", created.Handle, stmt.Handle)
	}

	// Get non-existing statement
	_, ok = sm.GetStatement("non-existing-handle")
	if ok {
		t.Error("Expected not to find non-existing statement")
	}
}

func TestStatementManager_UpdateStatus(t *testing.T) {
	sm := NewStatementManager(1 * time.Hour)

	stmt := sm.CreateStatement("SELECT 1", "TEST_DB", "PUBLIC", "")

	tests := []struct {
		name           string
		status         StatementStatus
		expectComplete bool
	}{
		{
			name:           "Running",
			status:         StatementStatusRunning,
			expectComplete: false,
		},
		{
			name:           "Success",
			status:         StatementStatusSuccess,
			expectComplete: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok := sm.UpdateStatus(stmt.Handle, tt.status)
			if !ok {
				t.Error("Expected update to succeed")
			}

			updated, _ := sm.GetStatement(stmt.Handle)
			if updated.Status != tt.status {
				t.Errorf("Expected status %s, got %s", tt.status, updated.Status)
			}

			if tt.expectComplete && updated.CompletedOn == nil {
				t.Error("Expected CompletedOn to be set")
			}
		})
	}

	// Update non-existing statement
	ok := sm.UpdateStatus("non-existing", StatementStatusRunning)
	if ok {
		t.Error("Expected update of non-existing statement to fail")
	}
}

func TestStatementManager_SetResult(t *testing.T) {
	sm := NewStatementManager(1 * time.Hour)

	stmt := sm.CreateStatement("SELECT 1", "TEST_DB", "PUBLIC", "")
	sm.UpdateStatus(stmt.Handle, StatementStatusRunning)

	result := &Result{
		Columns: []string{"col1"},
		Rows:    [][]interface{}{{"value1"}},
	}

	ok := sm.SetResult(stmt.Handle, result)
	if !ok {
		t.Error("Expected SetResult to succeed")
	}

	updated, _ := sm.GetStatement(stmt.Handle)
	if updated.Status != StatementStatusSuccess {
		t.Errorf("Expected status %s, got %s", StatementStatusSuccess, updated.Status)
	}
	if updated.Result == nil {
		t.Error("Expected result to be set")
	}
	if diff := cmp.Diff(result.Columns, updated.Result.Columns); diff != "" {
		t.Errorf("Result columns mismatch (-want +got):\n%s", diff)
	}
	if updated.CompletedOn == nil {
		t.Error("Expected CompletedOn to be set")
	}
}

func TestStatementManager_SetError(t *testing.T) {
	sm := NewStatementManager(1 * time.Hour)

	stmt := sm.CreateStatement("SELECT invalid", "TEST_DB", "PUBLIC", "")
	sm.UpdateStatus(stmt.Handle, StatementStatusRunning)

	err := apierror.NewSnowflakeError(apierror.CodeSQLExecutionError, "Syntax error")

	ok := sm.SetError(stmt.Handle, err)
	if !ok {
		t.Error("Expected SetError to succeed")
	}

	updated, _ := sm.GetStatement(stmt.Handle)
	if updated.Status != StatementStatusFailed {
		t.Errorf("Expected status %s, got %s", StatementStatusFailed, updated.Status)
	}
	if updated.Error == nil {
		t.Error("Expected error to be set")
	}
	if updated.Error.Code != apierror.CodeSQLExecutionError {
		t.Errorf("Expected error code %s, got %s", apierror.CodeSQLExecutionError, updated.Error.Code)
	}
	if updated.CompletedOn == nil {
		t.Error("Expected CompletedOn to be set")
	}
}

func TestStatementManager_CancelStatement(t *testing.T) {
	sm := NewStatementManager(1 * time.Hour)

	// Test cancel running statement
	stmt := sm.CreateStatement("SELECT 1", "TEST_DB", "PUBLIC", "")
	sm.UpdateStatus(stmt.Handle, StatementStatusRunning)

	canceled := false
	sm.SetCancelFunc(stmt.Handle, func() {
		canceled = true
	})

	err := sm.CancelStatement(stmt.Handle)
	if err != nil {
		t.Errorf("Expected cancel to succeed, got error: %v", err)
	}

	if !canceled {
		t.Error("Expected cancel function to be called")
	}

	updated, _ := sm.GetStatement(stmt.Handle)
	if updated.Status != StatementStatusCanceled {
		t.Errorf("Expected status %s, got %s", StatementStatusCanceled, updated.Status)
	}

	// Test cancel non-existing statement
	err = sm.CancelStatement("non-existing")
	if err == nil {
		t.Error("Expected error when canceling non-existing statement")
	}

	// Test cancel completed statement
	stmt2 := sm.CreateStatement("SELECT 2", "TEST_DB", "PUBLIC", "")
	sm.SetResult(stmt2.Handle, &Result{})

	err = sm.CancelStatement(stmt2.Handle)
	if err == nil {
		t.Error("Expected error when canceling completed statement")
	}
}

func TestStatementManager_DeleteStatement(t *testing.T) {
	sm := NewStatementManager(1 * time.Hour)

	stmt := sm.CreateStatement("SELECT 1", "TEST_DB", "PUBLIC", "")

	// Verify statement exists
	_, ok := sm.GetStatement(stmt.Handle)
	if !ok {
		t.Fatal("Expected statement to exist")
	}

	// Delete statement
	sm.DeleteStatement(stmt.Handle)

	// Verify statement is gone
	_, ok = sm.GetStatement(stmt.Handle)
	if ok {
		t.Error("Expected statement to be deleted")
	}
}

func TestStatementManager_Cleanup(t *testing.T) {
	// Use short TTL for testing
	sm := &StatementManager{
		statements: make(map[string]*Statement),
		ttl:        100 * time.Millisecond,
	}

	// Create and complete a statement
	stmt := sm.CreateStatement("SELECT 1", "TEST_DB", "PUBLIC", "")
	sm.SetResult(stmt.Handle, &Result{})

	// Verify statement exists
	_, ok := sm.GetStatement(stmt.Handle)
	if !ok {
		t.Fatal("Expected statement to exist before cleanup")
	}

	// Wait for TTL to expire and run cleanup
	time.Sleep(150 * time.Millisecond)
	sm.cleanup()

	// Verify statement is cleaned up
	_, ok = sm.GetStatement(stmt.Handle)
	if ok {
		t.Error("Expected statement to be cleaned up after TTL")
	}
}

func TestGenerateStatementHandle(t *testing.T) {
	handle1 := generateStatementHandle()
	handle2 := generateStatementHandle()

	if handle1 == "" {
		t.Error("Expected handle to be non-empty")
	}

	if handle1 == handle2 {
		t.Error("Expected unique handles")
	}

	// Snowflake handles start with "01"
	if len(handle1) < 2 || handle1[:2] != "01" {
		t.Errorf("Expected handle to start with '01', got %s", handle1)
	}
}
