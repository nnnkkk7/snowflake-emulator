package session

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
)

// setupTestStore creates a test store with in-memory DuckDB.
func setupTestStore(t *testing.T) *Store {
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
	store, err := NewStore(mgr)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	return store
}

// TestStore_SaveAndLoad tests saving and loading sessions.
func TestStore_SaveAndLoad(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	session := &Session{
		ID:             1234567890123,
		Token:          "token-abc",
		Username:       "user1",
		Database:       "TEST_DB",
		CurrentSchema:  "PUBLIC",
		CreatedAt:      time.Now(),
		LastAccessedAt: time.Now(),
		ExpiresAt:      time.Now().Add(1 * time.Hour),
		Parameters:     map[string]interface{}{"key1": "value1"},
	}

	// Save session
	err := store.Save(ctx, session)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Load session
	loaded, err := store.Load(ctx, session.Token)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify loaded session matches original
	if loaded.ID != session.ID {
		t.Errorf("Expected ID %d, got %d", session.ID, loaded.ID)
	}
	if loaded.Token != session.Token {
		t.Errorf("Expected token %s, got %s", session.Token, loaded.Token)
	}
	if loaded.Username != session.Username {
		t.Errorf("Expected username %s, got %s", session.Username, loaded.Username)
	}
	if loaded.Database != session.Database {
		t.Errorf("Expected database %s, got %s", session.Database, loaded.Database)
	}
	if loaded.CurrentSchema != session.CurrentSchema {
		t.Errorf("Expected schema %s, got %s", session.CurrentSchema, loaded.CurrentSchema)
	}
}

// TestStore_LoadNonExistent tests loading a non-existent session.
func TestStore_LoadNonExistent(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	_, err := store.Load(ctx, "non-existent-token")
	if err == nil {
		t.Error("Expected error for non-existent token, got nil")
	}
}

// TestStore_Delete tests deleting sessions.
func TestStore_Delete(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	session := &Session{
		ID:             1234567890123,
		Token:          "token-abc",
		Username:       "user1",
		Database:       "TEST_DB",
		CurrentSchema:  "PUBLIC",
		CreatedAt:      time.Now(),
		LastAccessedAt: time.Now(),
		ExpiresAt:      time.Now().Add(1 * time.Hour),
		Parameters:     map[string]interface{}{},
	}

	// Save session
	err := store.Save(ctx, session)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Verify it exists
	_, err = store.Load(ctx, session.Token)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Delete session
	err = store.Delete(ctx, session.Token)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify it's gone
	_, err = store.Load(ctx, session.Token)
	if err == nil {
		t.Error("Expected error after deletion, got nil")
	}

	// Deleting non-existent session should not error
	err = store.Delete(ctx, "non-existent-token")
	if err != nil {
		t.Errorf("Delete() should not error for non-existent token: %v", err)
	}
}

// TestStore_Update tests updating existing sessions.
func TestStore_Update(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	session := &Session{
		ID:             1234567890123,
		Token:          "token-abc",
		Username:       "user1",
		Database:       "TEST_DB",
		CurrentSchema:  "PUBLIC",
		CreatedAt:      time.Now(),
		LastAccessedAt: time.Now(),
		ExpiresAt:      time.Now().Add(1 * time.Hour),
		Parameters:     map[string]interface{}{},
	}

	// Save initial session
	err := store.Save(ctx, session)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Update session
	session.Database = "NEW_DB"
	session.CurrentSchema = "NEW_SCHEMA"
	session.LastAccessedAt = time.Now().Add(1 * time.Minute)

	err = store.Save(ctx, session)
	if err != nil {
		t.Fatalf("Save() update error = %v", err)
	}

	// Load and verify updates
	loaded, err := store.Load(ctx, session.Token)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if loaded.Database != "NEW_DB" {
		t.Errorf("Expected database NEW_DB, got %s", loaded.Database)
	}
	if loaded.CurrentSchema != "NEW_SCHEMA" {
		t.Errorf("Expected schema NEW_SCHEMA, got %s", loaded.CurrentSchema)
	}
}

// TestStore_DeleteExpired tests deleting expired sessions.
func TestStore_DeleteExpired(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	// Create expired session
	expiredSession := &Session{
		ID:             1234567890001,
		Token:          "token-expired",
		Username:       "user1",
		Database:       "TEST_DB",
		CurrentSchema:  "PUBLIC",
		CreatedAt:      time.Now().Add(-2 * time.Hour),
		LastAccessedAt: time.Now().Add(-2 * time.Hour),
		ExpiresAt:      time.Now().Add(-1 * time.Hour),
		Parameters:     map[string]interface{}{},
	}

	// Create active session
	activeSession := &Session{
		ID:             1234567890002,
		Token:          "token-active",
		Username:       "user2",
		Database:       "TEST_DB",
		CurrentSchema:  "PUBLIC",
		CreatedAt:      time.Now(),
		LastAccessedAt: time.Now(),
		ExpiresAt:      time.Now().Add(1 * time.Hour),
		Parameters:     map[string]interface{}{},
	}

	// Save both sessions
	err := store.Save(ctx, expiredSession)
	if err != nil {
		t.Fatalf("Save() expired error = %v", err)
	}

	err = store.Save(ctx, activeSession)
	if err != nil {
		t.Fatalf("Save() active error = %v", err)
	}

	// Delete expired sessions
	count, err := store.DeleteExpired(ctx)
	if err != nil {
		t.Fatalf("DeleteExpired() error = %v", err)
	}

	if count != 1 {
		t.Errorf("Expected to delete 1 expired session, got %d", count)
	}

	// Verify expired session is gone
	_, err = store.Load(ctx, expiredSession.Token)
	if err == nil {
		t.Error("Expected error for expired session, got nil")
	}

	// Verify active session still exists
	_, err = store.Load(ctx, activeSession.Token)
	if err != nil {
		t.Errorf("Active session should still exist: %v", err)
	}
}

// TestStore_ListAll tests listing all sessions.
func TestStore_ListAll(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	// Create multiple sessions
	sessions := []*Session{
		{
			ID:             12345678901,
			Token:          "token-1",
			Username:       "user1",
			Database:       "DB1",
			CurrentSchema:  "PUBLIC",
			CreatedAt:      time.Now(),
			LastAccessedAt: time.Now(),
			ExpiresAt:      time.Now().Add(1 * time.Hour),
			Parameters:     map[string]interface{}{},
		},
		{
			ID:             12345678902,
			Token:          "token-2",
			Username:       "user2",
			Database:       "DB2",
			CurrentSchema:  "PUBLIC",
			CreatedAt:      time.Now(),
			LastAccessedAt: time.Now(),
			ExpiresAt:      time.Now().Add(1 * time.Hour),
			Parameters:     map[string]interface{}{},
		},
	}

	for _, session := range sessions {
		err := store.Save(ctx, session)
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}
	}

	// List all sessions
	allSessions, err := store.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}

	if len(allSessions) != 2 {
		t.Errorf("Expected 2 sessions, got %d", len(allSessions))
	}

	// Verify sessions are in the list
	tokens := make(map[string]bool)
	for _, s := range allSessions {
		tokens[s.Token] = true
	}

	for _, expected := range sessions {
		if !tokens[expected.Token] {
			t.Errorf("Expected to find token %s in list", expected.Token)
		}
	}
}

// TestStore_ConcurrentOperations tests concurrent store operations.
func TestStore_ConcurrentOperations(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	done := make(chan bool, 10)
	errors := make(chan error, 10)

	// Concurrently save sessions
	for i := 0; i < 10; i++ {
		go func(id int) {
			session := &Session{
				ID:             int64(1234567890000 + id),
				Token:          "token-" + string(rune('0'+id)),
				Username:       "user" + string(rune('0'+id)),
				Database:       "TEST_DB",
				CurrentSchema:  "PUBLIC",
				CreatedAt:      time.Now(),
				LastAccessedAt: time.Now(),
				ExpiresAt:      time.Now().Add(1 * time.Hour),
				Parameters:     map[string]interface{}{},
			}

			if err := store.Save(ctx, session); err != nil {
				errors <- err
				return
			}
			done <- true
		}(i)
	}

	// Wait for all operations to complete
	for i := 0; i < 10; i++ {
		select {
		case <-done:
			// Success
		case err := <-errors:
			t.Errorf("Concurrent Save() error: %v", err)
		}
	}

	// Verify all sessions were saved
	allSessions, err := store.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}

	if len(allSessions) != 10 {
		t.Errorf("Expected 10 sessions, got %d", len(allSessions))
	}
}

// TestManagerWithStore_Integration tests Manager using persistent Store.
func TestManagerWithStore_Integration(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer db.Close()

	mgr := connection.NewManager(db)
	store, err := NewStore(mgr)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Create manager with store
	sessionMgr := NewManagerWithStore(1*time.Hour, store)
	ctx := context.Background()

	// Create session
	session, err := sessionMgr.CreateSession(ctx, "user1", "TEST_DB", "PUBLIC")
	if err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}

	// Verify session is persisted
	loaded, err := store.Load(ctx, session.Token)
	if err != nil {
		t.Fatalf("Store should have session: %v", err)
	}

	if loaded.Token != session.Token {
		t.Errorf("Expected token %s, got %s", session.Token, loaded.Token)
	}

	// Close session
	err = sessionMgr.CloseSession(ctx, session.Token)
	if err != nil {
		t.Fatalf("CloseSession() error = %v", err)
	}

	// Verify session is removed from store
	_, err = store.Load(ctx, session.Token)
	if err == nil {
		t.Error("Session should be removed from store")
	}
}
