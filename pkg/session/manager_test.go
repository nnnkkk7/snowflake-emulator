package session

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// TestManager_CreateSession tests session creation with token generation.
func TestManager_CreateSession(t *testing.T) {
	mgr := NewManager(1 * time.Hour)

	tests := []struct {
		name     string
		username string
		database string
		schema   string
		wantErr  bool
	}{
		{
			name:     "ValidSession",
			username: "user1",
			database: "TEST_DB",
			schema:   "PUBLIC",
			wantErr:  false,
		},
		{
			name:     "EmptyUsername",
			username: "",
			database: "TEST_DB",
			schema:   "PUBLIC",
			wantErr:  true,
		},
		{
			name:     "EmptyDatabase",
			username: "user1",
			database: "",
			schema:   "PUBLIC",
			wantErr:  true,
		},
		{
			name:     "EmptySchema",
			username: "user1",
			database: "TEST_DB",
			schema:   "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			session, err := mgr.CreateSession(ctx, tt.username, tt.database, tt.schema)

			if (err != nil) != tt.wantErr {
				t.Errorf("CreateSession() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if session == nil {
					t.Error("Expected session, got nil")
					return
				}

				// Verify session properties
				if session.Username != tt.username {
					t.Errorf("Expected username %s, got %s", tt.username, session.Username)
				}
				if session.Database != tt.database {
					t.Errorf("Expected database %s, got %s", tt.database, session.Database)
				}
				if session.CurrentSchema != tt.schema {
					t.Errorf("Expected schema %s, got %s", tt.schema, session.CurrentSchema)
				}

				// Verify token is generated
				if session.Token == "" {
					t.Error("Expected non-empty token")
				}

				// Verify session ID is generated
				if session.ID == 0 {
					t.Error("Expected non-zero session ID")
				}

				// Verify timestamps
				if session.CreatedAt.IsZero() {
					t.Error("Expected non-zero CreatedAt")
				}
				if session.LastAccessedAt.IsZero() {
					t.Error("Expected non-zero LastAccessedAt")
				}
				if session.ExpiresAt.IsZero() {
					t.Error("Expected non-zero ExpiresAt")
				}

				// Verify expiration is in the future
				if !session.ExpiresAt.After(time.Now()) {
					t.Error("Expected ExpiresAt to be in the future")
				}
			}
		})
	}
}

// TestManager_ValidateSession tests session validation and retrieval.
func TestManager_ValidateSession(t *testing.T) {
	mgr := NewManager(1 * time.Hour)
	ctx := context.Background()

	// Create a test session
	session, err := mgr.CreateSession(ctx, "user1", "TEST_DB", "PUBLIC")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	tests := []struct {
		name    string
		token   string
		wantErr bool
	}{
		{
			name:    "ValidToken",
			token:   session.Token,
			wantErr: false,
		},
		{
			name:    "InvalidToken",
			token:   "invalid-token-12345",
			wantErr: true,
		},
		{
			name:    "EmptyToken",
			token:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validatedSession, err := mgr.ValidateSession(ctx, tt.token)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSession() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if validatedSession == nil {
					t.Error("Expected session, got nil")
					return
				}

				// Verify it's the same session
				if validatedSession.Token != session.Token {
					t.Errorf("Expected token %s, got %s", session.Token, validatedSession.Token)
				}
				if validatedSession.ID != session.ID {
					t.Errorf("Expected ID %d, got %d", session.ID, validatedSession.ID)
				}

				// Verify LastAccessedAt is updated
				if !validatedSession.LastAccessedAt.After(session.LastAccessedAt) {
					t.Error("Expected LastAccessedAt to be updated")
				}
			}
		})
	}
}

// TestManager_SessionExpiration tests session expiration handling.
func TestManager_SessionExpiration(t *testing.T) {
	// Create manager with very short expiration for testing
	mgr := NewManager(100 * time.Millisecond)
	ctx := context.Background()

	// Create a session
	session, err := mgr.CreateSession(ctx, "user1", "TEST_DB", "PUBLIC")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Session should be valid immediately
	_, err = mgr.ValidateSession(ctx, session.Token)
	if err != nil {
		t.Errorf("Session should be valid immediately: %v", err)
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Session should be expired now
	_, err = mgr.ValidateSession(ctx, session.Token)
	if err == nil {
		t.Error("Expected error for expired session, got nil")
	}
}

// TestManager_CloseSession tests session closure/logout.
func TestManager_CloseSession(t *testing.T) {
	mgr := NewManager(1 * time.Hour)
	ctx := context.Background()

	// Create a session
	session, err := mgr.CreateSession(ctx, "user1", "TEST_DB", "PUBLIC")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Verify session is valid
	_, err = mgr.ValidateSession(ctx, session.Token)
	if err != nil {
		t.Errorf("Session should be valid: %v", err)
	}

	// Close the session
	err = mgr.CloseSession(ctx, session.Token)
	if err != nil {
		t.Errorf("CloseSession() error = %v", err)
	}

	// Verify session is no longer valid
	_, err = mgr.ValidateSession(ctx, session.Token)
	if err == nil {
		t.Error("Expected error for closed session, got nil")
	}

	// Closing non-existent session should not error
	err = mgr.CloseSession(ctx, "non-existent-token")
	if err != nil {
		t.Errorf("CloseSession() should not error for non-existent token: %v", err)
	}
}

// TestManager_UpdateSessionContext tests updating session database/schema.
func TestManager_UpdateSessionContext(t *testing.T) {
	mgr := NewManager(1 * time.Hour)
	ctx := context.Background()

	// Create a session
	session, err := mgr.CreateSession(ctx, "user1", "TEST_DB", "PUBLIC")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	tests := []struct {
		name      string
		token     string
		database  string
		schema    string
		wantErr   bool
		checkFunc func(*testing.T, *Session)
	}{
		{
			name:     "UpdateDatabase",
			token:    session.Token,
			database: "NEW_DB",
			schema:   "",
			wantErr:  false,
			checkFunc: func(t *testing.T, s *Session) {
				if s.Database != "NEW_DB" {
					t.Errorf("Expected database NEW_DB, got %s", s.Database)
				}
				if s.CurrentSchema != "PUBLIC" {
					t.Errorf("Expected schema unchanged as PUBLIC, got %s", s.CurrentSchema)
				}
			},
		},
		{
			name:     "UpdateSchema",
			token:    session.Token,
			database: "",
			schema:   "NEW_SCHEMA",
			wantErr:  false,
			checkFunc: func(t *testing.T, s *Session) {
				if s.CurrentSchema != "NEW_SCHEMA" {
					t.Errorf("Expected schema NEW_SCHEMA, got %s", s.CurrentSchema)
				}
			},
		},
		{
			name:     "UpdateBoth",
			token:    session.Token,
			database: "ANOTHER_DB",
			schema:   "ANOTHER_SCHEMA",
			wantErr:  false,
			checkFunc: func(t *testing.T, s *Session) {
				if s.Database != "ANOTHER_DB" {
					t.Errorf("Expected database ANOTHER_DB, got %s", s.Database)
				}
				if s.CurrentSchema != "ANOTHER_SCHEMA" {
					t.Errorf("Expected schema ANOTHER_SCHEMA, got %s", s.CurrentSchema)
				}
			},
		},
		{
			name:     "InvalidToken",
			token:    "invalid-token",
			database: "DB",
			schema:   "SCHEMA",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.UpdateSessionContext(ctx, tt.token, tt.database, tt.schema)

			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateSessionContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.checkFunc != nil {
				// Retrieve updated session
				updatedSession, err := mgr.ValidateSession(ctx, tt.token)
				if err != nil {
					t.Fatalf("Failed to validate session: %v", err)
				}
				tt.checkFunc(t, updatedSession)
			}
		})
	}
}

// TestManager_ConcurrentSessions tests concurrent session operations.
func TestManager_ConcurrentSessions(t *testing.T) {
	mgr := NewManager(1 * time.Hour)
	ctx := context.Background()

	// Create multiple sessions concurrently
	done := make(chan *Session, 10)
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			session, err := mgr.CreateSession(ctx, "user1", "TEST_DB", "PUBLIC")
			if err != nil {
				errors <- err
				return
			}
			done <- session
		}(i)
	}

	// Collect sessions
	sessions := make([]*Session, 0, 10)
	for i := 0; i < 10; i++ {
		select {
		case session := <-done:
			sessions = append(sessions, session)
		case err := <-errors:
			t.Errorf("Concurrent CreateSession error: %v", err)
		}
	}

	if len(sessions) != 10 {
		t.Errorf("Expected 10 sessions, got %d", len(sessions))
	}

	// Verify all sessions have unique tokens
	tokenMap := make(map[string]bool)
	for _, session := range sessions {
		if tokenMap[session.Token] {
			t.Errorf("Duplicate token detected: %s", session.Token)
		}
		tokenMap[session.Token] = true
	}

	// Validate all sessions concurrently
	validationErrors := make(chan error, 10)
	validationDone := make(chan bool, 10)

	for _, session := range sessions {
		go func(s *Session) {
			_, err := mgr.ValidateSession(ctx, s.Token)
			if err != nil {
				validationErrors <- err
				return
			}
			validationDone <- true
		}(session)
	}

	// Check validation results
	for i := 0; i < 10; i++ {
		select {
		case <-validationDone:
			// Success
		case err := <-validationErrors:
			t.Errorf("Concurrent ValidateSession error: %v", err)
		}
	}
}

// TestManager_CleanupExpiredSessions tests automatic cleanup of expired sessions.
func TestManager_CleanupExpiredSessions(t *testing.T) {
	// Create manager with short expiration
	mgr := NewManager(100 * time.Millisecond)
	ctx := context.Background()

	// Create several sessions
	sessions := make([]*Session, 5)
	for i := 0; i < 5; i++ {
		session, err := mgr.CreateSession(ctx, "user1", "TEST_DB", "PUBLIC")
		if err != nil {
			t.Fatalf("Failed to create session %d: %v", i, err)
		}
		sessions[i] = session
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Run cleanup
	count := mgr.CleanupExpiredSessions(ctx)
	if count != 5 {
		t.Errorf("Expected to clean up 5 sessions, got %d", count)
	}

	// Verify all sessions are gone
	for i, session := range sessions {
		_, err := mgr.ValidateSession(ctx, session.Token)
		if err == nil {
			t.Errorf("Session %d should be cleaned up", i)
		}
	}
}

// TestSession_Copy tests session deep copy functionality.
func TestSession_Copy(t *testing.T) {
	original := &Session{
		ID:                      1234567890123,
		Token:                   "token-abc",
		MasterToken:             "master-token-xyz",
		Username:                "user1",
		Database:                "DB1",
		CurrentSchema:           "SCHEMA1",
		CreatedAt:               time.Now(),
		LastAccessedAt:          time.Now(),
		ExpiresAt:               time.Now().Add(1 * time.Hour),
		ValidityInSeconds:       3600,
		MasterValidityInSeconds: 14400,
		Parameters:              map[string]interface{}{"key1": "value1", "key2": 123},
	}

	copied := original.Copy()

	// Verify deep copy
	if diff := cmp.Diff(original, copied); diff != "" {
		t.Errorf("Copy() mismatch (-want +got):\n%s", diff)
	}

	// Verify it's a different object
	if original == copied {
		t.Error("Copy() should return a different object")
	}

	// Verify Parameters map is deep copied
	copied.Parameters["key1"] = "modified"
	if original.Parameters["key1"] == "modified" {
		t.Error("Modifying copied Parameters should not affect original")
	}
}
