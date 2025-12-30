// Package session provides session management for the Snowflake emulator.
package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// Session represents an active Snowflake session.
type Session struct {
	ID                      int64
	Token                   string
	MasterToken             string
	Username                string
	Database                string
	CurrentSchema           string
	CreatedAt               time.Time
	LastAccessedAt          time.Time
	ExpiresAt               time.Time
	ValidityInSeconds       int64
	MasterValidityInSeconds int64
	Parameters              map[string]interface{}
}

// Manager manages Snowflake sessions.
type Manager struct {
	sessions       map[string]*Session // token -> session
	masterTokens   map[string]*Session // masterToken -> session
	sessionTimeout time.Duration
	mu             sync.RWMutex
	store          *Store // optional persistent storage
}

// NewManager creates a new session manager.
func NewManager(sessionTimeout time.Duration) *Manager {
	return &Manager{
		sessions:       make(map[string]*Session),
		masterTokens:   make(map[string]*Session),
		sessionTimeout: sessionTimeout,
	}
}

// CreateSession creates a new session with a unique token.
func (m *Manager) CreateSession(ctx context.Context, username, database, schema string) (*Session, error) {
	if username == "" {
		return nil, fmt.Errorf("username cannot be empty")
	}
	if database == "" {
		return nil, fmt.Errorf("database cannot be empty")
	}
	if schema == "" {
		return nil, fmt.Errorf("schema cannot be empty")
	}

	// Generate unique int64 session ID using timestamp
	sessionID := time.Now().UnixNano()

	// Generate secure random token
	token, err := generateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate token: %w", err)
	}

	// Generate master token
	masterToken, err := generateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate master token: %w", err)
	}

	now := time.Now()
	session := &Session{
		ID:                      sessionID,
		Token:                   token,
		MasterToken:             masterToken,
		Username:                username,
		Database:                database,
		CurrentSchema:           schema,
		CreatedAt:               now,
		LastAccessedAt:          now,
		ExpiresAt:               now.Add(m.sessionTimeout),
		ValidityInSeconds:       int64(m.sessionTimeout.Seconds()),
		MasterValidityInSeconds: int64(m.sessionTimeout.Seconds()) * 4,
		Parameters:              make(map[string]interface{}),
	}

	m.mu.Lock()
	m.sessions[token] = session
	m.masterTokens[masterToken] = session
	m.mu.Unlock()

	// Persist to store if available
	if m.store != nil {
		if err := m.store.Save(ctx, session); err != nil {
			// Remove from memory if persistence failed
			m.mu.Lock()
			delete(m.sessions, token)
			delete(m.masterTokens, masterToken)
			m.mu.Unlock()
			return nil, fmt.Errorf("failed to persist session: %w", err)
		}
	}

	return session.Copy(), nil
}

// ValidateSession validates a session token and returns the session if valid.
// It also updates the LastAccessedAt timestamp.
func (m *Manager) ValidateSession(_ context.Context, token string) (*Session, error) {
	if token == "" {
		return nil, fmt.Errorf("token cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[token]
	if !exists {
		return nil, fmt.Errorf("invalid session token")
	}

	// Check if session is expired
	if time.Now().After(session.ExpiresAt) {
		// Remove expired session
		delete(m.sessions, token)
		return nil, fmt.Errorf("session expired")
	}

	// Update last accessed time
	session.LastAccessedAt = time.Now()

	return session.Copy(), nil
}

// CloseSession closes a session (logout).
func (m *Manager) CloseSession(ctx context.Context, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get session to find master token
	session, exists := m.sessions[token]
	if exists {
		// Delete both session token and master token
		delete(m.sessions, token)
		delete(m.masterTokens, session.MasterToken)
	}

	// Delete from store if available
	if m.store != nil {
		if err := m.store.Delete(ctx, token); err != nil {
			return fmt.Errorf("failed to delete session from store: %w", err)
		}
	}

	return nil
}

// UpdateSessionContext updates the database and/or schema for a session.
func (m *Manager) UpdateSessionContext(_ context.Context, token, database, schema string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[token]
	if !exists {
		return fmt.Errorf("invalid session token")
	}

	// Update database if provided
	if database != "" {
		session.Database = database
	}

	// Update schema if provided
	if schema != "" {
		session.CurrentSchema = schema
	}

	session.LastAccessedAt = time.Now()

	return nil
}

// CleanupExpiredSessions removes all expired sessions and returns the count.
func (m *Manager) CleanupExpiredSessions(_ context.Context) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	count := 0

	for token, session := range m.sessions {
		if now.After(session.ExpiresAt) {
			delete(m.sessions, token)
			count++
		}
	}

	return count
}

// RenewToken generates a new session token using master token
func (m *Manager) RenewToken(_ context.Context, masterToken string) (*Session, string, error) {
	if masterToken == "" {
		return nil, "", fmt.Errorf("master token cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.masterTokens[masterToken]
	if !exists {
		return nil, "", fmt.Errorf("invalid master token")
	}

	// Check master token expiry (4x session timeout)
	masterExpiry := session.CreatedAt.Add(time.Duration(session.MasterValidityInSeconds) * time.Second)
	if time.Now().After(masterExpiry) {
		delete(m.masterTokens, masterToken)
		delete(m.sessions, session.Token)
		return nil, "", fmt.Errorf("master token expired")
	}

	// Revoke old session token
	delete(m.sessions, session.Token)

	// Generate new session token
	newToken, err := generateToken()
	if err != nil {
		return nil, "", fmt.Errorf("failed to generate new token: %w", err)
	}

	session.Token = newToken
	session.LastAccessedAt = time.Now()
	session.ExpiresAt = time.Now().Add(m.sessionTimeout)

	m.sessions[newToken] = session

	return session.Copy(), newToken, nil
}

// UpdateLastAccessed updates the last accessed time for a session (heartbeat)
func (m *Manager) UpdateLastAccessed(_ context.Context, token string) error {
	if token == "" {
		return fmt.Errorf("token cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[token]
	if !exists {
		return fmt.Errorf("session not found")
	}

	// Check if session is expired
	if time.Now().After(session.ExpiresAt) {
		delete(m.sessions, token)
		return fmt.Errorf("session expired")
	}

	session.LastAccessedAt = time.Now()

	return nil
}

// Copy creates a deep copy of the session.
func (s *Session) Copy() *Session {
	if s == nil {
		return nil
	}

	// Copy parameters map
	params := make(map[string]interface{})
	for k, v := range s.Parameters {
		params[k] = v
	}

	return &Session{
		ID:                      s.ID,
		Token:                   s.Token,
		MasterToken:             s.MasterToken,
		Username:                s.Username,
		Database:                s.Database,
		CurrentSchema:           s.CurrentSchema,
		CreatedAt:               s.CreatedAt,
		LastAccessedAt:          s.LastAccessedAt,
		ExpiresAt:               s.ExpiresAt,
		ValidityInSeconds:       s.ValidityInSeconds,
		MasterValidityInSeconds: s.MasterValidityInSeconds,
		Parameters:              params,
	}
}

// generateToken generates a secure random token.
func generateToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
