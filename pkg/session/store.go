package session

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
)

// Store provides persistent storage for sessions using DuckDB.
type Store struct {
	mgr *connection.Manager
}

// NewStore creates a new session store with DuckDB backend.
func NewStore(mgr *connection.Manager) (*Store, error) {
	store := &Store{
		mgr: mgr,
	}

	// Initialize sessions table
	if err := store.initTable(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize sessions table: %w", err)
	}

	return store, nil
}

// initTable creates the sessions table if it doesn't exist.
func (s *Store) initTable(ctx context.Context) error {
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS _sessions (
			token VARCHAR PRIMARY KEY,
			id VARCHAR NOT NULL,
			username VARCHAR NOT NULL,
			database_name VARCHAR NOT NULL,
			current_schema VARCHAR NOT NULL,
			created_at TIMESTAMP NOT NULL,
			last_accessed_at TIMESTAMP NOT NULL,
			expires_at TIMESTAMP NOT NULL,
			parameters VARCHAR
		)
	`

	_, err := s.mgr.Exec(ctx, createTableSQL)
	return err
}

// Save saves a session to persistent storage.
func (s *Store) Save(ctx context.Context, session *Session) error {
	// Serialize parameters to JSON
	paramsJSON, err := json.Marshal(session.Parameters)
	if err != nil {
		return fmt.Errorf("failed to marshal parameters: %w", err)
	}

	// Use INSERT OR REPLACE to handle both insert and update
	insertSQL := `
		INSERT OR REPLACE INTO _sessions (
			token, id, username, database_name, current_schema,
			created_at, last_accessed_at, expires_at, parameters
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err = s.mgr.Exec(ctx, insertSQL,
		session.Token,
		session.ID,
		session.Username,
		session.Database,
		session.CurrentSchema,
		session.CreatedAt,
		session.LastAccessedAt,
		session.ExpiresAt,
		string(paramsJSON),
	)
	if err != nil {
		return fmt.Errorf("failed to save session: %w", err)
	}

	return nil
}

// Load loads a session from persistent storage.
func (s *Store) Load(ctx context.Context, token string) (*Session, error) {
	selectSQL := `
		SELECT id, username, database_name, current_schema,
			   created_at, last_accessed_at, expires_at, parameters
		FROM _sessions
		WHERE token = ?
	`

	var session Session
	var paramsJSON string

	err := s.mgr.QueryRow(ctx, selectSQL, token).Scan(
		&session.ID,
		&session.Username,
		&session.Database,
		&session.CurrentSchema,
		&session.CreatedAt,
		&session.LastAccessedAt,
		&session.ExpiresAt,
		&paramsJSON,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("session not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load session: %w", err)
	}

	session.Token = token

	// Deserialize parameters
	if err := json.Unmarshal([]byte(paramsJSON), &session.Parameters); err != nil {
		return nil, fmt.Errorf("failed to unmarshal parameters: %w", err)
	}

	return &session, nil
}

// Delete deletes a session from persistent storage.
func (s *Store) Delete(ctx context.Context, token string) error {
	deleteSQL := `DELETE FROM _sessions WHERE token = ?`
	_, err := s.mgr.Exec(ctx, deleteSQL, token)
	return err
}

// DeleteExpired deletes all expired sessions and returns the count.
func (s *Store) DeleteExpired(ctx context.Context) (int, error) {
	// First count expired sessions
	countSQL := `SELECT COUNT(*) FROM _sessions WHERE expires_at < ?`
	var count int64
	err := s.mgr.QueryRow(ctx, countSQL, time.Now()).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count expired sessions: %w", err)
	}

	// Delete expired sessions
	deleteSQL := `DELETE FROM _sessions WHERE expires_at < ?`
	_, err = s.mgr.Exec(ctx, deleteSQL, time.Now())
	if err != nil {
		return 0, fmt.Errorf("failed to delete expired sessions: %w", err)
	}

	return int(count), nil
}

// ListAll returns all sessions from storage.
func (s *Store) ListAll(ctx context.Context) ([]*Session, error) {
	selectSQL := `
		SELECT token, id, username, database_name, current_schema,
			   created_at, last_accessed_at, expires_at, parameters
		FROM _sessions
		ORDER BY created_at DESC
	`

	rows, err := s.mgr.Query(ctx, selectSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query sessions: %w", err)
	}
	defer rows.Close()

	var sessions []*Session
	for rows.Next() {
		var session Session
		var paramsJSON string

		err := rows.Scan(
			&session.Token,
			&session.ID,
			&session.Username,
			&session.Database,
			&session.CurrentSchema,
			&session.CreatedAt,
			&session.LastAccessedAt,
			&session.ExpiresAt,
			&paramsJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan session: %w", err)
		}

		// Deserialize parameters
		if err := json.Unmarshal([]byte(paramsJSON), &session.Parameters); err != nil {
			return nil, fmt.Errorf("failed to unmarshal parameters: %w", err)
		}

		sessions = append(sessions, &session)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return sessions, nil
}

// NewManagerWithStore creates a session manager that uses persistent storage.
func NewManagerWithStore(sessionTimeout time.Duration, store *Store) *Manager {
	mgr := NewManager(sessionTimeout)
	mgr.store = store
	return mgr
}
