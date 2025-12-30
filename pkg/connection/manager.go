package connection

import (
	"context"
	"database/sql"
	"sync"
)

// Manager manages DuckDB connections with proper locking.
//
// The Manager ensures thread-safe access to the DuckDB database:
//   - Query operations can be concurrent (reads)
//   - Exec operations are serialized using a mutex (writes)
//   - Transactions are also serialized to maintain consistency
type Manager struct {
	db      *sql.DB
	writeMu sync.Mutex
}

// NewManager creates a new connection manager for the given database.
func NewManager(db *sql.DB) *Manager {
	return &Manager{db: db}
}

// Query executes a read query (can be concurrent).
// Multiple goroutines can call Query simultaneously.
func (m *Manager) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return m.db.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
func (m *Manager) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return m.db.QueryRowContext(ctx, query, args...)
}

// Exec executes a write operation (serialized).
// Write operations are serialized using a mutex to prevent conflicts.
func (m *Manager) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return m.db.ExecContext(ctx, query, args...)
}

// ExecTx executes multiple statements in a transaction.
// The transaction is serialized using the same write mutex.
// If the provided function returns an error, the transaction is rolled back.
func (m *Manager) ExecTx(ctx context.Context, fn func(*sql.Tx) error) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// DB returns the underlying database connection.
// This is useful for operations that need direct access to the sql.DB,
// such as setting connection pool parameters.
func (m *Manager) DB() *sql.DB {
	return m.db
}
