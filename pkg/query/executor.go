package query

import (
	"context"
	"fmt"

	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

// Executor executes SQL queries against DuckDB with Snowflake SQL translation.
type Executor struct {
	mgr        *connection.Manager
	repo       *metadata.Repository
	translator *Translator
}

// QueryResult represents the result of a query execution.
type QueryResult struct {
	Columns     []string
	ColumnTypes []types.ColumnMetadata
	Rows        [][]interface{}
}

// ExecResult represents the result of a non-query execution (INSERT, UPDATE, DELETE, etc.).
type ExecResult struct {
	RowsAffected int64
}

// NewExecutor creates a new query executor.
func NewExecutor(mgr *connection.Manager, repo *metadata.Repository) *Executor {
	return &Executor{
		mgr:        mgr,
		repo:       repo,
		translator: NewTranslator(),
	}
}

// Query executes a SELECT query and returns results.
func (e *Executor) Query(ctx context.Context, sql string) (*QueryResult, error) {
	// Translate Snowflake SQL to DuckDB SQL
	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	// Execute query
	rows, err := e.mgr.Query(ctx, translatedSQL)
	if err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Capture column types before iterating (using TypeMapper)
	columnTypes := InferColumnMetadata(columns, rows)

	// Fetch all rows
	var resultRows [][]interface{}
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert values to appropriate types
		row := make([]interface{}, len(columns))
		for i, val := range values {
			row[i] = convertValue(val)
		}

		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return &QueryResult{
		Columns:     columns,
		ColumnTypes: columnTypes,
		Rows:        resultRows,
	}, nil
}

// Execute executes a non-query SQL statement (INSERT, UPDATE, DELETE, CREATE, DROP, etc.).
func (e *Executor) Execute(ctx context.Context, sql string) (*ExecResult, error) {
	// Use classifier to detect DDL statements that need metadata tracking
	classifier := NewClassifier()

	// For CREATE TABLE, we need to register it in metadata
	if classifier.IsCreateTable(sql) {
		return e.executeCreateTable(ctx, sql)
	}

	// For DROP TABLE, we need to remove it from metadata
	if classifier.IsDropTable(sql) {
		return e.executeDropTable(ctx, sql)
	}

	// Translate Snowflake SQL to DuckDB SQL
	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	// Execute statement
	result, err := e.mgr.Exec(ctx, translatedSQL)
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return &ExecResult{
		RowsAffected: rowsAffected,
	}, nil
}

// executeCreateTable handles CREATE TABLE statements with metadata registration.
func (e *Executor) executeCreateTable(ctx context.Context, sql string) (*ExecResult, error) {
	// Execute the CREATE TABLE in DuckDB first
	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	if _, err := e.mgr.Exec(ctx, translatedSQL); err != nil {
		return nil, fmt.Errorf("create table execution error: %w", err)
	}

	// Note: In a full implementation, we would parse the CREATE TABLE statement
	// and register it in metadata. For now, we just execute it.
	// This would require SQL parsing to extract table name, columns, etc.

	return &ExecResult{
		RowsAffected: 0,
	}, nil
}

// executeDropTable handles DROP TABLE statements with metadata cleanup.
func (e *Executor) executeDropTable(ctx context.Context, sql string) (*ExecResult, error) {
	// Execute the DROP TABLE in DuckDB first
	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	if _, err := e.mgr.Exec(ctx, translatedSQL); err != nil {
		return nil, fmt.Errorf("drop table execution error: %w", err)
	}

	// Note: In a full implementation, we would remove the table from metadata.
	// This would require SQL parsing to extract the table name.

	return &ExecResult{
		RowsAffected: 0,
	}, nil
}

// convertValue converts database values to appropriate Go types.
func convertValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case []byte:
		// Convert byte slices to strings
		return string(v)
	case int64:
		// Keep as int64 for now, could convert to int if needed
		return v
	case float64:
		return v
	case bool:
		return v
	case string:
		return v
	default:
		// For other types, return as-is
		return v
	}
}
