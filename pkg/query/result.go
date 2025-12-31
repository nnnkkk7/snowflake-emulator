// Package query provides SQL query execution against DuckDB with Snowflake SQL translation.
package query

import (
	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

// BindingValue represents a parameter binding value for SQL queries.
// This mirrors the REST API v2 binding format.
type BindingValue struct {
	Type  string // FIXED, TEXT, REAL, BOOLEAN, DATE, TIME, TIMESTAMP, etc.
	Value string // String representation of the value
}

// QueryBindingValue is an alias for BindingValue for backward compatibility.
//
// Deprecated: Use BindingValue instead.
//
//nolint:revive // Keeping for backward compatibility
type QueryBindingValue = BindingValue

// Result represents the result of a SELECT query execution.
type Result struct {
	Columns     []string
	ColumnTypes []types.ColumnMetadata
	Rows        [][]interface{}
}

// ExecResult represents the result of a non-query execution (INSERT, UPDATE, DELETE, etc.).
type ExecResult struct {
	RowsAffected int64
}

// CopyResult contains the result of a COPY INTO operation.
type CopyResult struct {
	RowsLoaded   int64
	RowsInserted int64
	FilesLoaded  int
	Errors       []string
}

// MergeResult contains the result of a MERGE operation.
type MergeResult struct {
	RowsInserted int64
	RowsUpdated  int64
	RowsDeleted  int64
}
