// Package query provides SQL query execution against DuckDB with Snowflake SQL translation.
package query

import (
	"context"
)

// SQLExecutor defines the interface for SQL execution.
// This breaks the circular dependency between processors and executor.
type SQLExecutor interface {
	// Execute runs a non-query SQL statement (INSERT, UPDATE, DELETE, etc.).
	// This method includes statement classification and may delegate to processors.
	Execute(ctx context.Context, sql string) (*ExecResult, error)

	// ExecuteRaw runs a SQL statement without classification or processor delegation.
	// Use this from processors to avoid infinite recursion.
	ExecuteRaw(ctx context.Context, sql string) (*ExecResult, error)

	// Query executes a SELECT query and returns results.
	Query(ctx context.Context, sql string) (*Result, error)

	// ExecuteWithBindings executes a non-query statement with parameter bindings.
	ExecuteWithBindings(ctx context.Context, sql string, bindings map[string]*BindingValue) (*ExecResult, error)

	// QueryWithBindings executes a SELECT query with parameter bindings.
	QueryWithBindings(ctx context.Context, sql string, bindings map[string]*BindingValue) (*Result, error)
}

// CopyExecutor defines the interface for COPY INTO execution.
// This allows the executor to delegate COPY operations without importing the processor.
type CopyExecutor interface {
	// ExecuteCopy executes a COPY INTO statement.
	ExecuteCopy(ctx context.Context, sql string, schemaCtx SchemaContext) (*CopyResult, error)
}

// MergeExecutor defines the interface for MERGE INTO execution.
type MergeExecutor interface {
	// ExecuteMerge executes a MERGE INTO statement.
	ExecuteMerge(ctx context.Context, sql string) (*MergeResult, error)
}

// SQLTranslator defines the interface for SQL translation.
type SQLTranslator interface {
	// Translate converts Snowflake SQL to DuckDB-compatible SQL.
	Translate(sql string) (string, error)
}

// StatementClassifier defines the interface for SQL classification.
type StatementClassifier interface {
	// Classify analyzes a SQL statement and returns its classification.
	Classify(sql string) ClassifyResult

	// IsCreateTable checks if the SQL is a CREATE TABLE statement.
	IsCreateTable(sql string) bool

	// IsDropTable checks if the SQL is a DROP TABLE statement.
	IsDropTable(sql string) bool

	// IsCopy checks if the SQL is a COPY INTO statement.
	IsCopy(sql string) bool

	// IsMerge checks if the SQL is a MERGE INTO statement.
	IsMerge(sql string) bool

	// IsTransaction checks if the SQL is a transaction statement.
	IsTransaction(sql string) bool
}

// TableNamer provides table name resolution for DuckDB.
type TableNamer interface {
	// BuildDuckDBTableName constructs a DuckDB table name from Snowflake components.
	// Pattern: DATABASE.SCHEMA_TABLE
	BuildDuckDBTableName(database, schema, table string) string

	// ParseTableReference parses a table reference into database, schema, and table components.
	ParseTableReference(ref string) (database, schema, table string)
}

// SchemaContext provides database/schema context for operations.
type SchemaContext struct {
	DatabaseName string
	SchemaName   string
	SchemaID     string
}
