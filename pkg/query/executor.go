package query

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

// Binding validation regexes to prevent SQL injection
var (
	// Date format: YYYY-MM-DD
	dateRegex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	// Time format: HH:MM:SS or HH:MM:SS.fraction
	timeRegex = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}(\.\d+)?$`)
	// Timestamp format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DDTHH:MM:SS with optional timezone
	timestampRegex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:?\d{2}|Z)?$`)
	// Question mark placeholder for positional parameters
	questionMarkRegex = regexp.MustCompile(`\?`)
)

// Executor executes SQL queries against DuckDB with Snowflake SQL translation.
type Executor struct {
	mgr            *connection.Manager
	repo           *metadata.Repository
	translator     *Translator
	classifier     *Classifier
	copyProcessor  *CopyProcessor
	mergeProcessor *MergeProcessor
}

// ExecutorOption configures an Executor.
type ExecutorOption func(*Executor)

// WithCopyProcessor sets the COPY processor for executing COPY INTO statements.
func WithCopyProcessor(processor *CopyProcessor) ExecutorOption {
	return func(e *Executor) {
		e.copyProcessor = processor
	}
}

// WithMergeProcessor sets the MERGE processor for executing MERGE INTO statements.
func WithMergeProcessor(processor *MergeProcessor) ExecutorOption {
	return func(e *Executor) {
		e.mergeProcessor = processor
	}
}

// NewExecutor creates a new query executor.
func NewExecutor(mgr *connection.Manager, repo *metadata.Repository, opts ...ExecutorOption) *Executor {
	e := &Executor{
		mgr:        mgr,
		repo:       repo,
		translator: NewTranslator(),
		classifier: NewClassifier(),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Configure applies options to an existing Executor.
// Use this to resolve circular dependencies when processors need the executor reference.
func (e *Executor) Configure(opts ...ExecutorOption) {
	for _, opt := range opts {
		opt(e)
	}
}

// Query executes a SELECT query and returns results.
func (e *Executor) Query(ctx context.Context, sql string) (*Result, error) {
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

	return &Result{
		Columns:     columns,
		ColumnTypes: columnTypes,
		Rows:        resultRows,
	}, nil
}

// QueryWithBindings executes a SELECT query with parameter bindings and returns results.
// Bindings are keyed by position (e.g., "1", "2", "3") and replace :1, :2, :3 placeholders.
func (e *Executor) QueryWithBindings(ctx context.Context, sql string, bindings map[string]*QueryBindingValue) (*Result, error) {
	if len(bindings) == 0 {
		return e.Query(ctx, sql)
	}

	// Replace binding placeholders with actual values
	boundSQL, err := e.applyBindings(sql, bindings)
	if err != nil {
		return nil, fmt.Errorf("binding error: %w", err)
	}

	return e.Query(ctx, boundSQL)
}

// applyBindings replaces :N or :name placeholders with actual values from bindings.
// Snowflake uses :1, :2, etc. for positional parameters and :p0, :p1, etc. for named parameters.
// Keys are sorted by length descending so that longer keys are replaced first,
// preventing partial matches (e.g., :p10 is replaced before :p1).
func (e *Executor) applyBindings(sql string, bindings map[string]*QueryBindingValue) (string, error) {
	// Sort keys by length descending to prevent partial matches
	keys := make([]string, 0, len(bindings))
	for k := range bindings {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) != len(keys[j]) {
			return len(keys[i]) > len(keys[j])
		}
		return keys[i] > keys[j]
	})

	result := sql
	for _, key := range keys {
		binding := bindings[key]
		if binding == nil {
			continue
		}
		value, err := formatBindingValue(binding)
		if err != nil {
			return "", fmt.Errorf("error formatting binding %s: %w", key, err)
		}
		result = replaceBindingPlaceholder(result, key, value)
	}

	// Also handle ? placeholders (positional, 1-based)
	result = e.replaceQuestionMarkPlaceholders(result, bindings)

	return result, nil
}

// replaceBindingPlaceholder replaces all occurrences of :key in sql with value,
// only when :key is not followed by a word character (letter, digit, or underscore).
func replaceBindingPlaceholder(sql, key, value string) string {
	placeholder := ":" + key
	placeholderLen := len(placeholder)
	var b strings.Builder
	b.Grow(len(sql))

	i := 0
	for i < len(sql) {
		if i+placeholderLen <= len(sql) && sql[i:i+placeholderLen] == placeholder {
			// Check that the next character (if any) is not a word character
			end := i + placeholderLen
			if end >= len(sql) || !isBindingWordChar(sql[end]) {
				b.WriteString(value)
				i = end
				continue
			}
		}
		b.WriteByte(sql[i])
		i++
	}
	return b.String()
}

// isBindingWordChar returns true if b is a letter, digit, or underscore.
func isBindingWordChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

// replaceQuestionMarkPlaceholders replaces ? placeholders with binding values.
func (e *Executor) replaceQuestionMarkPlaceholders(sql string, bindings map[string]*QueryBindingValue) string {
	// Find all ? placeholders
	matches := questionMarkRegex.FindAllStringIndex(sql, -1)
	if len(matches) == 0 {
		return sql
	}

	// Replace from end to start to preserve indices
	result := sql
	for i := len(matches) - 1; i >= 0; i-- {
		key := strconv.Itoa(i + 1) // 1-based
		binding := bindings[key]
		if binding == nil {
			continue
		}

		value, err := formatBindingValue(binding)
		if err != nil {
			continue // Skip on error
		}

		start := matches[i][0]
		end := matches[i][1]
		result = result[:start] + value + result[end:]
	}

	return result
}

// formatBindingValue formats a binding value for SQL substitution.
//
//nolint:gocyclo // switch statement for type handling inherently has many branches
func formatBindingValue(b *QueryBindingValue) (string, error) {
	if b == nil || b.Value == nil {
		return ValueNull, nil
	}

	value := *b.Value

	switch strings.ToUpper(b.Type) {
	case TypeText, "VARCHAR", "STRING":
		// Escape single quotes and wrap in quotes
		escaped := strings.ReplaceAll(value, "'", "''")
		return "'" + escaped + "'", nil

	case "FIXED", "INTEGER", "BIGINT", "SMALLINT", "TINYINT":
		// Validate it's a number
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return "", fmt.Errorf("invalid integer value: %s", value)
		}
		return value, nil

	case "REAL", "FLOAT", "DOUBLE", "NUMBER", "DECIMAL":
		// Validate it's a number
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			return "", fmt.Errorf("invalid float value: %s", value)
		}
		return value, nil

	case "BOOLEAN":
		lower := strings.ToLower(value)
		if lower == "true" || lower == "1" {
			return "TRUE", nil
		}
		return "FALSE", nil

	case "DATE":
		// Validate date format to prevent SQL injection
		if !dateRegex.MatchString(value) {
			return "", fmt.Errorf("invalid DATE format: %s (expected YYYY-MM-DD)", value)
		}
		return "DATE '" + value + "'", nil

	case "TIME":
		// Validate time format to prevent SQL injection
		if !timeRegex.MatchString(value) {
			return "", fmt.Errorf("invalid TIME format: %s (expected HH:MM:SS)", value)
		}
		return "TIME '" + value + "'", nil

	case "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ":
		// The Snowflake drivers may send timestamps as either:
		// - Formatted strings: "2024-01-01 00:00:00"
		// - Nanoseconds since epoch: "1704067200000000000"
		if timestampRegex.MatchString(value) {
			return "TIMESTAMP '" + value + "'", nil
		}
		// Try parsing as nanoseconds since epoch
		nanos, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid TIMESTAMP format: %s (expected YYYY-MM-DD HH:MM:SS or epoch nanoseconds)", value)
		}
		t := time.Unix(0, nanos).UTC()
		return "TIMESTAMP '" + t.Format("2006-01-02 15:04:05.999999999") + "'", nil

	case ValueNull:
		return ValueNull, nil

	default:
		// Default to text treatment
		escaped := strings.ReplaceAll(value, "'", "''")
		return "'" + escaped + "'", nil
	}
}

// ExecuteWithBindings executes a non-query SQL statement with parameter bindings.
// Bindings are keyed by position (e.g., "1", "2", "3") and replace :1, :2, :3 placeholders.
func (e *Executor) ExecuteWithBindings(ctx context.Context, sql string, bindings map[string]*QueryBindingValue) (*ExecResult, error) {
	if len(bindings) == 0 {
		return e.Execute(ctx, sql)
	}

	// Replace binding placeholders with actual values
	boundSQL, err := e.applyBindings(sql, bindings)
	if err != nil {
		return nil, fmt.Errorf("binding error: %w", err)
	}

	return e.Execute(ctx, boundSQL)
}

// Execute executes a non-query SQL statement (INSERT, UPDATE, DELETE, CREATE, DROP, etc.).
func (e *Executor) Execute(ctx context.Context, sql string) (*ExecResult, error) {
	// For CREATE TABLE, we need to register it in metadata
	if e.classifier.IsCreateTable(sql) {
		return e.executeCreateTable(ctx, sql)
	}

	// For DROP TABLE, we need to remove it from metadata
	if e.classifier.IsDropTable(sql) {
		return e.executeDropTable(ctx, sql)
	}

	// Handle transaction control statements
	if IsTransaction(sql) {
		return e.executeTransaction(ctx, sql)
	}

	// Handle COPY INTO statements
	if IsCopy(sql) {
		return e.executeCopy(ctx, sql)
	}

	// Handle MERGE INTO statements
	if IsMerge(sql) {
		return e.executeMerge(ctx, sql)
	}

	// Execute regular SQL statement
	return e.executeRaw(ctx, sql)
}

// executeRaw executes a SQL statement without classification or processor delegation.
// Use this from processors (COPY, MERGE) to avoid infinite recursion.
// This is a private method as it's only called from same-package processors.
func (e *Executor) executeRaw(ctx context.Context, sql string) (*ExecResult, error) {
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

// executeTransaction handles transaction control statements (BEGIN, COMMIT, ROLLBACK).
// DuckDB supports transactions, so we pass them through directly.
func (e *Executor) executeTransaction(ctx context.Context, sql string) (*ExecResult, error) {
	// DuckDB supports BEGIN, COMMIT, and ROLLBACK
	// We execute them directly without translation
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))

	// Normalize transaction statements for DuckDB
	var duckDBSQL string
	switch {
	case strings.HasPrefix(upperSQL, "BEGIN") || strings.HasPrefix(upperSQL, "START TRANSACTION"):
		duckDBSQL = "BEGIN TRANSACTION"
	case strings.HasPrefix(upperSQL, "COMMIT"):
		duckDBSQL = "COMMIT"
	case strings.HasPrefix(upperSQL, "ROLLBACK"):
		duckDBSQL = "ROLLBACK"
	default:
		return nil, fmt.Errorf("unknown transaction statement: %s", sql)
	}

	if _, err := e.mgr.Exec(ctx, duckDBSQL); err != nil {
		return nil, fmt.Errorf("transaction error: %w", err)
	}

	return &ExecResult{
		RowsAffected: 0,
	}, nil
}

// executeCopy handles COPY INTO statements.
func (e *Executor) executeCopy(ctx context.Context, sql string) (*ExecResult, error) {
	if e.copyProcessor == nil {
		return nil, fmt.Errorf("COPY processor not configured")
	}

	// Parse the COPY statement
	stmt, err := e.copyProcessor.ParseCopyStatement(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse COPY statement: %w", err)
	}

	// Resolve schema ID from target database/schema names if provided
	var schemaID string
	if stmt.TargetDatabase != "" && stmt.TargetSchema != "" {
		// Look up database by name
		db, err := e.repo.GetDatabaseByName(ctx, stmt.TargetDatabase)
		if err != nil {
			return nil, fmt.Errorf("database %s not found: %w", stmt.TargetDatabase, err)
		}
		// Look up schema by name
		schema, err := e.repo.GetSchemaByName(ctx, db.ID, stmt.TargetSchema)
		if err != nil {
			return nil, fmt.Errorf("schema %s not found in database %s: %w", stmt.TargetSchema, stmt.TargetDatabase, err)
		}
		schemaID = schema.ID
	}

	// Execute COPY INTO with resolved schema context
	result, err := e.copyProcessor.ExecuteCopyInto(ctx, stmt, schemaID)
	if err != nil {
		return nil, fmt.Errorf("COPY INTO failed: %w", err)
	}

	return &ExecResult{
		RowsAffected: result.RowsLoaded,
	}, nil
}

// executeMerge handles MERGE INTO statements.
func (e *Executor) executeMerge(ctx context.Context, sql string) (*ExecResult, error) {
	if e.mergeProcessor == nil {
		return nil, fmt.Errorf("MERGE processor not configured")
	}

	// Parse the MERGE statement
	stmt, err := e.mergeProcessor.ParseMergeStatement(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse MERGE statement: %w", err)
	}

	// Execute MERGE
	result, err := e.mergeProcessor.ExecuteMerge(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("MERGE failed: %w", err)
	}

	return &ExecResult{
		RowsAffected: result.RowsInserted + result.RowsUpdated + result.RowsDeleted,
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

// ExecuteWithHistory wraps Execute with query history tracking.
func (e *Executor) ExecuteWithHistory(ctx context.Context, sessionID, queryID, sql string, bindings map[string]*QueryBindingValue) (*ExecResult, error) {
	startTime := time.Now()

	// Record query start (non-blocking on failure)
	entry, err := e.repo.RecordQueryStart(ctx, sessionID, queryID, sql)
	if err != nil {
		log.Printf("Failed to record query start: %v", err)
	}

	// Execute with bindings
	result, execErr := e.ExecuteWithBindings(ctx, sql, bindings)

	// Calculate execution time
	executionTimeMs := time.Since(startTime).Milliseconds()

	// Record result
	if entry != nil {
		if execErr != nil {
			_ = e.repo.RecordQueryFailure(ctx, entry.ID, execErr.Error(), executionTimeMs)
		} else {
			_ = e.repo.RecordQuerySuccess(ctx, entry.ID, result.RowsAffected, executionTimeMs)
		}
	}

	return result, execErr
}

// QueryWithHistory wraps Query with query history tracking.
func (e *Executor) QueryWithHistory(ctx context.Context, sessionID, queryID, sql string, bindings map[string]*QueryBindingValue) (*Result, error) {
	startTime := time.Now()

	// Record query start (non-blocking on failure)
	entry, err := e.repo.RecordQueryStart(ctx, sessionID, queryID, sql)
	if err != nil {
		log.Printf("Failed to record query start: %v", err)
	}

	// Execute query with bindings
	result, execErr := e.QueryWithBindings(ctx, sql, bindings)

	// Calculate execution time
	executionTimeMs := time.Since(startTime).Milliseconds()

	// Record result
	if entry != nil {
		if execErr != nil {
			_ = e.repo.RecordQueryFailure(ctx, entry.ID, execErr.Error(), executionTimeMs)
		} else {
			var rowCount int64
			if result != nil {
				rowCount = int64(len(result.Rows))
			}
			_ = e.repo.RecordQuerySuccess(ctx, entry.ID, rowCount, executionTimeMs)
		}
	}

	return result, execErr
}
