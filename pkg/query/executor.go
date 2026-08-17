package query

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/stage"
)

// Binding validation regexes to prevent SQL injection
var (
	// Date format: YYYY-MM-DD
	dateRegex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	// Time format: HH:MM:SS or HH:MM:SS.fraction
	timeRegex = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}(\.\d+)?$`)
	// Timestamp format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DDTHH:MM:SS with optional timezone
	timestampRegex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:?\d{2}|Z)?$`)
)

// Executor executes SQL queries against DuckDB with Snowflake SQL translation.
type Executor struct {
	mgr            *connection.Manager
	repo           *metadata.Repository
	translator     *Translator
	copyProcessor  *CopyProcessor
	mergeProcessor *MergeProcessor
	stageMgr       *stage.Manager
}

type createStageStatement struct {
	Database    string
	Schema      string
	Name        string
	IfNotExists bool
}

type putStatement struct {
	LocalPath    string
	Database     string
	Schema       string
	StageName    string
	StagePath    string
	AutoCompress bool
	Overwrite    bool
}

type removeStatement struct {
	Database  string
	Schema    string
	StageName string
	StagePath string
	Pattern   string
}

type listStatement struct {
	Database  string
	Schema    string
	StageName string
	StagePath string
	Pattern   string
}

type PutFileTransferPlan struct {
	LocalPath           string
	Database            string
	Schema              string
	StageName           string
	StagePath           string
	StageLocalDirectory string
	AutoCompress        bool
	Overwrite           bool
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

func WithStageManager(manager *stage.Manager) ExecutorOption {
	return func(e *Executor) {
		e.stageMgr = manager
	}
}

// NewExecutor creates a new query executor.
func NewExecutor(mgr *connection.Manager, repo *metadata.Repository, opts ...ExecutorOption) *Executor {
	e := &Executor{
		mgr:        mgr,
		repo:       repo,
		translator: NewTranslator(),
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
	if isListStatement(sql) {
		return e.QueryList(ctx, ExecutionContext{}, sql)
	}

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

func (e *Executor) QueryWithContext(
	ctx context.Context,
	execCtx ExecutionContext,
	sqlText string,
) (*Result, error) {
	if isListStatement(sqlText) {
		return e.QueryList(ctx, execCtx, sqlText)
	}

	return e.Query(ctx, sqlText)
}

// applyBindings replaces :N placeholders with actual values from bindings.
// Snowflake uses :1, :2 or :v1, :v2, etc. for positional parameters.
func (e *Executor) applyBindings(
	sql string,
	bindings map[string]*QueryBindingValue,
) (string, error) {
	normalized := make(map[string]*QueryBindingValue, len(bindings))

	for key, binding := range bindings {
		normalizedKey := normalizeBindingKey(key)
		if normalizedKey == "" {
			return "", fmt.Errorf("invalid binding key %q", key)
		}

		normalized[normalizedKey] = binding
	}

	keys := make([]int, 0, len(normalized))
	for key := range normalized {
		pos, err := strconv.Atoi(key)
		if err != nil {
			return "", fmt.Errorf("invalid binding key %q: must be numeric after normalization", key)
		}

		keys = append(keys, pos)
	}

	sort.Sort(sort.Reverse(sort.IntSlice(keys)))

	result := sql

	for _, pos := range keys {
		key := strconv.Itoa(pos)
		binding := normalized[key]
		if binding == nil {
			continue
		}

		value, err := formatBindingValue(binding)
		if err != nil {
			return "", fmt.Errorf("error formatting binding %s: %w", key, err)
		}

		placeholders := []string{
			":" + key,
			":v" + key,
			":V" + key,
		}

		for _, placeholder := range placeholders {
			result = strings.ReplaceAll(result, placeholder, value)
		}
	}

	result = e.replaceQuestionMarkPlaceholders(result, normalized)

	return result, nil
}

func normalizeBindingKey(key string) string {
	key = strings.TrimSpace(key)
	key = strings.TrimPrefix(key, ":")
	key = strings.TrimPrefix(key, "v")
	key = strings.TrimPrefix(key, "V")

	if key == "" {
		return ""
	}

	if _, err := strconv.Atoi(key); err != nil {
		return ""
	}

	return key
}

// replaceQuestionMarkPlaceholders replaces ? placeholders with binding values.
func (e *Executor) replaceQuestionMarkPlaceholders(sql string, bindings map[string]*QueryBindingValue) string {
	// Find all ? placeholders
	re := regexp.MustCompile(`\?`)
	matches := re.FindAllStringIndex(sql, -1)
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
	if b == nil {
		return ValueNull, nil
	}

	switch strings.ToUpper(b.Type) {
	case TypeText, "VARCHAR", "STRING":
		// Escape single quotes and wrap in quotes
		escaped := strings.ReplaceAll(b.Value, "'", "''")
		return "'" + escaped + "'", nil

	case "FIXED", "INTEGER", "BIGINT", "SMALLINT", "TINYINT":
		// Validate it's a number
		if _, err := strconv.ParseInt(b.Value, 10, 64); err != nil {
			return "", fmt.Errorf("invalid integer value: %s", b.Value)
		}
		return b.Value, nil

	case "REAL", "FLOAT", "DOUBLE", "NUMBER", "DECIMAL":
		// Validate it's a number
		if _, err := strconv.ParseFloat(b.Value, 64); err != nil {
			return "", fmt.Errorf("invalid float value: %s", b.Value)
		}
		return b.Value, nil

	case "BOOLEAN":
		lower := strings.ToLower(b.Value)
		if lower == "true" || lower == "1" {
			return "TRUE", nil
		}
		return "FALSE", nil

	case "DATE":
		// Validate date format to prevent SQL injection
		if !dateRegex.MatchString(b.Value) {
			return "", fmt.Errorf("invalid DATE format: %s (expected YYYY-MM-DD)", b.Value)
		}
		return "DATE '" + b.Value + "'", nil

	case "TIME":
		// Validate time format to prevent SQL injection
		if !timeRegex.MatchString(b.Value) {
			return "", fmt.Errorf("invalid TIME format: %s (expected HH:MM:SS)", b.Value)
		}
		return "TIME '" + b.Value + "'", nil

	case "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ":
		// Validate timestamp format to prevent SQL injection
		if !timestampRegex.MatchString(b.Value) {
			return "", fmt.Errorf("invalid TIMESTAMP format: %s (expected YYYY-MM-DD HH:MM:SS)", b.Value)
		}
		return "TIMESTAMP '" + b.Value + "'", nil

	case ValueNull:
		return ValueNull, nil

	default:
		// Default to text treatment
		escaped := strings.ReplaceAll(b.Value, "'", "''")
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

func (e *Executor) ExecuteWithBindingsAndContext(
	ctx context.Context,
	execCtx ExecutionContext,
	sqlText string,
	bindings map[string]*QueryBindingValue,
) (*ExecResult, error) {
	if len(bindings) == 0 {
		return e.ExecuteWithContext(ctx, execCtx, sqlText)
	}

	boundSQL, err := e.applyBindings(sqlText, bindings)
	if err != nil {
		return nil, fmt.Errorf("binding error: %w", err)
	}

	return e.ExecuteWithContext(ctx, execCtx, boundSQL)
}

// Execute executes a non-query SQL statement (INSERT, UPDATE, DELETE, CREATE, DROP, etc.).
func (e *Executor) Execute(ctx context.Context, sqlText string) (*ExecResult, error) {
	return e.ExecuteWithContext(ctx, ExecutionContext{}, sqlText)
}

func (e *Executor) ExecuteWithContext(
	ctx context.Context,
	execCtx ExecutionContext,
	sqlText string,
) (*ExecResult, error) {
	classifier := NewClassifier()
	log.Printf("ExecuteWithContext sql=%q", sqlText)

	// For CREATE STAGE, we need to register it in metadata
	if classifier.IsCreateStage(sqlText) {
		return e.executeCreateStage(ctx, execCtx, sqlText)
	}

	// For PUT, we need to upload a local file into an internal stage
	if classifier.IsPut(sqlText) {
		log.Printf("ExecuteWithContext detected PUT")
		return e.executePut(ctx, execCtx, sqlText)
	}

	if isRemoveStatement(sqlText) {
		return e.executeRemove(ctx, execCtx, sqlText)
	}

	if isListStatement(sqlText) {
		return e.executeList(ctx, execCtx, sqlText)
	}

	// For CREATE TABLE, we need to register it in metadata
	if classifier.IsCreateTable(sqlText) {
		return e.executeCreateTable(ctx, execCtx, sqlText)
	}

	if classifier.IsDropTable(sqlText) {
		return e.executeDropTable(ctx, execCtx, sqlText)
	}

	// Handle transaction control statements
	if IsTransaction(sqlText) {
		return e.executeTransaction(ctx, sqlText)
	}

	// Handle COPY INTO statements
	if IsCopy(sqlText) {
		return e.executeCopy(ctx, execCtx, sqlText)
	}

	// Handle MERGE INTO statements
	if IsMerge(sqlText) {
		return e.executeMerge(ctx, execCtx, sqlText)
	}

	// Qualify unqualified DELETE statements using the current schema.
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlText)), "DELETE FROM ") {
		sqlText = qualifyDeleteWithSchema(sqlText, execCtx.CurrentSchema)
	}

	// Execute regular SQL statement
	return e.executeRawWithContext(ctx, execCtx, sqlText)
}

func (e *Executor) ExecuteWithHistoryAndContext(ctx context.Context, execCtx ExecutionContext, queryID string, sqlText string) (*ExecResult, error) {
	startTime := time.Now()

	// Record query start (non-blocking on failure)
	entry, err := e.repo.RecordQueryStart(ctx, execCtx.SessionID, queryID, sqlText)
	if err != nil {
		log.Printf("Failed to record query start: %v", err)
	}

	result, execErr := e.ExecuteWithContext(ctx, execCtx, sqlText)

	// Calculate execution time
	executionTimeMs := time.Since(startTime).Milliseconds()

	// Record result
	if entry != nil {
		if execErr != nil {
			_ = e.repo.RecordQueryFailure(ctx, entry.ID, execErr.Error(), executionTimeMs)
		} else {
			var rowsAffected int64
			if result != nil {
				rowsAffected = result.RowsAffected
			}

			_ = e.repo.RecordQuerySuccess(ctx, entry.ID, rowsAffected, executionTimeMs)
		}
	}

	return result, execErr
}

// executeRaw executes a SQL statement without classification or processor delegation.
// Use this from processors (COPY, MERGE) to avoid infinite recursion.
// This is a private method as it's only called from same-package processors.
func (e *Executor) executeRawWithContext(
	ctx context.Context,
	execCtx ExecutionContext,
	sql string,
) (*ExecResult, error) {
	sql = rewriteTableRefsToPhysicalDuckDB(sql, execCtx.CurrentSchema)

	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	result, err := e.mgr.Exec(ctx, translatedSQL)
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return &ExecResult{RowsAffected: rowsAffected}, nil
}

func (e *Executor) executeRaw(ctx context.Context, sql string) (*ExecResult, error) {
	return e.executeRawWithContext(ctx, ExecutionContext{}, sql)
}

func (e *Executor) executeCreateTable(
	ctx context.Context,
	execCtx ExecutionContext,
	sql string,
) (*ExecResult, error) {
	sql = rewriteTableRefsToPhysicalDuckDB(sql, execCtx.CurrentSchema)

	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	if _, err := e.mgr.Exec(ctx, translatedSQL); err != nil {
		return nil, fmt.Errorf("create table execution error: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}

func (e *Executor) executeDropTable(
	ctx context.Context,
	execCtx ExecutionContext,
	sql string,
) (*ExecResult, error) {
	sql = rewriteTableRefsToPhysicalDuckDB(sql, execCtx.CurrentSchema)

	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	if _, err := e.mgr.Exec(ctx, translatedSQL); err != nil {
		return nil, fmt.Errorf("drop table execution error: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
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
func (e *Executor) executeCopy(
	ctx context.Context,
	execCtx ExecutionContext,
	sql string,
) (*ExecResult, error) {
	if e.copyProcessor == nil {
		return nil, fmt.Errorf("COPY processor not configured")
	}

	stmt, err := e.copyProcessor.ParseCopyStatement(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse COPY statement: %w", err)
	}

	// Use target database/schema if present; otherwise use session context.
	if stmt.TargetDatabase == "" {
		stmt.TargetDatabase = execCtx.Database
	}
	if stmt.TargetSchema == "" {
		stmt.TargetSchema = execCtx.CurrentSchema
	}

	// Use same context for stage when stage is unqualified.
	schemaID, err := e.resolveSchemaID(
		ctx,
		execCtx,
		stmt.TargetDatabase,
		stmt.TargetSchema,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve COPY schema context: %w", err)
	}

	result, err := e.copyProcessor.ExecuteCopyInto(ctx, stmt, schemaID)
	if err != nil {
		return nil, fmt.Errorf("COPY INTO failed: %w", err)
	}

	return &ExecResult{
		RowsAffected: result.RowsLoaded,
	}, nil
}

// executeMerge handles MERGE INTO statements.
func (e *Executor) executeMerge(
	ctx context.Context,
	execCtx ExecutionContext,
	sql string,
) (*ExecResult, error) {
	if e.mergeProcessor == nil {
		return nil, fmt.Errorf("MERGE processor not configured")
	}

	log.Printf("executeMerge sql=%s", sql)

	stmt, err := e.mergeProcessor.ParseMergeStatement(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse MERGE statement: %w", err)
	}

	if stmt.TargetDatabase == "" {
		stmt.TargetDatabase = execCtx.Database
	}

	if stmt.TargetSchema == "" {
		stmt.TargetSchema = execCtx.CurrentSchema
	}

	result, err := e.mergeProcessor.ExecuteMerge(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("MERGE failed: %w", err)
	}

	return &ExecResult{
		RowsAffected: result.RowsInserted + result.RowsUpdated + result.RowsDeleted,
	}, nil
}

func (e *Executor) executeCreateStage(ctx context.Context, execCtx ExecutionContext, sqlText string) (*ExecResult, error) {
	if e.stageMgr == nil {
		return nil, fmt.Errorf("stage manager not configured")
	}

	stmt, err := parseCreateStageStatement(sqlText)
	if err != nil {
		return nil, err
	}

	schemaID, err := e.resolveSchemaID(ctx, execCtx, stmt.Database, stmt.Schema)
	if err != nil {
		return nil, err
	}

	_, err = e.stageMgr.CreateStage(
		ctx,
		schemaID,
		stmt.Name,
		"INTERNAL",
		"",
		"",
	)
	if err != nil {
		// Para IF NOT EXISTS, ignore duplicate.
		if stmt.IfNotExists && strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return &ExecResult{RowsAffected: 0}, nil
		}

		return nil, fmt.Errorf("create stage failed: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}

func (e *Executor) executePut(ctx context.Context, execCtx ExecutionContext, sqlText string) (*ExecResult, error) {
	if e.stageMgr == nil {
		return nil, fmt.Errorf("stage manager not configured")
	}

	stmt, err := parsePutStatement(sqlText)
	if err != nil {
		return nil, err
	}

	schemaID, err := e.resolveSchemaID(ctx, execCtx, stmt.Database, stmt.Schema)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(stmt.LocalPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open PUT source file %s: %w", stmt.LocalPath, err)
	}
	defer file.Close()

	targetFileName := filepath.Base(stmt.LocalPath)

	var reader io.Reader = file

	if stmt.AutoCompress {
		var buf bytes.Buffer

		gz := gzip.NewWriter(&buf)
		if _, err := io.Copy(gz, file); err != nil {
			_ = gz.Close()
			return nil, fmt.Errorf("failed to gzip PUT source file: %w", err)
		}

		if err := gz.Close(); err != nil {
			return nil, fmt.Errorf("failed to close gzip writer: %w", err)
		}

		reader = &buf

		if !strings.HasSuffix(strings.ToLower(targetFileName), ".gz") {
			targetFileName += ".gz"
		}
	}

	stageFileName := targetFileName
	if stmt.StagePath != "" {
		stageFileName = strings.Trim(stmt.StagePath, "/") + "/" + targetFileName
	}

	if err := e.stageMgr.PutFile(ctx, schemaID, stmt.StageName, stageFileName, reader); err != nil {
		return nil, fmt.Errorf("PUT failed: %w", err)
	}

	return &ExecResult{RowsAffected: 1}, nil
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
func (e *Executor) ExecuteWithHistory(ctx context.Context, sessionID, queryID, sql string) (*ExecResult, error) {
	startTime := time.Now()

	// Record query start (non-blocking on failure)
	entry, err := e.repo.RecordQueryStart(ctx, sessionID, queryID, sql)
	if err != nil {
		log.Printf("Failed to record query start: %v", err)
	}

	// Execute the query
	result, execErr := e.Execute(ctx, sql)

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
func (e *Executor) QueryWithHistory(ctx context.Context, sessionID, queryID, sql string) (*Result, error) {
	startTime := time.Now()

	// Record query start (non-blocking on failure)
	entry, err := e.repo.RecordQueryStart(ctx, sessionID, queryID, sql)
	if err != nil {
		log.Printf("Failed to record query start: %v", err)
	}

	// Execute the query
	result, execErr := e.Query(ctx, sql)

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

func (e *Executor) BuildPutFileTransferPlan(
	ctx context.Context,
	execCtx ExecutionContext,
	sqlText string,
) (*PutFileTransferPlan, error) {
	stmt, err := parsePutStatement(sqlText)
	if err != nil {
		return nil, err
	}

	schemaID, err := e.resolveSchemaID(ctx, execCtx, stmt.Database, stmt.Schema)
	if err != nil {
		return nil, err
	}

	// Ensure the stage exists.
	if e.stageMgr == nil {
		return nil, fmt.Errorf("stage manager not configured")
	}

	// Aqui precisamos expor/calcular o diretório físico do stage.
	stageLocalDirectory, err := e.resolveLocalStageDirectory(ctx, schemaID, stmt.StageName, stmt.StagePath)
	if err != nil {
		return nil, err
	}

	return &PutFileTransferPlan{
		LocalPath:           stmt.LocalPath,
		Database:            stmt.Database,
		Schema:              stmt.Schema,
		StageName:           stmt.StageName,
		StagePath:           stmt.StagePath,
		StageLocalDirectory: stageLocalDirectory,
		AutoCompress:        stmt.AutoCompress,
		Overwrite:           stmt.Overwrite,
	}, nil
}

func (e *Executor) resolveLocalStageDirectory(
	ctx context.Context,
	schemaID string,
	stageName string,
	stagePath string,
) (string, error) {
	stageName = strings.ToUpper(strings.TrimSpace(stageName))

	// Garante que o stage existe na metadata.
	if _, err := e.stageMgr.GetStage(ctx, schemaID, stageName); err != nil {
		return "", fmt.Errorf("stage %s not found: %w", stageName, err)
	}

	baseDir := strings.TrimSpace(os.Getenv("STAGE_DIR"))
	if baseDir == "" {
		baseDir = "/data/stages"
	}

	dir := filepath.Join(baseDir, schemaID, stageName)

	if stagePath != "" {
		dir = filepath.Join(dir, filepath.FromSlash(strings.Trim(stagePath, "/")))
	}

	if err := os.MkdirAll(dir, 0o777); err != nil {
		return "", fmt.Errorf("create local stage directory failed: %w", err)
	}

	_ = os.Chmod(dir, 0o777)

	return dir, nil
}

func parseCreateStageStatement(sqlText string) (*createStageStatement, error) {
	normalized := strings.TrimSpace(sqlText)
	upper := strings.ToUpper(normalized)

	ifNotExists := strings.Contains(upper, "IF NOT EXISTS")

	re := regexp.MustCompile(`(?is)CREATE\s+(?:OR\s+REPLACE\s+)?STAGE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s;]+)`)
	matches := re.FindStringSubmatch(normalized)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid CREATE STAGE statement: %s", sqlText)
	}

	database, schema, name := splitQualifiedObjectName(matches[1])

	return &createStageStatement{
		Database:    database,
		Schema:      schema,
		Name:        name,
		IfNotExists: ifNotExists,
	}, nil
}

func parsePutStatement(sqlText string) (*putStatement, error) {
	normalized := strings.TrimSpace(sqlText)

	re := regexp.MustCompile(`(?is)^PUT\s+'?file://([^'\s]+)'?\s+@([^\s]+)(.*)$`)
	matches := re.FindStringSubmatch(normalized)
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid PUT statement: %s", sqlText)
	}

	localPath := matches[1]
	stageRef := matches[2]
	options := strings.ToUpper(matches[3])

	stageObject, stagePath := splitStageRef(stageRef)
	database, schema, stageName := splitQualifiedObjectName(stageObject)

	return &putStatement{
		LocalPath:    localPath,
		Database:     database,
		Schema:       schema,
		StageName:    stageName,
		StagePath:    stagePath,
		AutoCompress: strings.Contains(options, "AUTO_COMPRESS") && strings.Contains(options, "TRUE"),
		Overwrite:    strings.Contains(options, "OVERWRITE") && strings.Contains(options, "TRUE"),
	}, nil
}

func splitStageRef(ref string) (stageObject string, stagePath string) {
	ref = strings.TrimSpace(ref)
	ref = strings.TrimPrefix(ref, "@")

	parts := strings.SplitN(ref, "/", 2)
	stageObject = parts[0]

	if len(parts) > 1 {
		stagePath = strings.Trim(parts[1], "/")
	}

	return stageObject, stagePath
}

func splitQualifiedObjectName(name string) (database string, schema string, object string) {
	name = strings.TrimSpace(name)
	name = strings.Trim(name, `"`)

	parts := strings.Split(name, ".")

	for i := range parts {
		parts[i] = strings.Trim(parts[i], `"`)
		parts[i] = strings.ToUpper(strings.TrimSpace(parts[i]))
	}

	switch len(parts) {
	case 1:
		return "", "", parts[0]
	case 2:
		return "", parts[0], parts[1]
	default:
		return parts[len(parts)-3], parts[len(parts)-2], parts[len(parts)-1]
	}
}

func (e *Executor) resolveSchemaID(
	ctx context.Context,
	execCtx ExecutionContext,
	databaseName string,
	schemaName string,
) (string, error) {
	databaseName = strings.TrimSpace(databaseName)
	schemaName = strings.TrimSpace(schemaName)

	if databaseName == "" {
		databaseName = strings.TrimSpace(execCtx.Database)
	}

	if schemaName == "" {
		schemaName = strings.TrimSpace(execCtx.CurrentSchema)
	}

	if databaseName == "" && schemaName == "" {
		return "", fmt.Errorf(
			"no current database or schema; use a fully-qualified object name",
		)
	}

	if databaseName == "" {
		return "", fmt.Errorf(
			"no current database; use a fully-qualified object name",
		)
	}

	if schemaName == "" {
		return "", fmt.Errorf(
			"no current schema; use a fully-qualified object name",
		)
	}

	db, err := e.repo.GetDatabaseByName(ctx, databaseName)
	if err != nil {
		return "", fmt.Errorf("database %s not found: %w", databaseName, err)
	}

	schemaObj, err := e.repo.GetSchemaByName(ctx, db.ID, schemaName)
	if err != nil {
		return "", fmt.Errorf(
			"schema %s not found in database %s: %w",
			schemaName,
			databaseName,
			err,
		)
	}

	return schemaObj.ID, nil
}

func qualifyDeleteWithSchema(sqlText string, schemaName string) string {
	trimmed := strings.TrimSpace(sqlText)
	upper := strings.ToUpper(trimmed)

	// Do not rewrite simple cleanup deletes:
	// DELETE FROM "E2E_TEST"
	// DELETE FROM "E2E_TEST_INGEST"
	if !strings.Contains(upper, " USING ") &&
		!strings.Contains(upper, "\nUSING ") &&
		!strings.Contains(upper, "\tUSING ") {
		return sqlText
	}

	schemaName = strings.TrimSpace(schemaName)
	schemaName = strings.Trim(schemaName, `"`)
	if schemaName == "" {
		return sqlText
	}

	// DELETE FROM "E2E_TEST" T
	deleteFromRe := regexp.MustCompile(`(?i)\bDELETE\s+FROM\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)(\s+[A-Za-z_][A-Za-z0-9_]*)?`)

	sqlText = deleteFromRe.ReplaceAllStringFunc(sqlText, func(match string) string {
		parts := strings.Fields(match)
		if len(parts) < 3 {
			return match
		}

		tableRef := parts[2]

		if strings.Contains(tableRef, ".") {
			return match
		}

		tableName := strings.Trim(tableRef, `"`)
		qualified := buildDuckDBQualifiedTableName(schemaName, tableName)

		if len(parts) > 3 {
			return "DELETE FROM " + qualified + " " + parts[3]
		}

		return "DELETE FROM " + qualified
	})

	// USING "E2E_TEST_INGEST" S
	usingRe := regexp.MustCompile(`(?i)\bUSING\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)(\s+[A-Za-z_][A-Za-z0-9_]*)?`)

	sqlText = usingRe.ReplaceAllStringFunc(sqlText, func(match string) string {
		parts := strings.Fields(match)
		if len(parts) < 2 {
			return match
		}

		tableRef := parts[1]

		if strings.Contains(tableRef, ".") {
			return match
		}

		// Do not touch USING (
		if strings.HasPrefix(tableRef, "(") {
			return match
		}

		tableName := strings.Trim(tableRef, `"`)
		qualified := buildDuckDBQualifiedTableName(schemaName, tableName)

		if len(parts) > 2 {
			return "USING " + qualified + " " + parts[2]
		}

		return "USING " + qualified
	})

	// FROM "E2E_TEST_INGEST" inside subqueries.
	fromRe := regexp.MustCompile(`(?i)\bFROM\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)(\s+[A-Za-z_][A-Za-z0-9_]*)?`)

	sqlText = fromRe.ReplaceAllStringFunc(sqlText, func(match string) string {
		parts := strings.Fields(match)
		if len(parts) < 2 {
			return match
		}

		tableRef := parts[1]

		if strings.Contains(tableRef, ".") {
			return match
		}

		if strings.HasPrefix(tableRef, "(") || strings.HasPrefix(tableRef, "@") {
			return match
		}

		tableName := strings.Trim(tableRef, `"`)
		qualified := buildDuckDBQualifiedTableName(schemaName, tableName)

		if len(parts) > 2 {
			return "FROM " + qualified + " " + parts[2]
		}

		return "FROM " + qualified
	})

	return sqlText
}

func isRemoveStatement(sqlText string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlText)), "REMOVE ")
}

func (e *Executor) executeRemove(
	ctx context.Context,
	execCtx ExecutionContext,
	sqlText string,
) (*ExecResult, error) {
	if e.stageMgr == nil {
		return nil, fmt.Errorf("stage manager not configured")
	}

	stmt, err := parseRemoveStatement(sqlText)
	if err != nil {
		return nil, err
	}

	schemaID, err := e.resolveSchemaID(ctx, execCtx, stmt.Database, stmt.Schema)
	if err != nil {
		return nil, err
	}

	files, err := e.stageMgr.ListFiles(ctx, schemaID, stmt.StageName, stmt.Pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list stage files for REMOVE: %w", err)
	}

	var removed int64

	for _, file := range files {
		if stmt.StagePath != "" && !strings.HasPrefix(file.Name, stmt.StagePath) {
			continue
		}

		if err := e.stageMgr.RemoveFile(ctx, schemaID, stmt.StageName, file.Name); err != nil {
			return nil, fmt.Errorf("failed to remove stage file %s: %w", file.Name, err)
		}

		removed++
	}

	return &ExecResult{RowsAffected: removed}, nil
}

func parseRemoveStatement(sqlText string) (*removeStatement, error) {
	normalized := strings.TrimSpace(sqlText)

	re := regexp.MustCompile(`(?is)^REMOVE\s+@([^\s]+)(?:\s+PATTERN\s*=\s*'([^']+)')?\s*;?$`)
	matches := re.FindStringSubmatch(normalized)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid REMOVE statement: %s", sqlText)
	}

	stageRef := strings.TrimSpace(matches[1])

	stageObject, stagePath := splitStageRef(stageRef)
	database, schema, stageName := splitQualifiedObjectName(stageObject)

	stmt := &removeStatement{
		Database:  database,
		Schema:    schema,
		StageName: stageName,
		StagePath: strings.Trim(stagePath, "/"),
	}

	if len(matches) > 2 {
		stmt.Pattern = strings.TrimSpace(matches[2])
	}

	return stmt, nil
}

func isListStatement(sqlText string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlText)), "LIST ")
}

func (e *Executor) executeList(
	ctx context.Context,
	execCtx ExecutionContext,
	sqlText string,
) (*ExecResult, error) {
	if e.stageMgr == nil {
		return nil, fmt.Errorf("stage manager not configured")
	}

	stmt, err := parseListStatement(sqlText)
	if err != nil {
		return nil, err
	}

	schemaID, err := e.resolveSchemaID(ctx, execCtx, stmt.Database, stmt.Schema)
	if err != nil {
		return nil, err
	}

	files, err := e.stageMgr.ListFiles(ctx, schemaID, stmt.StageName, stmt.Pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list stage files: %w", err)
	}

	var count int64

	for _, file := range files {
		if stmt.StagePath != "" && !strings.HasPrefix(file.Name, stmt.StagePath) {
			continue
		}

		count++
	}

	return &ExecResult{
		RowsAffected: count,
	}, nil
}

func parseListStatement(sqlText string) (*listStatement, error) {
	normalized := strings.TrimSpace(sqlText)

	re := regexp.MustCompile(`(?is)^LIST\s+@([^\s]+)(?:\s+PATTERN\s*=\s*'([^']+)')?\s*;?$`)
	matches := re.FindStringSubmatch(normalized)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid LIST statement: %s", sqlText)
	}

	stageRef := strings.TrimSpace(matches[1])

	stageObject, stagePath := splitStageRef(stageRef)
	database, schema, stageName := splitQualifiedObjectName(stageObject)

	stmt := &listStatement{
		Database:  database,
		Schema:    schema,
		StageName: stageName,
		StagePath: strings.Trim(stagePath, "/"),
	}

	if len(matches) > 2 {
		stmt.Pattern = strings.TrimSpace(matches[2])
	}

	return stmt, nil
}

func (e *Executor) QueryList(
	ctx context.Context,
	execCtx ExecutionContext,
	sqlText string,
) (*Result, error) {
	if e.stageMgr == nil {
		return nil, fmt.Errorf("stage manager not configured")
	}

	stmt, err := parseListStatement(sqlText)
	if err != nil {
		return nil, err
	}

	schemaID, err := e.resolveSchemaID(ctx, execCtx, stmt.Database, stmt.Schema)
	if err != nil {
		return nil, err
	}

	files, err := e.stageMgr.ListFiles(ctx, schemaID, stmt.StageName, stmt.Pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list stage files: %w", err)
	}

	// Reusa o próprio Query() para criar Columns/ColumnTypes no formato correto do projeto.
	result, err := e.Query(ctx, `SELECT '' AS name, 0::BIGINT AS size WHERE FALSE`)
	if err != nil {
		return nil, fmt.Errorf("failed to build LIST result metadata: %w", err)
	}

	rows := make([][]interface{}, 0)

	for _, file := range files {
		if stmt.StagePath != "" && !strings.HasPrefix(file.Name, stmt.StagePath) {
			continue
		}

		rows = append(rows, []interface{}{
			file.Name,
			file.Size,
		})
	}

	result.Rows = rows

	return result, nil
}

func IsListStatement(sqlText string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlText)), "LIST ")
}
