// Package query provides SQL query execution including COPY INTO support.
package query

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/stage"
)

// CopyStatement represents a parsed COPY INTO statement.
type CopyStatement struct {
	TargetTable    string
	TargetDatabase string
	TargetSchema   string
	StageName      string
	StageSchemaID  string
	StagePath      string
	FileFormat     FileFormatOptions
	Files          []string // Specific files to load
	Pattern        string   // File pattern
	OnError        string   // CONTINUE, SKIP_FILE, ABORT
	PurgeFiles     bool     // Whether to purge files after loading
	ValidationMode bool     // Whether to validate only
}

// FileFormatOptions contains file format settings for COPY.
type FileFormatOptions struct {
	Type            string // CSV, JSON, PARQUET
	FieldDelimiter  string
	RecordDelimiter string
	SkipHeader      int
	TrimSpace       bool
	NullIf          []string
	DateFormat      string
	TimestampFormat string
	StripOuterArray bool // For JSON
	StripNullValues bool // For JSON
}

// CopyResult contains the result of a COPY INTO operation.
type CopyResult struct {
	RowsLoaded   int64
	RowsInserted int64
	FilesLoaded  int
	Errors       []string
}

// CopyHandler handles COPY INTO operations.
type CopyHandler struct {
	stageMgr *stage.Manager
	repo     *metadata.Repository
	executor *Executor
}

// NewCopyHandler creates a new COPY handler.
func NewCopyHandler(stageMgr *stage.Manager, repo *metadata.Repository, executor *Executor) *CopyHandler {
	return &CopyHandler{
		stageMgr: stageMgr,
		repo:     repo,
		executor: executor,
	}
}

// ParseCopyStatement parses a COPY INTO SQL statement.
func (h *CopyHandler) ParseCopyStatement(sql string) (*CopyStatement, error) {
	sql = strings.TrimSpace(sql)

	// Basic regex for COPY INTO table FROM @stage
	// COPY INTO table FROM @stage[/path] [FILE_FORMAT = (...)] [FILES = (...)] [PATTERN = '...']
	copyRegex := regexp.MustCompile(`(?i)COPY\s+INTO\s+([^\s(]+)\s+FROM\s+@([^\s/]+)(/[^\s]*)?`)
	matches := copyRegex.FindStringSubmatch(sql)

	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid COPY INTO syntax: %s", sql)
	}

	stmt := &CopyStatement{
		StageName: strings.ToUpper(matches[2]),
		FileFormat: FileFormatOptions{
			Type:            "CSV",
			FieldDelimiter:  ",",
			RecordDelimiter: "\n",
			SkipHeader:      0,
		},
		OnError: "ABORT",
	}

	// Parse table name (may include database.schema.table)
	tableParts := strings.Split(matches[1], ".")
	switch len(tableParts) {
	case 1:
		stmt.TargetTable = strings.ToUpper(tableParts[0])
	case 2:
		stmt.TargetSchema = strings.ToUpper(tableParts[0])
		stmt.TargetTable = strings.ToUpper(tableParts[1])
	case 3:
		stmt.TargetDatabase = strings.ToUpper(tableParts[0])
		stmt.TargetSchema = strings.ToUpper(tableParts[1])
		stmt.TargetTable = strings.ToUpper(tableParts[2])
	default:
		return nil, fmt.Errorf("invalid table name: %s", matches[1])
	}

	// Parse stage path
	if len(matches) > 3 && matches[3] != "" {
		stmt.StagePath = matches[3][1:] // Remove leading /
	}

	// Parse FILE_FORMAT
	ffRegex := regexp.MustCompile(`(?i)FILE_FORMAT\s*=\s*\(([^)]+)\)`)
	if ffMatch := ffRegex.FindStringSubmatch(sql); len(ffMatch) > 1 {
		h.parseFileFormatOptions(&stmt.FileFormat, ffMatch[1])
	}

	// Parse PATTERN
	patternRegex := regexp.MustCompile(`(?i)PATTERN\s*=\s*'([^']+)'`)
	if patternMatch := patternRegex.FindStringSubmatch(sql); len(patternMatch) > 1 {
		stmt.Pattern = patternMatch[1]
	}

	// Parse ON_ERROR
	onErrorRegex := regexp.MustCompile(`(?i)ON_ERROR\s*=\s*(\w+)`)
	if onErrorMatch := onErrorRegex.FindStringSubmatch(sql); len(onErrorMatch) > 1 {
		stmt.OnError = strings.ToUpper(onErrorMatch[1])
	}

	// Parse PURGE
	if strings.Contains(strings.ToUpper(sql), "PURGE = TRUE") {
		stmt.PurgeFiles = true
	}

	// Parse VALIDATION_MODE
	if strings.Contains(strings.ToUpper(sql), "VALIDATION_MODE") {
		stmt.ValidationMode = true
	}

	return stmt, nil
}

// parseFileFormatOptions parses FILE_FORMAT options string.
func (h *CopyHandler) parseFileFormatOptions(opts *FileFormatOptions, optStr string) {
	optStr = strings.TrimSpace(optStr)

	// TYPE
	typeRegex := regexp.MustCompile(`(?i)TYPE\s*=\s*(\w+)`)
	if match := typeRegex.FindStringSubmatch(optStr); len(match) > 1 {
		opts.Type = strings.ToUpper(match[1])
	}

	// FIELD_DELIMITER
	fdRegex := regexp.MustCompile(`(?i)FIELD_DELIMITER\s*=\s*'([^']*)'`)
	if match := fdRegex.FindStringSubmatch(optStr); len(match) > 1 {
		opts.FieldDelimiter = match[1]
	}

	// RECORD_DELIMITER
	rdRegex := regexp.MustCompile(`(?i)RECORD_DELIMITER\s*=\s*'([^']*)'`)
	if match := rdRegex.FindStringSubmatch(optStr); len(match) > 1 {
		opts.RecordDelimiter = match[1]
	}

	// SKIP_HEADER
	shRegex := regexp.MustCompile(`(?i)SKIP_HEADER\s*=\s*(\d+)`)
	if match := shRegex.FindStringSubmatch(optStr); len(match) > 1 {
		if val, err := strconv.Atoi(match[1]); err == nil {
			opts.SkipHeader = val
		}
	}

	// TRIM_SPACE
	if strings.Contains(strings.ToUpper(optStr), "TRIM_SPACE = TRUE") {
		opts.TrimSpace = true
	}

	// STRIP_OUTER_ARRAY (JSON)
	if strings.Contains(strings.ToUpper(optStr), "STRIP_OUTER_ARRAY = TRUE") {
		opts.StripOuterArray = true
	}
}

// ExecuteCopyInto executes a COPY INTO statement.
func (h *CopyHandler) ExecuteCopyInto(ctx context.Context, stmt *CopyStatement, defaultSchemaID string) (*CopyResult, error) {
	result := &CopyResult{}

	// Resolve schema ID for the stage
	schemaID := stmt.StageSchemaID
	if schemaID == "" {
		schemaID = defaultSchemaID
	}
	if schemaID == "" {
		return nil, fmt.Errorf("schema context required for COPY INTO")
	}

	// Get stage
	stageObj, err := h.stageMgr.GetStage(ctx, schemaID, stmt.StageName)
	if err != nil {
		return nil, fmt.Errorf("stage %s not found: %w", stmt.StageName, err)
	}

	// List files in stage
	files, err := h.stageMgr.ListFiles(ctx, schemaID, stageObj.Name, stmt.Pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list stage files: %w", err)
	}

	// Filter by path if specified
	if stmt.StagePath != "" {
		var filtered []stage.StageFile
		for _, f := range files {
			if strings.HasPrefix(f.Name, stmt.StagePath) {
				filtered = append(filtered, f)
			}
		}
		files = filtered
	}

	if len(files) == 0 {
		return result, nil // No files to load
	}

	// Load each file
	for _, file := range files {
		var rowsLoaded int64
		var loadErr error

		switch strings.ToUpper(stmt.FileFormat.Type) {
		case "CSV":
			rowsLoaded, loadErr = h.loadCSVFile(ctx, stmt, schemaID, file.Name)
		case "JSON":
			rowsLoaded, loadErr = h.loadJSONFile(ctx, stmt, schemaID, file.Name)
		default:
			loadErr = fmt.Errorf("unsupported file format: %s", stmt.FileFormat.Type)
		}

		if loadErr != nil {
			switch stmt.OnError {
			case "CONTINUE":
				result.Errors = append(result.Errors, fmt.Sprintf("File %s: %v", file.Name, loadErr))
				continue
			case "SKIP_FILE":
				result.Errors = append(result.Errors, fmt.Sprintf("Skipped file %s: %v", file.Name, loadErr))
				continue
			default: // ABORT
				return result, fmt.Errorf("error loading file %s: %w", file.Name, loadErr)
			}
		}

		result.RowsLoaded += rowsLoaded
		result.RowsInserted += rowsLoaded
		result.FilesLoaded++

		// Purge file if requested
		if stmt.PurgeFiles && !stmt.ValidationMode {
			if err := h.stageMgr.RemoveFile(ctx, schemaID, stmt.StageName, file.Name); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("Failed to purge %s: %v", file.Name, err))
			}
		}
	}

	return result, nil
}

// loadCSVFile loads a CSV file into the target table.
func (h *CopyHandler) loadCSVFile(ctx context.Context, stmt *CopyStatement, schemaID, fileName string) (int64, error) {
	// Get file reader
	reader, err := h.stageMgr.GetFile(ctx, schemaID, stmt.StageName, fileName)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	csvReader := csv.NewReader(reader)
	if stmt.FileFormat.FieldDelimiter != "" {
		csvReader.Comma = rune(stmt.FileFormat.FieldDelimiter[0])
	}
	csvReader.FieldsPerRecord = -1 // Variable field count

	// Skip header rows
	for i := 0; i < stmt.FileFormat.SkipHeader; i++ {
		_, err := csvReader.Read()
		if err == io.EOF {
			return 0, nil
		}
		if err != nil {
			return 0, fmt.Errorf("failed to skip header: %w", err)
		}
	}

	// Read all records
	records, err := csvReader.ReadAll()
	if err != nil {
		return 0, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return 0, nil
	}

	// Build INSERT statement
	// Use the same naming convention as repository.CreateTable: DB.SCHEMA_TABLE
	tableName := stmt.TargetTable
	if stmt.TargetSchema != "" && stmt.TargetDatabase != "" {
		// Fully qualified: DB.SCHEMA_TABLE
		tableName = stmt.TargetDatabase + "." + stmt.TargetSchema + "_" + tableName
	} else if stmt.TargetSchema != "" {
		// Schema qualified: SCHEMA_TABLE
		tableName = stmt.TargetSchema + "_" + tableName
	}

	var rowsInserted int64
	for _, record := range records {
		// Build VALUES clause
		values := make([]string, len(record))
		for i, val := range record {
			if stmt.FileFormat.TrimSpace {
				val = strings.TrimSpace(val)
			}

			// Check for NULL values
			isNull := false
			for _, nullVal := range stmt.FileFormat.NullIf {
				if val == nullVal {
					isNull = true
					break
				}
			}

			if isNull || val == "" {
				values[i] = ValueNull
			} else {
				// Escape single quotes
				values[i] = "'" + strings.ReplaceAll(val, "'", "''") + "'"
			}
		}

		insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, strings.Join(values, ", "))

		_, err := h.executor.Execute(ctx, insertSQL)
		if err != nil {
			return rowsInserted, fmt.Errorf("failed to insert row: %w", err)
		}
		rowsInserted++
	}

	return rowsInserted, nil
}

// loadJSONFile loads a JSON file into the target table.
func (h *CopyHandler) loadJSONFile(ctx context.Context, stmt *CopyStatement, schemaID, fileName string) (int64, error) {
	// Get file reader
	reader, err := h.stageMgr.GetFile(ctx, schemaID, stmt.StageName, fileName)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		return 0, fmt.Errorf("failed to read JSON file: %w", err)
	}

	// Parse JSON
	var data interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		return 0, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Handle array of objects
	var records []map[string]interface{}
	switch v := data.(type) {
	case []interface{}:
		if stmt.FileFormat.StripOuterArray {
			for _, item := range v {
				if obj, ok := item.(map[string]interface{}); ok {
					records = append(records, obj)
				}
			}
		} else {
			// Each array element as a single VARIANT column
			for _, item := range v {
				records = append(records, map[string]interface{}{"$1": item})
			}
		}
	case map[string]interface{}:
		records = append(records, v)
	default:
		return 0, fmt.Errorf("unsupported JSON structure")
	}

	if len(records) == 0 {
		return 0, nil
	}

	// Build table name
	// Use the same naming convention as repository.CreateTable: DB.SCHEMA_TABLE
	tableName := stmt.TargetTable
	if stmt.TargetSchema != "" && stmt.TargetDatabase != "" {
		// Fully qualified: DB.SCHEMA_TABLE
		tableName = stmt.TargetDatabase + "." + stmt.TargetSchema + "_" + tableName
	} else if stmt.TargetSchema != "" {
		// Schema qualified: SCHEMA_TABLE
		tableName = stmt.TargetSchema + "_" + tableName
	}

	var rowsInserted int64
	for _, record := range records {
		// Convert record to JSON string for VARIANT column
		jsonBytes, err := json.Marshal(record)
		if err != nil {
			return rowsInserted, fmt.Errorf("failed to serialize record: %w", err)
		}

		// Insert as JSON/VARIANT
		insertSQL := fmt.Sprintf("INSERT INTO %s VALUES ('%s')", tableName, strings.ReplaceAll(string(jsonBytes), "'", "''"))

		_, err = h.executor.Execute(ctx, insertSQL)
		if err != nil {
			return rowsInserted, fmt.Errorf("failed to insert JSON row: %w", err)
		}
		rowsInserted++
	}

	return rowsInserted, nil
}
