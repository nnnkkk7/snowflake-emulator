// Package query provides SQL query execution including COPY INTO support.
package query

import (
	"bytes"
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
	TargetTable       string
	TargetDatabase    string
	TargetSchema      string
	TargetColumns     []string
	SelectExpressions []string
	StageName         string
	StageSchemaID     string
	StagePath         string
	FileFormat        FileFormatOptions
	Files             []string // Specific files to load
	Pattern           string   // File pattern
	OnError           string   // CONTINUE, SKIP_FILE, ABORT
	PurgeFiles        bool     // Whether to purge files after loading
	ValidationMode    bool     // Whether to validate only
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

// copyPatterns holds pre-compiled regex patterns for COPY statement parsing.
// Stored in CopyProcessor to avoid global state and enable heap allocation.
type copyPatterns struct {
	copyInto        *regexp.Regexp
	copyIntoSelect  *regexp.Regexp
	fileFormat      *regexp.Regexp
	pattern         *regexp.Regexp
	onError         *regexp.Regexp
	formatType      *regexp.Regexp
	fieldDelimiter  *regexp.Regexp
	recordDelimiter *regexp.Regexp
	skipHeader      *regexp.Regexp
}

// newCopyPatterns creates pre-compiled regex patterns.
func newCopyPatterns() *copyPatterns {
	return &copyPatterns{
		copyInto: regexp.MustCompile(`(?i)COPY\s+INTO\s+([^\s(]+)\s+FROM\s+@([^\s/]+)(/\S*)?`),

		copyIntoSelect: regexp.MustCompile(`(?is)^COPY\s+INTO\s+([^\s(]+)\s*(?:\((.*?)\))?\s+FROM\s*\(\s*SELECT\s+(.*?)\s+FROM\s+(@[^\s)]+)\s*\)\s*FILE_FORMAT\s*=\s*\(([^)]*)\)\s*$`),

		fileFormat:      regexp.MustCompile(`(?i)FILE_FORMAT\s*=\s*\(([^)]+)\)`),
		pattern:         regexp.MustCompile(`(?i)PATTERN\s*=\s*'([^']+)'`),
		onError:         regexp.MustCompile(`(?i)ON_ERROR\s*=\s*(\w+)`),
		formatType:      regexp.MustCompile(`(?i)TYPE\s*=\s*(\w+)`),
		fieldDelimiter:  regexp.MustCompile(`(?i)FIELD_DELIMITER\s*=\s*'([^']*)'`),
		recordDelimiter: regexp.MustCompile(`(?i)RECORD_DELIMITER\s*=\s*'([^']*)'`),
		skipHeader:      regexp.MustCompile(`(?i)SKIP_HEADER\s*=\s*(\d+)`),
	}
}

// extractMatch extracts the first capture group from a regex match.
// Returns defaultVal if no match is found.
func extractMatch(re *regexp.Regexp, input, defaultVal string) string {
	if match := re.FindStringSubmatch(input); len(match) > 1 {
		return match[1]
	}
	return defaultVal
}

// extractMatchUpper extracts the first capture group and converts to uppercase.
func extractMatchUpper(re *regexp.Regexp, input, defaultVal string) string {
	return strings.ToUpper(extractMatch(re, input, defaultVal))
}

// CopyProcessor handles COPY INTO operations.
type CopyProcessor struct {
	stageMgr   *stage.Manager
	repo       *metadata.Repository
	executor   *Executor
	tableNamer *DefaultTableNamer
	patterns   *copyPatterns
}

// NewCopyProcessor creates a new COPY handler.
func NewCopyProcessor(stageMgr *stage.Manager, repo *metadata.Repository, executor *Executor) *CopyProcessor {
	return &CopyProcessor{
		stageMgr:   stageMgr,
		repo:       repo,
		executor:   executor,
		tableNamer: NewTableNamer(),
		patterns:   newCopyPatterns(),
	}
}

// ParseCopyStatement parses a COPY INTO SQL statement.
func (h *CopyProcessor) ParseCopyStatement(sql string) (*CopyStatement, error) {
	sql = strings.TrimSpace(sql)

	if stmt, err := h.parseCopyIntoSelect(sql); err == nil {
		return stmt, nil
	}

	// Match COPY INTO table FROM @stage[/path]
	matches := h.patterns.copyInto.FindStringSubmatch(sql)
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
	if ffMatch := h.patterns.fileFormat.FindStringSubmatch(sql); len(ffMatch) > 1 {
		h.parseFileFormatOptions(&stmt.FileFormat, ffMatch[1])
	}

	// Parse PATTERN
	if p := extractMatch(h.patterns.pattern, sql, ""); p != "" {
		stmt.Pattern = p
	}

	// Parse ON_ERROR
	stmt.OnError = extractMatchUpper(h.patterns.onError, sql, "ABORT")

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
func (h *CopyProcessor) parseFileFormatOptions(opts *FileFormatOptions, optStr string) {
	optStr = strings.TrimSpace(optStr)

	// TYPE
	if t := extractMatchUpper(h.patterns.formatType, optStr, ""); t != "" {
		opts.Type = t
	}

	// FIELD_DELIMITER
	if fd := extractMatch(h.patterns.fieldDelimiter, optStr, ""); fd != "" {
		opts.FieldDelimiter = fd
	}

	// RECORD_DELIMITER
	if rd := extractMatch(h.patterns.recordDelimiter, optStr, ""); rd != "" {
		opts.RecordDelimiter = rd
	}

	// SKIP_HEADER
	if sh := extractMatch(h.patterns.skipHeader, optStr, ""); sh != "" {
		if val, err := strconv.Atoi(sh); err == nil {
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
//
//nolint:gocyclo // complex file loading logic with multiple format handlers
func (h *CopyProcessor) ExecuteCopyInto(ctx context.Context, stmt *CopyStatement, defaultSchemaID string) (*CopyResult, error) {
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
//
//nolint:gocyclo // CSV parsing logic with multiple format options
func (h *CopyProcessor) loadCSVFile(ctx context.Context, stmt *CopyStatement, schemaID, fileName string) (int64, error) {
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

	// Build INSERT statement using centralized table naming
	tableName := buildDuckDBQualifiedTableName(stmt.TargetSchema, stmt.TargetTable)

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

		_, err := h.executor.executeRaw(ctx, insertSQL)
		if err != nil {
			return rowsInserted, fmt.Errorf("failed to insert row: %w", err)
		}
		rowsInserted++
	}

	return rowsInserted, nil
}

// loadJSONFile loads a JSON file into the target table.
func (h *CopyProcessor) loadJSONFile(ctx context.Context, stmt *CopyStatement, schemaID, fileName string) (int64, error) {
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

	records, err := parseJSONRecords(content, stmt.FileFormat.StripOuterArray)
	if err != nil {
		return 0, err
	}

	if len(records) == 0 {
		return 0, nil
	}

	// Build table name using centralized table naming
	tableName := buildDuckDBQualifiedTableName(stmt.TargetSchema, stmt.TargetTable)

	var rowsInserted int64
	for _, record := range records {
		values := make([]string, 0)

		if len(stmt.TargetColumns) > 0 && len(stmt.SelectExpressions) > 0 {
			for _, expr := range stmt.SelectExpressions {
				valueSQL, err := jsonSelectExpressionToSQLValue(expr, record)
				if err != nil {
					return rowsInserted, err
				}

				values = append(values, valueSQL)
			}
		} else {
			jsonBytes, err := json.Marshal(record)
			if err != nil {
				return rowsInserted, fmt.Errorf("failed to serialize record: %w", err)
			}

			values = append(values, "'"+strings.ReplaceAll(string(jsonBytes), "'", "''")+"'")
		}

		insertSQL := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			tableName,
			strings.Join(quoteColumnNames(stmt.TargetColumns), ", "),
			strings.Join(values, ", "),
		)

		_, err = h.executor.executeRaw(ctx, insertSQL)
		if err != nil {
			return rowsInserted, fmt.Errorf("failed to insert JSON row: %w", err)
		}

		rowsInserted++
	}

	return rowsInserted, nil
}

func (h *CopyProcessor) parseCopyIntoSelect(sql string) (*CopyStatement, error) {
	matches := h.patterns.copyIntoSelect.FindStringSubmatch(sql)
	if len(matches) < 6 {
		return nil, fmt.Errorf("invalid COPY INTO SELECT syntax")
	}

	targetTableRef := strings.TrimSpace(matches[1])
	targetColumnsText := strings.TrimSpace(matches[2])
	selectExpressionsText := strings.TrimSpace(matches[3])
	stageLocation := strings.TrimSpace(matches[4])
	fileFormatText := strings.TrimSpace(matches[5])

	stmt := &CopyStatement{
		FileFormat: FileFormatOptions{
			Type:            "CSV",
			FieldDelimiter:  ",",
			RecordDelimiter: "\n",
			SkipHeader:      0,
		},
		OnError: "ABORT",
	}

	// Target table
	targetDatabase, targetSchema, targetTable := splitQualifiedObjectName(targetTableRef)
	stmt.TargetDatabase = targetDatabase
	stmt.TargetSchema = targetSchema
	stmt.TargetTable = targetTable

	// Target columns
	if targetColumnsText != "" {
		stmt.TargetColumns = cleanIdentList(splitCommaList(targetColumnsText))
	}

	// SELECT expressions
	if selectExpressionsText != "" {
		stmt.SelectExpressions = splitCommaList(selectExpressionsText)
	}

	// Stage location
	stageObject, stagePath := splitStageRef(stageLocation)
	_, _, stageName := splitQualifiedObjectName(stageObject)

	stmt.StageName = stageName
	stmt.StagePath = stagePath

	h.parseFileFormatOptions(&stmt.FileFormat, fileFormatText)

	return stmt, nil
}

func splitCommaList(input string) []string {
	var result []string
	var current strings.Builder

	depth := 0
	inSingleQuote := false
	inDoubleQuote := false

	for _, r := range input {
		switch r {
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
			current.WriteRune(r)

		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
			current.WriteRune(r)

		case '(':
			if !inSingleQuote && !inDoubleQuote {
				depth++
			}
			current.WriteRune(r)

		case ')':
			if !inSingleQuote && !inDoubleQuote && depth > 0 {
				depth--
			}
			current.WriteRune(r)

		case ',':
			if !inSingleQuote && !inDoubleQuote && depth == 0 {
				value := strings.TrimSpace(current.String())
				if value != "" {
					result = append(result, value)
				}
				current.Reset()
			} else {
				current.WriteRune(r)
			}

		default:
			current.WriteRune(r)
		}
	}

	value := strings.TrimSpace(current.String())
	if value != "" {
		result = append(result, value)
	}

	return result
}

func cleanIdentList(values []string) []string {
	result := make([]string, 0, len(values))

	for _, value := range values {
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"`)
		value = strings.ToUpper(value)

		if value != "" {
			result = append(result, value)
		}
	}

	return result
}

func jsonSelectExpressionToSQLValue(expr string, record map[string]interface{}) (string, error) {
	expr = strings.TrimSpace(expr)

	if strings.EqualFold(expr, "CURRENT_TIMESTAMP()") {
		return "CURRENT_TIMESTAMP", nil
	}

	re := regexp.MustCompile(`(?i)^\$1\s*:\s*"([^"]+)"$`)
	matches := re.FindStringSubmatch(expr)
	if len(matches) < 2 {
		return "", fmt.Errorf("unsupported JSON select expression: %s", expr)
	}

	fieldName := matches[1]

	value, exists := record[fieldName]
	if !exists {
		return ValueNull, nil
	}

	return jsonValueToSQLLiteral(value), nil
}

func jsonValueToSQLLiteral(value interface{}) string {
	if value == nil {
		return ValueNull
	}

	switch v := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"

	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)

	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"

	default:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return ValueNull
		}

		return "'" + strings.ReplaceAll(string(jsonBytes), "'", "''") + "'"
	}
}

func parseJSONRecords(content []byte, stripOuterArray bool) ([]map[string]interface{}, error) {
	content = bytes.TrimSpace(content)
	if len(content) == 0 {
		return nil, nil
	}

	var records []map[string]interface{}

	// First try normal JSON: object or array.
	var data interface{}
	if err := json.Unmarshal(content, &data); err == nil {
		switch v := data.(type) {
		case []interface{}:
			if stripOuterArray {
				for _, item := range v {
					if obj, ok := item.(map[string]interface{}); ok {
						records = append(records, obj)
					}
				}
			} else {
				for _, item := range v {
					records = append(records, map[string]interface{}{"$1": item})
				}
			}

		case map[string]interface{}:
			records = append(records, v)

		default:
			return nil, fmt.Errorf("unsupported JSON structure")
		}

		return records, nil
	}

	// Fallback: NDJSON / JSON Lines.
	lines := bytes.Split(content, []byte("\n"))

	for lineNumber, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		var record map[string]interface{}
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("failed to parse JSON line %d: %w", lineNumber+1, err)
		}

		records = append(records, record)
	}

	return records, nil
}

func buildDuckDBQualifiedTableName(schemaName string, tableName string) string {
	schemaName = normalizeSnowflakeIdent(schemaName)
	tableName = normalizeSnowflakeIdent(tableName)

	if schemaName == "" {
		return quoteDuckDBIdent(tableName)
	}

	return quoteDuckDBIdent(schemaName) + "." + quoteDuckDBIdent(tableName)
}

func quoteColumnNames(columns []string) []string {
	quoted := make([]string, 0, len(columns))

	for _, col := range columns {
		col = strings.TrimSpace(col)
		col = strings.Trim(col, `"`)
		if col == "" {
			continue
		}

		quoted = append(quoted, quoteDuckDBIdent(strings.ToUpper(col)))
	}

	return quoted
}
