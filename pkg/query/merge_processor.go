// Package query provides SQL query execution including MERGE INTO support.
package query

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// MergeAction represents the action to take in a WHEN clause.
type MergeAction int

const (
	// MergeActionUpdate represents WHEN MATCHED THEN UPDATE.
	MergeActionUpdate MergeAction = iota
	// MergeActionDelete represents WHEN MATCHED THEN DELETE.
	MergeActionDelete
	// MergeActionInsert represents WHEN NOT MATCHED THEN INSERT.
	MergeActionInsert
)

// SetClause represents a single SET column = value assignment.
type SetClause struct {
	Column string
	Value  string
}

// WhenClause represents a WHEN MATCHED or WHEN NOT MATCHED clause.
type WhenClause struct {
	IsMatched  bool        // true for WHEN MATCHED, false for WHEN NOT MATCHED
	Condition  string      // Additional AND condition (optional)
	Action     MergeAction // UPDATE, DELETE, or INSERT
	SetClauses []SetClause // For UPDATE SET
	InsertCols []string    // For INSERT (column list)
	InsertVals []string    // For INSERT (VALUES)
}

// MergeStatement represents a parsed MERGE INTO statement.
type MergeStatement struct {
	TargetTable string       // Target table name (may include db.schema.table)
	TargetAlias string       // Alias for target table
	SourceTable string       // Source table name or subquery
	SourceAlias string       // Alias for source table
	OnCondition string       // JOIN condition
	WhenClauses []WhenClause // List of WHEN clauses
}

// mergePatterns holds pre-compiled regex patterns for MERGE statement parsing.
type mergePatterns struct {
	mergeInto      *regexp.Regexp
	using          *regexp.Regexp
	onCondition    *regexp.Regexp
	whenMatched    *regexp.Regexp
	whenNotMatched *regexp.Regexp
	thenUpdate     *regexp.Regexp
	thenDelete     *regexp.Regexp
	thenInsert     *regexp.Regexp
	setClause      *regexp.Regexp
	insertValues   *regexp.Regexp
}

// newMergePatterns creates pre-compiled regex patterns for MERGE parsing.
// Note: Go regexp doesn't support lookahead, so we use simpler patterns
// and handle boundary detection in the parsing logic.
func newMergePatterns() *mergePatterns {
	return &mergePatterns{
		// MERGE INTO target [AS alias] - alias must not be USING
		mergeInto: regexp.MustCompile(`(?i)MERGE\s+INTO\s+(\S+)(?:\s+AS\s+(\w+)|\s+([a-zA-Z_][a-zA-Z0-9_]*))?(?:\s+USING)`),
		// USING source [AS alias] or USING (subquery) [AS alias] - alias must not be ON
		using: regexp.MustCompile(`(?i)USING\s+(\([^)]+\)|[^\s(]+)(?:\s+AS\s+(\w+)|\s+([a-zA-Z_][a-zA-Z0-9_]*))?(?:\s+ON)`),
		// ON condition - we'll extract until WHEN in the parsing logic
		onCondition: regexp.MustCompile(`(?i)\bON\s+(.+)`),
		// WHEN MATCHED [AND condition] THEN
		whenMatched: regexp.MustCompile(`(?i)WHEN\s+MATCHED(?:\s+AND\s+(.+?))?\s+THEN`),
		// WHEN NOT MATCHED [AND condition] THEN
		whenNotMatched: regexp.MustCompile(`(?i)WHEN\s+NOT\s+MATCHED(?:\s+AND\s+(.+?))?\s+THEN`),
		// THEN UPDATE SET ... - we'll handle boundary in parsing logic
		thenUpdate: regexp.MustCompile(`(?i)THEN\s+UPDATE\s+SET\s+(.+)`),
		// THEN DELETE
		thenDelete: regexp.MustCompile(`(?i)THEN\s+DELETE`),
		// THEN INSERT (cols) VALUES (vals) or THEN INSERT VALUES (vals)
		thenInsert: regexp.MustCompile(`(?i)THEN\s+INSERT\s*(?:\(([^)]*)\))?\s*VALUES\s*\(([^)]+)\)`),
		// SET column = value pattern
		setClause: regexp.MustCompile(`(?i)(\w+(?:\.\w+)?)\s*=\s*([^,]+)`),
		// INSERT (cols) VALUES (vals) capture
		insertValues: regexp.MustCompile(`(?i)\(([^)]+)\)`),
	}
}

// MergeProcessor handles MERGE INTO operations.
type MergeProcessor struct {
	executor   *Executor
	translator *Translator
	patterns   *mergePatterns
}

// NewMergeProcessor creates a new MERGE handler.
func NewMergeProcessor(executor *Executor) *MergeProcessor {
	return &MergeProcessor{
		executor:   executor,
		translator: NewTranslator(),
		patterns:   newMergePatterns(),
	}
}

// ParseMergeStatement parses a MERGE INTO SQL statement.
//
//nolint:gocyclo // parsing logic inherently has many branches
func (h *MergeProcessor) ParseMergeStatement(sql string) (*MergeStatement, error) {
	sql = strings.TrimSpace(sql)

	stmt := &MergeStatement{}

	// Parse MERGE INTO target [AS alias]
	mergeMatch := h.patterns.mergeInto.FindStringSubmatch(sql)
	if len(mergeMatch) < 2 {
		return nil, fmt.Errorf("invalid MERGE INTO syntax: missing target table")
	}
	stmt.TargetTable = mergeMatch[1]
	// Check for alias (either with AS or without)
	if len(mergeMatch) > 2 && mergeMatch[2] != "" {
		stmt.TargetAlias = mergeMatch[2]
	} else if len(mergeMatch) > 3 && mergeMatch[3] != "" {
		stmt.TargetAlias = mergeMatch[3]
	}

	// Parse USING source [AS alias]
	usingMatch := h.patterns.using.FindStringSubmatch(sql)
	if len(usingMatch) < 2 {
		return nil, fmt.Errorf("invalid MERGE syntax: missing USING clause")
	}
	stmt.SourceTable = usingMatch[1]
	// Check for alias (either with AS or without)
	if len(usingMatch) > 2 && usingMatch[2] != "" {
		stmt.SourceAlias = usingMatch[2]
	} else if len(usingMatch) > 3 && usingMatch[3] != "" {
		stmt.SourceAlias = usingMatch[3]
	}

	// Parse ON condition - extract until first WHEN keyword
	onMatch := h.patterns.onCondition.FindStringSubmatch(sql)
	if len(onMatch) < 2 {
		return nil, fmt.Errorf("invalid MERGE syntax: missing ON condition")
	}
	onCondition := onMatch[1]
	// Truncate at WHEN keyword (case-insensitive)
	whenIdx := strings.Index(strings.ToUpper(onCondition), " WHEN")
	if whenIdx == -1 {
		whenIdx = strings.Index(strings.ToUpper(onCondition), "\nWHEN")
	}
	if whenIdx == -1 {
		whenIdx = strings.Index(strings.ToUpper(onCondition), "\tWHEN")
	}
	if whenIdx != -1 {
		onCondition = onCondition[:whenIdx]
	}
	stmt.OnCondition = strings.TrimSpace(onCondition)

	// Parse WHEN clauses
	whenClauses, err := h.parseWhenClauses(sql)
	if err != nil {
		return nil, fmt.Errorf("error parsing WHEN clauses: %w", err)
	}
	if len(whenClauses) == 0 {
		return nil, fmt.Errorf("invalid MERGE syntax: at least one WHEN clause required")
	}
	stmt.WhenClauses = whenClauses

	return stmt, nil
}

// parseWhenClauses extracts all WHEN clauses from the SQL.
func (h *MergeProcessor) parseWhenClauses(sql string) ([]WhenClause, error) {
	var clauses []WhenClause

	// Find all WHEN MATCHED clauses
	// We need to find the positions of all WHEN clauses and parse them in order
	upperSQL := strings.ToUpper(sql)

	// Split by WHEN keyword and process each section
	whenPattern := regexp.MustCompile(`(?i)\bWHEN\s+`)
	whenIndices := whenPattern.FindAllStringIndex(sql, -1)

	for i, idx := range whenIndices {
		start := idx[0]
		var end int
		if i < len(whenIndices)-1 {
			end = whenIndices[i+1][0]
		} else {
			end = len(sql)
		}

		whenSection := sql[start:end]
		upperWhenSection := upperSQL[start:end]

		clause, err := h.parseWhenClause(whenSection, upperWhenSection)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, clause)
	}

	return clauses, nil
}

// parseWhenClause parses a single WHEN clause.
//
//nolint:gocyclo // parsing logic inherently has many branches
func (h *MergeProcessor) parseWhenClause(section, upperSection string) (WhenClause, error) {
	clause := WhenClause{}

	// Determine if MATCHED or NOT MATCHED
	switch {
	case strings.Contains(upperSection, "NOT MATCHED"):
		clause.IsMatched = false
		// Check for additional AND condition
		notMatchedMatch := h.patterns.whenNotMatched.FindStringSubmatch(section)
		if len(notMatchedMatch) > 1 && notMatchedMatch[1] != "" {
			clause.Condition = strings.TrimSpace(notMatchedMatch[1])
		}
	case strings.Contains(upperSection, "MATCHED"):
		clause.IsMatched = true
		// Check for additional AND condition
		matchedMatch := h.patterns.whenMatched.FindStringSubmatch(section)
		if len(matchedMatch) > 1 && matchedMatch[1] != "" {
			clause.Condition = strings.TrimSpace(matchedMatch[1])
		}
	default:
		return clause, fmt.Errorf("invalid WHEN clause: %s", section)
	}

	// Determine action (UPDATE, DELETE, or INSERT)
	switch {
	case strings.Contains(upperSection, "THEN DELETE"):
		clause.Action = MergeActionDelete
	case strings.Contains(upperSection, "THEN UPDATE"):
		clause.Action = MergeActionUpdate
		// Parse SET clauses
		updateMatch := h.patterns.thenUpdate.FindStringSubmatch(section)
		if len(updateMatch) > 1 {
			setStr := updateMatch[1]
			// Truncate at WHEN keyword if present (for multi-clause MERGE)
			whenIdx := strings.Index(strings.ToUpper(setStr), " WHEN")
			if whenIdx != -1 {
				setStr = setStr[:whenIdx]
			}
			setClauses, err := h.parseSetClauses(setStr)
			if err != nil {
				return clause, err
			}
			clause.SetClauses = setClauses
		}
	case strings.Contains(upperSection, "THEN INSERT"):
		clause.Action = MergeActionInsert
		// Parse INSERT columns and values
		insertMatch := h.patterns.thenInsert.FindStringSubmatch(section)
		if len(insertMatch) >= 3 {
			if insertMatch[1] != "" {
				clause.InsertCols = parseCommaSeparated(insertMatch[1])
			}
			clause.InsertVals = parseCommaSeparated(insertMatch[2])
		} else if len(insertMatch) >= 2 {
			// VALUES only (no column list)
			clause.InsertVals = parseCommaSeparated(insertMatch[1])
		}
	default:
		return clause, fmt.Errorf("invalid WHEN clause action: %s", section)
	}

	return clause, nil
}

// parseSetClauses parses UPDATE SET assignments.
func (h *MergeProcessor) parseSetClauses(setStr string) ([]SetClause, error) {
	var clauses []SetClause

	// Split by comma, but be careful of commas inside function calls
	parts := splitByCommaRespectingParens(setStr)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Match column = value
		eqIdx := strings.Index(part, "=")
		if eqIdx == -1 {
			return nil, fmt.Errorf("invalid SET clause: %s", part)
		}

		clauses = append(clauses, SetClause{
			Column: strings.TrimSpace(part[:eqIdx]),
			Value:  strings.TrimSpace(part[eqIdx+1:]),
		})
	}

	return clauses, nil
}

// parseCommaSeparated splits a comma-separated string into parts.
func parseCommaSeparated(s string) []string {
	parts := splitByCommaRespectingParens(s)
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// splitByCommaRespectingParens splits by comma while respecting parentheses nesting.
func splitByCommaRespectingParens(s string) []string {
	var parts []string
	var current strings.Builder
	depth := 0

	for _, r := range s {
		switch r {
		case '(':
			depth++
			current.WriteRune(r)
		case ')':
			depth--
			current.WriteRune(r)
		case ',':
			if depth == 0 {
				parts = append(parts, current.String())
				current.Reset()
			} else {
				current.WriteRune(r)
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// ExecuteMerge executes a parsed MERGE statement.
// Strategy: Try native DuckDB MERGE first. If unsupported, decompose into UPDATE/DELETE/INSERT.
func (h *MergeProcessor) ExecuteMerge(ctx context.Context, stmt *MergeStatement) (*MergeResult, error) {
	result := &MergeResult{}

	// Build the native MERGE SQL
	mergeSQL := h.buildMergeSQL(stmt)

	// Try native execution first (DuckDB 1.4+ supports MERGE)
	execResult, err := h.executor.executeRaw(ctx, mergeSQL)
	if err == nil {
		// Native MERGE succeeded
		// DuckDB returns total rows affected; we can't distinguish insert/update/delete
		result.RowsUpdated = execResult.RowsAffected
		return result, nil
	}

	// If native MERGE fails (older DuckDB version), decompose into separate statements
	return h.executeDecomposedMerge(ctx, stmt)
}

// buildMergeSQL constructs the MERGE SQL statement for native execution.
func (h *MergeProcessor) buildMergeSQL(stmt *MergeStatement) string {
	var sb strings.Builder

	// MERGE INTO target [alias]
	sb.WriteString("MERGE INTO ")
	sb.WriteString(stmt.TargetTable)
	if stmt.TargetAlias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(stmt.TargetAlias)
	}

	// USING source [alias]
	sb.WriteString(" USING ")
	sb.WriteString(stmt.SourceTable)
	if stmt.SourceAlias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(stmt.SourceAlias)
	}

	// ON condition
	sb.WriteString(" ON ")
	sb.WriteString(stmt.OnCondition)

	// WHEN clauses
	for i := range stmt.WhenClauses {
		sb.WriteString(" ")
		sb.WriteString(h.buildWhenClause(&stmt.WhenClauses[i]))
	}

	return sb.String()
}

// buildWhenClause builds a single WHEN clause.
func (h *MergeProcessor) buildWhenClause(when *WhenClause) string {
	var sb strings.Builder

	if when.IsMatched {
		sb.WriteString("WHEN MATCHED")
	} else {
		sb.WriteString("WHEN NOT MATCHED")
	}

	if when.Condition != "" {
		sb.WriteString(" AND ")
		sb.WriteString(when.Condition)
	}

	sb.WriteString(" THEN ")

	switch when.Action {
	case MergeActionDelete:
		sb.WriteString("DELETE")
	case MergeActionUpdate:
		sb.WriteString("UPDATE SET ")
		var sets []string
		for _, sc := range when.SetClauses {
			sets = append(sets, sc.Column+" = "+sc.Value)
		}
		sb.WriteString(strings.Join(sets, ", "))
	case MergeActionInsert:
		sb.WriteString("INSERT")
		if len(when.InsertCols) > 0 {
			sb.WriteString(" (")
			sb.WriteString(strings.Join(when.InsertCols, ", "))
			sb.WriteString(")")
		}
		sb.WriteString(" VALUES (")
		sb.WriteString(strings.Join(when.InsertVals, ", "))
		sb.WriteString(")")
	}

	return sb.String()
}

// executeDecomposedMerge executes MERGE as separate UPDATE/DELETE/INSERT statements.
// This fallback is used when native MERGE is not supported.
func (h *MergeProcessor) executeDecomposedMerge(ctx context.Context, stmt *MergeStatement) (*MergeResult, error) {
	result := &MergeResult{}

	// Process WHEN MATCHED clauses first (UPDATE/DELETE)
	for i := range stmt.WhenClauses {
		when := &stmt.WhenClauses[i]
		if !when.IsMatched {
			continue
		}

		switch when.Action {
		case MergeActionUpdate:
			rows, err := h.executeMatchedUpdate(ctx, stmt, when)
			if err != nil {
				return result, fmt.Errorf("MERGE UPDATE failed: %w", err)
			}
			result.RowsUpdated += rows

		case MergeActionDelete:
			rows, err := h.executeMatchedDelete(ctx, stmt, when)
			if err != nil {
				return result, fmt.Errorf("MERGE DELETE failed: %w", err)
			}
			result.RowsDeleted += rows
		}
	}

	// Process WHEN NOT MATCHED clauses (INSERT)
	for i := range stmt.WhenClauses {
		when := &stmt.WhenClauses[i]
		if when.IsMatched {
			continue
		}

		if when.Action == MergeActionInsert {
			rows, err := h.executeNotMatchedInsert(ctx, stmt, when)
			if err != nil {
				return result, fmt.Errorf("MERGE INSERT failed: %w", err)
			}
			result.RowsInserted += rows
		}
	}

	return result, nil
}

// executeMatchedUpdate executes UPDATE for WHEN MATCHED THEN UPDATE.
func (h *MergeProcessor) executeMatchedUpdate(ctx context.Context, stmt *MergeStatement, when *WhenClause) (int64, error) {
	// Build: UPDATE target SET ... FROM source WHERE join_condition [AND when_condition]
	// DuckDB requires the table name (not alias) in UPDATE clause
	var sb strings.Builder

	sb.WriteString("UPDATE ")
	sb.WriteString(stmt.TargetTable)
	sb.WriteString(" SET ")

	// Replace target alias with table name in SET clauses
	var sets []string
	for _, sc := range when.SetClauses {
		col := sc.Column
		val := sc.Value
		// If column has alias prefix matching target alias, replace with table name
		if stmt.TargetAlias != "" {
			col = strings.Replace(col, stmt.TargetAlias+".", stmt.TargetTable+".", 1)
		}
		sets = append(sets, col+" = "+val)
	}
	sb.WriteString(strings.Join(sets, ", "))

	// FROM clause for the source
	sb.WriteString(" FROM ")
	sb.WriteString(stmt.SourceTable)
	if stmt.SourceAlias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(stmt.SourceAlias)
	}

	// WHERE clause with join condition
	// Replace target alias with table name in condition
	onCondition := stmt.OnCondition
	if stmt.TargetAlias != "" {
		onCondition = strings.ReplaceAll(onCondition, stmt.TargetAlias+".", stmt.TargetTable+".")
	}
	sb.WriteString(" WHERE ")
	sb.WriteString(onCondition)

	// Additional AND condition
	if when.Condition != "" {
		condition := when.Condition
		if stmt.TargetAlias != "" {
			condition = strings.ReplaceAll(condition, stmt.TargetAlias+".", stmt.TargetTable+".")
		}
		sb.WriteString(" AND ")
		sb.WriteString(condition)
	}

	execResult, err := h.executor.executeRaw(ctx, sb.String())
	if err != nil {
		return 0, err
	}

	return execResult.RowsAffected, nil
}

// executeMatchedDelete executes DELETE for WHEN MATCHED THEN DELETE.
func (h *MergeProcessor) executeMatchedDelete(ctx context.Context, stmt *MergeStatement, when *WhenClause) (int64, error) {
	// Build: DELETE FROM target USING source WHERE join_condition [AND when_condition]
	var sb strings.Builder

	sb.WriteString("DELETE FROM ")
	sb.WriteString(stmt.TargetTable)

	// USING clause for the source (DuckDB syntax)
	sb.WriteString(" USING ")
	sb.WriteString(stmt.SourceTable)
	if stmt.SourceAlias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(stmt.SourceAlias)
	}

	// WHERE clause with join condition
	sb.WriteString(" WHERE ")
	sb.WriteString(stmt.OnCondition)

	// Additional AND condition
	if when.Condition != "" {
		sb.WriteString(" AND ")
		sb.WriteString(when.Condition)
	}

	execResult, err := h.executor.executeRaw(ctx, sb.String())
	if err != nil {
		return 0, err
	}

	return execResult.RowsAffected, nil
}

// executeNotMatchedInsert executes INSERT for WHEN NOT MATCHED THEN INSERT.
func (h *MergeProcessor) executeNotMatchedInsert(ctx context.Context, stmt *MergeStatement, when *WhenClause) (int64, error) {
	// Build: INSERT INTO target (cols) SELECT vals FROM source WHERE NOT EXISTS (...)
	var sb strings.Builder

	sb.WriteString("INSERT INTO ")
	sb.WriteString(stmt.TargetTable)

	if len(when.InsertCols) > 0 {
		sb.WriteString(" (")
		sb.WriteString(strings.Join(when.InsertCols, ", "))
		sb.WriteString(")")
	}

	// SELECT from source where no match exists
	sb.WriteString(" SELECT ")
	sb.WriteString(strings.Join(when.InsertVals, ", "))
	sb.WriteString(" FROM ")
	sb.WriteString(stmt.SourceTable)
	if stmt.SourceAlias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(stmt.SourceAlias)
	}

	// WHERE NOT EXISTS to find non-matching rows
	sb.WriteString(" WHERE NOT EXISTS (SELECT 1 FROM ")
	sb.WriteString(stmt.TargetTable)
	if stmt.TargetAlias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(stmt.TargetAlias)
	}
	sb.WriteString(" WHERE ")
	sb.WriteString(stmt.OnCondition)
	sb.WriteString(")")

	// Additional AND condition for the source
	if when.Condition != "" {
		sb.WriteString(" AND ")
		sb.WriteString(when.Condition)
	}

	execResult, err := h.executor.executeRaw(ctx, sb.String())
	if err != nil {
		return 0, err
	}

	return execResult.RowsAffected, nil
}
