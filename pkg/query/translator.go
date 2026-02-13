package query

import (
	"fmt"
	"sort"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

// typeMappingEntry represents a Snowflake to DuckDB type name mapping.
type typeMappingEntry struct {
	from string // Snowflake type name (uppercase)
	to   string // DuckDB type name
}

// Translator converts Snowflake SQL to DuckDB-compatible SQL using AST manipulation.
type Translator struct {
	functionMap  map[string]FunctionTranslator
	typeMappings []typeMappingEntry
}

// FunctionTranslator defines how to translate a specific function.
type FunctionTranslator struct {
	Handler func(fn *sqlparser.FuncExpr) sqlparser.Expr // Custom handler for complex transformations
	Name    string                                      // DuckDB function name (for simple renames)
}

// NewTranslator creates a new SQL translator with registered function mappings.
func NewTranslator() *Translator {
	t := &Translator{
		functionMap: make(map[string]FunctionTranslator),
	}
	t.registerFunctions()
	t.registerTypeMappings()
	return t
}

// registerFunctions registers all Snowflake to DuckDB function translations.
func (t *Translator) registerFunctions() {
	// Simple function renames
	t.functionMap["IFF"] = FunctionTranslator{Name: "IF"}
	t.functionMap["NVL"] = FunctionTranslator{Name: "COALESCE"}
	t.functionMap["IFNULL"] = FunctionTranslator{Name: "COALESCE"}
	t.functionMap["LISTAGG"] = FunctionTranslator{Name: "STRING_AGG"}
	t.functionMap["OBJECT_CONSTRUCT"] = FunctionTranslator{Name: "json_object"}
	t.functionMap["FLATTEN"] = FunctionTranslator{Name: "UNNEST"}

	// NVL2: Transform in-place by modifying the FuncExpr
	// NVL2(a, b, c) → IF(a IS NOT NULL, b, c)
	t.functionMap["NVL2"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			if len(fn.Exprs) != 3 {
				return fn
			}
			// Modify the function name
			fn.Name = sqlparser.NewColIdent("IF")
			// Wrap the first argument with IS NOT NULL
			if aliased, ok := fn.Exprs[0].(*sqlparser.AliasedExpr); ok {
				aliased.Expr = &sqlparser.IsExpr{
					Operator: "is not null",
					Expr:     aliased.Expr,
				}
			}
			return fn
		},
	}

	// TO_VARIANT: Marks for post-processing (can't replace node type with Walk)
	t.functionMap["TO_VARIANT"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			// Mark for post-processing by setting a unique marker name
			fn.Name = sqlparser.NewColIdent("__TO_VARIANT__")
			return fn
		},
	}

	// PARSE_JSON: Marks for post-processing
	t.functionMap["PARSE_JSON"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			fn.Name = sqlparser.NewColIdent("__PARSE_JSON__")
			return fn
		},
	}

	// DATEADD: Marks for post-processing
	// DATEADD(part, n, date) → (date + INTERVAL n part)
	t.functionMap["DATEADD"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			fn.Name = sqlparser.NewColIdent("__DATEADD__")
			return fn
		},
	}

	// DATEDIFF: Marks for post-processing
	// DATEDIFF(part, start, end) → DATE_DIFF('part', start, end)
	t.functionMap["DATEDIFF"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			fn.Name = sqlparser.NewColIdent("__DATEDIFF__")
			return fn
		},
	}
}

// Translate converts Snowflake SQL to DuckDB-compatible SQL.
func (t *Translator) Translate(sql string) (string, error) {
	if sql == "" {
		return "", fmt.Errorf("empty SQL statement")
	}

	// Trim whitespace
	sql = strings.TrimSpace(sql)

	// DDL statements with type names - translate Snowflake types but skip AST parsing
	// (sqlparser adds unwanted backticks when serializing DDL back to string)
	upperSQL := strings.ToUpper(sql)
	if strings.HasPrefix(upperSQL, "CREATE ") ||
		strings.HasPrefix(upperSQL, "ALTER ") {
		return t.translateDataTypes(sql), nil
	}

	// Other DDL/meta statements - pass through unchanged
	// SHOW/DESCRIBE/EXPLAIN cause vitess-sqlparser to panic
	if strings.HasPrefix(upperSQL, "DROP ") ||
		strings.HasPrefix(upperSQL, "TRUNCATE ") ||
		strings.HasPrefix(upperSQL, "SHOW ") ||
		strings.HasPrefix(upperSQL, "DESCRIBE ") ||
		strings.HasPrefix(upperSQL, "DESC ") ||
		strings.HasPrefix(upperSQL, "EXPLAIN ") {
		return sql, nil
	}

	// Parse the SQL statement into an AST
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// If parsing fails, still translate data types for graceful degradation
		// DuckDB might handle some Snowflake syntax directly
		return t.translateDataTypes(sql), nil
	}

	// Walk the AST and transform functions in-place
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if n, ok := node.(*sqlparser.FuncExpr); ok {
			funcName := strings.ToUpper(n.Name.String())
			if translator, exists := t.functionMap[funcName]; exists {
				if translator.Handler != nil {
					// Apply handler - modifies the node in-place or marks it
					translator.Handler(n)
				} else if translator.Name != "" {
					// Simple function rename - modify in-place
					n.Name = sqlparser.NewColIdent(translator.Name)
				}
			}
		}
		return true, nil
	}, stmt)

	// Convert AST back to string
	result := sqlparser.String(stmt)

	// Apply post-processing for transformations that couldn't be done in-place
	result = t.handleComplexTransformations(result)

	// Translate Snowflake data types in CAST/convert expressions only
	// (avoid replacing column names that happen to match type names like "text")
	result = t.translateCastTypes(result)

	return result, nil
}

// handleComplexTransformations handles transformations that require more than simple renames.
// This handles marked functions and CURRENT_TIMESTAMP/CURRENT_DATE.
func (t *Translator) handleComplexTransformations(sql string) string {
	// Remove "from dual" added by vitess-sqlparser (Oracle-style, not needed in DuckDB)
	sql = removeDualSuffix(sql)

	// Remove parentheses from CURRENT_TIMESTAMP() and CURRENT_DATE()
	sql = strings.ReplaceAll(sql, "current_timestamp()", "CURRENT_TIMESTAMP")
	sql = strings.ReplaceAll(sql, "current_date()", "CURRENT_DATE")

	// Handle TO_VARIANT: __TO_VARIANT__(x) → CAST(x AS JSON)
	sql = t.transformMarkedFunction(sql, "__TO_VARIANT__", func(args string) string {
		return fmt.Sprintf("CAST(%s AS JSON)", args)
	})

	// Handle PARSE_JSON: __PARSE_JSON__(x) → CAST(x AS JSON)
	sql = t.transformMarkedFunction(sql, "__PARSE_JSON__", func(args string) string {
		return fmt.Sprintf("CAST(%s AS JSON)", args)
	})

	// Handle DATEADD: __DATEADD__(part, n, date) → (CAST(date AS DATE) + interval n part)
	sql = t.transformDATEADD(sql)

	// Handle DATEDIFF: __DATEDIFF__(part, start, end) → DATE_DIFF('part', start, end)
	sql = t.transformDATEDIFF(sql)

	return sql
}

// transformMarkedFunction transforms a marked function using a custom transformer.
func (t *Translator) transformMarkedFunction(sql, marker string, transformer func(args string) string) string {
	for {
		idx := strings.Index(sql, marker+"(")
		if idx == -1 {
			break
		}

		// Find the matching closing parenthesis
		start := idx + len(marker) + 1
		depth := 1
		end := start
		for end < len(sql) && depth > 0 {
			switch sql[end] {
			case '(':
				depth++
			case ')':
				depth--
			}
			end++
		}

		if depth == 0 {
			args := sql[start : end-1]
			replacement := transformer(args)
			sql = sql[:idx] + replacement + sql[end:]
		} else {
			break
		}
	}
	return sql
}

// transformDATEADD transforms DATEADD: __DATEADD__(part, n, date) → (CAST(date AS DATE) + interval n part)
func (t *Translator) transformDATEADD(sql string) string {
	return t.transformMarkedFunction(sql, "__DATEADD__", func(args string) string {
		parts := splitFunctionArgs(args, 3)
		if len(parts) != 3 {
			return "__DATEADD__(" + args + ")"
		}
		part := strings.TrimSpace(parts[0])
		n := strings.TrimSpace(parts[1])
		date := strings.TrimSpace(parts[2])
		// Cast date argument to DATE to handle string literals
		return fmt.Sprintf("(CAST(%s AS DATE) + interval %s %s)", date, n, part)
	})
}

// transformDATEDIFF transforms DATEDIFF: __DATEDIFF__(part, start, end) → DATE_DIFF('part', CAST(start AS DATE), CAST(end AS DATE))
func (t *Translator) transformDATEDIFF(sql string) string {
	return t.transformMarkedFunction(sql, "__DATEDIFF__", func(args string) string {
		parts := splitFunctionArgs(args, 3)
		if len(parts) != 3 {
			return "__DATEDIFF__(" + args + ")"
		}
		part := strings.TrimSpace(parts[0])
		startDate := strings.TrimSpace(parts[1])
		endDate := strings.TrimSpace(parts[2])
		// Cast date arguments to DATE to handle string literals
		return fmt.Sprintf("DATE_DIFF('%s', CAST(%s AS DATE), CAST(%s AS DATE))", part, startDate, endDate)
	})
}

// removeDualSuffix removes " from dual" suffix (case-insensitive) without regex.
func removeDualSuffix(sql string) string {
	// Trim trailing whitespace first
	trimmed := strings.TrimRight(sql, " \t\n\r")
	lower := strings.ToLower(trimmed)

	// Check for " from dual" at the end
	suffix := " from dual"
	if strings.HasSuffix(lower, suffix) {
		return trimmed[:len(trimmed)-len(suffix)]
	}
	return sql
}

// registerTypeMappings populates the type mapping table, sorted by name length descending.
// Longer type names are processed first to prevent partial matches
// (e.g., TIMESTAMP_NTZ is replaced before TIMESTAMP).
func (t *Translator) registerTypeMappings() {
	mappings := []typeMappingEntry{
		{"TIMESTAMP_NTZ", "TIMESTAMP"},
		{"TIMESTAMP_LTZ", "TIMESTAMPTZ"},
		{"TIMESTAMP_TZ", "TIMESTAMPTZ"},
		{"CHARACTER", "VARCHAR"},
		{"VARBINARY", "BLOB"},
		{"DATETIME", "TIMESTAMP"},
		{"BYTEINT", "TINYINT"},
		{"VARIANT", "JSON"},
		{"NUMBER", "NUMERIC"},
		{"STRING", "VARCHAR"},
		{"OBJECT", "JSON"},
		{"BINARY", "BLOB"},
		{"FLOAT4", "FLOAT"},
		{"FLOAT8", "DOUBLE"},
		{"ARRAY", "JSON"},
		{"TEXT", "VARCHAR"},
		{"CHAR", "VARCHAR"},
	}

	// Sort by from length descending to ensure longer types are replaced first
	sort.Slice(mappings, func(i, j int) bool {
		return len(mappings[i].from) > len(mappings[j].from)
	})

	t.typeMappings = mappings
}

// translateDataTypes replaces Snowflake type names with DuckDB equivalents in SQL text.
// It protects string literals from replacement and uses word-boundary-aware matching.
func (t *Translator) translateDataTypes(sql string) string {
	// Protect string literals from replacement
	protected, literals := protectStringLiterals(sql)

	// Replace each Snowflake type with its DuckDB equivalent
	for _, m := range t.typeMappings {
		protected = replaceTypeWord(protected, m.from, m.to)
	}

	// Restore string literals
	return restoreStringLiterals(protected, literals)
}

// translateCastTypes translates Snowflake type names only within convert() expressions.
// vitess-sqlparser converts CAST(x AS TYPE) to convert(x, TYPE), so we target that pattern.
// This avoids false positives where column names happen to match type names (e.g., "text").
func (t *Translator) translateCastTypes(sql string) string {
	// Find all convert( occurrences and translate the type argument
	lower := strings.ToLower(sql)
	result := strings.Builder{}
	result.Grow(len(sql))

	i := 0
	for i < len(sql) {
		// Look for "convert(" case-insensitively
		if i+8 <= len(sql) && lower[i:i+8] == "convert(" {
			// Find the matching closing paren
			start := i + 8
			depth := 1
			end := start
			for end < len(sql) && depth > 0 {
				switch sql[end] {
				case '(':
					depth++
				case ')':
					depth--
				}
				end++
			}

			if depth == 0 {
				// args = everything between convert( and )
				args := sql[start : end-1]
				// Find the last comma (separating expr from type)
				lastComma := strings.LastIndex(args, ",")
				if lastComma >= 0 {
					expr := args[:lastComma]
					typePart := strings.TrimSpace(args[lastComma+1:])
					// Translate the type name
					translatedType := t.translateSingleType(typePart)
					result.WriteString(sql[i : i+8])
					result.WriteString(expr)
					result.WriteString(", ")
					result.WriteString(translatedType)
					result.WriteByte(')')
					i = end
					continue
				}
			}
		}

		result.WriteByte(sql[i])
		i++
	}
	return result.String()
}

// translateSingleType translates a single type name (case-insensitive).
func (t *Translator) translateSingleType(typeName string) string {
	upper := strings.ToUpper(strings.TrimSpace(typeName))
	for _, m := range t.typeMappings {
		if upper == m.from {
			return m.to
		}
	}
	return typeName
}

// protectStringLiterals replaces single-quoted string literals with placeholders
// to prevent type name replacement inside strings.
func protectStringLiterals(sql string) (string, []string) {
	var literals []string
	var result strings.Builder
	result.Grow(len(sql))

	i := 0
	for i < len(sql) {
		if sql[i] == '\'' {
			// Find the end of the string literal (handle escaped quotes '')
			j := i + 1
			for j < len(sql) {
				if sql[j] == '\'' {
					if j+1 < len(sql) && sql[j+1] == '\'' {
						j += 2 // Skip escaped quote
					} else {
						j++ // End of literal
						break
					}
				} else {
					j++
				}
			}
			literal := sql[i:j]
			placeholder := fmt.Sprintf("__STRLIT_%d__", len(literals))
			literals = append(literals, literal)
			result.WriteString(placeholder)
			i = j
		} else {
			result.WriteByte(sql[i])
			i++
		}
	}
	return result.String(), literals
}

// restoreStringLiterals replaces placeholders with the original string literals.
func restoreStringLiterals(sql string, literals []string) string {
	for i, lit := range literals {
		placeholder := fmt.Sprintf("__STRLIT_%d__", i)
		sql = strings.Replace(sql, placeholder, lit, 1)
	}
	return sql
}

// replaceTypeWord replaces all occurrences of a type name as a whole word (case-insensitive).
// Word boundaries are defined by non-word characters (anything not a letter, digit, or underscore).
func replaceTypeWord(sql, from, to string) string {
	fromUpper := strings.ToUpper(from)
	fromLen := len(from)
	var result strings.Builder
	result.Grow(len(sql))

	i := 0
	for i < len(sql) {
		// Check if we have enough characters remaining
		if i+fromLen > len(sql) {
			result.WriteString(sql[i:])
			break
		}

		// Case-insensitive comparison at current position
		if strings.EqualFold(sql[i:i+fromLen], fromUpper) {
			// Check word boundaries
			leftOK := i == 0 || !isWordChar(sql[i-1])
			rightOK := i+fromLen >= len(sql) || !isWordChar(sql[i+fromLen])

			if leftOK && rightOK {
				result.WriteString(to)
				i += fromLen
				continue
			}
		}

		result.WriteByte(sql[i])
		i++
	}
	return result.String()
}

// isWordChar returns true if the byte is a word character (letter, digit, or underscore).
func isWordChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

// splitFunctionArgs splits function arguments respecting parentheses nesting.
// expectedCount is a hint for the expected number of arguments.
func splitFunctionArgs(args string, expectedCount int) []string {
	result := make([]string, 0, expectedCount)
	depth := 0
	start := 0

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				result = append(result, args[start:i])
				start = i + 1
			}
		}
	}

	// Add the last argument
	if start < len(args) {
		result = append(result, args[start:])
	}

	return result
}
