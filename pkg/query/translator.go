package query

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// Translator converts Snowflake SQL to DuckDB-compatible SQL using AST manipulation.
type Translator struct {
	parser      *sqlparser.Parser
	functionMap map[string]FunctionTranslator
}

// FunctionTranslator defines how to translate a specific function.
type FunctionTranslator struct {
	Handler func(fn *sqlparser.FuncExpr) sqlparser.Expr // Custom handler for complex transformations
	Name    string                                      // DuckDB function name (for simple renames)
}

// NewTranslator creates a new SQL translator with registered function mappings.
func NewTranslator() *Translator {
	t := &Translator{
		parser:      sqlparser.NewTestParser(),
		functionMap: make(map[string]FunctionTranslator),
	}
	t.registerFunctions()
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
			fn.Name = sqlparser.NewIdentifierCI("IF")
			fn.Exprs[0] = &sqlparser.IsExpr{
				Left:  fn.Exprs[0],
				Right: sqlparser.IsNotNullOp,
			}
			return fn
		},
	}

	// TO_VARIANT: Marks for post-processing (can't replace node type with Walk)
	t.functionMap["TO_VARIANT"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			fn.Name = sqlparser.NewIdentifierCI("__TO_VARIANT__")
			return fn
		},
	}

	// PARSE_JSON: Marks for post-processing
	t.functionMap["PARSE_JSON"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			fn.Name = sqlparser.NewIdentifierCI("__PARSE_JSON__")
			return fn
		},
	}

	// DATEADD: Marks for post-processing
	// DATEADD(part, n, date) → (date + INTERVAL n part)
	t.functionMap["DATEADD"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			fn.Name = sqlparser.NewIdentifierCI("__DATEADD__")
			return fn
		},
	}

	// DATEDIFF: Marks for post-processing
	// DATEDIFF(part, start, end) → DATE_DIFF('part', start, end)
	t.functionMap["DATEDIFF"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			fn.Name = sqlparser.NewIdentifierCI("__DATEDIFF__")
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

	// Skip AST transformation for DDL statements - they don't need function translation
	// and the sqlparser adds unwanted backticks when serializing back to string.
	// Also skip SHOW/DESCRIBE/EXPLAIN which the parser does not handle for translation.
	upperSQL := strings.ToUpper(sql)
	if strings.HasPrefix(upperSQL, "CREATE ") ||
		strings.HasPrefix(upperSQL, "DROP ") ||
		strings.HasPrefix(upperSQL, "ALTER ") ||
		strings.HasPrefix(upperSQL, "TRUNCATE ") ||
		strings.HasPrefix(upperSQL, "SHOW ") ||
		strings.HasPrefix(upperSQL, "DESCRIBE ") ||
		strings.HasPrefix(upperSQL, "DESC ") ||
		strings.HasPrefix(upperSQL, "EXPLAIN ") {
		return sql, nil
	}

	// Parse the SQL statement into an AST
	stmt, err := t.parser.Parse(sql)
	if err != nil {
		// If parsing fails, return original SQL
		// DuckDB might handle some Snowflake syntax directly
		// This provides graceful degradation for unsupported syntax
		return sql, nil
	}

	// Walk the AST and transform functions in-place
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if n, ok := node.(*sqlparser.FuncExpr); ok {
			funcName := strings.ToUpper(n.Name.String())
			if translator, exists := t.functionMap[funcName]; exists {
				if translator.Handler != nil {
					translator.Handler(n)
				} else if translator.Name != "" {
					n.Name = sqlparser.NewIdentifierCI(translator.Name)
				}
			}
		}
		return true, nil
	}, stmt)

	// Convert AST back to string
	result := sqlparser.String(stmt)

	// Apply post-processing for transformations that couldn't be done in-place
	result = t.handleComplexTransformations(result)

	return result, nil
}

// handleComplexTransformations handles transformations that require more than simple renames.
// This handles marked functions and CURRENT_TIMESTAMP/CURRENT_DATE.
func (t *Translator) handleComplexTransformations(sql string) string {
	// Remove MySQL-style identifier quoting added by the sqlparser (not used by DuckDB)
	sql = strings.ReplaceAll(sql, "`", "")

	// Remove "from dual" added by the sqlparser (Oracle-style, not needed in DuckDB)
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
		return fmt.Sprintf("DATE_DIFF('%s', CAST(%s AS DATE), CAST(%s AS DATE))", part, startDate, endDate)
	})
}

// removeDualSuffix removes " from dual" suffix (case-insensitive) without regex.
func removeDualSuffix(sql string) string {
	trimmed := strings.TrimRight(sql, " \t\n\r")
	lower := strings.ToLower(trimmed)

	suffix := " from dual"
	if strings.HasSuffix(lower, suffix) {
		return trimmed[:len(trimmed)-len(suffix)]
	}
	return sql
}

// splitFunctionArgs splits function arguments respecting parentheses nesting.
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

	if start < len(args) {
		result = append(result, args[start:])
	}

	return result
}
