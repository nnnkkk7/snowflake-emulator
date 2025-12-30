package query

import (
	"fmt"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

// Translator converts Snowflake SQL to DuckDB-compatible SQL using AST manipulation.
type Translator struct {
	functionMap map[string]FunctionTranslator
}

// FunctionTranslator defines how to translate a specific function.
type FunctionTranslator struct {
	Name    string                                       // DuckDB function name (for simple renames)
	Handler func(fn *sqlparser.FuncExpr) sqlparser.Expr // Custom handler for complex transformations
}

// NewTranslator creates a new SQL translator with registered function mappings.
func NewTranslator() *Translator {
	t := &Translator{
		functionMap: make(map[string]FunctionTranslator),
	}
	t.registerFunctions()
	return t
}

// registerFunctions registers all Snowflake to DuckDB function translations.
func (t *Translator) registerFunctions() {
	// Simple function renames (Phase 1)
	t.functionMap["IFF"] = FunctionTranslator{Name: "IF"}
	t.functionMap["NVL"] = FunctionTranslator{Name: "COALESCE"}
	t.functionMap["IFNULL"] = FunctionTranslator{Name: "COALESCE"}
	t.functionMap["LISTAGG"] = FunctionTranslator{Name: "STRING_AGG"}

	// Note: CURRENT_TIMESTAMP() and CURRENT_DATE() removal of parentheses
	// is handled in post-processing for Phase 1 since Walk doesn't support
	// node type replacement (FuncExpr → ColName)

	t.functionMap["NVL2"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			// NVL2(a, b, c) → IF(a IS NOT NULL, b, c)
			if len(fn.Exprs) != 3 {
				return fn
			}
			return &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("IF"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{
						Expr: &sqlparser.IsExpr{
							Operator: "is not null",
							Expr:     fn.Exprs[0].(*sqlparser.AliasedExpr).Expr,
						},
					},
					fn.Exprs[1],
					fn.Exprs[2],
				},
			}
		},
	}

	t.functionMap["TO_VARIANT"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			// TO_VARIANT(x) → CONVERT(x, JSON)
			if len(fn.Exprs) != 1 {
				return fn
			}
			return &sqlparser.ConvertExpr{
				Expr: fn.Exprs[0].(*sqlparser.AliasedExpr).Expr,
				Type: &sqlparser.ConvertType{Type: "JSON"},
			}
		},
	}

	t.functionMap["PARSE_JSON"] = FunctionTranslator{
		Handler: func(fn *sqlparser.FuncExpr) sqlparser.Expr {
			// PARSE_JSON(str) → CONVERT(str, JSON)
			if len(fn.Exprs) != 1 {
				return fn
			}
			return &sqlparser.ConvertExpr{
				Expr: fn.Exprs[0].(*sqlparser.AliasedExpr).Expr,
				Type: &sqlparser.ConvertType{Type: "JSON"},
			}
		},
	}

	// Phase 2 functions (placeholders for future implementation)
	// DATEADD, DATEDIFF, FLATTEN, OBJECT_CONSTRUCT will be added in Phase 2
}

// Translate converts Snowflake SQL to DuckDB-compatible SQL.
func (t *Translator) Translate(sql string) (string, error) {
	if sql == "" {
		return "", fmt.Errorf("empty SQL statement")
	}

	// Trim whitespace
	sql = strings.TrimSpace(sql)

	// Parse the SQL statement into an AST
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// If parsing fails, return original SQL
		// DuckDB might handle some Snowflake syntax directly
		// This provides graceful degradation for unsupported syntax
		return sql, nil
	}

	// Walk the AST and transform functions in-place
	modified := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *sqlparser.FuncExpr:
			funcName := strings.ToUpper(n.Name.String())
			if translator, ok := t.functionMap[funcName]; ok {
				if translator.Handler != nil {
					// For complex transformations, we can't replace in-place with Walk
					// We'll handle these after the walk
					// For now, just mark as modified
					modified = true
				} else if translator.Name != "" {
					// Simple function rename - modify in-place
					n.Name = sqlparser.NewColIdent(translator.Name)
					modified = true
				}
			}
		}
		return true, nil
	}, stmt)

	// If simple renames were done, convert back to string
	if modified {
		return sqlparser.String(stmt), nil
	}

	// For complex transformations (TO_VARIANT, PARSE_JSON, NVL2), we need a different approach
	// For Phase 1, we'll use a simplified string-based approach for these specific cases
	result := sqlparser.String(stmt)
	result = t.handleComplexTransformations(result)

	return result, nil
}

// handleComplexTransformations handles transformations that require more than simple renames.
// This is a simplified approach for Phase 1. Phase 2 will implement full AST reconstruction.
func (t *Translator) handleComplexTransformations(sql string) string {
	// Remove parentheses from CURRENT_TIMESTAMP() and CURRENT_DATE()
	// vitess-sqlparser represents these as FuncExpr, but DuckDB prefers them without parens
	sql = strings.Replace(sql, "current_timestamp()", "CURRENT_TIMESTAMP", -1)
	sql = strings.Replace(sql, "current_date()", "CURRENT_DATE", -1)

	// Complex transformations (TO_VARIANT, PARSE_JSON, NVL2) are deferred to Phase 2
	return sql
}
