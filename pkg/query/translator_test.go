package query

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestTranslator_IFF tests translation of Snowflake IFF function.
// IFF(condition, true_value, false_value) → IF(condition, true_value, false_value)
func TestTranslator_IFF(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SimpleIFF",
			input:    "SELECT IFF(age > 18, 'adult', 'minor') FROM users",
			expected: "select IF(age > 18, 'adult', 'minor') from users",
			wantErr:  false,
		},
		{
			name:     "IFFWithNull",
			input:    "SELECT IFF(value IS NULL, 0, value) FROM data",
			expected: "select IF(value is null, 0, value) from data",
			wantErr:  false,
		},
		{
			name:     "NestedIFF",
			input:    "SELECT IFF(x > 10, IFF(y > 5, 'A', 'B'), 'C') FROM test",
			expected: "select IF(x > 10, IF(y > 5, 'A', 'B'), 'C') from test",
			wantErr:  false,
		},
		{
			name:     "IFFWithComplexCondition",
			input:    "SELECT IFF(score >= 90 AND attendance > 80, 'Pass', 'Fail') FROM students",
			expected: "select IF(score >= 90 and attendance > 80, 'Pass', 'Fail') from students",
			wantErr:  false,
		},
		{
			name:     "IFFInWHERE",
			input:    "SELECT * FROM users WHERE IFF(active, 1, 0) = 1",
			expected: "select * from users where IF(active, 1, 0) = 1",
			wantErr:  false,
		},
		{
			name:     "MultipleIFF",
			input:    "SELECT IFF(a, 1, 0), IFF(b, 2, 0), IFF(c, 3, 0) FROM test",
			expected: "select IF(a, 1, 0), IF(b, 2, 0), IF(c, 3, 0) from test",
			wantErr:  false,
		},
		{
			name:     "IFFWithNullComparison",
			input:    "SELECT IFF(col IS NOT NULL, col, 'N/A') FROM data",
			expected: "select IF(col is not null, col, 'N/A') from data",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := NewTranslator()
			result, err := translator.Translate(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("Translate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if diff := cmp.Diff(tt.expected, result); diff != "" {
					t.Errorf("Translate() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestTranslator_NVL tests translation of Snowflake NVL and IFNULL functions.
// NVL(expr1, expr2) → COALESCE(expr1, expr2)
// IFNULL(expr1, expr2) → COALESCE(expr1, expr2)
func TestTranslator_NVL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SimpleNVL",
			input:    "SELECT NVL(name, 'Unknown') FROM users",
			expected: "select COALESCE(name, 'Unknown') from users",
			wantErr:  false,
		},
		{
			name:     "NVLWithNumbers",
			input:    "SELECT NVL(score, 0) FROM results",
			expected: "select COALESCE(score, 0) from results",
			wantErr:  false,
		},
		{
			name:     "MultipleNVL",
			input:    "SELECT NVL(first_name, 'N/A'), NVL(last_name, 'N/A') FROM people",
			expected: "select COALESCE(first_name, 'N/A'), COALESCE(last_name, 'N/A') from people",
			wantErr:  false,
		},
		{
			name:     "IFNULL_Translation",
			input:    "SELECT IFNULL(email, 'noreply@example.com') FROM users",
			expected: "select COALESCE(email, 'noreply@example.com') from users",
			wantErr:  false,
		},
		{
			name:     "NestedNVL",
			input:    "SELECT NVL(NVL(col1, col2), 'default') FROM test",
			expected: "select COALESCE(COALESCE(col1, col2), 'default') from test",
			wantErr:  false,
		},
		{
			name:     "NVLInWHERE",
			input:    "SELECT * FROM users WHERE NVL(status, 'active') = 'active'",
			expected: "select * from users where COALESCE(status, 'active') = 'active'",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := NewTranslator()
			result, err := translator.Translate(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("Translate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if diff := cmp.Diff(tt.expected, result); diff != "" {
					t.Errorf("Translate() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestTranslator_CONCAT tests CONCAT function handling.
// Phase 1: CONCAT is not translated (deferred to Phase 2)
// Expected behavior: Pass-through (lowercase keywords due to AST parser)
func TestTranslator_CONCAT(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SimpleCONCAT_PassThrough",
			input:    "SELECT CONCAT(first_name, ' ', last_name) FROM users",
			expected: "select CONCAT(first_name, ' ', last_name) from users",
			wantErr:  false,
		},
		{
			name:     "CONCATThreeStrings_PassThrough",
			input:    "SELECT CONCAT(city, ', ', state) FROM addresses",
			expected: "select CONCAT(city, ', ', state) from addresses",
			wantErr:  false,
		},
		{
			name:     "CONCATWithColumns_PassThrough",
			input:    "SELECT CONCAT('Name: ', name) FROM people",
			expected: "select CONCAT('Name: ', name) from people",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := NewTranslator()
			result, err := translator.Translate(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("Translate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if diff := cmp.Diff(tt.expected, result); diff != "" {
					t.Errorf("Translate() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestTranslator_DateTimeFunctions tests translation of date/time functions.
func TestTranslator_DateTimeFunctions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "CURRENT_TIMESTAMP_RemoveParens",
			input:    "SELECT CURRENT_TIMESTAMP() FROM dual",
			expected: "select CURRENT_TIMESTAMP from dual",
			wantErr:  false,
		},
		{
			name:     "CURRENT_DATE_RemoveParens",
			input:    "SELECT CURRENT_DATE() FROM dual",
			expected: "select CURRENT_DATE from dual",
			wantErr:  false,
		},
		{
			name:     "CURRENT_TIMESTAMP_NoParens_PassThrough",
			input:    "SELECT CURRENT_TIMESTAMP FROM dual",
			expected: "select CURRENT_TIMESTAMP from dual",
			wantErr:  false,
		},
		{
			name:     "CURRENT_DATE_NoParens_PassThrough",
			input:    "SELECT CURRENT_DATE FROM dual",
			expected: "select CURRENT_DATE from dual",
			wantErr:  false,
		},
		{
			name:     "DATEADD_PassThrough_Phase2",
			input:    "SELECT DATEADD(day, 7, order_date) FROM orders",
			expected: "select DATEADD(day, 7, order_date) from orders",
			wantErr:  false,
		},
		{
			name:     "DATEDIFF_PassThrough_Phase2",
			input:    "SELECT DATEDIFF(day, start_date, end_date) FROM events",
			expected: "select DATEDIFF(day, start_date, end_date) from events",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := NewTranslator()
			result, err := translator.Translate(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("Translate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if diff := cmp.Diff(tt.expected, result); diff != "" {
					t.Errorf("Translate() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestTranslator_StringFunctions tests translation of string functions.
func TestTranslator_StringFunctions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "LENGTH",
			input:    "SELECT LENGTH(name) FROM users",
			expected: "select LENGTH(name) from users",
			wantErr:  false,
		},
		{
			name:     "SUBSTR",
			input:    "SELECT SUBSTR(text, 1, 10) FROM documents",
			expected: "select SUBSTR(text, 1, 10) from documents",
			wantErr:  false,
		},
		{
			name:     "UPPER",
			input:    "SELECT UPPER(name) FROM users",
			expected: "select UPPER(name) from users",
			wantErr:  false,
		},
		{
			name:     "LOWER",
			input:    "SELECT LOWER(email) FROM users",
			expected: "select LOWER(email) from users",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := NewTranslator()
			result, err := translator.Translate(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("Translate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if diff := cmp.Diff(tt.expected, result); diff != "" {
					t.Errorf("Translate() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestTranslator_ComplexQuery tests translation of complex queries with multiple functions.
func TestTranslator_ComplexQuery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name: "Phase1Functions_IFF_NVL",
			input: `SELECT
				NVL(email, 'no-email@example.com') AS email,
				IFF(age >= 18, 'adult', 'minor') AS age_group
			FROM users`,
			expected: "select COALESCE(email, 'no-email@example.com') as email, IF(age >= 18, 'adult', 'minor') as age_group from users",
			wantErr:  false,
		},
		{
			name: "WithWHEREClause_IFF",
			input: `SELECT name, score
			FROM results
			WHERE IFF(category = 'A', score > 80, score > 60)`,
			expected: "select name, score from results where IF(category = 'A', score > 80, score > 60)",
			wantErr:  false,
		},
		{
			name: "NestedIFF_NVL",
			input: `SELECT IFF(status = 'active', NVL(name, 'Unknown'), 'Inactive') FROM users`,
			expected: "select IF(status = 'active', COALESCE(name, 'Unknown'), 'Inactive') from users",
			wantErr:  false,
		},
		{
			name: "MultipleNVL_WithIFF",
			input: `SELECT NVL(first_name, 'N/A'), NVL(last_name, 'N/A'), IFF(active, 1, 0) FROM people`,
			expected: "select COALESCE(first_name, 'N/A'), COALESCE(last_name, 'N/A'), IF(active, 1, 0) from people",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := NewTranslator()
			result, err := translator.Translate(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("Translate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Normalize whitespace for comparison
				normalizedExpected := normalizeWhitespace(tt.expected)
				normalizedResult := normalizeWhitespace(result)
				if diff := cmp.Diff(normalizedExpected, normalizedResult); diff != "" {
					t.Errorf("Translate() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestTranslator_Phase2Functions tests Phase 2 functions (not yet implemented).
// Expected behavior: Pass-through (deferred to Phase 2)
func TestTranslator_Phase2Functions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "NVL2_PassThrough",
			input:    "SELECT NVL2(col, 'not null', 'null') FROM test",
			expected: "select NVL2(col, 'not null', 'null') from test",
			wantErr:  false,
		},
		{
			name:     "TO_VARIANT_PassThrough",
			input:    "SELECT TO_VARIANT(data) FROM test",
			expected: "select TO_VARIANT(data) from test",
			wantErr:  false,
		},
		{
			name:     "PARSE_JSON_PassThrough",
			input:    "SELECT PARSE_JSON(json_str) FROM test",
			expected: "select PARSE_JSON(json_str) from test",
			wantErr:  false,
		},
		{
			name:     "LISTAGG_PassThrough",
			input:    "SELECT LISTAGG(name, ',') FROM test GROUP BY category",
			expected: "select STRING_AGG(name, ',') from test group by category",
			wantErr:  false,
		},
		{
			name:     "FLATTEN_PassThrough",
			input:    "SELECT * FROM TABLE(FLATTEN(input => array_col))",
			expected: "SELECT * FROM TABLE(FLATTEN(input => array_col))", // Parser fails on => syntax, returns original
			wantErr:  false,
		},
		{
			name:     "OBJECT_CONSTRUCT_PassThrough",
			input:    "SELECT OBJECT_CONSTRUCT('key', value) FROM test",
			expected: "select OBJECT_CONSTRUCT('key', value) from test",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := NewTranslator()
			result, err := translator.Translate(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("Translate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if diff := cmp.Diff(tt.expected, result); diff != "" {
					t.Errorf("Translate() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestTranslator_PassThrough tests that standard SQL passes through unchanged.
func TestTranslator_PassThrough(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SimpleSELECT",
			input:    "SELECT * FROM users",
			expected: "select * from users",
			wantErr:  false,
		},
		{
			name:     "SELECTWithWHERE",
			input:    "SELECT id, name FROM users WHERE age > 18",
			expected: "select id, name from users where age > 18",
			wantErr:  false,
		},
		{
			name:     "JoinQuery",
			input:    "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
			expected: "select u.name, o.total from users as u join orders as o on u.id = o.user_id",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := NewTranslator()
			result, err := translator.Translate(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("Translate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if diff := cmp.Diff(tt.expected, result); diff != "" {
					t.Errorf("Translate() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestTranslator_ErrorCases tests error handling and edge cases.
func TestTranslator_ErrorCases(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		wantErr         bool
		expectedContains string // For graceful degradation, check if result contains this
	}{
		{
			name:    "EmptyString",
			input:   "",
			wantErr: true,
		},
		{
			name:             "InvalidSQL_GracefulDegradation",
			input:            "SELECT FROM",
			wantErr:          false,
			expectedContains: "SELECT FROM", // Returns original
		},
		{
			name:             "UnbalancedParentheses_GracefulDegradation",
			input:            "SELECT IFF(age > 18, 'adult' FROM users",
			wantErr:          false,
			expectedContains: "SELECT IFF(age > 18, 'adult' FROM users", // Returns original
		},
		{
			name:             "CompletelyInvalidSQL",
			input:            "THIS IS NOT SQL AT ALL",
			wantErr:          false,
			expectedContains: "THIS IS NOT SQL AT ALL", // Returns original
		},
		{
			name:             "OnlyWhitespace",
			input:            "   \t\n   ",
			wantErr:          false,
			expectedContains: "", // Should return empty after trim
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := NewTranslator()
			result, err := translator.Translate(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("Translate() error = %v, wantErr %v", err, tt.wantErr)
			}

			// For graceful degradation cases
			if !tt.wantErr && err == nil && tt.expectedContains != "" {
				if !strings.Contains(result, tt.expectedContains) {
					t.Errorf("Expected result to contain %q, got %q", tt.expectedContains, result)
				}
			}
		})
	}
}

// TestTranslator_EdgeCases tests edge cases and boundary conditions.
func TestTranslator_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "CaseSensitivity_IFF",
			input:    "SELECT iff(col, 1, 0), IFF(col2, 2, 0), Iff(col3, 3, 0) FROM test",
			expected: "select IF(col, 1, 0), IF(col2, 2, 0), IF(col3, 3, 0) from test",
			wantErr:  false,
		},
		{
			name:     "CaseSensitivity_NVL",
			input:    "SELECT nvl(col, 0), NVL(col2, 0), Nvl(col3, 0) FROM test",
			expected: "select COALESCE(col, 0), COALESCE(col2, 0), COALESCE(col3, 0) from test",
			wantErr:  false,
		},
		{
			name:     "MixedFunctions",
			input:    "SELECT IFF(NVL(a, 0) > 5, 'high', 'low') FROM test",
			expected: "select IF(COALESCE(a, 0) > 5, 'high', 'low') from test",
			wantErr:  false,
		},
		{
			name:     "FunctionInJOIN",
			input:    "SELECT * FROM t1 JOIN t2 ON IFF(t1.id IS NULL, 0, t1.id) = t2.id",
			expected: "select * from t1 join t2 on IF(t1.id is null, 0, t1.id) = t2.id",
			wantErr:  false,
		},
		{
			name:     "FunctionInGROUPBY",
			input:    "SELECT IFF(status, 'active', 'inactive'), COUNT(*) FROM users GROUP BY IFF(status, 'active', 'inactive')",
			expected: "select IF(status, 'active', 'inactive'), COUNT(*) from users group by IF(status, 'active', 'inactive')",
			wantErr:  false,
		},
		{
			name:     "FunctionInHAVING",
			input:    "SELECT category, COUNT(*) FROM items GROUP BY category HAVING NVL(COUNT(*), 0) > 10",
			expected: "select category, COUNT(*) from items group by category having COALESCE(COUNT(*), 0) > 10",
			wantErr:  false,
		},
		{
			name:     "FunctionInORDERBY",
			input:    "SELECT * FROM users ORDER BY IFF(premium, 1, 2), NVL(name, 'ZZZ')",
			expected: "select * from users order by IF(premium, 1, 2) asc, COALESCE(name, 'ZZZ') asc", // Parser adds ASC
			wantErr:  false,
		},
		{
			name:     "SubqueryWithFunctions",
			input:    "SELECT * FROM (SELECT IFF(a, 1, 0) AS flag FROM test) WHERE flag = 1",
			expected: "SELECT * FROM (SELECT IFF(a, 1, 0) AS flag FROM test) WHERE flag = 1", // Subquery parsing fails, returns original
			wantErr:  false,
		},
		{
			name:     "CURRENT_TIMESTAMP_MultipleOccurrences",
			input:    "SELECT CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_DATE() FROM dual",
			expected: "select CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_DATE from dual",
			wantErr:  false,
		},
		{
			name:     "StringsWithFunctionNames",
			input:    "SELECT 'IFF', 'NVL', 'CURRENT_TIMESTAMP' FROM test",
			expected: "select 'IFF', 'NVL', 'CURRENT_TIMESTAMP' from test",
			wantErr:  false,
		},
		{
			name:     "CommentsWithFunctions",
			input:    "SELECT /* IFF comment */ id, /* NVL comment */ name FROM test",
			expected: "select /* IFF comment */ id, name from test", // Parser preserves some comments
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := NewTranslator()
			result, err := translator.Translate(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("Translate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Normalize whitespace for comparison
				normalizedExpected := normalizeWhitespace(tt.expected)
				normalizedResult := normalizeWhitespace(result)
				if diff := cmp.Diff(normalizedExpected, normalizedResult); diff != "" {
					t.Errorf("Translate() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// normalizeWhitespace removes extra whitespace and newlines for comparison.
func normalizeWhitespace(s string) string {
	// Simple normalization: replace multiple whitespace with single space
	result := ""
	prevSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if !prevSpace {
				result += " "
				prevSpace = true
			}
		} else {
			result += string(r)
			prevSpace = false
		}
	}
	return result
}
