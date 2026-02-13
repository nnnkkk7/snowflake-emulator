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
// CONCAT is passed through without translation.
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
			expected: "select CURRENT_TIMESTAMP",
			wantErr:  false,
		},
		{
			name:     "CURRENT_DATE_RemoveParens",
			input:    "SELECT CURRENT_DATE() FROM dual",
			expected: "select CURRENT_DATE",
			wantErr:  false,
		},
		{
			name:     "CURRENT_TIMESTAMP_NoParens_PassThrough",
			input:    "SELECT CURRENT_TIMESTAMP FROM dual",
			expected: "select CURRENT_TIMESTAMP",
			wantErr:  false,
		},
		{
			name:     "CURRENT_DATE_NoParens_PassThrough",
			input:    "SELECT CURRENT_DATE FROM dual",
			expected: "select CURRENT_DATE",
			wantErr:  false,
		},
		{
			name:     "DATEADD_Translation",
			input:    "SELECT DATEADD(day, 7, order_date) FROM orders",
			expected: "select (CAST(order_date AS DATE) + interval 7 day) from orders",
			wantErr:  false,
		},
		{
			name:     "DATEDIFF_Translation",
			input:    "SELECT DATEDIFF(day, start_date, end_date) FROM events",
			expected: "select DATE_DIFF('day', CAST(start_date AS DATE), CAST(end_date AS DATE)) from events",
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
			name: "BasicFunctions_IFF_NVL",
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
			name:     "NestedIFF_NVL",
			input:    `SELECT IFF(status = 'active', NVL(name, 'Unknown'), 'Inactive') FROM users`,
			expected: "select IF(status = 'active', COALESCE(name, 'Unknown'), 'Inactive') from users",
			wantErr:  false,
		},
		{
			name:     "MultipleNVL_WithIFF",
			input:    `SELECT NVL(first_name, 'N/A'), NVL(last_name, 'N/A'), IFF(active, 1, 0) FROM people`,
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

// TestTranslator_NVL2 tests NVL2 function translation.
// NVL2(expr, not_null_result, null_result) → IF(expr IS NOT NULL, not_null_result, null_result)
func TestTranslator_NVL2(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SimpleNVL2",
			input:    "SELECT NVL2(col, 'not null', 'null') FROM test",
			expected: "select IF(col is not null, 'not null', 'null') from test",
			wantErr:  false,
		},
		{
			name:     "NVL2WithNumbers",
			input:    "SELECT NVL2(score, score * 2, 0) FROM results",
			expected: "select IF(score is not null, score * 2, 0) from results",
			wantErr:  false,
		},
		{
			name:     "NVL2InWHERE",
			input:    "SELECT * FROM users WHERE NVL2(status, 1, 0) = 1",
			expected: "select * from users where IF(status is not null, 1, 0) = 1",
			wantErr:  false,
		},
		{
			name:     "MultipleNVL2",
			input:    "SELECT NVL2(a, 'A', 'X'), NVL2(b, 'B', 'Y') FROM test",
			expected: "select IF(a is not null, 'A', 'X'), IF(b is not null, 'B', 'Y') from test",
			wantErr:  false,
		},
		{
			name:     "NestedNVL2WithNVL",
			input:    "SELECT NVL2(col, NVL(inner_col, 'default'), 'null') FROM test",
			expected: "select IF(col is not null, COALESCE(inner_col, 'default'), 'null') from test",
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

// TestTranslator_TO_VARIANT tests TO_VARIANT function translation.
// TO_VARIANT(x) → CAST(x AS JSON) (DuckDB compatible)
func TestTranslator_TO_VARIANT(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SimpleTO_VARIANT",
			input:    "SELECT TO_VARIANT(data) FROM test",
			expected: "select CAST(data AS JSON) from test",
			wantErr:  false,
		},
		{
			name:     "TO_VARIANTWithString",
			input:    "SELECT TO_VARIANT('hello') FROM dual",
			expected: "select CAST('hello' AS JSON)",
			wantErr:  false,
		},
		{
			name:     "TO_VARIANTWithNumber",
			input:    "SELECT TO_VARIANT(123) FROM dual",
			expected: "select CAST(123 AS JSON)",
			wantErr:  false,
		},
		{
			name:     "MultipleTO_VARIANT",
			input:    "SELECT TO_VARIANT(a), TO_VARIANT(b) FROM test",
			expected: "select CAST(a AS JSON), CAST(b AS JSON) from test",
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

// TestTranslator_PARSE_JSON tests PARSE_JSON function translation.
// PARSE_JSON(str) → CAST(str AS JSON) (DuckDB compatible)
func TestTranslator_PARSE_JSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SimplePARSE_JSON",
			input:    "SELECT PARSE_JSON(json_str) FROM test",
			expected: "select CAST(json_str AS JSON) from test",
			wantErr:  false,
		},
		{
			name:     "PARSE_JSONWithLiteral",
			input:    `SELECT PARSE_JSON('{"key": "value"}') FROM dual`,
			expected: `select CAST('{\"key\": \"value\"}' AS JSON)`,
			wantErr:  false,
		},
		{
			name:     "PARSE_JSONInWHERE",
			input:    "SELECT * FROM test WHERE PARSE_JSON(data) IS NOT NULL",
			expected: "select * from test where CAST(data AS JSON) is not null",
			wantErr:  false,
		},
		{
			name:     "MultiplePARSE_JSON",
			input:    "SELECT PARSE_JSON(a), PARSE_JSON(b) FROM test",
			expected: "select CAST(a AS JSON), CAST(b AS JSON) from test",
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

// TestTranslator_DATEADD tests DATEADD function translation.
// DATEADD(part, n, date) → (CAST(date AS DATE) + INTERVAL n part) for DuckDB
func TestTranslator_DATEADD(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "DATEADDDays",
			input:    "SELECT DATEADD(day, 7, order_date) FROM orders",
			expected: "select (CAST(order_date AS DATE) + interval 7 day) from orders",
			wantErr:  false,
		},
		{
			name:     "DATEADDMonths",
			input:    "SELECT DATEADD(month, 1, start_date) FROM subscriptions",
			expected: "select (CAST(start_date AS DATE) + interval 1 month) from subscriptions",
			wantErr:  false,
		},
		{
			name:     "DATEADDYears",
			input:    "SELECT DATEADD(year, 5, birth_date) FROM users",
			expected: "select (CAST(birth_date AS DATE) + interval 5 year) from users",
			wantErr:  false,
		},
		{
			name:     "DATEADDNegative",
			input:    "SELECT DATEADD(day, -30, CURRENT_DATE()) FROM dual",
			expected: "select (CAST(CURRENT_DATE AS DATE) + interval -30 day)",
			wantErr:  false,
		},
		{
			name:     "DATEADDHours",
			input:    "SELECT DATEADD(hour, 24, created_at) FROM events",
			expected: "select (CAST(created_at AS DATE) + interval 24 hour) from events",
			wantErr:  false,
		},
		{
			name:     "MultipleDATEADD",
			input:    "SELECT DATEADD(day, 1, date1), DATEADD(month, 2, date2) FROM test",
			expected: "select (CAST(date1 AS DATE) + interval 1 day), (CAST(date2 AS DATE) + interval 2 month) from test",
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

// TestTranslator_DATEDIFF tests DATEDIFF function translation.
// DATEDIFF(part, start, end) → DATE_DIFF('part', start, end) for DuckDB
func TestTranslator_DATEDIFF(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "DATEDIFFDays",
			input:    "SELECT DATEDIFF(day, start_date, end_date) FROM events",
			expected: "select DATE_DIFF('day', CAST(start_date AS DATE), CAST(end_date AS DATE)) from events",
			wantErr:  false,
		},
		{
			name:     "DATEDIFFMonths",
			input:    "SELECT DATEDIFF(month, hire_date, CURRENT_DATE()) FROM employees",
			expected: "select DATE_DIFF('month', CAST(hire_date AS DATE), CAST(CURRENT_DATE AS DATE)) from employees",
			wantErr:  false,
		},
		{
			name:     "DATEDIFFYears",
			input:    "SELECT DATEDIFF(year, birth_date, CURRENT_DATE()) FROM users",
			expected: "select DATE_DIFF('year', CAST(birth_date AS DATE), CAST(CURRENT_DATE AS DATE)) from users",
			wantErr:  false,
		},
		{
			name:     "DATEDIFFInWHERE",
			input:    "SELECT * FROM orders WHERE DATEDIFF(day, order_date, ship_date) > 5",
			expected: "select * from orders where DATE_DIFF('day', CAST(order_date AS DATE), CAST(ship_date AS DATE)) > 5",
			wantErr:  false,
		},
		{
			name:     "MultipleDATEDIFF",
			input:    "SELECT DATEDIFF(day, a, b), DATEDIFF(month, c, d) FROM test",
			expected: "select DATE_DIFF('day', CAST(a AS DATE), CAST(b AS DATE)), DATE_DIFF('month', CAST(c AS DATE), CAST(d AS DATE)) from test",
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

// TestTranslator_OBJECT_CONSTRUCT tests OBJECT_CONSTRUCT function translation.
// OBJECT_CONSTRUCT('key1', val1, ...) → json_object('key1', val1, ...) for DuckDB
func TestTranslator_OBJECT_CONSTRUCT(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SimpleOBJECT_CONSTRUCT",
			input:    "SELECT OBJECT_CONSTRUCT('key', value) FROM test",
			expected: "select json_object('key', value) from test",
			wantErr:  false,
		},
		{
			name:     "OBJECT_CONSTRUCTMultipleKeys",
			input:    "SELECT OBJECT_CONSTRUCT('name', name, 'age', age) FROM users",
			expected: "select json_object('name', name, 'age', age) from users",
			wantErr:  false,
		},
		{
			name:     "OBJECT_CONSTRUCTWithLiterals",
			input:    "SELECT OBJECT_CONSTRUCT('status', 'active', 'count', 42) FROM dual",
			expected: "select json_object('status', 'active', 'count', 42)",
			wantErr:  false,
		},
		{
			name:     "MultipleOBJECT_CONSTRUCT",
			input:    "SELECT OBJECT_CONSTRUCT('a', 1), OBJECT_CONSTRUCT('b', 2) FROM test",
			expected: "select json_object('a', 1), json_object('b', 2) from test",
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

// TestTranslator_FLATTEN tests FLATTEN function translation.
// Note: FLATTEN with => syntax is not supported by vitess-sqlparser,
// so it falls back to original SQL (graceful degradation).
// Simple FLATTEN(array) → UNNEST(array) for DuckDB
func TestTranslator_FLATTEN(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SimpleFLATTEN",
			input:    "SELECT FLATTEN(array_col) FROM test",
			expected: "select UNNEST(array_col) from test",
			wantErr:  false,
		},
		{
			name:     "FLATTENWithNamedParam_GracefulDegradation",
			input:    "SELECT * FROM TABLE(FLATTEN(input => array_col))",
			expected: "SELECT * FROM TABLE(FLATTEN(input => array_col))", // Parser fails on => syntax
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

// TestTranslator_LISTAGG tests LISTAGG function translation.
// LISTAGG(col, sep) → STRING_AGG(col, sep)
func TestTranslator_LISTAGG(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SimpleLISTAGG",
			input:    "SELECT LISTAGG(name, ',') FROM test GROUP BY category",
			expected: "select STRING_AGG(name, ',') from test group by category",
			wantErr:  false,
		},
		{
			name:     "LISTAGGWithSpace",
			input:    "SELECT LISTAGG(name, ', ') FROM test GROUP BY category",
			expected: "select STRING_AGG(name, ', ') from test group by category",
			wantErr:  false,
		},
		{
			name:     "MultipleLISTAGG",
			input:    "SELECT LISTAGG(a, '-'), LISTAGG(b, '|') FROM test GROUP BY c",
			expected: "select STRING_AGG(a, '-'), STRING_AGG(b, '|') from test group by c",
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

// TestTranslator_CombinedFunctions tests combinations of multiple translated functions.
func TestTranslator_CombinedFunctions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "NVL2WithDATEADD",
			input:    "SELECT NVL2(end_date, DATEADD(day, 7, end_date), CURRENT_DATE()) FROM projects",
			expected: "select IF(end_date is not null, (CAST(end_date AS DATE) + interval 7 day), CURRENT_DATE) from projects",
			wantErr:  false,
		},
		{
			name:     "PARSE_JSONWithNVL",
			input:    "SELECT NVL(PARSE_JSON(json_col), PARSE_JSON('{}')) FROM test",
			expected: "select COALESCE(CAST(json_col AS JSON), CAST('{}' AS JSON)) from test",
			wantErr:  false,
		},
		{
			name:     "IFFWithDATEDIFF",
			input:    "SELECT IFF(DATEDIFF(day, start_date, end_date) > 30, 'long', 'short') FROM events",
			expected: "select IF(DATE_DIFF('day', CAST(start_date AS DATE), CAST(end_date AS DATE)) > 30, 'long', 'short') from events",
			wantErr:  false,
		},
		{
			name:     "OBJECT_CONSTRUCTWithNVL",
			input:    "SELECT OBJECT_CONSTRUCT('name', NVL(name, 'Unknown')) FROM users",
			expected: "select json_object('name', COALESCE(name, 'Unknown')) from users",
			wantErr:  false,
		},
		{
			name:     "ComplexCombined",
			input:    "SELECT NVL2(data, TO_VARIANT(data), PARSE_JSON('null')) FROM test",
			expected: "select IF(data is not null, CAST(data AS JSON), CAST('null' AS JSON)) from test",
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
		name             string
		input            string
		wantErr          bool
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
			expected: "select CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_DATE",
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

// TestTranslator_InformationSchema tests that INFORMATION_SCHEMA queries work correctly.
// vitess-sqlparser adds backticks around reserved words like "tables" and "columns",
// which DuckDB rejects. The translator must strip them.
func TestTranslator_InformationSchema(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SelectAllFromInformationSchemaTables",
			input:    "SELECT * FROM INFORMATION_SCHEMA.TABLES",
			expected: "select * from INFORMATION_SCHEMA.tables",
		},
		{
			name:     "SelectColumnsFromInformationSchemaTables",
			input:    "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'",
			expected: "select TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = 'PUBLIC'",
		},
		{
			name:     "InformationSchemaColumns",
			input:    "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'USERS'",
			expected: "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'USERS'",
		},
		{
			name:     "InformationSchemaSchemata",
			input:    "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
			expected: "select SCHEMA_NAME from INFORMATION_SCHEMA.SCHEMATA",
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

// TestTranslator_TypeTranslation_DDL tests type translation in CREATE TABLE statements.
func TestTranslator_TypeTranslation_DDL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "CreateTableWithSnowflakeTypes",
			input:    "CREATE TABLE foo (col1 NUMBER, col2 TEXT, col3 TIMESTAMP_NTZ)",
			expected: "CREATE TABLE foo (col1 NUMERIC, col2 VARCHAR, col3 TIMESTAMP)",
		},
		{
			name:     "CreateTableWithTimestampVariants",
			input:    "CREATE TABLE foo (ts1 TIMESTAMP_NTZ, ts2 TIMESTAMP_LTZ, ts3 TIMESTAMP_TZ)",
			expected: "CREATE TABLE foo (ts1 TIMESTAMP, ts2 TIMESTAMPTZ, ts3 TIMESTAMPTZ)",
		},
		{
			name:     "CreateTableWithSemiStructured",
			input:    "CREATE TABLE foo (data VARIANT, meta OBJECT, items ARRAY)",
			expected: "CREATE TABLE foo (data JSON, meta JSON, items JSON)",
		},
		{
			name:     "CreateTableWithStringTypes",
			input:    "CREATE TABLE foo (a STRING, b TEXT, c CHAR, d CHARACTER)",
			expected: "CREATE TABLE foo (a VARCHAR, b VARCHAR, c VARCHAR, d VARCHAR)",
		},
		{
			name:     "CreateTableWithBinaryTypes",
			input:    "CREATE TABLE foo (a BINARY, b VARBINARY)",
			expected: "CREATE TABLE foo (a BLOB, b BLOB)",
		},
		{
			name:     "CreateTableWithFloatTypes",
			input:    "CREATE TABLE foo (a FLOAT4, b FLOAT8)",
			expected: "CREATE TABLE foo (a FLOAT, b DOUBLE)",
		},
		{
			name:     "CreateTableWithByteint",
			input:    "CREATE TABLE foo (a BYTEINT)",
			expected: "CREATE TABLE foo (a TINYINT)",
		},
		{
			name:     "CreateTableWithDatetime",
			input:    "CREATE TABLE foo (a DATETIME)",
			expected: "CREATE TABLE foo (a TIMESTAMP)",
		},
		{
			name:     "CreateTableWithParameterizedTypes",
			input:    "CREATE TABLE foo (a NUMBER(10,2), b TEXT)",
			expected: "CREATE TABLE foo (a NUMERIC(10,2), b VARCHAR)",
		},
		{
			name:     "CreateTablePreservesDuckDBTypes",
			input:    "CREATE TABLE foo (a INTEGER, b VARCHAR, c BOOLEAN, d DATE)",
			expected: "CREATE TABLE foo (a INTEGER, b VARCHAR, c BOOLEAN, d DATE)",
		},
		{
			name:     "CreateTableCaseInsensitive",
			input:    "CREATE TABLE foo (a number, b text, c timestamp_ntz)",
			expected: "CREATE TABLE foo (a NUMERIC, b VARCHAR, c TIMESTAMP)",
		},
		{
			name:     "CreateTableIfNotExists",
			input:    "CREATE TABLE IF NOT EXISTS foo (id NUMBER, name TEXT)",
			expected: "CREATE TABLE IF NOT EXISTS foo (id NUMERIC, name VARCHAR)",
		},
		{
			name:     "CreateTableMixedTypes",
			input:    "CREATE TABLE foo (id NUMBER, name TEXT, ts TIMESTAMP_NTZ, tz TIMESTAMP_TZ, data VARIANT)",
			expected: "CREATE TABLE foo (id NUMERIC, name VARCHAR, ts TIMESTAMP, tz TIMESTAMPTZ, data JSON)",
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

// TestTranslator_TypeTranslation_CAST tests type translation in CAST expressions.
// Note: vitess-sqlparser converts CAST(x AS type) to convert(x, type) syntax,
// but the type name translation still applies correctly.
func TestTranslator_TypeTranslation_CAST(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "CastToNumber",
			input:    "SELECT CAST('123' AS NUMBER)",
			expected: "select convert('123', NUMERIC)",
		},
		{
			name:     "CastToText",
			input:    "SELECT CAST(123 AS TEXT)",
			expected: "select convert(123, VARCHAR)",
		},
		{
			name:     "CastToTimestampNTZ",
			input:    "SELECT CAST(col AS TIMESTAMP_NTZ)",
			expected: "select convert(col, TIMESTAMP)",
		},
		{
			name:     "CastToVariant",
			input:    "SELECT CAST(col AS VARIANT)",
			expected: "select convert(col, JSON)",
		},
		{
			name:     "MultipleCasts",
			input:    "SELECT CAST(a AS NUMBER), CAST(b AS TEXT) FROM test",
			expected: "select convert(a, NUMERIC), convert(b, VARCHAR) from test",
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

// TestTranslator_TypeTranslation_ALTER tests type translation in ALTER TABLE statements.
func TestTranslator_TypeTranslation_ALTER(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "AlterTableAddColumn",
			input:    "ALTER TABLE foo ADD COLUMN data VARIANT",
			expected: "ALTER TABLE foo ADD COLUMN data JSON",
		},
		{
			name:     "AlterTableAddColumnNumber",
			input:    "ALTER TABLE foo ADD COLUMN score NUMBER",
			expected: "ALTER TABLE foo ADD COLUMN score NUMERIC",
		},
		{
			name:     "AlterTableAddColumnTimestamp",
			input:    "ALTER TABLE foo ADD COLUMN ts TIMESTAMP_NTZ",
			expected: "ALTER TABLE foo ADD COLUMN ts TIMESTAMP",
		},
		{
			name:     "AlterTableModifyColumn",
			input:    "ALTER TABLE foo MODIFY COLUMN name TEXT",
			expected: "ALTER TABLE foo MODIFY COLUMN name VARCHAR",
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

// TestTranslator_TypeTranslation_EdgeCases tests edge cases in type translation.
func TestTranslator_TypeTranslation_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "TypeInsideStringLiteral_NotReplaced",
			input:    "CREATE TABLE foo (name VARCHAR DEFAULT 'TEXT')",
			expected: "CREATE TABLE foo (name VARCHAR DEFAULT 'TEXT')",
		},
		{
			name:     "TypeInsideStringLiteral_Number",
			input:    "CREATE TABLE foo (name VARCHAR DEFAULT 'NUMBER')",
			expected: "CREATE TABLE foo (name VARCHAR DEFAULT 'NUMBER')",
		},
		{
			name:     "ColumnNameContainingTypeName",
			input:    "CREATE TABLE foo (text_column VARCHAR, number_value INTEGER)",
			expected: "CREATE TABLE foo (text_column VARCHAR, number_value INTEGER)",
		},
		{
			name:     "VarcharNotAffected",
			input:    "CREATE TABLE foo (a VARCHAR(100), b VARCHAR)",
			expected: "CREATE TABLE foo (a VARCHAR(100), b VARCHAR)",
		},
		{
			name:     "TimestampNotAffected_WhenBare",
			input:    "CREATE TABLE foo (a TIMESTAMP)",
			expected: "CREATE TABLE foo (a TIMESTAMP)",
		},
		{
			name:     "ObjectConstructNotAffected",
			input:    "CREATE TABLE foo (obj_construct_col VARCHAR)",
			expected: "CREATE TABLE foo (obj_construct_col VARCHAR)",
		},
		{
			name:     "MultipleStringLiterals",
			input:    "CREATE TABLE foo (a TEXT DEFAULT 'TEXT', b NUMBER DEFAULT 'NUMBER')",
			expected: "CREATE TABLE foo (a VARCHAR DEFAULT 'TEXT', b NUMERIC DEFAULT 'NUMBER')",
		},
		{
			name:     "DropTablePassthrough",
			input:    "DROP TABLE IF EXISTS foo",
			expected: "DROP TABLE IF EXISTS foo",
		},
		// DML edge cases: type names in non-type contexts should NOT be replaced
		{
			name:     "DML_StringLiteralInWhere_TEXT",
			input:    "SELECT * FROM foo WHERE category = 'TEXT'",
			expected: "select * from foo where category = 'TEXT'",
		},
		{
			name:     "DML_StringLiteralInWhere_NUMBER",
			input:    "SELECT * FROM foo WHERE type = 'NUMBER'",
			expected: "select * from foo where type = 'NUMBER'",
		},
		{
			name:     "DML_StringLiteralInWhere_VARIANT",
			input:    "SELECT * FROM foo WHERE kind = 'VARIANT'",
			expected: "select * from foo where kind = 'VARIANT'",
		},
		{
			name:     "DML_ColumnNamedText",
			input:    "SELECT text FROM documents WHERE id = 1",
			expected: "select text from documents where id = 1",
		},
		{
			name:     "DML_ColumnNamedNumber",
			input:    "SELECT number FROM items WHERE active = true",
			expected: "select number from items where active = true",
		},
		{
			name:     "DML_ColumnAliasMatchingType",
			input:    "SELECT name AS text FROM users",
			expected: "select name as text from users",
		},
		{
			name:     "DML_TypeNameInFunctionArg",
			input:    "SELECT UPPER(text) FROM documents",
			expected: "select UPPER(text) from documents",
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
