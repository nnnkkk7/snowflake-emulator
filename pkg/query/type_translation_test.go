package query

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTranslateTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// CREATE TABLE column definitions
		{
			name:     "CreateTableIssue45Types",
			input:    "CREATE TABLE t (id NUMBER, name TEXT, created TIMESTAMP_NTZ, local TIMESTAMP_LTZ, zoned TIMESTAMP_TZ)",
			expected: "CREATE TABLE t (id DECIMAL(38,0), name VARCHAR, created TIMESTAMP, local TIMESTAMPTZ, zoned TIMESTAMPTZ)",
		},
		{
			name:     "NumberWithPrecisionAndScale",
			input:    "CREATE TABLE t (a NUMBER(10,2), b NUMBER(10), c NUMERIC, d DECIMAL(5, 1))",
			expected: "CREATE TABLE t (a DECIMAL(10,2), b DECIMAL(10), c DECIMAL(38,0), d DECIMAL(5, 1))",
		},
		{
			name:     "FloatTypesAreDouble",
			input:    "CREATE TABLE t (a FLOAT, b FLOAT4, c FLOAT8, d REAL, e DOUBLE PRECISION, f DOUBLE)",
			expected: "CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e DOUBLE, f DOUBLE)",
		},
		{
			name:     "StringTypes",
			input:    "CREATE TABLE t (a STRING, b CHAR(10), c CHARACTER, d NCHAR VARYING(5), e CHAR VARYING, f NVARCHAR2(3), g VARCHAR(20))",
			expected: "CREATE TABLE t (a VARCHAR, b VARCHAR(10), c VARCHAR, d VARCHAR(5), e VARCHAR, f VARCHAR(3), g VARCHAR(20))",
		},
		{
			name:     "BinarySemiStructuredAndMisc",
			input:    "CREATE TABLE t (a BINARY(16), b VARBINARY, c VARIANT, d OBJECT, e ARRAY, f BYTEINT, g DATETIME, h TIMESTAMP_NTZ(9), i TIME(3))",
			expected: "CREATE TABLE t (a BLOB, b BLOB, c JSON, d JSON, e JSON, f TINYINT, g TIMESTAMP, h TIMESTAMP, i TIME)",
		},
		{
			name:     "DuckDBTypesUnchanged",
			input:    "CREATE TABLE t (a INTEGER, b VARCHAR, c BOOLEAN, d DATE, e TIMESTAMP, f TIMESTAMP WITH TIME ZONE)",
			expected: "CREATE TABLE t (a INTEGER, b VARCHAR, c BOOLEAN, d DATE, e TIMESTAMP, f TIMESTAMP WITH TIME ZONE)",
		},
		{
			name:     "CaseInsensitive",
			input:    "create table t (id number, name text)",
			expected: "create table t (id DECIMAL(38,0), name VARCHAR)",
		},
		{
			name:     "ColumnsNamedLikeTypes",
			input:    "CREATE TABLE text (number INTEGER, text TEXT, \"number\" NUMBER, array ARRAY)",
			expected: "CREATE TABLE text (number INTEGER, text VARCHAR, \"number\" DECIMAL(38,0), array JSON)",
		},
		{
			name:     "ConstraintsAndDefaults",
			input:    "CREATE OR REPLACE TABLE IF NOT EXISTS t (id NUMBER NOT NULL DEFAULT 0, note TEXT DEFAULT 'TEXT', PRIMARY KEY (id), CONSTRAINT u UNIQUE (note))",
			expected: "CREATE OR REPLACE TABLE IF NOT EXISTS t (id DECIMAL(38,0) NOT NULL DEFAULT 0, note VARCHAR DEFAULT 'TEXT', PRIMARY KEY (id), CONSTRAINT u UNIQUE (note))",
		},
		{
			name:     "CreateTableAsSelectKeepsColumnNames",
			input:    "CREATE TABLE t2 AS SELECT text, number FROM docs",
			expected: "CREATE TABLE t2 AS SELECT text, number FROM docs",
		},
		{
			name:     "CreateTableAsSelectWithCast",
			input:    "CREATE TABLE t2 AS SELECT CAST(text AS NUMBER) AS number FROM docs",
			expected: "CREATE TABLE t2 AS SELECT CAST(text AS DECIMAL(38,0)) AS number FROM docs",
		},
		{
			name:     "CreateViewUnchanged",
			input:    "CREATE VIEW v (text, number) AS SELECT a, b FROM t",
			expected: "CREATE VIEW v (text, number) AS SELECT a, b FROM t",
		},

		// ALTER TABLE
		{
			name:     "AlterTableAddColumn",
			input:    "ALTER TABLE t ADD COLUMN amount NUMBER(12,2)",
			expected: "ALTER TABLE t ADD COLUMN amount DECIMAL(12,2)",
		},
		{
			name:     "AlterTableAddWithoutColumnKeyword",
			input:    "ALTER TABLE t ADD IF NOT EXISTS text TEXT, payload VARIANT",
			expected: "ALTER TABLE t ADD IF NOT EXISTS text VARCHAR, payload JSON",
		},
		{
			name:     "AlterTableSetDataType",
			input:    "ALTER TABLE t ALTER COLUMN c SET DATA TYPE NUMBER(10,2)",
			expected: "ALTER TABLE t ALTER COLUMN c SET DATA TYPE DECIMAL(10,2)",
		},
		{
			name:     "AlterTableRenameKeepsIdentifiers",
			input:    "ALTER TABLE text RENAME COLUMN number TO string",
			expected: "ALTER TABLE text RENAME COLUMN number TO string",
		},

		// CAST and ::
		{
			name:     "Cast",
			input:    "SELECT CAST(x AS NUMBER), TRY_CAST(y AS TEXT) FROM t",
			expected: "SELECT CAST(x AS DECIMAL(38,0)), TRY_CAST(y AS VARCHAR) FROM t",
		},
		{
			name:     "CastWithArgs",
			input:    "SELECT CAST(x AS NUMBER(10,2)) FROM t",
			expected: "SELECT CAST(x AS DECIMAL(10,2)) FROM t",
		},
		{
			name:     "NestedCast",
			input:    "SELECT CAST(CAST(a AS NUMBER) AS TEXT) FROM t",
			expected: "SELECT CAST(CAST(a AS DECIMAL(38,0)) AS VARCHAR) FROM t",
		},
		{
			name:     "CastWithStringContainingParen",
			input:    "SELECT CAST('unmatched (' AS TEXT), CAST(x AS NUMBER) FROM t",
			expected: "SELECT CAST('unmatched (' AS VARCHAR), CAST(x AS DECIMAL(38,0)) FROM t",
		},
		{
			name:     "CastExpressionWithAlias",
			input:    "SELECT CAST(x AS VARIANT) AS text FROM t",
			expected: "SELECT CAST(x AS JSON) AS text FROM t",
		},
		{
			name:     "DoubleColonCast",
			input:    "SELECT text, x::NUMBER, y::TIMESTAMP_NTZ FROM docs",
			expected: "SELECT text, x::DECIMAL(38,0), y::TIMESTAMP FROM docs",
		},

		// Identifiers and literals that look like types
		{
			name:     "SelectColumnsNamedLikeTypes",
			input:    "SELECT text, number, object, array FROM docs QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1",
			expected: "SELECT text, number, object, array FROM docs QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1",
		},
		{
			name:     "FunctionNamedLikeCast",
			input:    "SELECT myconvert(a, text), mycast(b AS number) FROM t",
			expected: "SELECT myconvert(a, text), mycast(b AS number) FROM t",
		},
		{
			name:     "StringLiterals",
			input:    "SELECT 'CAST(x AS NUMBER)', 'it''s ::TEXT', $$CAST(y AS TEXT)$$ FROM t",
			expected: "SELECT 'CAST(x AS NUMBER)', 'it''s ::TEXT', $$CAST(y AS TEXT)$$ FROM t",
		},
		{
			name:     "Comments",
			input:    "CREATE TABLE t ( -- id NUMBER\n id NUMBER /* name TEXT */)",
			expected: "CREATE TABLE t ( -- id NUMBER\n id DECIMAL(38,0) /* name TEXT */)",
		},
		{
			name:     "DropTableNamedLikeType",
			input:    "DROP TABLE text",
			expected: "DROP TABLE text",
		},
		{
			name:     "MultiStatementStartingWithDrop",
			input:    "DROP SCHEMA IF EXISTS s; CREATE SCHEMA s; CREATE TABLE s.t (id NUMBER, name TEXT); INSERT INTO s.t SELECT 1, 'TEXT'",
			expected: "DROP SCHEMA IF EXISTS s; CREATE SCHEMA s; CREATE TABLE s.t (id DECIMAL(38,0), name VARCHAR); INSERT INTO s.t SELECT 1, 'TEXT'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.expected, translateTypes(tt.input)); diff != "" {
				t.Errorf("translateTypes() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRewriteConvertToCast(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"Simple", "select convert(x, DECIMAL(38, 0)) from t", "select CAST(x AS DECIMAL(38, 0)) from t"},
		{"Nested", "select convert(convert(a, DECIMAL(38, 0)), JSON) from t", "select CAST(CAST(a AS DECIMAL(38, 0)) AS JSON) from t"},
		{"ExpressionArg", "select convert(f(a, b), date) from t", "select CAST(f(a, b) AS date) from t"},
		{"UsingCharsetUnchanged", "select convert(x using utf8) from t", "select convert(x using utf8) from t"},
		{"StringLiteralUnchanged", "select 'convert(x, date)' from t", "select 'convert(x, date)' from t"},
		{"OtherFunctionUnchanged", "select myconvert(x, date) from t", "select myconvert(x, date) from t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.expected, rewriteConvertToCast(tt.input)); diff != "" {
				t.Errorf("rewriteConvertToCast() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTokenizeSQL_RoundTrip(t *testing.T) {
	inputs := []string{
		"SELECT 'a''b', \"Quoted \"\"Id\"\"\", $$dollar ' text$$, x::NUMBER -- tail",
		"/* unterminated comment",
		"SELECT 'unterminated",
		"SELECT 'back\\'slash' FROM t; SELECT 1",
	}
	for _, in := range inputs {
		var b strings.Builder
		for _, tok := range tokenizeSQL(in) {
			b.WriteString(tok.text)
		}
		if b.String() != in {
			t.Errorf("tokenizeSQL round trip = %q, want %q", b.String(), in)
		}
	}
}

// TestExecutor_SnowflakeTypes runs translated SQL against DuckDB and checks the resulting types.
func TestExecutor_SnowflakeTypes(t *testing.T) {
	executor, _ := setupTestExecutor(t)
	ctx := context.Background()

	ddl := `CREATE TABLE sf_types (
		id NUMBER,
		amount NUMBER(10,2),
		ratio FLOAT,
		name TEXT,
		code STRING,
		created_at TIMESTAMP_NTZ,
		local_at TIMESTAMP_LTZ,
		zoned_at TIMESTAMP_TZ,
		payload VARIANT,
		raw BINARY,
		text TEXT
	)`
	if _, err := executor.Execute(ctx, ddl); err != nil {
		t.Fatalf("CREATE TABLE with Snowflake types failed: %v", err)
	}
	if _, err := executor.Execute(ctx, "ALTER TABLE sf_types ADD COLUMN extra NUMBER(5,1)"); err != nil {
		t.Fatalf("ALTER TABLE with Snowflake types failed: %v", err)
	}
	if _, err := executor.Execute(ctx, "INSERT INTO sf_types (id, amount, ratio, name, text, extra) VALUES (1, 12.5, 0.1, 'a', 'body', 1.5)"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	result, err := executor.Query(ctx, `SELECT typeof(id), typeof(amount), typeof(ratio), typeof(name), typeof(code),
		typeof(created_at), typeof(local_at), typeof(zoned_at), typeof(payload), typeof(raw), typeof(extra), text
		FROM sf_types`)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	want := []interface{}{
		"DECIMAL(38,0)", "DECIMAL(10,2)", "DOUBLE", "VARCHAR", "VARCHAR",
		"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", "JSON", "BLOB", "DECIMAL(5,1)", "body",
	}
	if diff := cmp.Diff(want, result.Rows[0]); diff != "" {
		t.Errorf("column types mismatch (-want +got):\n%s", diff)
	}

	casts, err := executor.Query(ctx, "SELECT CAST('12.34' AS NUMBER(10,2)) AS a, CAST(CAST('7' AS NUMBER) AS TEXT) AS b, CAST('2024-01-15' AS DATE) AS c")
	if err != nil {
		t.Fatalf("CAST query failed: %v", err)
	}
	if casts.ColumnTypes[0].Type != "NUMBER" {
		t.Errorf("CAST AS NUMBER(10,2) column type = %q, want NUMBER", casts.ColumnTypes[0].Type)
	}
	if casts.Rows[0][0] != "12.34" {
		t.Errorf("CAST AS NUMBER(10,2) value = %#v, want \"12.34\"", casts.Rows[0][0])
	}
	if casts.Rows[0][1] != "7" {
		t.Errorf("nested CAST = %v, want \"7\"", casts.Rows[0][1])
	}
}
