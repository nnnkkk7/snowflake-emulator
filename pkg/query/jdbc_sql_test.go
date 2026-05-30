package query

import (
	"strings"
	"testing"
)

func TestNormalizeJDBCMetadataSQL(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		contains string
	}{
		{
			name:     "show tables in schema with quoted identifiers",
			in:       `show /* JDBC:DatabaseMetaData.getTables() */ tables in schema "TEST_DB"."PUBLIC"`,
			contains: "_metadata_tables",
		},
		{
			name:     "show objects in account",
			in:       `show /* JDBC:DatabaseMetaData.getTables() */ objects in account`,
			contains: "_metadata_tables",
		},
		{
			name:     "show objects in database",
			in:       `show /* JDBC:DatabaseMetaData.getTables() */ objects in database "TEST_DB"`,
			contains: "UPPER(d.name) = 'TEST_DB'",
		},
		{
			name:     "show databases in account",
			in:       `show /* JDBC:DatabaseMetaData.getCatalogs() */ databases in account`,
			contains: "_metadata_databases",
		},
		{
			name:     "show schemas in account",
			in:       `show /* JDBC:DatabaseMetaData.getSchemas() */ schemas in account`,
			contains: "is_default",
		},
		{
			name:     "show schemas in database",
			in:       `show /* JDBC:DatabaseMetaData.getSchemas() */ schemas in database "TEST_DB"`,
			contains: `"database_name"`,
		},
		{
			name:     "show columns in account",
			in:       `show /* JDBC:DatabaseMetaData.getColumns() */ columns in account`,
			contains: "column_name",
		},
		{
			name:     "show procedures in account",
			in:       `show /* JDBC:DatabaseMetaData.getProcedures() */ procedures in account`,
			contains: "WHERE FALSE",
		},
		{
			name:     "passthrough select",
			in:       "SELECT 1",
			contains: "SELECT 1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NormalizeJDBCMetadataSQL(tc.in)
			if !strings.Contains(strings.ToUpper(got), strings.ToUpper(tc.contains)) {
				t.Fatalf("NormalizeJDBCMetadataSQL() = %q, want substring %q", got, tc.contains)
			}
		})
	}
}
