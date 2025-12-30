package query

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTypeMapper_MapDuckDBType(t *testing.T) {
	mapper := NewTypeMapper()

	testCases := []struct {
		duckType     string
		expectedType string
	}{
		{"BIGINT", "NUMBER"},
		{"INTEGER", "NUMBER"},
		{"INT", "NUMBER"},
		{"SMALLINT", "NUMBER"},
		{"DOUBLE", "FLOAT"},
		{"FLOAT", "FLOAT"},
		{"VARCHAR", "TEXT"},
		{"TEXT", "TEXT"},
		{"STRING", "TEXT"},
		{"TIMESTAMP", "TIMESTAMP_NTZ"},
		{"TIMESTAMPTZ", "TIMESTAMP_TZ"},
		{"DATE", "DATE"},
		{"TIME", "TIME"},
		{"BOOLEAN", "BOOLEAN"},
		{"BOOL", "BOOLEAN"},
		{"DECIMAL", "NUMBER"},
		{"BLOB", "BINARY"},
		{"JSON", "VARIANT"},
		{"LIST", "ARRAY"},
		{"STRUCT", "OBJECT"},
		{"MAP", "OBJECT"},
		{"UNKNOWN_TYPE", "TEXT"}, // fallback
	}

	for _, tc := range testCases {
		t.Run(tc.duckType, func(t *testing.T) {
			result := mapper.MapDuckDBType(tc.duckType)
			if diff := cmp.Diff(tc.expectedType, result); diff != "" {
				t.Errorf("MapDuckDBType(%s) mismatch (-want +got):\n%s", tc.duckType, diff)
			}
		})
	}
}

func TestInferColumnMetadata_WithoutRows(t *testing.T) {
	columns := []string{"id", "name", "created_at"}
	result := InferColumnMetadata(columns, nil)

	if len(result) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(result))
	}

	// Without rows, all types default to TEXT
	for i, col := range result {
		if col.Name != columns[i] {
			t.Errorf("column %d: expected name %s, got %s", i, columns[i], col.Name)
		}
		if col.Type != "TEXT" {
			t.Errorf("column %d: expected type TEXT (default), got %s", i, col.Type)
		}
		if !col.Nullable {
			t.Errorf("column %d: expected nullable true, got false", i)
		}
	}
}
