// Package query provides SQL query execution and type mapping.
package query

import (
	"database/sql"

	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

// TypeMapper provides DuckDB to Snowflake type mapping functionality.
type TypeMapper struct {
	typeMapping map[string]string
}

// NewTypeMapper creates a new type mapper with default mappings.
func NewTypeMapper() *TypeMapper {
	return &TypeMapper{
		typeMapping: map[string]string{
			"BIGINT":       "NUMBER",
			"INTEGER":      "NUMBER",
			"INT":          "NUMBER",
			"SMALLINT":     "NUMBER",
			"TINYINT":      "NUMBER",
			"HUGEINT":      "NUMBER",
			"DOUBLE":       "FLOAT",
			"FLOAT":        "FLOAT",
			"REAL":         "FLOAT",
			"VARCHAR":      "TEXT",
			"TEXT":         "TEXT",
			"STRING":       "TEXT",
			"TIMESTAMP":    "TIMESTAMP_NTZ",
			"TIMESTAMP_NS": "TIMESTAMP_NTZ",
			"TIMESTAMP_MS": "TIMESTAMP_NTZ",
			"TIMESTAMP_S":  "TIMESTAMP_NTZ",
			"TIMESTAMPTZ":  "TIMESTAMP_TZ",
			"DATE":         "DATE",
			"TIME":         "TIME",
			"BOOLEAN":      "BOOLEAN",
			"BOOL":         "BOOLEAN",
			"DECIMAL":      "NUMBER",
			"NUMERIC":      "NUMBER",
			"BLOB":         "BINARY",
			"BYTEA":        "BINARY",
			"UUID":         "TEXT",
			"INTERVAL":     "TEXT",
			"JSON":         "VARIANT",
			"LIST":         "ARRAY",
			"STRUCT":       "OBJECT",
			"MAP":          "OBJECT",
		},
	}
}

// MapDuckDBType converts a DuckDB type to its Snowflake equivalent.
func (m *TypeMapper) MapDuckDBType(duckType string) string {
	if sfType, ok := m.typeMapping[duckType]; ok {
		return sfType
	}
	return "TEXT" // Default fallback
}

// InferRowType generates column metadata from column names and optional sql.Rows.
func (m *TypeMapper) InferRowType(columns []string, rows *sql.Rows) []types.ColumnMetadata {
	rowType := make([]types.ColumnMetadata, len(columns))

	for i, col := range columns {
		meta := types.ColumnMetadata{
			Name:     col,
			Type:     "TEXT", // Default type
			Nullable: true,
		}

		// If we have rows, try to infer types from column types
		if rows != nil {
			columnTypes, err := rows.ColumnTypes()
			if err == nil && i < len(columnTypes) {
				dbType := columnTypes[i].DatabaseTypeName()
				meta.Type = m.MapDuckDBType(dbType)

				if length, ok := columnTypes[i].Length(); ok {
					meta.Length = length
				}
				if precision, scale, ok := columnTypes[i].DecimalSize(); ok {
					meta.Precision = precision
					meta.Scale = scale
				}
				if nullable, ok := columnTypes[i].Nullable(); ok {
					meta.Nullable = nullable
				}
			}
		}

		rowType[i] = meta
	}

	return rowType
}

// DefaultTypeMapper is the default type mapper instance.
var DefaultTypeMapper = NewTypeMapper()

// MapDuckDBTypeToSnowflake is a convenience function using the default mapper.
func MapDuckDBTypeToSnowflake(duckType string) string {
	return DefaultTypeMapper.MapDuckDBType(duckType)
}

// InferColumnMetadata is a convenience function using the default mapper.
func InferColumnMetadata(columns []string, rows *sql.Rows) []types.ColumnMetadata {
	return DefaultTypeMapper.InferRowType(columns, rows)
}
