// Package types provides Snowflake data type definitions and mappings to DuckDB.
package types

// SnowflakeType represents a Snowflake data type.
// This type is used for type conversion between Snowflake and DuckDB.
type SnowflakeType string

// Snowflake data type constants
const (
	// Numeric types
	TypeNumber  SnowflakeType = "NUMBER"
	TypeInteger SnowflakeType = "INTEGER"
	TypeFloat   SnowflakeType = "FLOAT"

	// String types
	TypeVarchar SnowflakeType = "VARCHAR"

	// Boolean type
	TypeBoolean SnowflakeType = "BOOLEAN"

	// Temporal types
	TypeDate         SnowflakeType = "DATE"
	TypeTime         SnowflakeType = "TIME"
	TypeTimestamp    SnowflakeType = "TIMESTAMP"
	TypeTimestampLTZ SnowflakeType = "TIMESTAMP_LTZ"
	TypeTimestampTZ  SnowflakeType = "TIMESTAMP_TZ"

	// Semi-structured types (Phase 2)
	TypeVariant SnowflakeType = "VARIANT"
	TypeObject  SnowflakeType = "OBJECT"
	TypeArray   SnowflakeType = "ARRAY"

	// Binary type
	TypeBinary SnowflakeType = "BINARY"

	// Geographic types
	TypeGeography SnowflakeType = "GEOGRAPHY"
)

// ToDuckDBType converts a Snowflake type to the corresponding DuckDB type.
//
// Returns:
//   - DuckDB type name as string
//   - For unknown types, returns "VARCHAR" as a safe default
func (t SnowflakeType) ToDuckDBType() string {
	switch t {
	case TypeNumber:
		return "DOUBLE"
	case TypeInteger:
		return "BIGINT"
	case TypeFloat:
		return "DOUBLE"
	case TypeVarchar:
		return "VARCHAR"
	case TypeBoolean:
		return "BOOLEAN"
	case TypeDate:
		return "DATE"
	case TypeTime:
		return "TIME"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeTimestampLTZ, TypeTimestampTZ:
		return "TIMESTAMPTZ"
	case TypeVariant, TypeObject:
		return "JSON"
	case TypeArray:
		return "JSON" // DuckDB LIST requires element type, so we use JSON for flexibility
	case TypeBinary:
		return "BLOB"
	case TypeGeography:
		return "VARCHAR" // Store WKT (Well-Known Text) format
	default:
		return "VARCHAR" // Safe default for unknown types
	}
}

// IsNumeric returns true if the type is a numeric type (NUMBER, INTEGER, FLOAT).
func (t SnowflakeType) IsNumeric() bool {
	switch t {
	case TypeNumber, TypeInteger, TypeFloat:
		return true
	default:
		return false
	}
}

// IsString returns true if the type is a string type (VARCHAR).
func (t SnowflakeType) IsString() bool {
	return t == TypeVarchar
}

// IsTemporal returns true if the type is a date/time type.
func (t SnowflakeType) IsTemporal() bool {
	switch t {
	case TypeDate, TypeTime, TypeTimestamp, TypeTimestampLTZ, TypeTimestampTZ:
		return true
	default:
		return false
	}
}
