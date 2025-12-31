// Package query provides SQL query execution against DuckDB with Snowflake SQL translation.
package query

import (
	"strings"
)

// DefaultTableNamer implements TableNamer interface with Snowflake naming conventions.
type DefaultTableNamer struct{}

// NewTableNamer creates a new table namer.
func NewTableNamer() *DefaultTableNamer {
	return &DefaultTableNamer{}
}

// BuildDuckDBTableName constructs a DuckDB table name from Snowflake components.
// Pattern: DATABASE.SCHEMA_TABLE (per CLAUDE.md convention)
//
// Examples:
//   - BuildDuckDBTableName("TEST_DB", "PUBLIC", "USERS") -> "TEST_DB.PUBLIC_USERS"
//   - BuildDuckDBTableName("", "PUBLIC", "USERS") -> "PUBLIC_USERS"
//   - BuildDuckDBTableName("", "", "USERS") -> "USERS"
func (n *DefaultTableNamer) BuildDuckDBTableName(database, schema, table string) string {
	// Normalize to uppercase (Snowflake convention for unquoted identifiers)
	database = strings.ToUpper(strings.TrimSpace(database))
	schema = strings.ToUpper(strings.TrimSpace(schema))
	table = strings.ToUpper(strings.TrimSpace(table))

	// Build name based on available components
	if database != "" && schema != "" {
		// Fully qualified: DATABASE.SCHEMA_TABLE
		return database + "." + schema + "_" + table
	}
	if schema != "" {
		// Schema qualified: SCHEMA_TABLE
		return schema + "_" + table
	}
	// Table only
	return table
}

// ParseTableReference parses a table reference into database, schema, and table components.
// Handles formats: table, schema.table, database.schema.table
func (n *DefaultTableNamer) ParseTableReference(ref string) (database, schema, table string) {
	ref = strings.TrimSpace(ref)
	parts := strings.Split(ref, ".")

	switch len(parts) {
	case 1:
		return "", "", strings.ToUpper(parts[0])
	case 2:
		return "", strings.ToUpper(parts[0]), strings.ToUpper(parts[1])
	case 3:
		return strings.ToUpper(parts[0]), strings.ToUpper(parts[1]), strings.ToUpper(parts[2])
	default:
		// For invalid formats, return as table name
		return "", "", strings.ToUpper(ref)
	}
}

// defaultTableNamer is the package-level instance for convenience functions.
var defaultTableNamer = NewTableNamer()

// BuildTableName is a convenience function that uses the default table namer.
func BuildTableName(database, schema, table string) string {
	return defaultTableNamer.BuildDuckDBTableName(database, schema, table)
}

// ParseTableRef is a convenience function that uses the default table namer.
func ParseTableRef(ref string) (database, schema, table string) {
	return defaultTableNamer.ParseTableReference(ref)
}
