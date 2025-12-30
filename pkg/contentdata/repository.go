// Package contentdata manages actual table data in DuckDB.
package contentdata

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

// Repository manages actual table data in DuckDB.
// It is separate from metadata.Repository which only manages schema metadata.
type Repository struct {
	mgr      *connection.Manager
	metaRepo *metadata.Repository
}

// NewRepository creates a new content data repository.
func NewRepository(mgr *connection.Manager, metaRepo *metadata.Repository) *Repository {
	return &Repository{
		mgr:      mgr,
		metaRepo: metaRepo,
	}
}

// CreateTable creates the actual DuckDB table for storing data.
// This is called after metadata.Repository.CreateTable creates the metadata entry.
func (r *Repository) CreateTable(ctx context.Context, databaseName, schemaName, tableName string, columns []metadata.ColumnDef) error {
	// Resolve fully qualified table name with schema prefix
	fqtn, err := r.resolveTableName(ctx, databaseName, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("failed to resolve table name: %w", err)
	}

	// Build column definitions for DuckDB
	colDefs := make([]string, len(columns))
	for i, col := range columns {
		duckType := snowflakeToDuckDBType(col.Type)
		nullable := "NULL"
		if !col.Nullable {
			nullable = "NOT NULL"
		}

		colDef := fmt.Sprintf("%s %s %s", col.Name, duckType, nullable)

		// Add PRIMARY KEY constraint
		if col.PrimaryKey {
			colDef += " PRIMARY KEY"
		}

		colDefs[i] = colDef
	}

	// Create table SQL
	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", fqtn, strings.Join(colDefs, ", "))

	// Execute creation
	if _, err := r.mgr.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to create DuckDB table: %w", err)
	}

	return nil
}

// DropTable drops the actual DuckDB table.
// This should be called after verifying metadata allows the drop.
func (r *Repository) DropTable(ctx context.Context, databaseName, schemaName, tableName string) error {
	// Resolve fully qualified table name with schema prefix
	fqtn, err := r.resolveTableName(ctx, databaseName, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("failed to resolve table name: %w", err)
	}

	// Drop table SQL
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", fqtn)

	// Execute drop
	if _, err := r.mgr.Exec(ctx, dropSQL); err != nil {
		return fmt.Errorf("failed to drop DuckDB table: %w", err)
	}

	return nil
}

// InsertData inserts rows into a table.
func (r *Repository) InsertData(ctx context.Context, databaseName, schemaName, tableName string, columns []string, values [][]interface{}) (int64, error) {
	// Resolve fully qualified table name
	fqtn, err := r.resolveTableName(ctx, databaseName, schemaName, tableName)
	if err != nil {
		return 0, fmt.Errorf("failed to resolve table name: %w", err)
	}

	if len(values) == 0 {
		return 0, nil
	}

	// Build INSERT statement
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		fqtn,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Execute inserts
	var totalRows int64
	for _, row := range values {
		result, err := r.mgr.Exec(ctx, insertSQL, row...)
		if err != nil {
			return totalRows, fmt.Errorf("failed to insert row: %w", err)
		}
		affected, _ := result.RowsAffected()
		totalRows += affected
	}

	return totalRows, nil
}

// ExecuteQuery executes a SELECT query and returns the result set.
func (r *Repository) ExecuteQuery(ctx context.Context, query string) (*sql.Rows, error) {
	rows, err := r.mgr.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	return rows, nil
}

// ExecuteDML executes an INSERT/UPDATE/DELETE statement and returns rows affected.
func (r *Repository) ExecuteDML(ctx context.Context, statement string, args ...interface{}) (int64, error) {
	result, err := r.mgr.Exec(ctx, statement, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute DML: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// resolveTableName resolves the fully qualified table name with schema prefix.
// Pattern: {database}.{schema}_{table}
func (r *Repository) resolveTableName(ctx context.Context, databaseName, schemaName, tableName string) (string, error) {
	// Normalize names to uppercase (Snowflake convention)
	databaseName = strings.ToUpper(databaseName)
	schemaName = strings.ToUpper(schemaName)
	tableName = strings.ToUpper(tableName)

	// Verify database exists in metadata
	db, err := r.metaRepo.GetDatabaseByName(ctx, databaseName)
	if err != nil {
		return "", fmt.Errorf("database not found: %w", err)
	}

	// Verify schema exists in metadata
	schemas, err := r.metaRepo.ListSchemas(ctx, db.ID)
	if err != nil {
		return "", fmt.Errorf("failed to list schemas: %w", err)
	}

	var schema *metadata.Schema
	for _, s := range schemas {
		if s.Name == schemaName {
			schema = s
			break
		}
	}

	if schema == nil {
		return "", fmt.Errorf("schema %s not found in database %s", schemaName, databaseName)
	}

	// Build fully qualified name with schema prefix
	// This matches the pattern used in metadata.Repository.CreateTable
	fqtn := fmt.Sprintf("%s.%s_%s", db.Name, schema.Name, tableName)

	return fqtn, nil
}

// snowflakeToDuckDBType maps Snowflake data types to DuckDB types.
func snowflakeToDuckDBType(snowflakeType string) string {
	// Normalize to uppercase
	sfType := strings.ToUpper(snowflakeType)

	// Map Snowflake types to DuckDB types
	mapping := map[string]string{
		// Numeric types
		"NUMBER":   "DECIMAL",
		"INT":      "INTEGER",
		"INTEGER":  "INTEGER",
		"BIGINT":   "BIGINT",
		"SMALLINT": "SMALLINT",
		"TINYINT":  "TINYINT",
		"BYTEINT":  "TINYINT",
		"FLOAT":    "DOUBLE",
		"FLOAT4":   "FLOAT",
		"FLOAT8":   "DOUBLE",
		"DOUBLE":   "DOUBLE",
		"REAL":     "FLOAT",
		"DECIMAL":  "DECIMAL",
		"NUMERIC":  "DECIMAL",

		// String types
		"VARCHAR":   "VARCHAR",
		"CHAR":      "VARCHAR",
		"CHARACTER": "VARCHAR",
		"STRING":    "VARCHAR",
		"TEXT":      "VARCHAR",
		"BINARY":    "BLOB",
		"VARBINARY": "BLOB",

		// Boolean
		"BOOLEAN": "BOOLEAN",

		// Date/Time types
		"DATE":          "DATE",
		"DATETIME":      "TIMESTAMP",
		"TIME":          "TIME",
		"TIMESTAMP":     "TIMESTAMP",
		"TIMESTAMP_LTZ": "TIMESTAMP WITH TIME ZONE",
		"TIMESTAMP_NTZ": "TIMESTAMP",
		"TIMESTAMP_TZ":  "TIMESTAMP WITH TIME ZONE",

		// Semi-structured types (DuckDB doesn't have native JSON, use VARCHAR)
		"VARIANT": "VARCHAR",
		"OBJECT":  "VARCHAR",
		"ARRAY":   "VARCHAR",
	}

	if duckType, ok := mapping[sfType]; ok {
		return duckType
	}

	// Default to VARCHAR for unknown types
	return "VARCHAR"
}
