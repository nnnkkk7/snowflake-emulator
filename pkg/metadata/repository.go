// Package metadata provides metadata management for Snowflake databases, schemas, and tables.
package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
)

// Repository manages Snowflake metadata (databases, schemas, tables) in DuckDB.
// Metadata is stored in special tables prefixed with _metadata_.
type Repository struct {
	mgr *connection.Manager
}

// Database represents a Snowflake database.
type Database struct {
	ID        string
	Name      string
	AccountID string // Snowflake account identifier
	Comment   string
	CreatedAt time.Time
	Owner     string
}

// Schema represents a Snowflake schema.
type Schema struct {
	ID         string
	DatabaseID string
	Name       string
	Comment    string
	CreatedAt  time.Time
	Owner      string
}

// Table represents a Snowflake table.
type Table struct {
	ID                string
	SchemaID          string
	Name              string
	TableType         string // BASE TABLE, VIEW, TEMPORARY, EXTERNAL
	Comment           string
	CreatedAt         time.Time
	Owner             string
	ClusteringKey     string
	ColumnDefinitions string // JSON string
}

// ColumnDef represents a table column definition.
type ColumnDef struct {
	Name       string
	Type       string
	Nullable   bool
	Default    *string
	PrimaryKey bool
}

// Stage represents a Snowflake stage for data loading.
type Stage struct {
	ID        string
	SchemaID  string
	Name      string
	StageType string // INTERNAL, EXTERNAL
	URL       string // For external stages
	Comment   string
	CreatedAt time.Time
	Owner     string
}

// FileFormat represents a Snowflake file format.
type FileFormat struct {
	ID         string
	SchemaID   string
	Name       string
	FormatType string // CSV, JSON, PARQUET
	Options    string // JSON encoded options
	Comment    string
	CreatedAt  time.Time
	Owner      string
}

// QueryHistoryEntry represents a query execution record.
type QueryHistoryEntry struct {
	ID              string
	SessionID       string
	QueryID         string
	SQLText         string
	Status          string // RUNNING, SUCCESS, FAILED, CANCELED
	RowsAffected    int64
	ExecutionTimeMs int64
	ErrorMessage    string
	StartedAt       time.Time
	CompletedAt     *time.Time
}

// NewRepository creates a new metadata repository.
// It initializes metadata tables if they don't exist.
func NewRepository(mgr *connection.Manager) (*Repository, error) {
	repo := &Repository{mgr: mgr}

	// Initialize metadata tables
	if err := repo.initMetadataTables(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize metadata tables: %w", err)
	}

	return repo, nil
}

// initMetadataTables creates metadata tables if they don't exist.
func (r *Repository) initMetadataTables(ctx context.Context) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS _metadata_databases (
			id VARCHAR PRIMARY KEY,
			name VARCHAR NOT NULL,
			account_id VARCHAR,
			comment VARCHAR,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			owner VARCHAR,
			UNIQUE(name)
		)`,
		`CREATE TABLE IF NOT EXISTS _metadata_schemas (
			id VARCHAR PRIMARY KEY,
			database_id VARCHAR NOT NULL,
			name VARCHAR NOT NULL,
			comment VARCHAR,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			owner VARCHAR,
			UNIQUE(database_id, name)
		)`,
		`CREATE TABLE IF NOT EXISTS _metadata_tables (
			id VARCHAR PRIMARY KEY,
			schema_id VARCHAR NOT NULL,
			name VARCHAR NOT NULL,
			table_type VARCHAR DEFAULT 'BASE TABLE',
			comment VARCHAR,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			owner VARCHAR,
			clustering_key VARCHAR,
			column_definitions VARCHAR,
			UNIQUE(schema_id, name)
		)`,
		`CREATE TABLE IF NOT EXISTS _metadata_stages (
			id VARCHAR PRIMARY KEY,
			schema_id VARCHAR NOT NULL,
			name VARCHAR NOT NULL,
			stage_type VARCHAR DEFAULT 'INTERNAL',
			url VARCHAR,
			comment VARCHAR,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			owner VARCHAR,
			UNIQUE(schema_id, name)
		)`,
		`CREATE TABLE IF NOT EXISTS _metadata_fileformats (
			id VARCHAR PRIMARY KEY,
			schema_id VARCHAR NOT NULL,
			name VARCHAR NOT NULL,
			format_type VARCHAR NOT NULL,
			options VARCHAR,
			comment VARCHAR,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			owner VARCHAR,
			UNIQUE(schema_id, name)
		)`,
		`CREATE TABLE IF NOT EXISTS _metadata_query_history (
			id VARCHAR PRIMARY KEY,
			session_id VARCHAR,
			query_id VARCHAR,
			sql_text TEXT,
			status VARCHAR NOT NULL,
			rows_affected BIGINT DEFAULT 0,
			execution_time_ms BIGINT DEFAULT 0,
			error_message TEXT,
			started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			completed_at TIMESTAMP
		)`,
	}

	for _, query := range queries {
		if _, err := r.mgr.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create metadata table: %w", err)
		}
	}

	return nil
}

// CreateDatabase creates a new database.
func (r *Repository) CreateDatabase(ctx context.Context, name, comment string) (*Database, error) {
	if name == "" {
		return nil, fmt.Errorf("database name cannot be empty")
	}

	// Normalize database name (Snowflake normalizes unquoted names to uppercase)
	normalizedName := strings.ToUpper(name)

	// Generate UUID for database ID
	id := uuid.New().String()

	// Execute database creation in a transaction for atomicity
	err := r.mgr.ExecTx(ctx, func(tx *sql.Tx) error {
		// Create DuckDB schema for the database
		schemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", normalizedName)
		if _, err := tx.ExecContext(ctx, schemaSQL); err != nil {
			return fmt.Errorf("failed to create DuckDB schema: %w", err)
		}

		// Insert metadata
		query := `INSERT INTO _metadata_databases (id, name, account_id, comment, created_at, owner)
		          VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)`
		accountID := "" // TODO: Populate when multi-tenancy is implemented
		if _, err := tx.ExecContext(ctx, query, id, normalizedName, accountID, comment, ""); err != nil {
			// Check if it's a duplicate
			if strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "Constraint Error") {
				return fmt.Errorf("database %s already exists", normalizedName)
			}
			return fmt.Errorf("failed to insert database metadata: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Retrieve the created database
	return r.GetDatabase(ctx, id)
}

// GetDatabase retrieves a database by ID.
func (r *Repository) GetDatabase(ctx context.Context, id string) (*Database, error) {
	query := `SELECT id, name, account_id, comment, created_at, owner
	          FROM _metadata_databases WHERE id = ?`

	row := r.mgr.DB().QueryRowContext(ctx, query, id)

	var db Database
	var createdAt sql.NullTime
	var comment sql.NullString
	var accountID sql.NullString
	var owner sql.NullString

	err := row.Scan(&db.ID, &db.Name, &accountID, &comment, &createdAt, &owner)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("database with ID %s not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get database: %w", err)
	}

	if accountID.Valid {
		db.AccountID = accountID.String
	}
	if comment.Valid {
		db.Comment = comment.String
	}
	if createdAt.Valid {
		db.CreatedAt = createdAt.Time
	}
	if owner.Valid {
		db.Owner = owner.String
	}

	return &db, nil
}

// GetDatabaseByName retrieves a database by name.
func (r *Repository) GetDatabaseByName(ctx context.Context, name string) (*Database, error) {
	// Normalize name
	normalizedName := strings.ToUpper(name)

	query := `SELECT id, name, account_id, comment, created_at, owner
	          FROM _metadata_databases WHERE name = ?`

	row := r.mgr.DB().QueryRowContext(ctx, query, normalizedName)

	var db Database
	var createdAt sql.NullTime
	var comment sql.NullString
	var accountID sql.NullString
	var owner sql.NullString

	err := row.Scan(&db.ID, &db.Name, &accountID, &comment, &createdAt, &owner)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("database %s not found", normalizedName)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get database: %w", err)
	}

	if accountID.Valid {
		db.AccountID = accountID.String
	}
	if comment.Valid {
		db.Comment = comment.String
	}
	if createdAt.Valid {
		db.CreatedAt = createdAt.Time
	}
	if owner.Valid {
		db.Owner = owner.String
	}

	return &db, nil
}

// ListDatabases retrieves all databases.
func (r *Repository) ListDatabases(ctx context.Context) ([]*Database, error) {
	query := `SELECT id, name, account_id, comment, created_at, owner
	          FROM _metadata_databases ORDER BY name`

	rows, err := r.mgr.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var databases []*Database
	for rows.Next() {
		var db Database
		var createdAt sql.NullTime
		var comment sql.NullString
		var accountID sql.NullString
		var owner sql.NullString

		if err := rows.Scan(&db.ID, &db.Name, &accountID, &comment, &createdAt, &owner); err != nil {
			return nil, fmt.Errorf("failed to scan database: %w", err)
		}

		if accountID.Valid {
			db.AccountID = accountID.String
		}
		if comment.Valid {
			db.Comment = comment.String
		}
		if createdAt.Valid {
			db.CreatedAt = createdAt.Time
		}
		if owner.Valid {
			db.Owner = owner.String
		}

		databases = append(databases, &db)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating databases: %w", err)
	}

	return databases, nil
}

// DropDatabase deletes a database and all its schemas.
func (r *Repository) DropDatabase(ctx context.Context, id string) error {
	// Get database first to verify it exists
	db, err := r.GetDatabase(ctx, id)
	if err != nil {
		return err
	}

	// Execute database drop in a transaction for atomicity
	err = r.mgr.ExecTx(ctx, func(tx *sql.Tx) error {
		// Drop DuckDB schema
		dropSchemaSQL := fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", db.Name)
		if _, err := tx.ExecContext(ctx, dropSchemaSQL); err != nil {
			return fmt.Errorf("failed to drop DuckDB schema: %w", err)
		}

		// Delete metadata (this will cascade delete schemas and tables due to foreign keys if we add them later)
		query := `DELETE FROM _metadata_databases WHERE id = ?`
		result, err := tx.ExecContext(ctx, query, id)
		if err != nil {
			return fmt.Errorf("failed to delete database metadata: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}

		if rowsAffected == 0 {
			return fmt.Errorf("database with ID %s not found", id)
		}

		return nil
	})

	return err
}

// UpdateDatabaseComment updates the comment of a database.
func (r *Repository) UpdateDatabaseComment(ctx context.Context, id, comment string) error {
	query := `UPDATE _metadata_databases SET comment = ? WHERE id = ?`
	result, err := r.mgr.Exec(ctx, query, comment, id)
	if err != nil {
		return fmt.Errorf("failed to update database comment: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("database with ID %s not found", id)
	}

	return nil
}

// CreateSchema creates a new schema in a database.
func (r *Repository) CreateSchema(ctx context.Context, databaseID, name, comment string) (*Schema, error) {
	if name == "" {
		return nil, fmt.Errorf("schema name cannot be empty")
	}

	// Normalize schema name
	normalizedName := strings.ToUpper(name)

	// Generate UUID for schema ID
	id := uuid.New().String()

	// Insert metadata
	query := `INSERT INTO _metadata_schemas (id, database_id, name, comment, created_at, owner)
	          VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)`
	if _, err := r.mgr.Exec(ctx, query, id, databaseID, normalizedName, comment, ""); err != nil {
		if strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "Constraint Error") {
			return nil, fmt.Errorf("schema %s already exists in database", normalizedName)
		}
		return nil, fmt.Errorf("failed to insert schema metadata: %w", err)
	}

	// Retrieve the created schema
	return r.GetSchema(ctx, id)
}

// GetSchema retrieves a schema by ID.
func (r *Repository) GetSchema(ctx context.Context, id string) (*Schema, error) {
	query := `SELECT id, database_id, name, comment, created_at, owner
	          FROM _metadata_schemas WHERE id = ?`

	row := r.mgr.DB().QueryRowContext(ctx, query, id)

	var schema Schema
	var createdAt sql.NullTime
	var comment sql.NullString
	var owner sql.NullString

	err := row.Scan(&schema.ID, &schema.DatabaseID, &schema.Name, &comment, &createdAt, &owner)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("schema with ID %s not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	if comment.Valid {
		schema.Comment = comment.String
	}
	if createdAt.Valid {
		schema.CreatedAt = createdAt.Time
	}
	if owner.Valid {
		schema.Owner = owner.String
	}

	return &schema, nil
}

// GetSchemaByName retrieves a schema by database ID and name.
func (r *Repository) GetSchemaByName(ctx context.Context, databaseID, name string) (*Schema, error) {
	query := `SELECT id, database_id, name, comment, created_at, owner
	          FROM _metadata_schemas WHERE database_id = ? AND name = ?`

	row := r.mgr.DB().QueryRowContext(ctx, query, databaseID, strings.ToUpper(name))

	var schema Schema
	var createdAt sql.NullTime
	var comment sql.NullString
	var owner sql.NullString

	err := row.Scan(&schema.ID, &schema.DatabaseID, &schema.Name, &comment, &createdAt, &owner)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("schema %s not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	if comment.Valid {
		schema.Comment = comment.String
	}
	if createdAt.Valid {
		schema.CreatedAt = createdAt.Time
	}
	if owner.Valid {
		schema.Owner = owner.String
	}

	return &schema, nil
}

// ListSchemas retrieves all schemas in a database.
func (r *Repository) ListSchemas(ctx context.Context, databaseID string) ([]*Schema, error) {
	query := `SELECT id, database_id, name, comment, created_at, owner
	          FROM _metadata_schemas WHERE database_id = ? ORDER BY name`

	rows, err := r.mgr.Query(ctx, query, databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to list schemas: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var schemas []*Schema
	for rows.Next() {
		var schema Schema
		var createdAt sql.NullTime
		var comment sql.NullString
		var owner sql.NullString

		if err := rows.Scan(&schema.ID, &schema.DatabaseID, &schema.Name, &comment, &createdAt, &owner); err != nil {
			return nil, fmt.Errorf("failed to scan schema: %w", err)
		}

		if comment.Valid {
			schema.Comment = comment.String
		}
		if createdAt.Valid {
			schema.CreatedAt = createdAt.Time
		}
		if owner.Valid {
			schema.Owner = owner.String
		}

		schemas = append(schemas, &schema)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating schemas: %w", err)
	}

	return schemas, nil
}

// DropSchema deletes a schema and all its tables.
func (r *Repository) DropSchema(ctx context.Context, id string) error {
	// Get schema first to verify it exists
	schema, err := r.GetSchema(ctx, id)
	if err != nil {
		return err
	}

	// Delete all tables in this schema first
	deleteTablesQuery := `DELETE FROM _metadata_tables WHERE schema_id = ?`
	if _, err := r.mgr.Exec(ctx, deleteTablesQuery, id); err != nil {
		return fmt.Errorf("failed to delete table metadata: %w", err)
	}

	// Delete schema metadata
	query := `DELETE FROM _metadata_schemas WHERE id = ?`
	result, err := r.mgr.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete schema metadata: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("schema with ID %s not found", id)
	}

	_ = schema // Suppress unused variable warning
	return nil
}

// CreateTable creates a new table in a schema.
func (r *Repository) CreateTable(ctx context.Context, schemaID, name string, columns []ColumnDef, comment string) (*Table, error) {
	if name == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("table must have at least one column")
	}

	// Normalize table name
	normalizedName := strings.ToUpper(name)

	// Generate UUID for table ID
	id := uuid.New().String()

	// Build column definitions for DuckDB table
	var colDefs []string
	var primaryKeys []string
	for _, col := range columns {
		colDef := fmt.Sprintf("%s %s", col.Name, col.Type)
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		if col.Default != nil {
			colDef += fmt.Sprintf(" DEFAULT %s", *col.Default)
		}
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
		colDefs = append(colDefs, colDef)
	}

	if len(primaryKeys) > 0 {
		colDefs = append(colDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ", ")))
	}

	// Get schema to determine database
	schema, err := r.GetSchema(ctx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Get database to construct fully qualified table name
	db, err := r.GetDatabase(ctx, schema.DatabaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database: %w", err)
	}

	// Serialize column definitions as JSON-like string
	columnDefsJSON := serializeColumnDefs(columns)

	// Execute table creation in a transaction for atomicity
	fullyQualifiedName := fmt.Sprintf("%s.%s_%s", db.Name, schema.Name, normalizedName)
	err = r.mgr.ExecTx(ctx, func(tx *sql.Tx) error {
		// Create DuckDB table with schema prefix to prevent naming conflicts
		createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", fullyQualifiedName, strings.Join(colDefs, ", "))
		if _, err := tx.ExecContext(ctx, createTableSQL); err != nil {
			return fmt.Errorf("failed to create DuckDB table: %w", err)
		}

		// Insert metadata
		query := `INSERT INTO _metadata_tables (id, schema_id, name, table_type, comment, created_at, owner, clustering_key, column_definitions)
		          VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?)`
		if _, err := tx.ExecContext(ctx, query, id, schemaID, normalizedName, "BASE TABLE", comment, "", "", columnDefsJSON); err != nil {
			if strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "Constraint Error") {
				return fmt.Errorf("table %s already exists in schema", normalizedName)
			}
			return fmt.Errorf("failed to insert table metadata: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Retrieve the created table
	return r.GetTable(ctx, id)
}

// GetTable retrieves a table by ID.
func (r *Repository) GetTable(ctx context.Context, id string) (*Table, error) {
	query := `SELECT id, schema_id, name, table_type, comment, created_at, owner, clustering_key, column_definitions
	          FROM _metadata_tables WHERE id = ?`

	row := r.mgr.DB().QueryRowContext(ctx, query, id)

	var table Table
	var createdAt sql.NullTime
	var comment sql.NullString
	var owner sql.NullString
	var clusteringKey sql.NullString
	var columnDefinitions sql.NullString

	err := row.Scan(&table.ID, &table.SchemaID, &table.Name, &table.TableType, &comment, &createdAt, &owner, &clusteringKey, &columnDefinitions)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("table with ID %s not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get table: %w", err)
	}

	if comment.Valid {
		table.Comment = comment.String
	}
	if createdAt.Valid {
		table.CreatedAt = createdAt.Time
	}
	if owner.Valid {
		table.Owner = owner.String
	}
	if clusteringKey.Valid {
		table.ClusteringKey = clusteringKey.String
	}
	if columnDefinitions.Valid {
		table.ColumnDefinitions = columnDefinitions.String
	}

	return &table, nil
}

// GetTableByName retrieves a table by schema ID and name.
func (r *Repository) GetTableByName(ctx context.Context, schemaID, name string) (*Table, error) {
	query := `SELECT id, schema_id, name, table_type, comment, created_at, owner, clustering_key, column_definitions
	          FROM _metadata_tables WHERE schema_id = ? AND name = ?`

	row := r.mgr.DB().QueryRowContext(ctx, query, schemaID, strings.ToUpper(name))

	var table Table
	var createdAt sql.NullTime
	var comment sql.NullString
	var owner sql.NullString
	var clusteringKey sql.NullString
	var columnDefinitions sql.NullString

	err := row.Scan(&table.ID, &table.SchemaID, &table.Name, &table.TableType, &comment, &createdAt, &owner, &clusteringKey, &columnDefinitions)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("table %s not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get table: %w", err)
	}

	if comment.Valid {
		table.Comment = comment.String
	}
	if createdAt.Valid {
		table.CreatedAt = createdAt.Time
	}
	if owner.Valid {
		table.Owner = owner.String
	}
	if clusteringKey.Valid {
		table.ClusteringKey = clusteringKey.String
	}
	if columnDefinitions.Valid {
		table.ColumnDefinitions = columnDefinitions.String
	}

	return &table, nil
}

// ListTables retrieves all tables in a schema.
func (r *Repository) ListTables(ctx context.Context, schemaID string) ([]*Table, error) {
	query := `SELECT id, schema_id, name, table_type, comment, created_at, owner, clustering_key, column_definitions
	          FROM _metadata_tables WHERE schema_id = ? ORDER BY name`

	rows, err := r.mgr.Query(ctx, query, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []*Table
	for rows.Next() {
		var table Table
		var createdAt sql.NullTime
		var comment sql.NullString
		var owner sql.NullString
		var clusteringKey sql.NullString
		var columnDefinitions sql.NullString

		if err := rows.Scan(&table.ID, &table.SchemaID, &table.Name, &table.TableType, &comment, &createdAt, &owner, &clusteringKey, &columnDefinitions); err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}

		if comment.Valid {
			table.Comment = comment.String
		}
		if createdAt.Valid {
			table.CreatedAt = createdAt.Time
		}
		if owner.Valid {
			table.Owner = owner.String
		}
		if clusteringKey.Valid {
			table.ClusteringKey = clusteringKey.String
		}
		if columnDefinitions.Valid {
			table.ColumnDefinitions = columnDefinitions.String
		}

		tables = append(tables, &table)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	return tables, nil
}

// DropTable deletes a table.
func (r *Repository) DropTable(ctx context.Context, id string) error {
	// Get table first to verify it exists
	table, err := r.GetTable(ctx, id)
	if err != nil {
		return err
	}

	// Get schema and database to construct fully qualified name
	schema, err := r.GetSchema(ctx, table.SchemaID)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	db, err := r.GetDatabase(ctx, schema.DatabaseID)
	if err != nil {
		return fmt.Errorf("failed to get database: %w", err)
	}

	// Execute table drop in a transaction for atomicity
	fullyQualifiedName := fmt.Sprintf("%s.%s_%s", db.Name, schema.Name, table.Name)
	err = r.mgr.ExecTx(ctx, func(tx *sql.Tx) error {
		// Drop DuckDB table with schema prefix
		dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", fullyQualifiedName)
		if _, err := tx.ExecContext(ctx, dropTableSQL); err != nil {
			return fmt.Errorf("failed to drop DuckDB table: %w", err)
		}

		// Delete metadata
		query := `DELETE FROM _metadata_tables WHERE id = ?`
		result, err := tx.ExecContext(ctx, query, id)
		if err != nil {
			return fmt.Errorf("failed to delete table metadata: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}

		if rowsAffected == 0 {
			return fmt.Errorf("table with ID %s not found", id)
		}

		return nil
	})

	return err
}

// UpdateTableComment updates the comment of a table.
func (r *Repository) UpdateTableComment(ctx context.Context, id, comment string) error {
	query := `UPDATE _metadata_tables SET comment = ? WHERE id = ?`
	result, err := r.mgr.Exec(ctx, query, comment, id)
	if err != nil {
		return fmt.Errorf("failed to update table comment: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("table with ID %s not found", id)
	}

	return nil
}

// serializeColumnDefs converts column definitions to a simple string format.
// For simplicity, we use a basic format: name:type:nullable:primarykey;...
func serializeColumnDefs(columns []ColumnDef) string {
	var parts []string
	for _, col := range columns {
		nullable := "true"
		if !col.Nullable {
			nullable = "false"
		}
		primaryKey := "false"
		if col.PrimaryKey {
			primaryKey = "true"
		}
		defaultVal := ""
		if col.Default != nil {
			defaultVal = *col.Default
		}
		part := fmt.Sprintf("%s:%s:%s:%s:%s", col.Name, col.Type, nullable, primaryKey, defaultVal)
		parts = append(parts, part)
	}
	return strings.Join(parts, ";")
}

// Stage CRUD Operations

// CreateStage creates a new stage in the specified schema.
func (r *Repository) CreateStage(ctx context.Context, schemaID, name, stageType, url, comment string) (*Stage, error) {
	if name == "" {
		return nil, fmt.Errorf("stage name cannot be empty")
	}

	normalizedName := strings.ToUpper(name)
	if stageType == "" {
		stageType = "INTERNAL"
	}

	id := uuid.New().String()

	query := `INSERT INTO _metadata_stages (id, schema_id, name, stage_type, url, comment, created_at, owner)
	          VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)`
	_, err := r.mgr.Exec(ctx, query, id, schemaID, normalizedName, stageType, url, comment, "")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "Constraint Error") {
			return nil, fmt.Errorf("stage %s already exists", normalizedName)
		}
		return nil, fmt.Errorf("failed to create stage: %w", err)
	}

	return r.GetStage(ctx, id)
}

// GetStage retrieves a stage by ID.
func (r *Repository) GetStage(ctx context.Context, id string) (*Stage, error) {
	query := `SELECT id, schema_id, name, stage_type, url, comment, created_at, owner
	          FROM _metadata_stages WHERE id = ?`

	row := r.mgr.DB().QueryRowContext(ctx, query, id)

	var stage Stage
	var createdAt sql.NullTime
	var comment, url, owner sql.NullString

	err := row.Scan(&stage.ID, &stage.SchemaID, &stage.Name, &stage.StageType, &url, &comment, &createdAt, &owner)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("stage with ID %s not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get stage: %w", err)
	}

	if createdAt.Valid {
		stage.CreatedAt = createdAt.Time
	}
	if comment.Valid {
		stage.Comment = comment.String
	}
	if url.Valid {
		stage.URL = url.String
	}
	if owner.Valid {
		stage.Owner = owner.String
	}

	return &stage, nil
}

// GetStageByName retrieves a stage by schema ID and name.
func (r *Repository) GetStageByName(ctx context.Context, schemaID, name string) (*Stage, error) {
	normalizedName := strings.ToUpper(name)
	query := `SELECT id, schema_id, name, stage_type, url, comment, created_at, owner
	          FROM _metadata_stages WHERE schema_id = ? AND name = ?`

	row := r.mgr.DB().QueryRowContext(ctx, query, schemaID, normalizedName)

	var stage Stage
	var createdAt sql.NullTime
	var comment, url, owner sql.NullString

	err := row.Scan(&stage.ID, &stage.SchemaID, &stage.Name, &stage.StageType, &url, &comment, &createdAt, &owner)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("stage %s not found", normalizedName)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get stage: %w", err)
	}

	if createdAt.Valid {
		stage.CreatedAt = createdAt.Time
	}
	if comment.Valid {
		stage.Comment = comment.String
	}
	if url.Valid {
		stage.URL = url.String
	}
	if owner.Valid {
		stage.Owner = owner.String
	}

	return &stage, nil
}

// ListStages returns all stages in a schema.
func (r *Repository) ListStages(ctx context.Context, schemaID string) ([]*Stage, error) {
	query := `SELECT id, schema_id, name, stage_type, url, comment, created_at, owner
	          FROM _metadata_stages WHERE schema_id = ? ORDER BY name`

	rows, err := r.mgr.DB().QueryContext(ctx, query, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to list stages: %w", err)
	}
	defer rows.Close()

	var stages []*Stage
	for rows.Next() {
		var stage Stage
		var createdAt sql.NullTime
		var comment, url, owner sql.NullString

		if err := rows.Scan(&stage.ID, &stage.SchemaID, &stage.Name, &stage.StageType, &url, &comment, &createdAt, &owner); err != nil {
			return nil, fmt.Errorf("failed to scan stage: %w", err)
		}

		if createdAt.Valid {
			stage.CreatedAt = createdAt.Time
		}
		if comment.Valid {
			stage.Comment = comment.String
		}
		if url.Valid {
			stage.URL = url.String
		}
		if owner.Valid {
			stage.Owner = owner.String
		}

		stages = append(stages, &stage)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate stages: %w", err)
	}

	return stages, nil
}

// DropStage deletes a stage by ID.
func (r *Repository) DropStage(ctx context.Context, id string) error {
	query := `DELETE FROM _metadata_stages WHERE id = ?`
	result, err := r.mgr.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to drop stage: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("stage with ID %s not found", id)
	}

	return nil
}

// FileFormat CRUD Operations

// CreateFileFormat creates a new file format in the specified schema.
func (r *Repository) CreateFileFormat(ctx context.Context, schemaID, name, formatType, options, comment string) (*FileFormat, error) {
	if name == "" {
		return nil, fmt.Errorf("file format name cannot be empty")
	}

	normalizedName := strings.ToUpper(name)
	id := uuid.New().String()

	query := `INSERT INTO _metadata_fileformats (id, schema_id, name, format_type, options, comment, created_at, owner)
	          VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)`
	_, err := r.mgr.Exec(ctx, query, id, schemaID, normalizedName, formatType, options, comment, "")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "Constraint Error") {
			return nil, fmt.Errorf("file format %s already exists", normalizedName)
		}
		return nil, fmt.Errorf("failed to create file format: %w", err)
	}

	return r.GetFileFormat(ctx, id)
}

// GetFileFormat retrieves a file format by ID.
func (r *Repository) GetFileFormat(ctx context.Context, id string) (*FileFormat, error) {
	query := `SELECT id, schema_id, name, format_type, options, comment, created_at, owner
	          FROM _metadata_fileformats WHERE id = ?`

	row := r.mgr.DB().QueryRowContext(ctx, query, id)

	var ff FileFormat
	var createdAt sql.NullTime
	var options, comment, owner sql.NullString

	err := row.Scan(&ff.ID, &ff.SchemaID, &ff.Name, &ff.FormatType, &options, &comment, &createdAt, &owner)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("file format with ID %s not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get file format: %w", err)
	}

	if createdAt.Valid {
		ff.CreatedAt = createdAt.Time
	}
	if options.Valid {
		ff.Options = options.String
	}
	if comment.Valid {
		ff.Comment = comment.String
	}
	if owner.Valid {
		ff.Owner = owner.String
	}

	return &ff, nil
}

// GetFileFormatByName retrieves a file format by schema ID and name.
func (r *Repository) GetFileFormatByName(ctx context.Context, schemaID, name string) (*FileFormat, error) {
	normalizedName := strings.ToUpper(name)
	query := `SELECT id, schema_id, name, format_type, options, comment, created_at, owner
	          FROM _metadata_fileformats WHERE schema_id = ? AND name = ?`

	row := r.mgr.DB().QueryRowContext(ctx, query, schemaID, normalizedName)

	var ff FileFormat
	var createdAt sql.NullTime
	var options, comment, owner sql.NullString

	err := row.Scan(&ff.ID, &ff.SchemaID, &ff.Name, &ff.FormatType, &options, &comment, &createdAt, &owner)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("file format %s not found", normalizedName)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get file format: %w", err)
	}

	if createdAt.Valid {
		ff.CreatedAt = createdAt.Time
	}
	if options.Valid {
		ff.Options = options.String
	}
	if comment.Valid {
		ff.Comment = comment.String
	}
	if owner.Valid {
		ff.Owner = owner.String
	}

	return &ff, nil
}

// ListFileFormats returns all file formats in a schema.
func (r *Repository) ListFileFormats(ctx context.Context, schemaID string) ([]*FileFormat, error) {
	query := `SELECT id, schema_id, name, format_type, options, comment, created_at, owner
	          FROM _metadata_fileformats WHERE schema_id = ? ORDER BY name`

	rows, err := r.mgr.DB().QueryContext(ctx, query, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to list file formats: %w", err)
	}
	defer rows.Close()

	var fileFormats []*FileFormat
	for rows.Next() {
		var ff FileFormat
		var createdAt sql.NullTime
		var options, comment, owner sql.NullString

		if err := rows.Scan(&ff.ID, &ff.SchemaID, &ff.Name, &ff.FormatType, &options, &comment, &createdAt, &owner); err != nil {
			return nil, fmt.Errorf("failed to scan file format: %w", err)
		}

		if createdAt.Valid {
			ff.CreatedAt = createdAt.Time
		}
		if options.Valid {
			ff.Options = options.String
		}
		if comment.Valid {
			ff.Comment = comment.String
		}
		if owner.Valid {
			ff.Owner = owner.String
		}

		fileFormats = append(fileFormats, &ff)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate file formats: %w", err)
	}

	return fileFormats, nil
}

// DropFileFormat deletes a file format by ID.
func (r *Repository) DropFileFormat(ctx context.Context, id string) error {
	query := `DELETE FROM _metadata_fileformats WHERE id = ?`
	result, err := r.mgr.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to drop file format: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("file format with ID %s not found", id)
	}

	return nil
}

// Query History Operations

// RecordQueryStart records the start of a query execution.
func (r *Repository) RecordQueryStart(ctx context.Context, sessionID, queryID, sqlText string) (*QueryHistoryEntry, error) {
	id := uuid.New().String()
	now := time.Now()

	query := `INSERT INTO _metadata_query_history (id, session_id, query_id, sql_text, status, started_at)
		VALUES (?, ?, ?, ?, 'RUNNING', ?)`

	if _, err := r.mgr.Exec(ctx, query, id, sessionID, queryID, sqlText, now); err != nil {
		return nil, fmt.Errorf("failed to record query start: %w", err)
	}

	return &QueryHistoryEntry{
		ID:        id,
		SessionID: sessionID,
		QueryID:   queryID,
		SQLText:   sqlText,
		Status:    "RUNNING",
		StartedAt: now,
	}, nil
}

// RecordQuerySuccess records a successful query completion.
func (r *Repository) RecordQuerySuccess(ctx context.Context, id string, rowsAffected int64, executionTimeMs int64) error {
	now := time.Now()
	query := `UPDATE _metadata_query_history
		SET status = 'SUCCESS', rows_affected = ?, execution_time_ms = ?, completed_at = ?
		WHERE id = ?`

	if _, err := r.mgr.Exec(ctx, query, rowsAffected, executionTimeMs, now, id); err != nil {
		return fmt.Errorf("failed to record query success: %w", err)
	}

	return nil
}

// RecordQueryFailure records a failed query completion.
func (r *Repository) RecordQueryFailure(ctx context.Context, id string, errorMessage string, executionTimeMs int64) error {
	now := time.Now()
	query := `UPDATE _metadata_query_history
		SET status = 'FAILED', error_message = ?, execution_time_ms = ?, completed_at = ?
		WHERE id = ?`

	if _, err := r.mgr.Exec(ctx, query, errorMessage, executionTimeMs, now, id); err != nil {
		return fmt.Errorf("failed to record query failure: %w", err)
	}

	return nil
}

// GetQueryHistory retrieves query history with optional limit.
func (r *Repository) GetQueryHistory(ctx context.Context, limit int) ([]*QueryHistoryEntry, error) {
	if limit <= 0 {
		limit = 100 // Default limit
	}

	query := `SELECT id, session_id, query_id, sql_text, status, rows_affected,
		execution_time_ms, error_message, started_at, completed_at
		FROM _metadata_query_history
		ORDER BY started_at DESC
		LIMIT ?`

	rows, err := r.mgr.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get query history: %w", err)
	}
	defer rows.Close()

	var entries []*QueryHistoryEntry
	for rows.Next() {
		var entry QueryHistoryEntry
		var sessionID, queryID, errorMessage sql.NullString
		var completedAt sql.NullTime

		err := rows.Scan(
			&entry.ID,
			&sessionID,
			&queryID,
			&entry.SQLText,
			&entry.Status,
			&entry.RowsAffected,
			&entry.ExecutionTimeMs,
			&errorMessage,
			&entry.StartedAt,
			&completedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan query history row: %w", err)
		}

		entry.SessionID = sessionID.String
		entry.QueryID = queryID.String
		entry.ErrorMessage = errorMessage.String
		if completedAt.Valid {
			entry.CompletedAt = &completedAt.Time
		}

		entries = append(entries, &entry)
	}

	return entries, nil
}

// GetQueryHistoryBySession retrieves query history for a specific session.
func (r *Repository) GetQueryHistoryBySession(ctx context.Context, sessionID string, limit int) ([]*QueryHistoryEntry, error) {
	if limit <= 0 {
		limit = 100
	}

	query := `SELECT id, session_id, query_id, sql_text, status, rows_affected,
		execution_time_ms, error_message, started_at, completed_at
		FROM _metadata_query_history
		WHERE session_id = ?
		ORDER BY started_at DESC
		LIMIT ?`

	rows, err := r.mgr.Query(ctx, query, sessionID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get query history by session: %w", err)
	}
	defer rows.Close()

	var entries []*QueryHistoryEntry
	for rows.Next() {
		var entry QueryHistoryEntry
		var sessionIDVal, queryID, errorMessage sql.NullString
		var completedAt sql.NullTime

		err := rows.Scan(
			&entry.ID,
			&sessionIDVal,
			&queryID,
			&entry.SQLText,
			&entry.Status,
			&entry.RowsAffected,
			&entry.ExecutionTimeMs,
			&errorMessage,
			&entry.StartedAt,
			&completedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan query history row: %w", err)
		}

		entry.SessionID = sessionIDVal.String
		entry.QueryID = queryID.String
		entry.ErrorMessage = errorMessage.String
		if completedAt.Valid {
			entry.CompletedAt = &completedAt.Time
		}

		entries = append(entries, &entry)
	}

	return entries, nil
}

// ClearQueryHistory removes old query history entries.
func (r *Repository) ClearQueryHistory(ctx context.Context, olderThan time.Time) (int64, error) {
	query := `DELETE FROM _metadata_query_history WHERE started_at < ?`
	result, err := r.mgr.Exec(ctx, query, olderThan)
	if err != nil {
		return 0, fmt.Errorf("failed to clear query history: %w", err)
	}

	return result.RowsAffected()
}
