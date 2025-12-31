// Package query provides SQL query execution and classification.
package query

import (
	"strings"

	"github.com/nnnkkk7/snowflake-emulator/pkg/config"
)

// StatementType represents the category of a SQL statement.
type StatementType int

// Statement types.
const (
	StatementTypeQuery       StatementType = iota // SELECT, SHOW, DESCRIBE
	StatementTypeDML                              // INSERT, UPDATE, DELETE
	StatementTypeDDLCreate                        // CREATE TABLE, CREATE DATABASE, etc.
	StatementTypeDDLDrop                          // DROP TABLE, DROP DATABASE, etc.
	StatementTypeDDLAlter                         // ALTER TABLE, etc.
	StatementTypeCopy                             // COPY INTO
	StatementTypeMerge                            // MERGE INTO
	StatementTypeTransaction                      // BEGIN, COMMIT, ROLLBACK
	StatementTypeOther                            // Unknown or unsupported
)

// Classifier provides SQL statement classification functionality.
type Classifier struct{}

// NewClassifier creates a new SQL classifier.
func NewClassifier() *Classifier {
	return &Classifier{}
}

// ClassifyResult contains the classification result of a SQL statement.
type ClassifyResult struct {
	Type            StatementType
	StatementTypeID config.StatementTypeID
	IsQuery         bool
	IsDDL           bool
	IsDML           bool
}

// Classify analyzes a SQL statement and returns its classification.
func (c *Classifier) Classify(sql string) ClassifyResult {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))

	// Check for query statements
	if c.isQueryStatement(upperSQL) {
		return ClassifyResult{
			Type:            StatementTypeQuery,
			StatementTypeID: config.StatementTypeSelect,
			IsQuery:         true,
			IsDDL:           false,
			IsDML:           false,
		}
	}

	// Check for DDL statements
	if strings.HasPrefix(upperSQL, "CREATE") {
		return ClassifyResult{
			Type:            StatementTypeDDLCreate,
			StatementTypeID: config.StatementTypeDDL,
			IsQuery:         false,
			IsDDL:           true,
			IsDML:           false,
		}
	}

	if strings.HasPrefix(upperSQL, "DROP") {
		return ClassifyResult{
			Type:            StatementTypeDDLDrop,
			StatementTypeID: config.StatementTypeDrop,
			IsQuery:         false,
			IsDDL:           true,
			IsDML:           false,
		}
	}

	if strings.HasPrefix(upperSQL, "ALTER") {
		return ClassifyResult{
			Type:            StatementTypeDDLAlter,
			StatementTypeID: config.StatementTypeDDL,
			IsQuery:         false,
			IsDDL:           true,
			IsDML:           false,
		}
	}

	// Check for COPY INTO statement
	if strings.HasPrefix(upperSQL, "COPY") {
		return ClassifyResult{
			Type:            StatementTypeCopy,
			StatementTypeID: config.StatementTypeDML, // COPY is treated as DML
			IsQuery:         false,
			IsDDL:           false,
			IsDML:           true,
		}
	}

	// Check for MERGE statement
	if strings.HasPrefix(upperSQL, "MERGE") {
		return ClassifyResult{
			Type:            StatementTypeMerge,
			StatementTypeID: config.StatementTypeDML, // MERGE is treated as DML
			IsQuery:         false,
			IsDDL:           false,
			IsDML:           true,
		}
	}

	// Check for transaction control statements
	if c.isTransactionStatement(upperSQL) {
		return ClassifyResult{
			Type:            StatementTypeTransaction,
			StatementTypeID: config.StatementTypeDML, // Transaction control statements
			IsQuery:         false,
			IsDDL:           false,
			IsDML:           false,
		}
	}

	// Default to DML for INSERT, UPDATE, DELETE, etc.
	return ClassifyResult{
		Type:            StatementTypeDML,
		StatementTypeID: config.StatementTypeDML,
		IsQuery:         false,
		IsDDL:           false,
		IsDML:           true,
	}
}

// isQueryStatement checks if the SQL is a query (read-only) statement.
func (c *Classifier) isQueryStatement(upperSQL string) bool {
	return strings.HasPrefix(upperSQL, "SELECT") ||
		strings.HasPrefix(upperSQL, "SHOW") ||
		strings.HasPrefix(upperSQL, "DESCRIBE") ||
		strings.HasPrefix(upperSQL, "DESC") ||
		strings.HasPrefix(upperSQL, "EXPLAIN")
}

// isTransactionStatement checks if the SQL is a transaction control statement.
func (c *Classifier) isTransactionStatement(upperSQL string) bool {
	return strings.HasPrefix(upperSQL, "BEGIN") ||
		strings.HasPrefix(upperSQL, "START TRANSACTION") ||
		strings.HasPrefix(upperSQL, "COMMIT") ||
		strings.HasPrefix(upperSQL, "ROLLBACK")
}

// IsCreateTable checks if the SQL is a CREATE TABLE statement.
func (c *Classifier) IsCreateTable(sql string) bool {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upperSQL, "CREATE TABLE")
}

// IsDropTable checks if the SQL is a DROP TABLE statement.
func (c *Classifier) IsDropTable(sql string) bool {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upperSQL, "DROP TABLE")
}

// IsCopy checks if the SQL is a COPY INTO statement.
func (c *Classifier) IsCopy(sql string) bool {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upperSQL, "COPY")
}

// DefaultClassifier is the default SQL classifier instance.
var DefaultClassifier = NewClassifier()

// ClassifySQL is a convenience function using the default classifier.
func ClassifySQL(sql string) ClassifyResult {
	return DefaultClassifier.Classify(sql)
}

// IsQuery is a convenience function to check if SQL is a query.
func IsQuery(sql string) bool {
	return DefaultClassifier.Classify(sql).IsQuery
}

// IsDDL is a convenience function to check if SQL is a DDL statement.
func IsDDL(sql string) bool {
	return DefaultClassifier.Classify(sql).IsDDL
}

// GetStatementTypeID is a convenience function to get the statement type ID.
func GetStatementTypeID(sql string) config.StatementTypeID {
	return DefaultClassifier.Classify(sql).StatementTypeID
}

// IsCopy is a convenience function to check if SQL is a COPY statement.
func IsCopy(sql string) bool {
	return DefaultClassifier.IsCopy(sql)
}

// IsMerge checks if the SQL is a MERGE INTO statement.
func (c *Classifier) IsMerge(sql string) bool {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upperSQL, "MERGE")
}

// IsTransaction checks if the SQL is a transaction control statement.
func (c *Classifier) IsTransaction(sql string) bool {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	return c.isTransactionStatement(upperSQL)
}

// IsMerge is a convenience function to check if SQL is a MERGE statement.
func IsMerge(sql string) bool {
	return DefaultClassifier.IsMerge(sql)
}

// IsTransaction is a convenience function to check if SQL is a transaction statement.
func IsTransaction(sql string) bool {
	return DefaultClassifier.IsTransaction(sql)
}

// IsBegin checks if the SQL is a BEGIN/START TRANSACTION statement.
func IsBegin(sql string) bool {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upperSQL, "BEGIN") || strings.HasPrefix(upperSQL, "START TRANSACTION")
}

// IsCommit checks if the SQL is a COMMIT statement.
func IsCommit(sql string) bool {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upperSQL, "COMMIT")
}

// IsRollback checks if the SQL is a ROLLBACK statement.
func IsRollback(sql string) bool {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upperSQL, "ROLLBACK")
}
