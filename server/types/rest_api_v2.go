// Package types provides API request/response types for Snowflake REST API v2.
package types

// SQL REST API v2 Types
// Reference: https://docs.snowflake.com/en/developer-guide/sql-api/

// SubmitStatementRequest represents POST /api/v2/statements request body.
type SubmitStatementRequest struct {
	Statement  string                   `json:"statement"`
	Timeout    int                      `json:"timeout,omitempty"`    // Timeout in seconds
	Database   string                   `json:"database,omitempty"`   // Database context
	Schema     string                   `json:"schema,omitempty"`     // Schema context
	Warehouse  string                   `json:"warehouse,omitempty"`  // Warehouse to use
	Role       string                   `json:"role,omitempty"`       // Role context
	Bindings   map[string]*BindingValue `json:"bindings,omitempty"`   // Parameter bindings
	Parameters map[string]string        `json:"parameters,omitempty"` // Session parameters
}

// BindingValue represents a parameter binding value.
type BindingValue struct {
	Type  string `json:"type"`  // FIXED, TEXT, REAL, BOOLEAN, DATE, TIME, TIMESTAMP, etc.
	Value string `json:"value"` // String representation of the value
}

// StatementResponse represents the response from statement operations.
type StatementResponse struct {
	ResultSetMetaData  *ResultSetMetaData `json:"resultSetMetaData,omitempty"`
	Data               [][]interface{}    `json:"data,omitempty"`
	Code               string             `json:"code"`
	StatementStatusURL string             `json:"statementStatusUrl,omitempty"`
	RequestID          string             `json:"requestId,omitempty"`
	SQLState           string             `json:"sqlState"`
	StatementHandle    string             `json:"statementHandle"`
	Message            string             `json:"message,omitempty"`
	CreatedOn          int64              `json:"createdOn,omitempty"`
}

// ResultSetMetaData contains metadata about the result set.
type ResultSetMetaData struct {
	NumRows       int64           `json:"numRows"`
	Format        string          `json:"format"` // "jsonv2" or "arrow"
	RowType       []RowTypeField  `json:"rowType"`
	PartitionInfo []PartitionInfo `json:"partitionInfo,omitempty"`
}

// RowTypeField describes a column in the result set.
type RowTypeField struct {
	Name       string `json:"name"`
	Database   string `json:"database,omitempty"`
	Schema     string `json:"schema,omitempty"`
	Table      string `json:"table,omitempty"`
	Type       string `json:"type"`
	Length     int64  `json:"length,omitempty"`
	Precision  int64  `json:"precision,omitempty"`
	Scale      int64  `json:"scale,omitempty"`
	Nullable   bool   `json:"nullable"`
	ByteLength int64  `json:"byteLength,omitempty"`
	Collation  string `json:"collation,omitempty"`
}

// PartitionInfo describes data partitioning.
type PartitionInfo struct {
	RowCount         int64 `json:"rowCount"`
	UncompressedSize int64 `json:"uncompressedSize"`
	CompressedSize   int64 `json:"compressedSize,omitempty"`
}

// CancelStatementResponse represents the response from canceling a statement.
type CancelStatementResponse struct {
	Code            string `json:"code"`
	SQLState        string `json:"sqlState"`
	Message         string `json:"message,omitempty"`
	StatementHandle string `json:"statementHandle"`
}

// Resource Management API Types

// DatabaseRequest represents a request to create/alter a database.
type DatabaseRequest struct {
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
}

// DatabaseResponse represents database information.
type DatabaseResponse struct {
	Name      string `json:"name"`
	Comment   string `json:"comment,omitempty"`
	Owner     string `json:"owner,omitempty"`
	CreatedOn string `json:"created_on,omitempty"`
}

// ListDatabasesResponse represents a list of databases.
type ListDatabasesResponse []DatabaseResponse

// SchemaRequest represents a request to create/alter a schema.
type SchemaRequest struct {
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
}

// SchemaResponse represents schema information.
type SchemaResponse struct {
	Name         string `json:"name"`
	DatabaseName string `json:"database_name,omitempty"`
	Comment      string `json:"comment,omitempty"`
	Owner        string `json:"owner,omitempty"`
	CreatedOn    string `json:"created_on,omitempty"`
}

// ListSchemasResponse represents a list of schemas.
type ListSchemasResponse []SchemaResponse

// TableResponse represents table information.
type TableResponse struct {
	Name      string `json:"name"`
	Database  string `json:"database_name,omitempty"`
	Schema    string `json:"schema_name,omitempty"`
	TableType string `json:"table_type,omitempty"` // BASE TABLE, VIEW, etc.
	Comment   string `json:"comment,omitempty"`
	Owner     string `json:"owner,omitempty"`
	CreatedOn string `json:"created_on,omitempty"`
	RowCount  int64  `json:"row_count,omitempty"`
	Bytes     int64  `json:"bytes,omitempty"`
}

// ListTablesResponse represents a list of tables.
type ListTablesResponse []TableResponse

// WarehouseRequest represents a request to create/alter a warehouse.
type WarehouseRequest struct {
	Name        string `json:"name"`
	Size        string `json:"warehouse_size,omitempty"` // X-SMALL, SMALL, MEDIUM, etc.
	AutoSuspend int    `json:"auto_suspend,omitempty"`   // Seconds
	AutoResume  bool   `json:"auto_resume,omitempty"`
	Comment     string `json:"comment,omitempty"`
}

// WarehouseResponse represents warehouse information.
type WarehouseResponse struct {
	Name        string `json:"name"`
	State       string `json:"state"` // STARTED, SUSPENDED, RESUMING, SUSPENDING
	Size        string `json:"size,omitempty"`
	Type        string `json:"type,omitempty"` // STANDARD, SNOWPARK-OPTIMIZED
	AutoSuspend int    `json:"auto_suspend,omitempty"`
	AutoResume  bool   `json:"auto_resume,omitempty"`
	Comment     string `json:"comment,omitempty"`
	Owner       string `json:"owner,omitempty"`
	CreatedOn   string `json:"created_on,omitempty"`
}

// ListWarehousesResponse represents a list of warehouses.
type ListWarehousesResponse []WarehouseResponse

// Common response wrapper for REST API v2
type RESTAPIV2Response struct {
	Code    string      `json:"code"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// Statement status codes
const (
	StatementStatusRunning  = "running"
	StatementStatusSuccess  = "success"
	StatementStatusFailed   = "failed"
	StatementStatusCanceled = "canceled"
)

// SQL State codes for REST API v2
const (
	SQLState00000 = "00000" // Success
	SQLState02000 = "02000" // No data
	SQLState22000 = "22000" // Data exception
	SQLState42000 = "42000" // Syntax error or access rule violation
)

// Response codes
const (
	ResponseCodeSuccess           = "090001" // Statement succeeded
	ResponseCodeStatementPending  = "333334" // Statement still running
	ResponseCodeStatementCanceled = "000604" // Statement canceled
)
