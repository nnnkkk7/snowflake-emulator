// Package types provides API request/response types for the gosnowflake protocol.
package types

// Session API Types

// LoginRequest is the top-level login request matching gosnowflake protocol
type LoginRequest struct {
	Data LoginRequestData `json:"data"`
}

// LoginRequestData contains the login request details.
type LoginRequestData struct {
	ClientAppID       string         `json:"CLIENT_APP_ID"`
	ClientAppVersion  string         `json:"CLIENT_APP_VERSION"`
	SVNRevision       string         `json:"SVN_REVISION,omitempty"`
	AccountName       string         `json:"ACCOUNT_NAME"`
	LoginName         string         `json:"LOGIN_NAME"`
	Password          string         `json:"PASSWORD"`
	DatabaseName      string         `json:"databaseName,omitempty"`
	SchemaName        string         `json:"schemaName,omitempty"`
	WarehouseName     string         `json:"warehouseName,omitempty"`
	RoleName          string         `json:"roleName,omitempty"`
	SessionParams     map[string]any `json:"SESSION_PARAMETERS,omitempty"`
	ClientEnvironment map[string]any `json:"CLIENT_ENVIRONMENT,omitempty"`
}

// LoginResponse is the response to a login request.
type LoginResponse struct {
	Success bool              `json:"success"`
	Message string            `json:"message,omitempty"`
	Code    string            `json:"code,omitempty"`
	Data    *LoginSuccessData `json:"data,omitempty"`
}

// LoginSuccessData contains successful login response data.
type LoginSuccessData struct {
	Token                   string             `json:"token"`
	MasterToken             string             `json:"masterToken"`
	ValidityInSeconds       int64              `json:"validityInSeconds"`
	MasterValidityInSeconds int64              `json:"masterValidityInSeconds"`
	SessionID               int64              `json:"sessionId"`
	Parameters              []ParameterBinding `json:"parameters"`
	SessionInfo             SessionInfo        `json:"sessionInfo"`
}

// ParameterBinding represents a session parameter name-value pair.
type ParameterBinding struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// SessionInfo contains session context information.
type SessionInfo struct {
	DatabaseName  string `json:"databaseName"`
	SchemaName    string `json:"schemaName"`
	WarehouseName string `json:"warehouseName"`
	RoleName      string `json:"roleName"`
}

// TokenRequest for renewing tokens with master token
type TokenRequest struct {
	MasterToken string `json:"masterToken"`
	RequestType string `json:"requestType"` // "RENEW" or "ISSUE"
}

// TokenResponse is the response to a token renewal request.
type TokenResponse struct {
	Success bool              `json:"success"`
	Message string            `json:"message,omitempty"`
	Code    string            `json:"code,omitempty"`
	Data    *TokenSuccessData `json:"data,omitempty"`
}

// TokenSuccessData contains successful token response data.
type TokenSuccessData struct {
	SessionToken      string `json:"sessionToken"`
	ValidityInSeconds int64  `json:"validityInSeconds"`
}

// HeartbeatRequest for session keep-alive
type HeartbeatRequest struct {
	RequestID string `json:"request_id,omitempty"`
}

// HeartbeatResponse is the response to a heartbeat request.
type HeartbeatResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Code    string `json:"code,omitempty"`
}

// Query API Types

// QueryRequest is a SQL query execution request.
type QueryRequest struct {
	SQLText    string                 `json:"sqlText"`
	Bindings   map[string]interface{} `json:"bindings,omitempty"`
	Parameters map[string]string      `json:"parameters,omitempty"`
}

// QueryResponse is the response to a query request.
type QueryResponse struct {
	Success bool              `json:"success"`
	Message string            `json:"message,omitempty"`
	Code    string            `json:"code,omitempty"`
	Data    *QuerySuccessData `json:"data,omitempty"`
}

// QuerySuccessData contains successful query response data.
type QuerySuccessData struct {
	QueryID           string           `json:"queryId"`
	SQLState          string           `json:"sqlState,omitempty"`
	StatementTypeID   int64            `json:"statementTypeId"`
	RowType           []ColumnMetadata `json:"rowtype,omitempty"`
	RowSet            [][]string       `json:"rowset,omitempty"`
	Total             int64            `json:"total"`
	Returned          int64            `json:"returned"`
	QueryResultFormat string           `json:"queryResultFormat"`
}

// ColumnMetadata describes a result column's type information.
type ColumnMetadata struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Length    int64  `json:"length,omitempty"`
	Precision int64  `json:"precision,omitempty"`
	Scale     int64  `json:"scale,omitempty"`
	Nullable  bool   `json:"nullable"`
}

// AbortRequest for query cancellation
type AbortRequest struct {
	QueryID string `json:"queryId"`
}

// AbortResponse is the response to a query abort request.
type AbortResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Code    string `json:"code,omitempty"`
}
