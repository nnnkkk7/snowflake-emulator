// Package config provides configuration constants for the Snowflake emulator.
package config

// Default database and schema settings.
const (
	DefaultDatabase = "TEST_DB"
	DefaultSchema   = "PUBLIC"
)

// StatementTypeID represents Snowflake statement type identifiers.
type StatementTypeID int64

// Statement type IDs matching Snowflake's wire protocol values.
const (
	StatementTypeSelect StatementTypeID = 0x1000 // 4096
	StatementTypeDML    StatementTypeID = 0x3000 // 12288 - generic DML
	StatementTypeInsert StatementTypeID = 0x3100 // 12544
	StatementTypeUpdate StatementTypeID = 0x3200 // 12800
	StatementTypeDelete StatementTypeID = 0x3300 // 13056
	StatementTypeMerge  StatementTypeID = 0x3400 // 13312
	StatementTypeCopy   StatementTypeID = 0x3600 // 13824
	StatementTypeCommit StatementTypeID = 0x5100 // 20736
	StatementTypeBegin  StatementTypeID = 0x5400 // 21504
	StatementTypeDDL    StatementTypeID = 0x6000 // 24576
	StatementTypeDrop   StatementTypeID = 0x6000 // 24576 - same as DDL
)

// QueryResultFormat defines the format of query results.
const (
	QueryResultFormatJSON = "json"
)

// Session parameter defaults.
const (
	DefaultTimezone               = "UTC"
	DefaultTimestampOutputFormat  = "YYYY-MM-DD HH24:MI:SS"
	DefaultClientSessionKeepAlive = "false"
	DefaultQueryTag               = ""
)

// SessionParameter represents a session parameter name.
type SessionParameter string

// Session parameter names.
const (
	ParamTimezone               SessionParameter = "TIMEZONE"
	ParamTimestampOutputFormat  SessionParameter = "TIMESTAMP_OUTPUT_FORMAT"
	ParamClientSessionKeepAlive SessionParameter = "CLIENT_SESSION_KEEP_ALIVE"
	ParamQueryTag               SessionParameter = "QUERY_TAG"
	ParamGoQueryResultFormat    SessionParameter = "GO_QUERY_RESULT_FORMAT"
)

// DefaultSessionParameters returns the default session parameters.
func DefaultSessionParameters() map[SessionParameter]string {
	return map[SessionParameter]string{
		ParamTimezone:               DefaultTimezone,
		ParamTimestampOutputFormat:  DefaultTimestampOutputFormat,
		ParamClientSessionKeepAlive: DefaultClientSessionKeepAlive,
		ParamQueryTag:               DefaultQueryTag,
		ParamGoQueryResultFormat:    QueryResultFormatJSON,
	}
}
