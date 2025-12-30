package apierror

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Snowflake-compatible error codes
const (
	// Authentication & Session Errors (390xxx)
	CodeAuthenticationFailed = "390100"
	CodeSessionExpired       = "390114"
	CodeSessionNotFound      = "390144"

	// SQL Compilation & Execution Errors (001xxx)
	CodeSQLCompilationError = "001003"
	CodeSQLExecutionError   = "001007"

	// Object Errors (002xxx)
	CodeObjectNotFound      = "002003"
	CodeObjectAlreadyExists = "002043"

	// System Errors (000xxx)
	CodeInternalError    = "000001"
	CodeInvalidParameter = "000002"
	CodePermissionDenied = "000003"
)

// SQLState represents SQL standard error states.
const (
	SQLStateSuccess              = "00000"
	SQLStateAuthenticationFailed = "28000"
	SQLStateSyntaxError          = "42000"
	SQLStateDataException        = "22000"
	SQLStateNoData               = "02000"
	SQLStateTableExists          = "42S01"
	SQLStateGeneralError         = "HY000"
)

// GetSQLState returns the SQL state for a given error code
func GetSQLState(code string) string {
	mapping := map[string]string{
		CodeAuthenticationFailed: SQLStateAuthenticationFailed,
		CodeSessionExpired:       SQLStateAuthenticationFailed,
		CodeSessionNotFound:      SQLStateAuthenticationFailed,
		CodeSQLCompilationError:  SQLStateSyntaxError,
		CodeSQLExecutionError:    SQLStateDataException,
		CodeObjectNotFound:       SQLStateNoData,
		CodeObjectAlreadyExists:  SQLStateTableExists,
	}

	if state, ok := mapping[code]; ok {
		return state
	}
	return SQLStateGeneralError
}

// SnowflakeError represents a Snowflake-compatible error.
type SnowflakeError struct {
	Code     string                 `json:"code"`
	Message  string                 `json:"message"`
	SQLState string                 `json:"sqlState,omitempty"`
	Data     map[string]interface{} `json:"data,omitempty"`
}

// Error implements the error interface.
func (e *SnowflakeError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// MarshalJSON implements custom JSON marshaling.
func (e *SnowflakeError) MarshalJSON() ([]byte, error) {
	type Alias SnowflakeError
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(e),
	})
}

// WithData adds data to the error.
func (e *SnowflakeError) WithData(key string, value interface{}) *SnowflakeError {
	if e.Data == nil {
		e.Data = make(map[string]interface{})
	}
	e.Data[key] = value
	return e
}

// Is checks if this error matches another error by code.
func (e *SnowflakeError) Is(target error) bool {
	var sfErr *SnowflakeError
	if errors.As(target, &sfErr) {
		return e.Code == sfErr.Code
	}
	return false
}

// ErrorResponse represents the JSON response structure for errors.
// This is the unified response type used by all handlers.
type ErrorResponse struct {
	Success  bool                   `json:"success"`
	Message  string                 `json:"message"`
	Code     string                 `json:"code"`
	SQLState string                 `json:"sqlState,omitempty"`
	Data     map[string]interface{} `json:"data,omitempty"`
}

// ToResponse converts the SnowflakeError to an ErrorResponse.
func (e *SnowflakeError) ToResponse() *ErrorResponse {
	data := make(map[string]interface{})

	// Copy data from error
	for k, v := range e.Data {
		data[k] = v
	}

	return &ErrorResponse{
		Success:  false,
		Message:  e.Message,
		Code:     e.Code,
		SQLState: e.SQLState,
		Data:     data,
	}
}

// NewSnowflakeError creates a new SnowflakeError with the given code and message.
func NewSnowflakeError(code, message string) *SnowflakeError {
	return &SnowflakeError{
		Code:     code,
		Message:  message,
		SQLState: GetSQLState(code),
		Data:     make(map[string]interface{}),
	}
}

// NewAuthenticationError creates an authentication error.
func NewAuthenticationError(message string) *SnowflakeError {
	return &SnowflakeError{
		Code:     CodeAuthenticationFailed,
		Message:  message,
		SQLState: SQLStateAuthenticationFailed,
		Data:     make(map[string]interface{}),
	}
}

// NewSessionNotFoundError creates a session not found error.
func NewSessionNotFoundError() *SnowflakeError {
	return &SnowflakeError{
		Code:     CodeSessionNotFound,
		Message:  "Session not found or expired",
		SQLState: SQLStateAuthenticationFailed,
		Data:     make(map[string]interface{}),
	}
}

// NewObjectNotFoundError creates an object not found error.
func NewObjectNotFoundError(objectType, objectName string) *SnowflakeError {
	message := fmt.Sprintf("Object not found: %s '%s'", objectType, objectName)
	return &SnowflakeError{
		Code:     CodeObjectNotFound,
		Message:  message,
		SQLState: SQLStateNoData,
		Data: map[string]interface{}{
			"objectType": objectType,
			"objectName": objectName,
		},
	}
}

// NewSQLCompilationError creates a SQL compilation error.
func NewSQLCompilationError(message string) *SnowflakeError {
	return &SnowflakeError{
		Code:     CodeSQLCompilationError,
		Message:  message,
		SQLState: SQLStateSyntaxError,
		Data:     make(map[string]interface{}),
	}
}

// NewInternalError creates an internal error.
func NewInternalError(message string) *SnowflakeError {
	return &SnowflakeError{
		Code:     CodeInternalError,
		Message:  message,
		SQLState: SQLStateGeneralError,
		Data:     make(map[string]interface{}),
	}
}

// NewInvalidParameterError creates an invalid parameter error.
func NewInvalidParameterError(paramName, reason string) *SnowflakeError {
	message := fmt.Sprintf("Invalid parameter '%s': %s", paramName, reason)
	return &SnowflakeError{
		Code:     CodeInvalidParameter,
		Message:  message,
		SQLState: SQLStateGeneralError,
		Data: map[string]interface{}{
			"paramName": paramName,
		},
	}
}

// NewSessionExpiredError creates a session expired error.
func NewSessionExpiredError() *SnowflakeError {
	return &SnowflakeError{
		Code:     CodeSessionExpired,
		Message:  "Session has expired",
		SQLState: SQLStateAuthenticationFailed,
		Data:     make(map[string]interface{}),
	}
}

// NewPermissionDeniedError creates a permission denied error.
func NewPermissionDeniedError(resource string) *SnowflakeError {
	message := fmt.Sprintf("Permission denied for resource: %s", resource)
	return &SnowflakeError{
		Code:     CodePermissionDenied,
		Message:  message,
		SQLState: SQLStateAuthenticationFailed,
		Data: map[string]interface{}{
			"resource": resource,
		},
	}
}

// WrapError wraps a standard Go error into a SnowflakeError.
func WrapError(code, message string, err error) *SnowflakeError {
	sfErr := &SnowflakeError{
		Code:     code,
		Message:  message,
		SQLState: SQLStateGeneralError,
		Data: map[string]interface{}{
			"originalError": err.Error(),
		},
	}
	return sfErr
}

// FromError converts a standard error to a SnowflakeError.
// If the error is already a SnowflakeError, it returns it as-is.
// If the error is nil, it returns nil.
// Otherwise, it wraps it as an internal error.
func FromError(err error) *SnowflakeError {
	if err == nil {
		return nil
	}

	var sfErr *SnowflakeError
	if errors.As(err, &sfErr) {
		return sfErr
	}

	return &SnowflakeError{
		Code:     CodeInternalError,
		Message:  err.Error(),
		SQLState: SQLStateGeneralError,
		Data:     make(map[string]interface{}),
	}
}
