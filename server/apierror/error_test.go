package apierror

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestSnowflakeError_Error tests error message formatting.
func TestSnowflakeError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *SnowflakeError
		expected string
	}{
		{
			name: "SimpleError",
			err: &SnowflakeError{
				Code:     "000001",
				Message:  "Test error message",
				SQLState: "42000",
			},
			expected: "[000001] Test error message",
		},
		{
			name: "ErrorWithDetails",
			err: &SnowflakeError{
				Code:     "002003",
				Message:  "Database not found",
				SQLState: "42S02",
				Data: map[string]interface{}{
					"database": "NONEXISTENT_DB",
				},
			},
			expected: "[002003] Database not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Error()
			if result != tt.expected {
				t.Errorf("Error() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// TestSnowflakeError_MarshalJSON tests JSON serialization.
func TestSnowflakeError_MarshalJSON(t *testing.T) {
	err := &SnowflakeError{
		Code:     "000001",
		Message:  "Test error",
		SQLState: "42000",
		Data: map[string]interface{}{
			"key": "value",
		},
	}

	data, marshalErr := json.Marshal(err)
	if marshalErr != nil {
		t.Fatalf("Marshal() error = %v", marshalErr)
	}

	// Unmarshal to verify structure
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	// Verify required fields
	if result["code"] != "000001" {
		t.Errorf("Expected code '000001', got %v", result["code"])
	}
	if result["message"] != "Test error" {
		t.Errorf("Expected message 'Test error', got %v", result["message"])
	}
	if result["sqlState"] != "42000" {
		t.Errorf("Expected sqlState '42000', got %v", result["sqlState"])
	}
}

// TestNewError tests error creation helpers.
func TestNewError(t *testing.T) {
	tests := []struct {
		name         string
		createFunc   func() *SnowflakeError
		expectedCode string
		expectedMsg  string
	}{
		{
			name: "AuthenticationError",
			createFunc: func() *SnowflakeError {
				return NewAuthenticationError("Invalid credentials")
			},
			expectedCode: CodeAuthenticationFailed,
			expectedMsg:  "Invalid credentials",
		},
		{
			name: "ObjectNotFoundError",
			createFunc: func() *SnowflakeError {
				return NewObjectNotFoundError("TABLE", "USERS")
			},
			expectedCode: CodeObjectNotFound,
			expectedMsg:  "Object not found: TABLE 'USERS'",
		},
		{
			name: "SQLCompilationError",
			createFunc: func() *SnowflakeError {
				return NewSQLCompilationError("Syntax error at line 1")
			},
			expectedCode: CodeSQLCompilationError,
			expectedMsg:  "Syntax error at line 1",
		},
		{
			name: "InternalError",
			createFunc: func() *SnowflakeError {
				return NewInternalError("Unexpected condition")
			},
			expectedCode: CodeInternalError,
			expectedMsg:  "Unexpected condition",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.createFunc()

			if err.Code != tt.expectedCode {
				t.Errorf("Expected code %s, got %s", tt.expectedCode, err.Code)
			}
			if err.Message != tt.expectedMsg {
				t.Errorf("Expected message %q, got %q", tt.expectedMsg, err.Message)
			}
		})
	}
}

// TestWrapError tests wrapping Go errors.
func TestWrapError(t *testing.T) {
	originalErr := errors.New("database connection failed")

	wrapped := WrapError(CodeInternalError, "Failed to connect", originalErr)

	if wrapped.Code != CodeInternalError {
		t.Errorf("Expected code %s, got %s", CodeInternalError, wrapped.Code)
	}

	// Verify the original error is preserved in Data
	if wrapped.Data == nil {
		t.Fatal("Expected Data to be set")
	}

	originalErrStr, ok := wrapped.Data["originalError"].(string)
	if !ok {
		t.Fatal("Expected originalError in Data")
	}

	if originalErrStr != originalErr.Error() {
		t.Errorf("Expected originalError %q, got %q", originalErr.Error(), originalErrStr)
	}
}

// TestErrorCodes tests that all error codes are defined.
func TestErrorCodes(t *testing.T) {
	codes := []struct {
		name  string
		value string
	}{
		{"CodeAuthenticationFailed", CodeAuthenticationFailed},
		{"CodeObjectNotFound", CodeObjectNotFound},
		{"CodeSQLCompilationError", CodeSQLCompilationError},
		{"CodeInternalError", CodeInternalError},
		{"CodeInvalidParameter", CodeInvalidParameter},
		{"CodeSessionExpired", CodeSessionExpired},
		{"CodePermissionDenied", CodePermissionDenied},
	}

	for _, tc := range codes {
		t.Run(tc.name, func(t *testing.T) {
			if tc.value == "" {
				t.Errorf("%s should not be empty", tc.name)
			}
		})
	}
}

// TestSnowflakeError_WithData tests adding data to errors.
func TestSnowflakeError_WithData(t *testing.T) {
	err := NewAuthenticationError("Invalid password")

	// Add data
	err.WithData("username", "user123")
	err.WithData("attempt", 3)

	if err.Data["username"] != "user123" {
		t.Errorf("Expected username 'user123', got %v", err.Data["username"])
	}

	if err.Data["attempt"] != 3 {
		t.Errorf("Expected attempt 3, got %v", err.Data["attempt"])
	}
}

// TestResponseFormat tests Snowflake error response format.
func TestResponseFormat(t *testing.T) {
	err := &SnowflakeError{
		Code:     "002003",
		Message:  "Object does not exist",
		SQLState: "42S02",
		Data: map[string]interface{}{
			"objectType": "TABLE",
			"objectName": "USERS",
		},
	}

	response := err.ToResponse()

	// Verify response structure
	if response.Success {
		t.Error("Expected success to be false")
	}

	if response.Message != err.Message {
		t.Errorf("Expected message %q, got %q", err.Message, response.Message)
	}

	if response.Code != err.Code {
		t.Errorf("Expected code %q, got %q", err.Code, response.Code)
	}

	if response.SQLState != err.SQLState {
		t.Errorf("Expected SQLState %q, got %q", err.SQLState, response.SQLState)
	}

	if response.Data == nil {
		t.Error("Expected data to be set")
	}
}

// TestFromStandardError tests converting standard errors to SnowflakeError.
func TestFromStandardError(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		expectedCode string
	}{
		{
			name:         "NilError",
			err:          nil,
			expectedCode: "",
		},
		{
			name:         "StandardError",
			err:          errors.New("something went wrong"),
			expectedCode: CodeInternalError,
		},
		{
			name:         "SnowflakeError",
			err:          NewAuthenticationError("auth failed"),
			expectedCode: CodeAuthenticationFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FromError(tt.err)

			if tt.err == nil {
				if result != nil {
					t.Error("Expected nil for nil input")
				}
				return
			}

			if result == nil {
				t.Fatal("Expected non-nil result")
			}

			if result.Code != tt.expectedCode {
				t.Errorf("Expected code %s, got %s", tt.expectedCode, result.Code)
			}
		})
	}
}

// TestErrorResponse_JSON tests JSON serialization of error responses.
func TestErrorResponse_JSON(t *testing.T) {
	err := NewObjectNotFoundError("DATABASE", "TEST_DB")
	response := err.ToResponse()

	data, marshalErr := json.Marshal(response)
	if marshalErr != nil {
		t.Fatalf("Marshal() error = %v", marshalErr)
	}

	// Unmarshal to verify structure
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	// Verify required fields
	if result["success"] != false {
		t.Error("Expected success to be false")
	}

	if result["message"] == nil {
		t.Error("Expected message to be set")
	}

	if result["code"] == nil {
		t.Error("Expected code to be set")
	}

	if result["sqlState"] == nil {
		t.Error("Expected sqlState to be set")
	}
}

// TestSnowflakeError_Is tests error comparison.
func TestSnowflakeError_Is(t *testing.T) {
	err1 := NewAuthenticationError("auth failed")
	err2 := NewAuthenticationError("different message")
	err3 := NewObjectNotFoundError("TABLE", "USERS")

	// Same error code should match
	if !err1.Is(err2) {
		t.Error("Expected errors with same code to match")
	}

	// Different error code should not match
	if err1.Is(err3) {
		t.Error("Expected errors with different codes not to match")
	}

	// Standard error should not match
	stdErr := errors.New("standard error")
	if err1.Is(stdErr) {
		t.Error("Expected SnowflakeError not to match standard error")
	}
}

// TestErrorResponse_Complete tests complete error response structure.
func TestErrorResponse_Complete(t *testing.T) {
	err := &SnowflakeError{
		Code:     CodeSQLCompilationError,
		Message:  "SQL compilation error: syntax error at line 1",
		SQLState: "42601",
		Data: map[string]interface{}{
			"line":   1,
			"column": 15,
			"query":  "SELECT FROM users",
		},
	}

	response := err.ToResponse()

	expected := &ErrorResponse{
		Success:  false,
		Message:  "SQL compilation error: syntax error at line 1",
		Code:     CodeSQLCompilationError,
		SQLState: "42601",
		Data: map[string]interface{}{
			"line":   1,
			"column": 15,
			"query":  "SELECT FROM users",
		},
	}

	if diff := cmp.Diff(expected, response); diff != "" {
		t.Errorf("ToResponse() mismatch (-want +got):\n%s", diff)
	}
}
