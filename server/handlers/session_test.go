package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/session"
	"github.com/nnnkkk7/snowflake-emulator/server/apierror"
	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

// setupTestHandler creates a test handler with dependencies.
func setupTestHandler(t *testing.T) *SessionHandler {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close DB: %v", err)
		}
	})

	mgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	sessionMgr := session.NewManager(1 * time.Hour)

	// Create test database and schema
	ctx := context.Background()
	database, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	_, err = repo.CreateSchema(ctx, database.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	return NewSessionHandler(sessionMgr, repo)
}

// TestSessionHandler_LoginRequest tests the login endpoint.
func TestSessionHandler_LoginRequest(t *testing.T) {
	handler := setupTestHandler(t)

	tests := []struct {
		name           string
		request        types.LoginRequest
		expectedStatus int
		checkResponse  func(*testing.T, *types.LoginResponse)
	}{
		{
			name: "ValidLogin",
			request: types.LoginRequest{
				Data: types.LoginRequestData{
					LoginName:    "testuser",
					Password:     "testpass",
					DatabaseName: "TEST_DB",
					SchemaName:   "PUBLIC",
				},
			},
			expectedStatus: http.StatusOK,
			checkResponse: func(t *testing.T, resp *types.LoginResponse) {
				if !resp.Success {
					t.Error("Expected success to be true")
				}
				if resp.Data == nil {
					t.Fatal("Expected data to be set")
				}
				if resp.Data.Token == "" {
					t.Error("Expected token to be set")
				}
				if resp.Data.MasterToken == "" {
					t.Error("Expected master token to be set")
				}
				if resp.Data.SessionID == 0 {
					t.Error("Expected session ID to be set")
				}
				if resp.Data.SessionInfo.DatabaseName != "TEST_DB" {
					t.Errorf("Expected database TEST_DB, got %s", resp.Data.SessionInfo.DatabaseName)
				}
				if resp.Data.SessionInfo.SchemaName != "PUBLIC" {
					t.Errorf("Expected schema PUBLIC, got %s", resp.Data.SessionInfo.SchemaName)
				}
			},
		},
		{
			name: "MissingUsername",
			request: types.LoginRequest{
				Data: types.LoginRequestData{
					LoginName:    "",
					Password:     "testpass",
					DatabaseName: "TEST_DB",
					SchemaName:   "PUBLIC",
				},
			},
			expectedStatus: http.StatusOK, // Snowflake returns 200 even for errors
			checkResponse: func(t *testing.T, resp *types.LoginResponse) {
				if resp.Success {
					t.Error("Expected success to be false")
				}
				if resp.Code != apierror.CodeAuthenticationFailed {
					t.Errorf("Expected code %s, got %s", apierror.CodeAuthenticationFailed, resp.Code)
				}
			},
		},
		{
			name: "MissingPassword",
			request: types.LoginRequest{
				Data: types.LoginRequestData{
					LoginName:    "testuser",
					Password:     "",
					DatabaseName: "TEST_DB",
					SchemaName:   "PUBLIC",
				},
			},
			expectedStatus: http.StatusOK,
			checkResponse: func(t *testing.T, resp *types.LoginResponse) {
				if resp.Success {
					t.Error("Expected success to be false")
				}
				if resp.Code != apierror.CodeAuthenticationFailed {
					t.Errorf("Expected code %s, got %s", apierror.CodeAuthenticationFailed, resp.Code)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request
			body, err := json.Marshal(tt.request)
			if err != nil {
				t.Fatalf("Failed to marshal request: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/session/v1/login-request", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			// Record response
			rr := httptest.NewRecorder()

			// Handle request
			handler.Login(rr, req)

			// Check status code
			if rr.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rr.Code)
			}

			// Parse response
			var resp types.LoginResponse
			if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
				t.Fatalf("Failed to unmarshal response: %v", err)
			}

			// Check response
			if tt.checkResponse != nil {
				tt.checkResponse(t, &resp)
			}
		})
	}
}

// TestSessionHandler_TokenRequest tests session token renewal with master token.
func TestSessionHandler_TokenRequest(t *testing.T) {
	handler := setupTestHandler(t)

	// First, create a session via login
	loginReq := types.LoginRequest{
		Data: types.LoginRequestData{
			LoginName:    "testuser",
			Password:     "testpass",
			DatabaseName: "TEST_DB",
			SchemaName:   "PUBLIC",
		},
	}

	body, _ := json.Marshal(loginReq)
	req := httptest.NewRequest(http.MethodPost, "/session/v1/login-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.Login(rr, req)

	var loginResp types.LoginResponse
	json.Unmarshal(rr.Body.Bytes(), &loginResp)

	if !loginResp.Success {
		t.Fatal("Login failed")
	}

	// Now test session token renewal with master token
	tokenReq := types.TokenRequest{
		MasterToken: loginResp.Data.MasterToken,
		RequestType: "RENEW",
	}

	body, _ = json.Marshal(tokenReq)
	req = httptest.NewRequest(http.MethodPost, "/session/token-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()

	handler.TokenRequest(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rr.Code)
	}

	var tokenResp types.TokenResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &tokenResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if !tokenResp.Success {
		t.Error("Expected success to be true")
	}

	if tokenResp.Data == nil {
		t.Fatal("Expected data to be set")
	}

	if tokenResp.Data.SessionToken == "" {
		t.Error("Expected new session token to be set")
	}

	// Verify new token is different from original token
	if tokenResp.Data.SessionToken == loginResp.Data.Token {
		t.Error("Expected new token to be different from original token")
	}
}

// TestSessionHandler_Logout tests session logout.
func TestSessionHandler_Logout(t *testing.T) {
	handler := setupTestHandler(t)

	// First, create a session via login
	loginReq := types.LoginRequest{
		Data: types.LoginRequestData{
			LoginName:    "testuser",
			Password:     "testpass",
			DatabaseName: "TEST_DB",
			SchemaName:   "PUBLIC",
		},
	}

	body, _ := json.Marshal(loginReq)
	req := httptest.NewRequest(http.MethodPost, "/session/v1/login-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.Login(rr, req)

	var loginResp types.LoginResponse
	json.Unmarshal(rr.Body.Bytes(), &loginResp)

	// Test logout
	logoutReq := LogoutRequest{
		Token: loginResp.Data.Token,
	}

	body, _ = json.Marshal(logoutReq)
	req = httptest.NewRequest(http.MethodPost, "/session/logout", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()

	handler.Logout(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rr.Code)
	}

	var logoutResp LogoutResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &logoutResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if !logoutResp.Success {
		t.Error("Expected success to be true")
	}

	// Verify session is gone - try to renew token with master token
	tokenReq := types.TokenRequest{
		MasterToken: loginResp.Data.MasterToken,
		RequestType: "RENEW",
	}

	body, _ = json.Marshal(tokenReq)
	req = httptest.NewRequest(http.MethodPost, "/session/token-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.TokenRequest(rr, req)

	// After logout, token renewal should still fail
	var tokenResp types.TokenResponse
	json.Unmarshal(rr.Body.Bytes(), &tokenResp)
	if tokenResp.Success {
		t.Error("Expected token renewal to fail after logout")
	}
}

// TestSessionHandler_InvalidJSON tests handling of invalid JSON.
func TestSessionHandler_InvalidJSON(t *testing.T) {
	handler := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/session/v1/login-request", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler.Login(rr, req)

	// Snowflake returns 200 even for errors
	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rr.Code)
	}

	var resp apierror.ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.Success {
		t.Error("Expected success to be false")
	}

	if resp.Code != apierror.CodeInvalidParameter {
		t.Errorf("Expected code %s, got %s", apierror.CodeInvalidParameter, resp.Code)
	}
}

// TestSessionHandler_UseContext tests using session context (database/schema).
func TestSessionHandler_UseContext(t *testing.T) {
	handler := setupTestHandler(t)

	// Create session
	loginReq := types.LoginRequest{
		Data: types.LoginRequestData{
			LoginName:    "testuser",
			Password:     "testpass",
			DatabaseName: "TEST_DB",
			SchemaName:   "PUBLIC",
		},
	}

	body, _ := json.Marshal(loginReq)
	req := httptest.NewRequest(http.MethodPost, "/session/v1/login-request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.Login(rr, req)

	var loginResp types.LoginResponse
	json.Unmarshal(rr.Body.Bytes(), &loginResp)

	// Change database/schema
	useReq := UseContextRequest{
		Token:    loginResp.Data.Token,
		Database: "TEST_DB",
		Schema:   "PUBLIC",
	}

	body, _ = json.Marshal(useReq)
	req = httptest.NewRequest(http.MethodPost, "/session/use", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()

	handler.UseContext(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rr.Code)
	}

	var useResp UseContextResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &useResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if !useResp.Success {
		t.Error("Expected success to be true")
	}
}

// TestExtractToken tests the token extraction from Authorization header.
func TestExtractToken(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		expected string
	}{
		{
			name:     "SnowflakeTokenWithQuotes",
			header:   `Snowflake Token="test-token-123"`,
			expected: "test-token-123",
		},
		{
			name:     "SnowflakeTokenWithoutQuotes",
			header:   `Snowflake Token=test-token-123`,
			expected: "test-token-123",
		},
		{
			name:     "BearerToken",
			header:   `Bearer test-token-123`,
			expected: "test-token-123",
		},
		{
			name:     "BearerTokenLowercase",
			header:   `bearer test-token-123`,
			expected: "test-token-123",
		},
		{
			name:     "SnowflakeLowercase",
			header:   `snowflake Token="test-token-123"`,
			expected: "test-token-123",
		},
		{
			name:     "EmptyHeader",
			header:   "",
			expected: "",
		},
		{
			name:     "InvalidFormat",
			header:   "Invalid format",
			expected: "",
		},
		{
			name:     "WhitespaceAround",
			header:   `  Snowflake Token="test-token"  `,
			expected: "test-token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}

			result := extractToken(req)
			if result != tt.expected {
				t.Errorf("extractToken() = %q, want %q", result, tt.expected)
			}
		})
	}
}
