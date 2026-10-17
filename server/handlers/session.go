package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/nnnkkk7/snowflake-emulator/pkg/config"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/session"
	"github.com/nnnkkk7/snowflake-emulator/server/apierror"
	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

// SessionHandler handles session-related HTTP requests.
type SessionHandler struct {
	sessionMgr *session.Manager
	repo       *metadata.Repository
}

// RenewSessionRequest represents a session renewal request (legacy).
type RenewSessionRequest struct {
	Token string `json:"token"`
}

// RenewSessionResponse represents a session renewal response (legacy).
type RenewSessionResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Code    string `json:"code,omitempty"`
}

// LogoutRequest represents a logout request.
type LogoutRequest struct {
	Token string `json:"token"`
}

// LogoutResponse represents a logout response.
type LogoutResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// UseContextRequest represents a USE DATABASE/SCHEMA request.
type UseContextRequest struct {
	Token    string `json:"token"`
	Database string `json:"database,omitempty"`
	Schema   string `json:"schema,omitempty"`
}

// UseContextResponse represents a USE context response.
type UseContextResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// NewSessionHandler creates a new session handler.
func NewSessionHandler(sessionMgr *session.Manager, repo *metadata.Repository) *SessionHandler {
	return &SessionHandler{
		sessionMgr: sessionMgr,
		repo:       repo,
	}
}

// Login handles login requests with gosnowflake protocol.
func (h *SessionHandler) Login(w http.ResponseWriter, r *http.Request) {
	var req types.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeInvalidParameter, "Invalid request body"))
		return
	}

	// Validate required fields
	if req.Data.LoginName == "" || req.Data.Password == "" {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeAuthenticationFailed, "Username and password are required"))
		return
	}

	// Set default database/schema if not provided
	database := req.Data.DatabaseName
	if database == "" {
		database = config.DefaultDatabase
	}

	schema := req.Data.SchemaName
	if schema == "" {
		schema = config.DefaultSchema
	}

	ctx := r.Context()

	// Ensure database exists (try to get it, create if not found)
	_, err := h.repo.GetDatabaseByName(ctx, database)
	if err != nil {
		// Database doesn't exist, create it
		_, err = h.repo.CreateDatabase(ctx, database, "Auto-created database")
		if err != nil {
			sendError(w, apierror.NewSnowflakeError(apierror.CodeInternalError, "Failed to initialize database"))
			return
		}
	}

	// Create session with master token support
	sess, err := h.sessionMgr.CreateSession(ctx, req.Data.LoginName, database, schema)
	if err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeInternalError, "Failed to create session"))
		return
	}

	// Build parameter bindings from default session parameters
	defaultParams := config.DefaultSessionParameters()
	parameters := []types.ParameterBinding{
		{Name: string(config.ParamTimezone), Value: defaultParams[config.ParamTimezone]},
		{Name: string(config.ParamTimestampOutputFormat), Value: defaultParams[config.ParamTimestampOutputFormat]},
		{Name: string(config.ParamClientSessionKeepAlive), Value: defaultParams[config.ParamClientSessionKeepAlive]},
		{Name: string(config.ParamQueryTag), Value: defaultParams[config.ParamQueryTag]},
		{Name: string(config.ParamGoQueryResultFormat), Value: defaultParams[config.ParamGoQueryResultFormat]},
	}

	// Add user-provided session parameters
	for k, v := range req.Data.SessionParams {
		// Convert any type to string for the response
		var strValue string
		switch val := v.(type) {
		case string:
			strValue = val
		case bool:
			if val {
				strValue = "true"
			} else {
				strValue = "false"
			}
		default:
			strValue = fmt.Sprintf("%v", v)
		}
		parameters = append(parameters, types.ParameterBinding{Name: k, Value: strValue})
	}

	// Build success response
	resp := types.LoginResponse{
		Success: true,
		Data: &types.LoginSuccessData{
			Token:                   sess.Token,
			MasterToken:             sess.MasterToken,
			ValidityInSeconds:       sess.ValidityInSeconds,
			MasterValidityInSeconds: sess.MasterValidityInSeconds,
			SessionID:               sess.ID,
			Parameters:              parameters,
			SessionInfo: types.SessionInfo{
				DatabaseName:  database,
				SchemaName:    schema,
				WarehouseName: req.Data.WarehouseName,
				RoleName:      req.Data.RoleName,
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// TokenRequest handles token renewal with master token.
func (h *SessionHandler) TokenRequest(w http.ResponseWriter, r *http.Request) {
	var req types.TokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeInvalidParameter, "Invalid request body"))
		return
	}

	if req.MasterToken == "" {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeInvalidParameter, "Master token is required"))
		return
	}

	ctx := r.Context()

	// Renew session token using master token
	sess, newToken, err := h.sessionMgr.RenewToken(ctx, req.MasterToken)
	if err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeSessionExpired, "Master token is invalid or expired"))
		return
	}

	resp := types.TokenResponse{
		Success: true,
		Data: &types.TokenSuccessData{
			SessionToken:      newToken,
			ValidityInSeconds: sess.ValidityInSeconds,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// Heartbeat handles session keep-alive requests.
func (h *SessionHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	token := extractToken(r)
	if token == "" {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeSessionNotFound, "Authorization token required"))
		return
	}

	ctx := r.Context()

	// Update last accessed time
	if err := h.sessionMgr.UpdateLastAccessed(ctx, token); err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeSessionNotFound, "Session not found"))
		return
	}

	resp := types.HeartbeatResponse{
		Success: true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// RenewSession handles session renewal requests (legacy - delegates to TokenRequest).
func (h *SessionHandler) RenewSession(w http.ResponseWriter, r *http.Request) {
	h.TokenRequest(w, r)
}

// Logout handles logout requests.
func (h *SessionHandler) Logout(w http.ResponseWriter, r *http.Request) {
	var req LogoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeInvalidParameter, "Invalid request body"))
		return
	}

	ctx := r.Context()

	// Close session
	if err := h.sessionMgr.CloseSession(ctx, req.Token); err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeInternalError, "Failed to close session"))
		return
	}

	// Write success response
	resp := LogoutResponse{
		Success: true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// UseContext handles USE DATABASE/SCHEMA requests.
func (h *SessionHandler) UseContext(w http.ResponseWriter, r *http.Request) {
	var req UseContextRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeInvalidParameter, "Invalid request body"))
		return
	}

	ctx := r.Context()

	// Update session context
	if err := h.sessionMgr.UpdateSessionContext(ctx, req.Token, req.Database, req.Schema); err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeInvalidParameter, "Failed to update session context"))
		return
	}

	// Write success response
	resp := UseContextResponse{
		Success: true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// sendError sends an error response using gosnowflake protocol.
func sendError(w http.ResponseWriter, err *apierror.SnowflakeError) {
	resp := apierror.ErrorResponse{
		Success:  false,
		Message:  err.Message,
		Code:     err.Code,
		SQLState: err.SQLState,
		Data:     err.Data,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // Snowflake returns 200 even for errors
	_ = json.NewEncoder(w).Encode(resp)
}

// CloseSession handles DELETE /session requests from gosnowflake.
func (h *SessionHandler) CloseSession(w http.ResponseWriter, r *http.Request) {
	token := extractToken(r)
	if token == "" {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeSessionNotFound, "Authorization token required"))
		return
	}

	ctx := r.Context()

	// Close session
	if err := h.sessionMgr.CloseSession(ctx, token); err != nil {
		// Session might already be closed, treat as success
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    nil,
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    nil,
	})
}

// extractToken extracts the session token from Authorization header.
// Supports multiple formats:
// - Snowflake Token="xxx" (gosnowflake format)
// - Bearer xxx (standard format)
func extractToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ""
	}

	auth = strings.TrimSpace(auth)

	// Parse "Snowflake Token="xxx"" (case-insensitive for "Snowflake")
	if len(auth) >= 17 && strings.EqualFold(auth[:10], "Snowflake ") {
		rest := auth[10:]
		if strings.HasPrefix(rest, "Token=\"") && strings.HasSuffix(rest, "\"") {
			return rest[7 : len(rest)-1]
		}
		// Handle without quotes: Snowflake Token=xxx
		if strings.HasPrefix(rest, "Token=") {
			return rest[6:]
		}
	}

	// Parse "Bearer xxx" (case-insensitive)
	if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
		return strings.TrimSpace(auth[7:])
	}

	return ""
}
