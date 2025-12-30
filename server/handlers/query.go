// Package handlers provides HTTP handlers for the Snowflake emulator API.
package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/nnnkkk7/snowflake-emulator/pkg/config"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
	"github.com/nnnkkk7/snowflake-emulator/pkg/session"
	"github.com/nnnkkk7/snowflake-emulator/server/apierror"
	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

// QueryHandler handles query execution HTTP requests.
type QueryHandler struct {
	executor   *query.Executor
	sessionMgr *session.Manager
}

// NewQueryHandler creates a new query handler.
func NewQueryHandler(executor *query.Executor, sessionMgr *session.Manager) *QueryHandler {
	return &QueryHandler{
		executor:   executor,
		sessionMgr: sessionMgr,
	}
}

// ExecuteQuery handles query execution requests with gosnowflake protocol.
func (h *QueryHandler) ExecuteQuery(w http.ResponseWriter, r *http.Request) {
	// Extract and validate token
	token := extractToken(r)
	if token == "" {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeSessionNotFound, "Authorization token required"))
		return
	}

	ctx := r.Context()

	// Validate session
	_, err := h.sessionMgr.ValidateSession(ctx, token)
	if err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeSessionExpired, "Session expired or invalid"))
		return
	}

	// Parse request using new gosnowflake protocol
	var req types.QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeInvalidParameter, "Invalid request body"))
		return
	}

	if req.SQLText == "" {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeInvalidParameter, "SQL text is required"))
		return
	}

	// Classify the SQL statement
	classification := query.ClassifySQL(req.SQLText)

	if classification.IsQuery {
		h.executeQuery(w, ctx, req.SQLText)
	} else {
		h.executeDML(w, ctx, req.SQLText)
	}
}

// executeQuery executes a SELECT query with gosnowflake protocol.
func (h *QueryHandler) executeQuery(w http.ResponseWriter, ctx context.Context, sqlText string) { //nolint:revive // context-as-argument: keeping w first for handler consistency
	result, err := h.executor.Query(ctx, sqlText)
	if err != nil {
		// Use apierror for error classification
		// Include the underlying error in the message for debugging
		sendError(w, apierror.WrapError(apierror.CodeSQLExecutionError, fmt.Sprintf("query execution failed: %v", err), err))
		return
	}

	// Generate unique query ID
	queryID := generateQueryID()

	// Use column types captured from actual query result
	rowType := result.ColumnTypes

	// Convert all values to strings for gosnowflake protocol
	rowSet := convertRowsToStrings(result.Rows)

	// Build success response
	resp := types.QueryResponse{
		Success: true,
		Data: &types.QuerySuccessData{
			QueryID:           queryID,
			SQLState:          apierror.SQLStateSuccess,
			StatementTypeID:   int64(config.StatementTypeSelect),
			RowType:           rowType,
			RowSet:            rowSet,
			Total:             int64(len(result.Rows)),
			Returned:          int64(len(result.Rows)),
			QueryResultFormat: config.QueryResultFormatJSON,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// executeDML executes a DML/DDL statement with gosnowflake protocol.
func (h *QueryHandler) executeDML(w http.ResponseWriter, ctx context.Context, sqlText string) { //nolint:revive // context-as-argument: keeping w first for handler consistency
	result, err := h.executor.Execute(ctx, sqlText)
	if err != nil {
		sendError(w, apierror.WrapError(apierror.CodeSQLExecutionError, "statement execution failed", err))
		return
	}

	// Generate unique query ID
	queryID := generateQueryID()

	// Get statement type ID using the classifier
	stmtTypeID := query.GetStatementTypeID(sqlText)

	// Build success response
	resp := types.QueryResponse{
		Success: true,
		Data: &types.QuerySuccessData{
			QueryID:           queryID,
			SQLState:          apierror.SQLStateSuccess,
			StatementTypeID:   int64(stmtTypeID),
			Total:             result.RowsAffected,
			Returned:          0,
			QueryResultFormat: config.QueryResultFormatJSON,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// AbortQuery handles query abort requests.
func (h *QueryHandler) AbortQuery(w http.ResponseWriter, r *http.Request) {
	var req types.AbortRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeInvalidParameter, "Invalid request body"))
		return
	}

	// Phase 1: Accept abort requests but don't track running queries yet
	resp := types.AbortResponse{
		Success: true,
		Message: "Query abort requested (not yet implemented)",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// generateQueryID generates a unique query ID.
func generateQueryID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp-only ID if random generation fails
		return fmt.Sprintf("01%d-00000000", time.Now().Unix())
	}
	timestamp := time.Now().Unix()
	return fmt.Sprintf("01%d-%s", timestamp, hex.EncodeToString(bytes))
}

// convertRowsToStrings converts all values in rows to strings for gosnowflake protocol.
func convertRowsToStrings(rows [][]interface{}) [][]string {
	result := make([][]string, len(rows))
	for i, row := range rows {
		strRow := make([]string, len(row))
		for j, val := range row {
			if val == nil {
				strRow[j] = ""
			} else {
				strRow[j] = fmt.Sprintf("%v", val)
			}
		}
		result[i] = strRow
	}
	return result
}
