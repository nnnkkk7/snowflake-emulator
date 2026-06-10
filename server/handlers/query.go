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
	sess, err := h.sessionMgr.ValidateSession(ctx, token)
	if err != nil {
		sendError(w, apierror.NewSnowflakeError(apierror.CodeSessionExpired, "Session expired or invalid"))
		return
	}
	sessionID := sess.ID

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

	bindings := convertGosnowflakeBindings(req.Bindings)

	if classification.IsQuery {
		h.executeQuery(w, ctx, sessionID, req.SQLText, bindings)
	} else {
		h.executeDML(w, ctx, sessionID, req.SQLText, bindings)
	}
}

// executeQuery executes a SELECT query with gosnowflake protocol.
func (h *QueryHandler) executeQuery(w http.ResponseWriter, ctx context.Context, sessionID int64, sqlText string, bindings map[string]*query.BindingValue) { //nolint:revive // context-as-argument: keeping w first for handler consistency
	// Generate unique query ID
	queryID := generateQueryID()

	var result *query.Result
	var err error

	if len(bindings) > 0 {
		result, err = h.executor.QueryWithBindings(ctx, sqlText, bindings)
	} else {
		result, err = h.executor.QueryWithHistory(
			ctx,
			fmt.Sprintf("%d", sessionID),
			queryID,
			sqlText,
		)
	}

	if err != nil {
		sendError(
			w,
			apierror.WrapError(
				apierror.CodeSQLExecutionError,
				fmt.Sprintf("query execution failed: %v", err),
				err,
			),
		)
		return
	}

	rowType := result.ColumnTypes
	rowSet := convertRowsToStrings(result.Rows)

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
func (h *QueryHandler) executeDML(w http.ResponseWriter, ctx context.Context, sessionID int64, sqlText string, bindings map[string]*query.BindingValue) { //nolint:revive // context-as-argument: keeping w first for handler consistency
	// Generate unique query ID
	queryID := generateQueryID()

	var result *query.ExecResult
	var err error

	if len(bindings) > 0 {
		result, err = h.executor.ExecuteWithBindings(ctx, sqlText, bindings)
	} else {
		result, err = h.executor.ExecuteWithHistory(
			ctx,
			fmt.Sprintf("%d", sessionID),
			queryID,
			sqlText,
		)
	}

	if err != nil {
		sendError(
			w,
			apierror.WrapError(
				apierror.CodeSQLExecutionError,
				"statement execution failed",
				err,
			),
		)
		return
	}

	stmtTypeID := query.GetStatementTypeID(sqlText)

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

	// TODO: Implement query cancellation tracking
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

func convertGosnowflakeBindings(
	input map[string]interface{},
) map[string]*query.BindingValue {
	if len(input) == 0 {
		return nil
	}

	result := make(map[string]*query.BindingValue, len(input))

	for key, raw := range input {
		switch v := raw.(type) {
		case map[string]interface{}:
			binding := &query.BindingValue{
				Type:  stringFromAny(v["type"]),
				Value: stringFromAny(v["value"]),
			}

			if binding.Type == "" {
				binding.Type = stringFromAny(v["TYPE"])
			}
			if binding.Value == "" {
				binding.Value = stringFromAny(v["VALUE"])
			}

			if binding.Type == "" {
				binding.Type = "TEXT"
			}

			result[key] = binding

		default:
			result[key] = &query.BindingValue{
				Type:  "TEXT",
				Value: fmt.Sprint(v),
			}
		}
	}

	return result
}

func stringFromAny(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}
