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

	// Convert bindings if present
	bindings := convertBindings(req.Bindings)

	// Classify the SQL statement
	classification := query.ClassifySQL(req.SQLText)

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

	// Execute query with history tracking and bindings
	result, err := h.executor.QueryWithHistory(ctx, fmt.Sprintf("%d", sessionID), queryID, sqlText, bindings)
	if err != nil {
		// Use apierror for error classification
		// Include the underlying error in the message for debugging
		sendError(w, apierror.WrapError(apierror.CodeSQLExecutionError, fmt.Sprintf("query execution failed: %v", err), err))
		return
	}

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
func (h *QueryHandler) executeDML(w http.ResponseWriter, ctx context.Context, sessionID int64, sqlText string, bindings map[string]*query.BindingValue) { //nolint:revive // context-as-argument: keeping w first for handler consistency
	// Generate unique query ID
	queryID := generateQueryID()

	// Execute statement with history tracking and bindings
	result, err := h.executor.ExecuteWithHistory(ctx, fmt.Sprintf("%d", sessionID), queryID, sqlText, bindings)
	if err != nil {
		sendError(w, apierror.WrapError(apierror.CodeSQLExecutionError, "statement execution failed", err))
		return
	}

	// Get statement type ID using the classifier
	stmtTypeID := query.GetStatementTypeID(sqlText)

	// Build response with rowset data for DML statements.
	// The Snowflake .NET driver's CalculateUpdateCount reads affected row counts
	// from the rowset data (not the total field) for INSERT/UPDATE/DELETE/MERGE.
	rowType, rowSet, returned := buildDMLRowset(stmtTypeID, result.RowsAffected)

	resp := types.QueryResponse{
		Success: true,
		Data: &types.QuerySuccessData{
			QueryID:           queryID,
			SQLState:          apierror.SQLStateSuccess,
			StatementTypeID:   int64(stmtTypeID),
			RowType:           rowType,
			RowSet:            rowSet,
			Total:             result.RowsAffected,
			Returned:          returned,
			QueryResultFormat: config.QueryResultFormatJSON,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// dmlColumnName returns the column name for the affected row count based on statement type.
func dmlColumnName(stmtTypeID config.StatementTypeID) (string, bool) {
	switch stmtTypeID {
	case config.StatementTypeInsert:
		return "number of rows inserted", true
	case config.StatementTypeUpdate:
		return "number of rows updated", true
	case config.StatementTypeDelete:
		return "number of rows deleted", true
	case config.StatementTypeMerge:
		return "number of rows merged", true
	default:
		return "", false
	}
}

// buildDMLRowset builds the rowType and rowSet for DML responses.
// For INSERT/UPDATE/DELETE/MERGE, it returns a single-row rowset with the affected count.
// For other statements (DDL, transaction control), it returns empty arrays.
func buildDMLRowset(stmtTypeID config.StatementTypeID, rowsAffected int64) ([]types.ColumnMetadata, [][]string, int64) {
	colName, isDML := dmlColumnName(stmtTypeID)
	if !isDML {
		return []types.ColumnMetadata{}, [][]string{}, 0
	}

	rowType := []types.ColumnMetadata{
		{
			Name:      colName,
			Type:      "fixed",
			Scale:     0,
			Precision: 19,
			Nullable:  false,
		},
	}
	rowSet := [][]string{
		{fmt.Sprintf("%d", rowsAffected)},
	}
	return rowType, rowSet, 1
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
