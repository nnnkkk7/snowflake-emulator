package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
	"github.com/nnnkkk7/snowflake-emulator/pkg/warehouse"
	"github.com/nnnkkk7/snowflake-emulator/server/apierror"
	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

// RestAPIv2Handler handles REST API v2 requests.
type RestAPIv2Handler struct {
	executor     *query.Executor
	stmtMgr      *query.StatementManager
	repo         *metadata.Repository
	warehouseMgr *warehouse.Manager
}

// NewRestAPIv2Handler creates a new REST API v2 handler.
func NewRestAPIv2Handler(executor *query.Executor, stmtMgr *query.StatementManager, repo *metadata.Repository) *RestAPIv2Handler {
	return &RestAPIv2Handler{
		executor:     executor,
		stmtMgr:      stmtMgr,
		repo:         repo,
		warehouseMgr: warehouse.NewManager(),
	}
}

// NewRestAPIv2HandlerWithWarehouse creates a new REST API v2 handler with warehouse manager.
func NewRestAPIv2HandlerWithWarehouse(executor *query.Executor, stmtMgr *query.StatementManager, repo *metadata.Repository, warehouseMgr *warehouse.Manager) *RestAPIv2Handler {
	return &RestAPIv2Handler{
		executor:     executor,
		stmtMgr:      stmtMgr,
		repo:         repo,
		warehouseMgr: warehouseMgr,
	}
}

// SubmitStatement handles POST /api/v2/statements.
func (h *RestAPIv2Handler) SubmitStatement(w http.ResponseWriter, r *http.Request) {
	var req types.SubmitStatementRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body", types.SQLState42000)
		return
	}

	if req.Statement == "" {
		h.sendError(w, http.StatusBadRequest, "Statement is required", types.SQLState42000)
		return
	}

	// Create statement record
	stmt := h.stmtMgr.CreateStatement(req.Statement, req.Database, req.Schema, req.Warehouse)
	h.stmtMgr.UpdateStatus(stmt.Handle, query.StatementStatusRunning)

	// Execute the statement synchronously
	ctx := r.Context()

	// Convert bindings from types.BindingValue to query.QueryBindingValue
	var result *query.Result
	var err error
	if len(req.Bindings) > 0 {
		bindings := convertBindings(req.Bindings)
		result, err = h.executor.QueryWithBindings(ctx, req.Statement, bindings)
	} else {
		result, err = h.executor.Query(ctx, req.Statement)
	}

	if err != nil {
		sfErr := apierror.NewSnowflakeError(apierror.CodeSQLExecutionError, err.Error())
		h.stmtMgr.SetError(stmt.Handle, sfErr)

		resp := types.StatementResponse{
			StatementHandle: stmt.Handle,
			Code:            apierror.CodeSQLExecutionError,
			SQLState:        types.SQLState42000,
			Message:         err.Error(),
			CreatedOn:       stmt.CreatedOn.Unix(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Store result
	h.stmtMgr.SetResult(stmt.Handle, result)

	// Build response
	resp := h.buildStatementResponse(stmt, result)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// GetStatement handles GET /api/v2/statements/{handle}.
func (h *RestAPIv2Handler) GetStatement(w http.ResponseWriter, r *http.Request) {
	handle := chi.URLParam(r, "handle")

	stmt, ok := h.stmtMgr.GetStatement(handle)
	if !ok {
		h.sendError(w, http.StatusNotFound, "Statement not found", types.SQLState02000)
		return
	}

	var resp types.StatementResponse

	switch stmt.Status {
	case query.StatementStatusRunning, query.StatementStatusPending:
		resp = types.StatementResponse{
			StatementHandle:    stmt.Handle,
			Code:               types.ResponseCodeStatementPending,
			SQLState:           types.SQLState00000,
			StatementStatusURL: "/api/v2/statements/" + stmt.Handle,
			CreatedOn:          stmt.CreatedOn.Unix(),
		}
	case query.StatementStatusSuccess:
		resp = h.buildStatementResponse(stmt, stmt.Result)
	case query.StatementStatusFailed:
		resp = types.StatementResponse{
			StatementHandle: stmt.Handle,
			Code:            stmt.Error.Code,
			SQLState:        types.SQLState42000,
			Message:         stmt.Error.Message,
			CreatedOn:       stmt.CreatedOn.Unix(),
		}
	case query.StatementStatusCanceled:
		resp = types.StatementResponse{
			StatementHandle: stmt.Handle,
			Code:            types.ResponseCodeStatementCanceled,
			SQLState:        types.SQLState00000,
			Message:         "Statement canceled",
			CreatedOn:       stmt.CreatedOn.Unix(),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// CancelStatement handles POST /api/v2/statements/{handle}/cancel.
func (h *RestAPIv2Handler) CancelStatement(w http.ResponseWriter, r *http.Request) {
	handle := chi.URLParam(r, "handle")

	stmt, ok := h.stmtMgr.GetStatement(handle)
	if !ok {
		h.sendError(w, http.StatusNotFound, "Statement not found", types.SQLState02000)
		return
	}

	if err := h.stmtMgr.CancelStatement(handle); err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error(), types.SQLState42000)
		return
	}

	resp := types.CancelStatementResponse{
		Code:            types.ResponseCodeStatementCanceled,
		SQLState:        types.SQLState00000,
		Message:         "Statement canceled",
		StatementHandle: stmt.Handle,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// buildStatementResponse builds a success response from a query result.
func (h *RestAPIv2Handler) buildStatementResponse(stmt *query.Statement, result *query.Result) types.StatementResponse {
	// Convert row type
	rowType := make([]types.RowTypeField, len(result.ColumnTypes))
	for i, col := range result.ColumnTypes {
		rowType[i] = types.RowTypeField{
			Name:      col.Name,
			Type:      col.Type,
			Length:    col.Length,
			Precision: col.Precision,
			Scale:     col.Scale,
			Nullable:  col.Nullable,
		}
	}

	// Convert data to interface{} slice
	data := make([][]interface{}, len(result.Rows))
	copy(data, result.Rows)

	return types.StatementResponse{
		StatementHandle: stmt.Handle,
		Code:            types.ResponseCodeSuccess,
		SQLState:        types.SQLState00000,
		CreatedOn:       stmt.CreatedOn.Unix(),
		ResultSetMetaData: &types.ResultSetMetaData{
			NumRows: int64(len(result.Rows)),
			Format:  "jsonv2",
			RowType: rowType,
		},
		Data: data,
	}
}

// sendError sends an error response.
func (h *RestAPIv2Handler) sendError(w http.ResponseWriter, statusCode int, message, sqlState string) {
	resp := types.StatementResponse{
		Code:     apierror.CodeInvalidParameter,
		SQLState: sqlState,
		Message:  message,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(resp)
}

// Resource Management Handlers

// ListDatabases handles GET /api/v2/databases.
func (h *RestAPIv2Handler) ListDatabases(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	databases, err := h.repo.ListDatabases(ctx)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err.Error(), types.SQLState42000)
		return
	}

	resp := make(types.ListDatabasesResponse, len(databases))
	for i, db := range databases {
		resp[i] = types.DatabaseResponse{
			Name:      db.Name,
			Comment:   db.Comment,
			Owner:     db.Owner,
			CreatedOn: db.CreatedAt.Format(time.RFC3339),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// GetDatabase handles GET /api/v2/databases/{database}.
func (h *RestAPIv2Handler) GetDatabase(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbName := chi.URLParam(r, "database")

	db, err := h.repo.GetDatabaseByName(ctx, dbName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Database not found", types.SQLState02000)
		return
	}

	resp := types.DatabaseResponse{
		Name:      db.Name,
		Comment:   db.Comment,
		Owner:     db.Owner,
		CreatedOn: db.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// CreateDatabase handles POST /api/v2/databases.
func (h *RestAPIv2Handler) CreateDatabase(w http.ResponseWriter, r *http.Request) {
	var req types.DatabaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body", types.SQLState42000)
		return
	}

	if req.Name == "" {
		h.sendError(w, http.StatusBadRequest, "Database name is required", types.SQLState42000)
		return
	}

	ctx := r.Context()

	db, err := h.repo.CreateDatabase(ctx, req.Name, req.Comment)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error(), types.SQLState42000)
		return
	}

	resp := types.DatabaseResponse{
		Name:      db.Name,
		Comment:   db.Comment,
		Owner:     db.Owner,
		CreatedOn: db.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// DeleteDatabase handles DELETE /api/v2/databases/{database}.
func (h *RestAPIv2Handler) DeleteDatabase(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbName := chi.URLParam(r, "database")

	if err := h.repo.DropDatabase(ctx, dbName); err != nil {
		h.sendError(w, http.StatusNotFound, err.Error(), types.SQLState02000)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ListSchemas handles GET /api/v2/databases/{database}/schemas.
func (h *RestAPIv2Handler) ListSchemas(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbName := chi.URLParam(r, "database")

	db, err := h.repo.GetDatabaseByName(ctx, dbName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Database not found", types.SQLState02000)
		return
	}

	schemas, err := h.repo.ListSchemas(ctx, db.ID)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err.Error(), types.SQLState42000)
		return
	}

	resp := make(types.ListSchemasResponse, len(schemas))
	for i, s := range schemas {
		resp[i] = types.SchemaResponse{
			Name:         s.Name,
			DatabaseName: dbName,
			Comment:      s.Comment,
			Owner:        s.Owner,
			CreatedOn:    s.CreatedAt.Format(time.RFC3339),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// GetSchema handles GET /api/v2/databases/{database}/schemas/{schema}.
func (h *RestAPIv2Handler) GetSchema(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbName := chi.URLParam(r, "database")
	schemaName := chi.URLParam(r, "schema")

	db, err := h.repo.GetDatabaseByName(ctx, dbName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Database not found", types.SQLState02000)
		return
	}

	schema, err := h.repo.GetSchemaByName(ctx, db.ID, schemaName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Schema not found", types.SQLState02000)
		return
	}

	resp := types.SchemaResponse{
		Name:         schema.Name,
		DatabaseName: dbName,
		Comment:      schema.Comment,
		Owner:        schema.Owner,
		CreatedOn:    schema.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// CreateSchema handles POST /api/v2/databases/{database}/schemas.
func (h *RestAPIv2Handler) CreateSchema(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbName := chi.URLParam(r, "database")

	var req types.SchemaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body", types.SQLState42000)
		return
	}

	if req.Name == "" {
		h.sendError(w, http.StatusBadRequest, "Schema name is required", types.SQLState42000)
		return
	}

	db, err := h.repo.GetDatabaseByName(ctx, dbName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Database not found", types.SQLState02000)
		return
	}

	schema, err := h.repo.CreateSchema(ctx, db.ID, req.Name, req.Comment)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error(), types.SQLState42000)
		return
	}

	resp := types.SchemaResponse{
		Name:         schema.Name,
		DatabaseName: dbName,
		Comment:      schema.Comment,
		Owner:        schema.Owner,
		CreatedOn:    schema.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// DeleteSchema handles DELETE /api/v2/databases/{database}/schemas/{schema}.
func (h *RestAPIv2Handler) DeleteSchema(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbName := chi.URLParam(r, "database")
	schemaName := chi.URLParam(r, "schema")

	db, err := h.repo.GetDatabaseByName(ctx, dbName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Database not found", types.SQLState02000)
		return
	}

	schema, err := h.repo.GetSchemaByName(ctx, db.ID, schemaName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Schema not found", types.SQLState02000)
		return
	}

	if err := h.repo.DropSchema(ctx, schema.ID); err != nil {
		h.sendError(w, http.StatusNotFound, err.Error(), types.SQLState02000)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ListTables handles GET /api/v2/databases/{database}/schemas/{schema}/tables.
func (h *RestAPIv2Handler) ListTables(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbName := chi.URLParam(r, "database")
	schemaName := chi.URLParam(r, "schema")

	db, err := h.repo.GetDatabaseByName(ctx, dbName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Database not found", types.SQLState02000)
		return
	}

	schema, err := h.repo.GetSchemaByName(ctx, db.ID, schemaName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Schema not found", types.SQLState02000)
		return
	}

	tables, err := h.repo.ListTables(ctx, schema.ID)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err.Error(), types.SQLState42000)
		return
	}

	resp := make(types.ListTablesResponse, len(tables))
	for i, t := range tables {
		resp[i] = types.TableResponse{
			Name:      t.Name,
			Database:  dbName,
			Schema:    schemaName,
			TableType: t.TableType,
			Comment:   t.Comment,
			Owner:     t.Owner,
			CreatedOn: t.CreatedAt.Format(time.RFC3339),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// GetTable handles GET /api/v2/databases/{database}/schemas/{schema}/tables/{table}.
func (h *RestAPIv2Handler) GetTable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbName := chi.URLParam(r, "database")
	schemaName := chi.URLParam(r, "schema")
	tableName := chi.URLParam(r, "table")

	db, err := h.repo.GetDatabaseByName(ctx, dbName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Database not found", types.SQLState02000)
		return
	}

	schema, err := h.repo.GetSchemaByName(ctx, db.ID, schemaName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Schema not found", types.SQLState02000)
		return
	}

	table, err := h.repo.GetTableByName(ctx, schema.ID, tableName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Table not found", types.SQLState02000)
		return
	}

	resp := types.TableResponse{
		Name:      table.Name,
		Database:  dbName,
		Schema:    schemaName,
		TableType: table.TableType,
		Comment:   table.Comment,
		Owner:     table.Owner,
		CreatedOn: table.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// DeleteTable handles DELETE /api/v2/databases/{database}/schemas/{schema}/tables/{table}.
func (h *RestAPIv2Handler) DeleteTable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbName := chi.URLParam(r, "database")
	schemaName := chi.URLParam(r, "schema")
	tableName := chi.URLParam(r, "table")

	db, err := h.repo.GetDatabaseByName(ctx, dbName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Database not found", types.SQLState02000)
		return
	}

	schema, err := h.repo.GetSchemaByName(ctx, db.ID, schemaName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Schema not found", types.SQLState02000)
		return
	}

	table, err := h.repo.GetTableByName(ctx, schema.ID, tableName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Table not found", types.SQLState02000)
		return
	}

	if err := h.repo.DropTable(ctx, table.ID); err != nil {
		h.sendError(w, http.StatusNotFound, err.Error(), types.SQLState02000)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Warehouse Management Handlers

// ListWarehouses handles GET /api/v2/warehouses.
func (h *RestAPIv2Handler) ListWarehouses(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	warehouses, err := h.warehouseMgr.ListWarehouses(ctx)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err.Error(), types.SQLState42000)
		return
	}

	resp := make(types.ListWarehousesResponse, len(warehouses))
	for i, wh := range warehouses {
		resp[i] = types.WarehouseResponse{
			Name:        wh.Name,
			State:       string(wh.State),
			Size:        wh.Size,
			Type:        "STANDARD",
			AutoSuspend: wh.AutoSuspend,
			AutoResume:  wh.AutoResume,
			Comment:     wh.Comment,
			Owner:       wh.Owner,
			CreatedOn:   wh.CreatedAt.Format(time.RFC3339),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// GetWarehouse handles GET /api/v2/warehouses/{warehouse}.
func (h *RestAPIv2Handler) GetWarehouse(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	warehouseName := chi.URLParam(r, "warehouse")

	wh, err := h.warehouseMgr.GetWarehouse(ctx, warehouseName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Warehouse not found", types.SQLState02000)
		return
	}

	resp := types.WarehouseResponse{
		Name:        wh.Name,
		State:       string(wh.State),
		Size:        wh.Size,
		Type:        "STANDARD",
		AutoSuspend: wh.AutoSuspend,
		AutoResume:  wh.AutoResume,
		Comment:     wh.Comment,
		Owner:       wh.Owner,
		CreatedOn:   wh.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// CreateWarehouse handles POST /api/v2/warehouses.
func (h *RestAPIv2Handler) CreateWarehouse(w http.ResponseWriter, r *http.Request) {
	var req types.WarehouseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body", types.SQLState42000)
		return
	}

	if req.Name == "" {
		h.sendError(w, http.StatusBadRequest, "Warehouse name is required", types.SQLState42000)
		return
	}

	ctx := r.Context()

	wh, err := h.warehouseMgr.CreateWarehouse(ctx, req.Name, req.Size, req.Comment)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error(), types.SQLState42000)
		return
	}

	resp := types.WarehouseResponse{
		Name:        wh.Name,
		State:       string(wh.State),
		Size:        wh.Size,
		Type:        "STANDARD",
		AutoSuspend: wh.AutoSuspend,
		AutoResume:  wh.AutoResume,
		Comment:     wh.Comment,
		Owner:       wh.Owner,
		CreatedOn:   wh.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// DeleteWarehouse handles DELETE /api/v2/warehouses/{warehouse}.
func (h *RestAPIv2Handler) DeleteWarehouse(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	warehouseName := chi.URLParam(r, "warehouse")

	if err := h.warehouseMgr.DropWarehouse(ctx, warehouseName); err != nil {
		h.sendError(w, http.StatusNotFound, err.Error(), types.SQLState02000)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ResumeWarehouse handles POST /api/v2/warehouses/{warehouse}:resume.
func (h *RestAPIv2Handler) ResumeWarehouse(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	warehouseName := chi.URLParam(r, "warehouse")

	if err := h.warehouseMgr.ResumeWarehouse(ctx, warehouseName); err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error(), types.SQLState42000)
		return
	}

	wh, err := h.warehouseMgr.GetWarehouse(ctx, warehouseName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, err.Error(), types.SQLState02000)
		return
	}

	resp := types.WarehouseResponse{
		Name:        wh.Name,
		State:       string(wh.State),
		Size:        wh.Size,
		Type:        "STANDARD",
		AutoSuspend: wh.AutoSuspend,
		AutoResume:  wh.AutoResume,
		Comment:     wh.Comment,
		Owner:       wh.Owner,
		CreatedOn:   wh.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// SuspendWarehouse handles POST /api/v2/warehouses/{warehouse}:suspend.
func (h *RestAPIv2Handler) SuspendWarehouse(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	warehouseName := chi.URLParam(r, "warehouse")

	if err := h.warehouseMgr.SuspendWarehouse(ctx, warehouseName); err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error(), types.SQLState42000)
		return
	}

	wh, err := h.warehouseMgr.GetWarehouse(ctx, warehouseName)
	if err != nil {
		h.sendError(w, http.StatusNotFound, err.Error(), types.SQLState02000)
		return
	}

	resp := types.WarehouseResponse{
		Name:        wh.Name,
		State:       string(wh.State),
		Size:        wh.Size,
		Type:        "STANDARD",
		AutoSuspend: wh.AutoSuspend,
		AutoResume:  wh.AutoResume,
		Comment:     wh.Comment,
		Owner:       wh.Owner,
		CreatedOn:   wh.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// convertBindings converts types.BindingValue map to query.QueryBindingValue map.
func convertBindings(bindings map[string]*types.BindingValue) map[string]*query.QueryBindingValue {
	if bindings == nil {
		return nil
	}

	result := make(map[string]*query.QueryBindingValue, len(bindings))
	for key, val := range bindings {
		if val != nil {
			result[key] = &query.QueryBindingValue{
				Type:  val.Type,
				Value: val.Value,
			}
		}
	}
	return result
}
