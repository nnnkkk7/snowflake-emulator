// Package main provides the entry point for the Snowflake emulator server.
package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
	"github.com/nnnkkk7/snowflake-emulator/pkg/session"
	"github.com/nnnkkk7/snowflake-emulator/pkg/stage"
	"github.com/nnnkkk7/snowflake-emulator/server/handlers"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = ":memory:"
	}

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Failed to close database: %v", err)
		}
	}()

	connMgr := connection.NewManager(db)

	repo, err := metadata.NewRepository(connMgr)
	if err != nil {
		log.Printf("Failed to create repository: %v", err)
		return
	}

	sessionMgr := session.NewManager(24 * time.Hour)
	stmtMgr := query.NewStatementManager(1 * time.Hour)

	executor := query.NewExecutor(connMgr, repo)

	// Initialize stage manager for COPY INTO support
	stageDir := os.Getenv("STAGE_DIR")
	if stageDir == "" {
		stageDir = "./stages"
	}
	stageMgr := stage.NewManager(repo, stageDir)

	// Initialize COPY handler and wire to executor
	copyHandler := query.NewCopyHandler(stageMgr, repo, executor)
	executor.SetCopyHandler(copyHandler)

	// Initialize MERGE handler and wire to executor
	mergeHandler := query.NewMergeHandler(executor)
	executor.SetMergeHandler(mergeHandler)

	sessionHandler := handlers.NewSessionHandler(sessionMgr, repo)
	queryHandler := handlers.NewQueryHandler(executor, sessionMgr)
	restAPIHandler := handlers.NewRestAPIv2Handler(executor, stmtMgr, repo)

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)

	r.Post("/session/v1/login-request", sessionHandler.Login)
	r.Post("/session/token-request", sessionHandler.TokenRequest)
	r.Post("/session/heartbeat", sessionHandler.Heartbeat)
	r.Post("/session/renew", sessionHandler.RenewSession)
	r.Post("/session/logout", sessionHandler.Logout)
	r.Post("/session/use", sessionHandler.UseContext)
	r.Post("/session", sessionHandler.CloseSession) // gosnowflake sends POST /session?delete=true

	r.Post("/queries/v1/query-request", queryHandler.ExecuteQuery)
	r.Post("/queries/v1/abort-request", queryHandler.AbortQuery)

	// REST API v2 endpoints
	r.Route("/api/v2", func(r chi.Router) {
		// Statement endpoints
		r.Post("/statements", restAPIHandler.SubmitStatement)
		r.Get("/statements/{handle}", restAPIHandler.GetStatement)
		r.Post("/statements/{handle}/cancel", restAPIHandler.CancelStatement)

		// Database endpoints
		r.Get("/databases", restAPIHandler.ListDatabases)
		r.Post("/databases", restAPIHandler.CreateDatabase)
		r.Get("/databases/{database}", restAPIHandler.GetDatabase)
		r.Put("/databases/{database}", restAPIHandler.AlterDatabase)
		r.Delete("/databases/{database}", restAPIHandler.DeleteDatabase)

		// Schema endpoints
		r.Get("/databases/{database}/schemas", restAPIHandler.ListSchemas)
		r.Post("/databases/{database}/schemas", restAPIHandler.CreateSchema)
		r.Get("/databases/{database}/schemas/{schema}", restAPIHandler.GetSchema)
		r.Delete("/databases/{database}/schemas/{schema}", restAPIHandler.DeleteSchema)

		// Table endpoints
		r.Get("/databases/{database}/schemas/{schema}/tables", restAPIHandler.ListTables)
		r.Post("/databases/{database}/schemas/{schema}/tables", restAPIHandler.CreateTable)
		r.Get("/databases/{database}/schemas/{schema}/tables/{table}", restAPIHandler.GetTable)
		r.Put("/databases/{database}/schemas/{schema}/tables/{table}", restAPIHandler.AlterTable)
		r.Delete("/databases/{database}/schemas/{schema}/tables/{table}", restAPIHandler.DeleteTable)

		// Warehouse endpoints
		r.Get("/warehouses", restAPIHandler.ListWarehouses)
		r.Post("/warehouses", restAPIHandler.CreateWarehouse)
		r.Get("/warehouses/{warehouse}", restAPIHandler.GetWarehouse)
		r.Delete("/warehouses/{warehouse}", restAPIHandler.DeleteWarehouse)
		r.Post("/warehouses/{warehouse}:resume", restAPIHandler.ResumeWarehouse)
		r.Post("/warehouses/{warehouse}:suspend", restAPIHandler.SuspendWarehouse)
	})

	// Telemetry endpoint - accept and ignore (gosnowflake sends telemetry data)
	r.Post("/telemetry/send", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	})

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			log.Printf("Failed to write health response: %v", err)
		}
	})

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("Starting Snowflake Emulator on port %s", port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err) //nolint:gocritic // exitAfterDefer: intentional - OS cleans up on exit
	}
}
