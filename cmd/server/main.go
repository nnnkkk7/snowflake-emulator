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

	executor := query.NewExecutor(connMgr, repo)

	sessionHandler := handlers.NewSessionHandler(sessionMgr, repo)
	queryHandler := handlers.NewQueryHandler(executor, sessionMgr)

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
