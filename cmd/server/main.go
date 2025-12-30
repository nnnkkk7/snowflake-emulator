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
	defer db.Close()

	connMgr := connection.NewManager(db)

	repo, err := metadata.NewRepository(connMgr)
	if err != nil {
		log.Fatalf("Failed to create repository: %v", err)
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

	r.Post("/queries/v1/query-request", queryHandler.ExecuteQuery)
	r.Post("/queries/v1/abort-request", queryHandler.AbortQuery)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	log.Printf("Starting Snowflake Emulator on port %s", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
