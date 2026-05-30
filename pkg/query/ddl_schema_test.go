package query

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

func TestParseCreateSchema(t *testing.T) {
	spec, ok := parseCreateSchema(`CREATE SCHEMA TEST_DB.ANALYTICS`)
	if !ok {
		t.Fatal("expected parse success")
	}
	if spec.database != "TEST_DB" || spec.schema != "ANALYTICS" {
		t.Fatalf("unexpected spec: %+v", spec)
	}

	spec, ok = parseCreateSchema(`CREATE SCHEMA IF NOT EXISTS analytics`)
	if !ok || !spec.ifNotExists || spec.schema != "ANALYTICS" || spec.database != "" {
		t.Fatalf("unexpected spec: %+v ok=%v", spec, ok)
	}
}

func TestExecuteCreateSchema(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("repo: %v", err)
	}

	database, err := repo.CreateDatabase(ctx, "TEST_DB", "")
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	_, err = repo.CreateSchema(ctx, database.ID, "PUBLIC", "")
	if err != nil {
		t.Fatalf("CreateSchema PUBLIC: %v", err)
	}

	executor := NewExecutor(mgr, repo)

	if _, err := executor.ExecuteInSession(ctx, `CREATE SCHEMA TEST_DB.ANALYTICS`, "TEST_DB", "PUBLIC"); err != nil {
		t.Fatalf("CREATE SCHEMA TEST_DB.ANALYTICS failed: %v", err)
	}

	schema, err := repo.GetSchemaByName(ctx, database.ID, "ANALYTICS")
	if err != nil {
		t.Fatalf("schema not found in metadata: %v", err)
	}
	if schema.Name != "ANALYTICS" {
		t.Fatalf("unexpected schema name: %s", schema.Name)
	}
}
