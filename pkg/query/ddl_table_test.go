package query

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

func TestRewriteCreateTableSQL(t *testing.T) {
	rewritten, spec, err := rewriteCreateTableSQL(
		`CREATE TABLE TEST_DB.ANALYTICS.EVENTS (id INT, name VARCHAR)`,
		"TEST_DB",
		"PUBLIC",
	)
	if err != nil {
		t.Fatalf("rewrite failed: %v", err)
	}
	if rewritten != `CREATE TABLE TEST_DB.ANALYTICS_EVENTS (id INT, name VARCHAR)` {
		t.Fatalf("unexpected rewrite: %s", rewritten)
	}
	if spec.database != "TEST_DB" || spec.schema != "ANALYTICS" || spec.table != "EVENTS" {
		t.Fatalf("unexpected spec: %+v", spec)
	}

	rewritten, _, err = rewriteCreateTableSQL(
		`CREATE TABLE EVENTS (id INT)`,
		"TEST_DB",
		"PUBLIC",
	)
	if err != nil {
		t.Fatalf("rewrite unqualified failed: %v", err)
	}
	if rewritten != `CREATE TABLE TEST_DB.PUBLIC_EVENTS (id INT)` {
		t.Fatalf("unexpected unqualified rewrite: %s", rewritten)
	}
}

func TestExecuteCreateTableQualifiedName(t *testing.T) {
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
	schema, err := repo.CreateSchema(ctx, database.ID, "ANALYTICS", "")
	if err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}

	executor := NewExecutor(mgr, repo)
	_, err = executor.ExecuteInSession(ctx,
		`CREATE TABLE TEST_DB.ANALYTICS.EVENTS (id INTEGER, name VARCHAR)`,
		"TEST_DB",
		"PUBLIC",
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	table, err := repo.GetTableByName(ctx, schema.ID, "EVENTS")
	if err != nil {
		t.Fatalf("table not found in metadata: %v", err)
	}
	if table.Name != "EVENTS" {
		t.Fatalf("unexpected table name: %s", table.Name)
	}
}

func TestParseCreateTableColumns(t *testing.T) {
	columns, ok := parseCreateTableColumns(`CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		name VARCHAR NOT NULL,
		PRIMARY KEY (id)
	)`)
	if !ok {
		t.Fatal("expected parse success")
	}
	if len(columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(columns))
	}
	if columns[0].Name != "ID" || !columns[0].PrimaryKey {
		t.Fatalf("unexpected first column: %+v", columns[0])
	}
	if columns[1].Name != "NAME" || columns[1].Nullable {
		t.Fatalf("unexpected second column: %+v", columns[1])
	}
}
