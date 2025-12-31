package query

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestDefaultTableNamer_BuildDuckDBTableName(t *testing.T) {
	tests := []struct {
		name     string
		database string
		schema   string
		table    string
		want     string
	}{
		{
			name:     "fully qualified",
			database: "TEST_DB",
			schema:   "PUBLIC",
			table:    "USERS",
			want:     "TEST_DB.PUBLIC_USERS",
		},
		{
			name:     "schema qualified",
			database: "",
			schema:   "PUBLIC",
			table:    "USERS",
			want:     "PUBLIC_USERS",
		},
		{
			name:     "table only",
			database: "",
			schema:   "",
			table:    "USERS",
			want:     "USERS",
		},
		{
			name:     "lowercase input normalized to uppercase",
			database: "test_db",
			schema:   "public",
			table:    "users",
			want:     "TEST_DB.PUBLIC_USERS",
		},
		{
			name:     "mixed case input normalized",
			database: "Test_DB",
			schema:   "Public",
			table:    "Users",
			want:     "TEST_DB.PUBLIC_USERS",
		},
		{
			name:     "whitespace trimmed",
			database: " TEST_DB ",
			schema:   " PUBLIC ",
			table:    " USERS ",
			want:     "TEST_DB.PUBLIC_USERS",
		},
		{
			name:     "empty database with schema",
			database: "",
			schema:   "ANALYTICS",
			table:    "EVENTS",
			want:     "ANALYTICS_EVENTS",
		},
	}

	namer := NewTableNamer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := namer.BuildDuckDBTableName(tt.database, tt.schema, tt.table)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("BuildDuckDBTableName() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDefaultTableNamer_ParseTableReference(t *testing.T) {
	tests := []struct {
		name         string
		ref          string
		wantDatabase string
		wantSchema   string
		wantTable    string
	}{
		{
			name:         "table only",
			ref:          "users",
			wantDatabase: "",
			wantSchema:   "",
			wantTable:    "USERS",
		},
		{
			name:         "schema.table",
			ref:          "public.users",
			wantDatabase: "",
			wantSchema:   "PUBLIC",
			wantTable:    "USERS",
		},
		{
			name:         "database.schema.table",
			ref:          "test_db.public.users",
			wantDatabase: "TEST_DB",
			wantSchema:   "PUBLIC",
			wantTable:    "USERS",
		},
		{
			name:         "already uppercase",
			ref:          "TEST_DB.PUBLIC.USERS",
			wantDatabase: "TEST_DB",
			wantSchema:   "PUBLIC",
			wantTable:    "USERS",
		},
		{
			name:         "whitespace trimmed",
			ref:          " public.users ",
			wantDatabase: "",
			wantSchema:   "PUBLIC",
			wantTable:    "USERS",
		},
		{
			name:         "too many parts falls back to table",
			ref:          "a.b.c.d",
			wantDatabase: "",
			wantSchema:   "",
			wantTable:    "A.B.C.D",
		},
	}

	namer := NewTableNamer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDB, gotSchema, gotTable := namer.ParseTableReference(tt.ref)
			if diff := cmp.Diff(tt.wantDatabase, gotDB); diff != "" {
				t.Errorf("database mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantSchema, gotSchema); diff != "" {
				t.Errorf("schema mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantTable, gotTable); diff != "" {
				t.Errorf("table mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildTableName_Convenience(t *testing.T) {
	// Test the package-level convenience function
	got := BuildTableName("TEST_DB", "PUBLIC", "USERS")
	want := "TEST_DB.PUBLIC_USERS"
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("BuildTableName() mismatch (-want +got):\n%s", diff)
	}
}

func TestParseTableRef_Convenience(t *testing.T) {
	// Test the package-level convenience function
	db, schema, table := ParseTableRef("test_db.public.users")
	if db != "TEST_DB" || schema != "PUBLIC" || table != "USERS" {
		t.Errorf("ParseTableRef() = (%q, %q, %q), want (TEST_DB, PUBLIC, USERS)", db, schema, table)
	}
}
