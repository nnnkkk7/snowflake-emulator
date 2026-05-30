package query

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

var createSchemaPattern = regexp.MustCompile(`(?is)^\s*create\s+(?:or\s+replace\s+)?schema\s+(?:if\s+not\s+exists\s+)?(?:(?:"([^"]+)"|([A-Za-z0-9_$]+))\.)?(?:"([^"]+)"|([A-Za-z0-9_$]+))(?:\s+comment\s*=\s*'((?:''|[^'])*)')?\s*;?\s*$`)

type createSchemaSpec struct {
	ifNotExists bool
	database    string
	schema      string
	comment     string
}

func parseCreateSchema(sql string) (createSchemaSpec, bool) {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(upperSQL, "CREATE SCHEMA") && !strings.HasPrefix(upperSQL, "CREATE OR REPLACE SCHEMA") {
		return createSchemaSpec{}, false
	}

	ifNotExists := strings.Contains(upperSQL, "IF NOT EXISTS")
	matches := createSchemaPattern.FindStringSubmatch(sql)
	if len(matches) == 0 {
		return createSchemaSpec{}, false
	}

	comment := strings.ReplaceAll(matches[5], "''", "'")
	return createSchemaSpec{
		ifNotExists: ifNotExists,
		database:    coalesceIdentifier(matches[1], matches[2]),
		schema:      coalesceIdentifier(matches[3], matches[4]),
		comment:     comment,
	}, true
}

func coalesceIdentifier(quoted, unquoted string) string {
	if quoted != "" {
		return strings.ToUpper(quoted)
	}
	return strings.ToUpper(unquoted)
}

func (e *Executor) executeCreateSchema(ctx context.Context, sql, defaultDatabase string) (*ExecResult, error) {
	spec, ok := parseCreateSchema(sql)
	if !ok {
		return nil, fmt.Errorf("unsupported CREATE SCHEMA syntax")
	}

	databaseName := spec.database
	if databaseName == "" {
		databaseName = strings.ToUpper(strings.TrimSpace(defaultDatabase))
	}
	if databaseName == "" {
		return nil, fmt.Errorf("database context is required for CREATE SCHEMA")
	}
	if spec.schema == "" {
		return nil, fmt.Errorf("schema name is required for CREATE SCHEMA")
	}

	db, err := e.repo.GetDatabaseByName(ctx, databaseName)
	if err != nil {
		db, err = e.repo.CreateDatabase(ctx, databaseName, "Auto-created database")
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return nil, fmt.Errorf("failed to resolve database %s: %w", databaseName, err)
			}
			db, err = e.repo.GetDatabaseByName(ctx, databaseName)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve database %s: %w", databaseName, err)
			}
		}
	}

	if spec.ifNotExists {
		if _, err := e.repo.GetSchemaByName(ctx, db.ID, spec.schema); err == nil {
			return &ExecResult{RowsAffected: 0}, nil
		}
	}

	if _, err := e.repo.CreateSchema(ctx, db.ID, spec.schema, spec.comment); err != nil {
		if spec.ifNotExists && strings.Contains(err.Error(), "already exists") {
			return &ExecResult{RowsAffected: 0}, nil
		}
		return nil, err
	}

	return &ExecResult{RowsAffected: 1}, nil
}
