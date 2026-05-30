package query

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

var createTableHeaderPattern = regexp.MustCompile(`(?is)^(\s*create\s+(?:or\s+replace\s+)?table\s+(?:if\s+not\s+exists\s+)?)(.+?)(\s*\()`)

type createTableSpec struct {
	ifNotExists bool
	database    string
	schema      string
	table       string
}

func parseCreateTableHeader(sql string) (createTableSpec, string, string, bool) {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(upperSQL, "CREATE TABLE") && !strings.HasPrefix(upperSQL, "CREATE OR REPLACE TABLE") {
		return createTableSpec{}, "", "", false
	}

	ifNotExists := strings.Contains(upperSQL, "IF NOT EXISTS")
	matches := createTableHeaderPattern.FindStringSubmatch(sql)
	if len(matches) < 4 {
		return createTableSpec{}, "", "", false
	}

	prefix := matches[1]
	tableRef := normalizeTableReference(matches[2])
	suffixStart := matches[3]

	database, schema, table := ParseTableRef(tableRef)
	return createTableSpec{
		ifNotExists: ifNotExists,
		database:    database,
		schema:      schema,
		table:       table,
	}, prefix, suffixStart, true
}

func normalizeTableReference(ref string) string {
	ref = strings.TrimSpace(ref)
	if strings.HasPrefix(ref, `"`) {
		parts := strings.Split(ref, ".")
		for i, part := range parts {
			part = strings.TrimSpace(part)
			part = strings.Trim(part, `"`)
			parts[i] = part
		}
		return strings.Join(parts, ".")
	}
	return ref
}

func rewriteCreateTableSQL(sql, defaultDatabase, defaultSchema string) (string, createTableSpec, error) {
	spec, prefix, suffixStart, ok := parseCreateTableHeader(sql)
	if !ok {
		return sql, createTableSpec{}, fmt.Errorf("unsupported CREATE TABLE syntax")
	}

	database := spec.database
	if database == "" {
		database = strings.ToUpper(strings.TrimSpace(defaultDatabase))
	}
	schema := spec.schema
	if schema == "" {
		schema = strings.ToUpper(strings.TrimSpace(defaultSchema))
	}
	if spec.table == "" {
		return "", createTableSpec{}, fmt.Errorf("table name is required for CREATE TABLE")
	}

	duckDBName := BuildTableName(database, schema, spec.table)
	resolved := createTableSpec{
		ifNotExists: spec.ifNotExists,
		database:    database,
		schema:      schema,
		table:       spec.table,
	}

	headerEnd := strings.Index(sql, suffixStart)
	if headerEnd < 0 {
		return "", createTableSpec{}, fmt.Errorf("unsupported CREATE TABLE syntax")
	}
	rest := sql[headerEnd:]
	rewritten := prefix + duckDBName + rest
	return rewritten, resolved, nil
}

func (e *Executor) executeCreateTable(ctx context.Context, sql, defaultDatabase, defaultSchema string) (*ExecResult, error) {
	rewritten, spec, err := rewriteCreateTableSQL(sql, defaultDatabase, defaultSchema)
	if err != nil {
		return nil, err
	}

	if spec.ifNotExists && spec.database != "" && spec.schema != "" {
		db, err := e.repo.GetDatabaseByName(ctx, spec.database)
		if err == nil {
			if schema, err := e.repo.GetSchemaByName(ctx, db.ID, spec.schema); err == nil {
				if _, err := e.repo.GetTableByName(ctx, schema.ID, spec.table); err == nil {
					return &ExecResult{RowsAffected: 0}, nil
				}
			}
		}
	}

	if spec.database != "" && spec.schema != "" {
		if columns, ok := parseCreateTableColumns(sql); ok && len(columns) > 0 {
			db, err := e.repo.GetDatabaseByName(ctx, spec.database)
			if err == nil {
				if schemaObj, err := e.repo.GetSchemaByName(ctx, db.ID, spec.schema); err == nil {
					if _, err := e.repo.CreateTable(ctx, schemaObj.ID, spec.table, columns, ""); err != nil {
						if spec.ifNotExists && strings.Contains(err.Error(), "already exists") {
							return &ExecResult{RowsAffected: 0}, nil
						}
						return nil, err
					}
					return &ExecResult{RowsAffected: 0}, nil
				}
			}
		}
	}

	translatedSQL, err := e.translator.Translate(rewritten)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	if _, err := e.mgr.Exec(ctx, translatedSQL); err != nil {
		if spec.ifNotExists && strings.Contains(err.Error(), "already exists") {
			return &ExecResult{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("create table execution error: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}
