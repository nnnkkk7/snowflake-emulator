package query

import (
	"regexp"
	"strings"
)

var (
	jdbcCommentPattern = regexp.MustCompile(`/\*[^*]*\*/`)
	jdbcLikePattern    = regexp.MustCompile(`(?is)\s+like\s+'([^']*)'`)

	showInSchemaPattern = regexp.MustCompile(`(?is)^\s*show\s+(?:/\*.*?\*/\s*)?(objects|tables|views|columns|schemas|databases|procedures|functions|user functions|primary keys|imported keys|exported keys)(?:\s+like\s+'[^']*')?\s+in\s+schema\s+(?:"([^"]+)"|([A-Za-z0-9_$]+))\.(?:"([^"]+)"|([A-Za-z0-9_$]+))\s*;?\s*$`)
	showInTablePattern = regexp.MustCompile(`(?is)^\s*show\s+(?:/\*.*?\*/\s*)?(columns|primary keys|imported keys|exported keys)(?:\s+like\s+'[^']*')?\s+in\s+table\s+(?:"([^"]+)"|([A-Za-z0-9_$]+))\.(?:"([^"]+)"|([A-Za-z0-9_$]+))\.(?:"([^"]+)"|([A-Za-z0-9_$]+))\s*;?\s*$`)
	showInDatabasePattern = regexp.MustCompile(`(?is)^\s*show\s+(?:/\*.*?\*/\s*)?(objects|tables|views|columns|schemas|procedures|functions|user functions)(?:\s+like\s+'[^']*')?\s+in\s+database\s+(?:"([^"]+)"|([A-Za-z0-9_$]+))\s*;?\s*$`)
	showInAccountPattern = regexp.MustCompile(`(?is)^\s*show\s+(?:/\*.*?\*/\s*)?(objects|tables|views|columns|schemas|databases|procedures|functions|user functions)(?:\s+like\s+'[^']*')?\s+in\s+account\s*;?\s*$`)
)

// NormalizeJDBCMetadataSQL adapts Snowflake JDBC metadata queries for DuckDB execution.
func NormalizeJDBCMetadataSQL(sql string) string {
	sql = cleanJDBCSQL(sql)
	if sql == "" {
		return sql
	}

	if matches := showInTablePattern.FindStringSubmatch(sql); len(matches) > 0 {
		return normalizeShowInTable(matches[1], coalesceMatch(matches[2], matches[3]), coalesceMatch(matches[4], matches[5]), coalesceMatch(matches[6], matches[7]))
	}

	if matches := showInSchemaPattern.FindStringSubmatch(sql); len(matches) > 0 {
		return normalizeShowInSchema(matches[1], coalesceMatch(matches[2], matches[3]), coalesceMatch(matches[4], matches[5]))
	}

	if matches := showInDatabasePattern.FindStringSubmatch(sql); len(matches) > 0 {
		return normalizeShowInDatabase(matches[1], coalesceMatch(matches[2], matches[3]))
	}

	if matches := showInAccountPattern.FindStringSubmatch(sql); len(matches) > 0 {
		return normalizeShowInAccount(matches[1])
	}

	return sql
}

func cleanJDBCSQL(sql string) string {
	sql = strings.TrimSpace(jdbcCommentPattern.ReplaceAllString(sql, " "))
	sql = jdbcLikePattern.ReplaceAllString(sql, "")
	return strings.Join(strings.Fields(sql), " ")
}

func normalizeShowInAccount(kind string) string {
	switch strings.ToLower(kind) {
	case "databases":
		return buildMetadataDatabasesQuery()
	case "schemas":
		return buildMetadataSchemasQuery("")
	case "columns":
		return buildMetadataColumnsQuery("", "", "")
	case "procedures", "functions", "user functions":
		return buildEmptyMetadataQuery("name")
	default:
		return buildMetadataObjectsQuery("", "", objectKindFilter(kind))
	}
}

func normalizeShowInDatabase(kind, database string) string {
	switch strings.ToLower(kind) {
	case "schemas":
		return buildMetadataSchemasQuery(database)
	case "columns":
		return buildMetadataColumnsQuery(database, "", "")
	case "procedures", "functions", "user functions":
		return buildEmptyMetadataQuery("name")
	default:
		return buildMetadataObjectsQuery(database, "", objectKindFilter(kind))
	}
}

func normalizeShowInSchema(kind, database, schema string) string {
	switch strings.ToLower(kind) {
	case "columns":
		return buildMetadataColumnsQuery(database, schema, "")
	case "procedures", "functions", "user functions":
		return buildEmptyMetadataQuery("name")
	default:
		return buildMetadataObjectsQuery(database, schema, objectKindFilter(kind))
	}
}

func normalizeShowInTable(kind, database, schema, table string) string {
	switch strings.ToLower(kind) {
	case "columns":
		return buildMetadataColumnsQuery(database, schema, table)
	default:
		return buildEmptyMetadataQuery("column_name")
	}
}

func objectKindFilter(kind string) string {
	switch strings.ToLower(kind) {
	case "tables":
		return "BASE TABLE"
	case "views":
		return "VIEW"
	default:
		return ""
	}
}

func coalesceMatch(quoted, unquoted string) string {
	if quoted != "" {
		return strings.ToUpper(quoted)
	}
	return strings.ToUpper(unquoted)
}

func buildMetadataObjectsQuery(database, schema, tableType string) string {
	query := `
SELECT
  t.created_at AS "created_on",
  t.name AS "name",
  d.name AS "database_name",
  s.name AS "schema_name",
  t.table_type AS "kind",
  COALESCE(t.comment, '') AS "comment"
FROM _metadata_tables t
JOIN _metadata_schemas s ON t.schema_id = s.id
JOIN _metadata_databases d ON s.database_id = d.id`

	var filters []string
	if database != "" {
		filters = append(filters, "UPPER(d.name) = '"+escapeSQLLiteral(database)+"'")
	}
	if schema != "" {
		filters = append(filters, "UPPER(s.name) = '"+escapeSQLLiteral(schema)+"'")
	}
	if tableType != "" {
		filters = append(filters, "UPPER(t.table_type) = '"+escapeSQLLiteral(tableType)+"'")
	}
	if len(filters) > 0 {
		query += "\nWHERE " + strings.Join(filters, " AND ")
	}
	return query + "\nORDER BY d.name, s.name, t.name"
}

func buildMetadataTablesQuery(database, schema string) string {
	return buildMetadataObjectsQuery(database, schema, "")
}

func buildMetadataSchemasQuery(database string) string {
	baseQuery := `
SELECT
  s.created_at AS "created_on",
  s.name AS "name",
  'N' AS "is_default",
  'N' AS "is_current",
  d.name AS "database_name",
  '' AS "owner",
  COALESCE(s.comment, '') AS "comment"
FROM _metadata_schemas s
JOIN _metadata_databases d ON s.database_id = d.id`

	if database == "" {
		return baseQuery + "\nORDER BY d.name, s.name"
	}

	return baseQuery + `
WHERE UPPER(d.name) = '` + escapeSQLLiteral(database) + `'
ORDER BY s.name`
}

func buildMetadataDatabasesQuery() string {
	return `
SELECT
  created_at AS "created_on",
  name AS "name",
  COALESCE(comment, '') AS "comment"
FROM _metadata_databases
ORDER BY name`
}

func buildMetadataColumnsQuery(database, schema, table string) string {
	query := `
SELECT
  t.name AS "table_name",
  s.name AS "schema_name",
  TRIM(part.parts[1]) AS "column_name",
  TRIM(part.parts[2]) AS "data_type",
  TRIM(part.parts[3]) AS "null?",
  TRIM(part.parts[5]) AS "default",
  CASE WHEN TRIM(part.parts[4]) = 'true' THEN 'Y' ELSE 'N' END AS "primary key",
  '' AS "unique key",
  '' AS "comment",
  d.name AS "database_name",
  '' AS "policy name"
FROM _metadata_tables t
JOIN _metadata_schemas s ON t.schema_id = s.id
JOIN _metadata_databases d ON s.database_id = d.id
CROSS JOIN LATERAL (
  SELECT string_split_regex(def_part, ':') AS parts
  FROM UNNEST(string_split(COALESCE(t.column_definitions, ''), ';')) AS t(def_part)
  WHERE def_part <> ''
) part`

	var filters []string
	if database != "" {
		filters = append(filters, "UPPER(d.name) = '"+escapeSQLLiteral(database)+"'")
	}
	if schema != "" {
		filters = append(filters, "UPPER(s.name) = '"+escapeSQLLiteral(schema)+"'")
	}
	if table != "" {
		filters = append(filters, "UPPER(t.name) = '"+escapeSQLLiteral(table)+"'")
	}
	if len(filters) > 0 {
		query += "\nWHERE " + strings.Join(filters, " AND ")
	}
	return query + "\nORDER BY d.name, s.name, t.name, TRIM(part.parts[1])"
}

func buildEmptyMetadataQuery(columnName string) string {
	return `SELECT CAST(NULL AS VARCHAR) AS "` + columnName + `" WHERE FALSE`
}

func escapeSQLLiteral(value string) string {
	return strings.ReplaceAll(strings.ToUpper(value), "'", "''")
}
