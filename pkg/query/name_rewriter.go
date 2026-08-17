package query

import (
	"regexp"
	"strings"
)

func rewriteTableRefsToPhysicalDuckDB(sqlText string, defaultSchema string) string {
	defaultSchema = normalizeSnowflakeIdent(defaultSchema)

	keywords := []string{
		`CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?`,
		`DROP\s+TABLE(?:\s+IF\s+EXISTS)?`,
		`DELETE\s+FROM`,
		`INSERT\s+INTO`,
		`UPDATE`,
		`MERGE\s+INTO`,
		`FROM`,
		`JOIN`,
		`USING`,
		`COPY\s+INTO`,
	}

	for _, keyword := range keywords {
		sqlText = rewriteKeywordTableRef(sqlText, keyword, defaultSchema)
	}

	return sqlText
}

func rewriteKeywordTableRef(sqlText string, keyword string, defaultSchema string) string {
	re := regexp.MustCompile(`(?is)\b(` + keyword + `)\s+((?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)(?:\s*\.\s*(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)){0,2})`)

	return re.ReplaceAllStringFunc(sqlText, func(match string) string {
		parts := re.FindStringSubmatch(match)
		if len(parts) < 3 {
			return match
		}

		kw := parts[1]
		ref := strings.TrimSpace(parts[2])

		if shouldSkipTableRewrite(ref) {
			return match
		}

		_, schema, table, alreadyPhysical := parseSnowflakeObjectRef(ref)

		if table == "" {
			return match
		}

		// Nome inteiro entre aspas: não mexe.
		// Ex: "INFORMATION_SCHEMA.COLUMNS", "PUBLIC_E2E_TEST"
		if alreadyPhysical {
			return match
		}

		if schema == "" {
			schema = defaultSchema
		}

		if schema == "" {
			return kw + " " + quoteDuckDBIdent(table)
		}

		return kw + " " + quoteDuckDBIdent(schema) + "." + quoteDuckDBIdent(table)
	})
}

func shouldSkipTableRewrite(ref string) bool {
	ref = strings.TrimSpace(ref)

	return ref == "" ||
		strings.HasPrefix(ref, "(") ||
		strings.HasPrefix(ref, "@") ||
		strings.Contains(strings.ToUpper(ref), "INFORMATION_SCHEMA.")
}

func parseSnowflakeObjectRef(ref string) (database string, schema string, table string, alreadyPhysical bool) {
	ref = strings.TrimSpace(ref)

	// Se o identificador inteiro está entre aspas, não divida por ponto.
	// Exemplos:
	// "INFORMATION_SCHEMA.COLUMNS"
	// "PUBLIC_E2E_TEST"
	if isSingleQuotedIdentifier(ref) {
		ident := normalizeSnowflakeIdent(ref)

		// Se tem ponto dentro das aspas, considera nome literal, não schema.table.
		if strings.Contains(ident, ".") {
			return "", "", ident, true
		}

		// Se já parece nome físico do DuckDB, também não reescreve.
		if strings.Contains(ident, "_") {
			return "", "", ident, true
		}

		return "", "", ident, false
	}

	parts := splitObjectRefParts(ref)

	switch len(parts) {
	case 1:
		return "", "", normalizeSnowflakeIdent(parts[0]), false
	case 2:
		return "", normalizeSnowflakeIdent(parts[0]), normalizeSnowflakeIdent(parts[1]), false
	case 3:
		return normalizeSnowflakeIdent(parts[0]), normalizeSnowflakeIdent(parts[1]), normalizeSnowflakeIdent(parts[2]), false
	default:
		return "", "", normalizeSnowflakeIdent(ref), false
	}
}

func isSingleQuotedIdentifier(ref string) bool {
	ref = strings.TrimSpace(ref)

	if len(ref) < 2 {
		return false
	}

	return strings.HasPrefix(ref, `"`) && strings.HasSuffix(ref, `"`) && countUnescapedDotsOutsideQuotes(ref) == 0
}

func countUnescapedDotsOutsideQuotes(ref string) int {
	inDoubleQuote := false
	count := 0

	for _, r := range ref {
		switch r {
		case '"':
			inDoubleQuote = !inDoubleQuote
		case '.':
			if !inDoubleQuote {
				count++
			}
		}
	}

	return count
}

func splitObjectRefParts(ref string) []string {
	var parts []string
	var current strings.Builder
	inDoubleQuote := false

	for _, r := range ref {
		switch r {
		case '"':
			inDoubleQuote = !inDoubleQuote
			current.WriteRune(r)
		case '.':
			if inDoubleQuote {
				current.WriteRune(r)
			} else {
				parts = append(parts, strings.TrimSpace(current.String()))
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}

	return parts
}

func normalizeSnowflakeIdent(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, `"`)
	value = strings.ReplaceAll(value, `""`, `"`)
	return strings.ToUpper(value)
}

func quoteDuckDBIdent(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, `"`)
	value = strings.ReplaceAll(value, `"`, `""`)
	return `"` + value + `"`
}
