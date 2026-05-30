package query

import (
	"strings"

	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

func parseCreateTableColumns(sql string) ([]metadata.ColumnDef, bool) {
	open := strings.Index(sql, "(")
	close := strings.LastIndex(sql, ")")
	if open < 0 || close <= open {
		return nil, false
	}

	body := sql[open+1 : close]
	parts := splitColumnDefinitions(body)
	if len(parts) == 0 {
		return nil, false
	}

	columns := make([]metadata.ColumnDef, 0, len(parts))
	for _, part := range parts {
		col, ok := parseColumnDefinition(part)
		if !ok {
			continue
		}
		columns = append(columns, col)
	}
	if len(columns) == 0 {
		return nil, false
	}
	return columns, true
}

func splitColumnDefinitions(body string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inSingleQuote := false

	for i := 0; i < len(body); i++ {
		ch := body[i]
		switch ch {
		case '\'':
			inSingleQuote = !inSingleQuote
			current.WriteByte(ch)
		case '(':
			if !inSingleQuote {
				depth++
			}
			current.WriteByte(ch)
		case ')':
			if !inSingleQuote {
				depth--
			}
			current.WriteByte(ch)
		case ',':
			if !inSingleQuote && depth == 0 {
				part := strings.TrimSpace(current.String())
				if part != "" {
					parts = append(parts, part)
				}
				current.Reset()
				continue
			}
			current.WriteByte(ch)
		default:
			current.WriteByte(ch)
		}
	}

	if tail := strings.TrimSpace(current.String()); tail != "" {
		parts = append(parts, tail)
	}
	return parts
}

func parseColumnDefinition(part string) (metadata.ColumnDef, bool) {
	upper := strings.ToUpper(strings.TrimSpace(part))
	if upper == "" {
		return metadata.ColumnDef{}, false
	}
	if strings.HasPrefix(upper, "PRIMARY KEY") ||
		strings.HasPrefix(upper, "FOREIGN KEY") ||
		strings.HasPrefix(upper, "UNIQUE") ||
		strings.HasPrefix(upper, "CONSTRAINT") ||
		strings.HasPrefix(upper, "CHECK") {
		return metadata.ColumnDef{}, false
	}

	tokens := strings.Fields(part)
	if len(tokens) < 2 {
		return metadata.ColumnDef{}, false
	}

	name := strings.Trim(tokens[0], `"`)
	colType := tokens[1]
	for i := 2; i < len(tokens); i++ {
		if strings.EqualFold(tokens[i], "NOT") && i+1 < len(tokens) && strings.EqualFold(tokens[i+1], "NULL") {
			break
		}
		if strings.EqualFold(tokens[i], "NULL") || strings.EqualFold(tokens[i], "NOT") {
			break
		}
		if strings.EqualFold(tokens[i], "PRIMARY") || strings.EqualFold(tokens[i], "DEFAULT") {
			break
		}
		colType += " " + tokens[i]
	}

	nullable := !strings.Contains(upper, "NOT NULL")
	primaryKey := strings.Contains(upper, "PRIMARY KEY")

	return metadata.ColumnDef{
		Name:       strings.ToUpper(name),
		Type:       strings.ToUpper(colType),
		Nullable:   nullable,
		PrimaryKey: primaryKey,
	}, true
}
