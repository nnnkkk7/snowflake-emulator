package query

import "testing"

func TestClassifier_LeadingCommentsAndQueryForms(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantTyp StatementType
		isQuery bool
	}{
		{"BlockCommentBeforeSelect", "/* app=reporting */ SELECT 1", StatementTypeQuery, true},
		{"LineCommentBeforeSelect", "-- trace\nSELECT 1", StatementTypeQuery, true},
		{"MultipleComments", "/* a */\n-- b\n  /* c */ SELECT 1", StatementTypeQuery, true},
		{"BlockCommentBeforeInsert", "/* tag */ INSERT INTO t VALUES (1)", StatementTypeDML, false},
		{"BlockCommentBeforeCreate", "/* tag */ CREATE TABLE t (id INT)", StatementTypeDDLCreate, false},
		{"CTE", "WITH x AS (SELECT 1 AS n) SELECT n FROM x", StatementTypeQuery, true},
		{"LowercaseCTE", "with x as (select 1) select * from x", StatementTypeQuery, true},
		{"ParenthesizedSelect", "(SELECT 1) UNION ALL (SELECT 2)", StatementTypeQuery, true},
		{"CommentThenCTE", "-- tag\nWITH x AS (SELECT 1) SELECT * FROM x", StatementTypeQuery, true},
		{"UnterminatedBlockComment", "/* never closed SELECT 1", StatementTypeDML, false},
		{"OnlyLineComment", "-- nothing here", StatementTypeDML, false},
		{"PlainSelect", "SELECT 1", StatementTypeQuery, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifySQL(tt.sql)
			if got.Type != tt.wantTyp {
				t.Errorf("ClassifySQL(%q).Type = %v, want %v", tt.sql, got.Type, tt.wantTyp)
			}
			if got.IsQuery != tt.isQuery {
				t.Errorf("ClassifySQL(%q).IsQuery = %v, want %v", tt.sql, got.IsQuery, tt.isQuery)
			}
		})
	}
}
