package query

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTranslateFunctions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"BOOLOR_AGG", `SELECT BOOLOR_AGG(COALESCE(e."CV", FALSE)) FROM t e`, `SELECT bool_or(COALESCE(e."CV", FALSE)) FROM t e`},
		{"GREATEST_IGNORE_NULLS", "SELECT GREATEST_IGNORE_NULLS(a, b, c)", "SELECT greatest(a, b, c)"},
		{"DAYOFWEEKISO", "SELECT DAYOFWEEKISO(submitted_at)", "SELECT isodow(submitted_at)"},
		{"ARRAY_SIZE", "SELECT ARRAY_SIZE(clients)", "SELECT len(clients)"},
		{"ARRAY_UNION_AGG", "SELECT ARRAY_UNION_AGG(shared_clients) FROM t", "SELECT list_distinct(flatten(list(shared_clients))) FROM t"},
		{
			"NestedRenamesInsideRewrite",
			"SELECT COALESCE(ARRAY_SIZE(ARRAY_UNION_AGG(DAYOFWEEKISO(x))), 0)",
			"SELECT COALESCE(len(list_distinct(flatten(list(isodow(x))))), 0)",
		},
		{"DATEADDQuotedPart", "SELECT DATEADD('day', 14, submitted_at)", "SELECT (submitted_at + INTERVAL (14) DAY)"},
		{
			"DATEADDExpressionCount",
			"SELECT DATEADD('day', 1 - DAYOFWEEKISO(submitted_at), submitted_at::DATE) AS bucket_week",
			"SELECT (submitted_at::DATE + INTERVAL (1 - isodow(submitted_at)) DAY) AS bucket_week",
		},
		{"DATEADDBarePartUntouched", "SELECT DATEADD(day, 7, order_date)", "SELECT DATEADD(day, 7, order_date)"},
		{"DATEDIFFQuotedPart", "SELECT DATEDIFF('minute', submitted_at, first_view_at)", "SELECT date_diff('minute', submitted_at, first_view_at)"},
		{"DATEDIFFBarePartUntouched", "SELECT DATEDIFF(day, a, b)", "SELECT DATEDIFF(day, a, b)"},
		{"NotACall", "SELECT isodow, array_size FROM t WHERE name = 'ARRAY_SIZE(x)'", "SELECT isodow, array_size FROM t WHERE name = 'ARRAY_SIZE(x)'"},
		{"IFFLeftToTheParser", "SELECT IFF(a, 1, 2)", "SELECT IFF(a, 1, 2)"},
		{"QuotedIdentifierUntouched", `SELECT "IFF"(a) FROM "DATEADD"`, `SELECT "IFF"(a) FROM "DATEADD"`},
		{"CommentUntouched", "/* IFF(a) */ SELECT 1 -- DAYOFWEEKISO(x)", "/* IFF(a) */ SELECT 1 -- DAYOFWEEKISO(x)"},
		{"UnbalancedCallUntouched", "SELECT ARRAY_UNION_AGG(x", "SELECT ARRAY_UNION_AGG(x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.expected, translateFunctions(tt.input)); diff != "" {
				t.Errorf("translateFunctions() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTranslatePassthroughFunctions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"IFF", "SELECT IFF(a > 1, 'x', 'y') FROM t", "SELECT IF(a > 1, 'x', 'y') FROM t"},
		{"IFFLowercase", "select iff(a, 1, 2)", "select IF(a, 1, 2)"},
		{"NotACall", "SELECT iff FROM t WHERE name = 'IFF(x)'", "SELECT iff FROM t WHERE name = 'IFF(x)'"},
		{"OtherRulesAlreadyApplied", "SELECT ARRAY_SIZE(x)", "SELECT ARRAY_SIZE(x)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.expected, translatePassthroughFunctions(tt.input)); diff != "" {
				t.Errorf("translatePassthroughFunctions() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestTranslator_FunctionsOnPassthrough covers a statement vitess-sqlparser cannot parse (quoted
// identifiers, ::casts, GROUPING SETS): the function rules still apply because they run on the
// text before parsing, and IFF is renamed after the parse failure.
func TestTranslator_FunctionsOnPassthrough(t *testing.T) {
	input := `SELECT IFF(GROUPING("g") = 0, 'g', 'all') AS "set", BOOLOR_AGG("flag"), DATEDIFF('minute', "a", "b"),
       DATEADD('day', 14, "a") <= "b", NULL::FLOAT
FROM "DB"."PUBLIC"."T"
GROUP BY GROUPING SETS ((), ("g"))`
	expected := `SELECT IF(GROUPING("g") = 0, 'g', 'all') AS "set", bool_or("flag"), date_diff('minute', "a", "b"),
       ("a" + INTERVAL (14) DAY) <= "b", NULL::DOUBLE
FROM "DB"."PUBLIC"."T"
GROUP BY GROUPING SETS ((), ("g"))`

	result, err := NewTranslator().Translate(input)
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("Translate() mismatch (-want +got):\n%s", diff)
	}
}
