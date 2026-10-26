package query

import (
	"strings"
)

// functionRenames maps Snowflake functions (uppercase) to the DuckDB function with the same
// semantics. DuckDB's greatest already ignores NULLs, so GREATEST_IGNORE_NULLS is a plain rename.
var functionRenames = map[string]string{
	"BOOLOR_AGG":            "bool_or",
	"GREATEST_IGNORE_NULLS": "greatest",
	"DAYOFWEEKISO":          "isodow",
	"ARRAY_SIZE":            "len",
}

// passthroughRenames covers functions the AST walk translates when a statement parses; they are
// renamed on the text only for statements the parser rejects.
var passthroughRenames = map[string]string{
	"IFF": "IF",
}

// functionRewrites maps Snowflake functions (uppercase) to a rewrite of their (already translated)
// arguments. A rewrite returns false to leave the call untouched.
var functionRewrites = map[string]func(args []string) (string, bool){
	// ARRAY_UNION_AGG(a) → list_distinct(flatten(list(a)))
	"ARRAY_UNION_AGG": func(args []string) (string, bool) {
		if len(args) != 1 {
			return "", false
		}
		return "list_distinct(flatten(list(" + args[0] + ")))", true
	},
	// DATEADD('day', n, ts) → (ts + INTERVAL (n) DAY); the quoted date part is what the AST path
	// cannot handle, and the timestamp keeps its time of day.
	"DATEADD": func(args []string) (string, bool) {
		part, ok := quotedDatePart(args, 3)
		if !ok {
			return "", false
		}
		return "(" + args[2] + " + INTERVAL (" + args[1] + ") " + part + ")", true
	},
	// DATEDIFF('minute', a, b) → date_diff('minute', a, b), without the DATE cast the bare-part
	// form applies, which would drop sub-day precision.
	"DATEDIFF": func(args []string) (string, bool) {
		part, ok := quotedDatePart(args, 3)
		if !ok {
			return "", false
		}
		return "date_diff('" + strings.ToLower(part) + "', " + args[1] + ", " + args[2] + ")", true
	},
}

// quotedDatePart returns the date part of a DATEADD/DATEDIFF-style call when it is a quoted string
// literal such as 'day'.
func quotedDatePart(args []string, count int) (string, bool) {
	if len(args) != count {
		return "", false
	}
	part := args[0]
	if len(part) < 3 || part[0] != '\'' || part[len(part)-1] != '\'' {
		return "", false
	}
	return strings.ToUpper(part[1 : len(part)-1]), true
}

// translateFunctions rewrites Snowflake function calls DuckDB does not know on the SQL text, so
// that statements the parser cannot handle are translated too. String literals, quoted
// identifiers and comments are never touched.
func translateFunctions(sql string) string {
	return translateFunctionsWith(sql, functionRenames)
}

// translatePassthroughFunctions renames the functions the AST walk would have handled, for a
// statement that failed to parse.
func translatePassthroughFunctions(sql string) string {
	return translateFunctionsWith(sql, passthroughRenames)
}

func translateFunctionsWith(sql string, renames map[string]string) string {
	toks := tokenizeSQL(sql)
	var out strings.Builder

	for i := 0; i < len(toks); i++ {
		tok := toks[i]
		open := callOpenParen(toks, i)
		if tok.kind != tokWord || open < 0 {
			out.WriteString(tok.text)
			continue
		}

		name := strings.ToUpper(tok.text)
		if to, ok := renames[name]; ok {
			out.WriteString(to)
			continue
		}

		rewrite, ok := functionRewrites[name]
		if !ok {
			out.WriteString(tok.text)
			continue
		}

		closeIdx := closingParen(toks, open)
		if closeIdx < 0 {
			out.WriteString(tok.text)
			continue
		}

		args := splitFunctionArgs(joinTokenTexts(toks[open+1:closeIdx]), 3)
		for j, arg := range args {
			args[j] = strings.TrimSpace(translateFunctionsWith(arg, renames))
		}

		if replacement, ok := rewrite(args); ok {
			out.WriteString(replacement)
			i = closeIdx
			continue
		}
		out.WriteString(tok.text)
	}

	return out.String()
}

// callOpenParen returns the index of the "(" that follows the word at i (skipping whitespace), or -1.
func callOpenParen(toks []sqlToken, i int) int {
	for j := i + 1; j < len(toks); j++ {
		switch {
		case toks[j].kind == tokSpace:
			continue
		case toks[j].kind == tokPunct && toks[j].text == "(":
			return j
		default:
			return -1
		}
	}
	return -1
}

// closingParen returns the index of the ")" matching the "(" at open, or -1.
func closingParen(toks []sqlToken, open int) int {
	depth := 0
	for j := open; j < len(toks); j++ {
		if toks[j].kind != tokPunct {
			continue
		}
		switch toks[j].text {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return j
			}
		}
	}
	return -1
}
