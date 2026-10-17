package query

import (
	"strings"
)

// typeRule describes how a Snowflake data type is rewritten for DuckDB.
type typeRule struct {
	to          string // DuckDB type name
	keepArgs    bool   // keep type arguments such as length or precision/scale
	defaultArgs string // arguments to use when the source type has none
}

// snowflakeTypeRules maps Snowflake data types (uppercase) to DuckDB types.
// Types that DuckDB already understands with the same semantics are not listed.
//
//nolint:goconst // type names are table data, not shared constants
var snowflakeTypeRules = map[string]typeRule{
	// Snowflake NUMBER defaults to NUMBER(38,0); DuckDB DECIMAL defaults to DECIMAL(18,3).
	"NUMBER":  {to: "DECIMAL", keepArgs: true, defaultArgs: "38,0"},
	"NUMERIC": {to: "DECIMAL", keepArgs: true, defaultArgs: "38,0"},
	"DECIMAL": {to: "DECIMAL", keepArgs: true, defaultArgs: "38,0"},
	"DEC":     {to: "DECIMAL", keepArgs: true, defaultArgs: "38,0"},
	"BYTEINT": {to: "TINYINT"},

	// All Snowflake floating-point types are 64-bit; DuckDB FLOAT/REAL are 32-bit.
	"FLOAT":            {to: "DOUBLE"},
	"FLOAT4":           {to: "DOUBLE"},
	"FLOAT8":           {to: "DOUBLE"},
	"REAL":             {to: "DOUBLE"},
	"DOUBLE PRECISION": {to: "DOUBLE"},

	"TEXT":              {to: "VARCHAR", keepArgs: true},
	"STRING":            {to: "VARCHAR", keepArgs: true},
	"CHAR":              {to: "VARCHAR", keepArgs: true},
	"CHARACTER":         {to: "VARCHAR", keepArgs: true},
	"NCHAR":             {to: "VARCHAR", keepArgs: true},
	"NVARCHAR":          {to: "VARCHAR", keepArgs: true},
	"NVARCHAR2":         {to: "VARCHAR", keepArgs: true},
	"CHAR VARYING":      {to: "VARCHAR", keepArgs: true},
	"CHARACTER VARYING": {to: "VARCHAR", keepArgs: true},
	"NCHAR VARYING":     {to: "VARCHAR", keepArgs: true},

	"BINARY":    {to: "BLOB"},
	"VARBINARY": {to: "BLOB"},

	"VARIANT": {to: "JSON"},
	"OBJECT":  {to: "JSON"},
	"ARRAY":   {to: "JSON"},

	// Fractional-seconds precision is dropped. DuckDB TIMESTAMPTZ and TIME take no
	// precision argument, so all timestamp types use microsecond precision for consistency
	// (Snowflake's default is nanoseconds; digits beyond microseconds are truncated).
	"TIMESTAMP_NTZ": {to: "TIMESTAMP"},
	"DATETIME":      {to: "TIMESTAMP"},
	"TIMESTAMP":     {to: "TIMESTAMP"},
	"TIMESTAMP_LTZ": {to: "TIMESTAMPTZ"},
	"TIMESTAMP_TZ":  {to: "TIMESTAMPTZ"},
	"TIME":          {to: "TIME"},
}

// constraintKeywords start a table constraint rather than a column definition.
var constraintKeywords = map[string]bool{
	"CONSTRAINT": true,
	"PRIMARY":    true,
	"UNIQUE":     true,
	"FOREIGN":    true,
	"CHECK":      true,
}

// translateTypes rewrites Snowflake data types to DuckDB types.
// Only positions where the grammar expects a data type are rewritten, so identifiers
// and string literals that happen to match a type name (e.g. a column named "text")
// are left untouched:
//   - CAST(expr AS type) and TRY_CAST(expr AS type)
//   - expr::type
//   - column definitions in CREATE TABLE name (...)
//   - ALTER TABLE ... ADD [COLUMN] [IF NOT EXISTS] name type
//   - ALTER TABLE ... [SET DATA] TYPE type
func translateTypes(sql string) string {
	toks := tokenizeSQL(sql)
	w := &typeWalker{toks: toks}
	for i := range toks {
		if toks[i].kind != tokSpace {
			w.sig = append(w.sig, i)
		}
	}
	w.walk()
	if len(w.replacements) == 0 {
		return sql
	}

	var b strings.Builder
	b.Grow(len(sql) + 16)
	for i := 0; i < len(toks); i++ {
		if r, ok := w.replacements[i]; ok {
			b.WriteString(r.text)
			i = r.end - 1
			continue
		}
		b.WriteString(toks[i].text)
	}
	return b.String()
}

// typeReplacement replaces tokens [start, end) with text.
type typeReplacement struct {
	end  int
	text string
}

type parenKind int

const (
	parenOther parenKind = iota
	parenCast
	parenColumns
)

// columnState tracks the position within a CREATE TABLE column definition.
type columnState int

const (
	columnExpectName columnState = iota
	columnExpectType
	columnDone
)

type parenFrame struct {
	kind   parenKind
	column columnState
}

// alterState tracks ALTER TABLE ... ADD column definitions.
type alterState int

const (
	alterNone alterState = iota
	alterExpectName
	alterExpectType
)

// typeWalker finds data type positions in a token stream.
type typeWalker struct {
	toks         []sqlToken
	sig          []int // indices of non-space tokens
	replacements map[int]typeReplacement

	// Per-statement state, reset at each ';'.
	stmtWord  string // first word of the statement
	seenTable bool   // TABLE keyword seen at depth 0
	columnsOK bool   // CREATE TABLE column list may still appear
	inAdd     bool   // inside an ALTER TABLE ... ADD clause
	alter     alterState
	stack     []parenFrame
}

func (w *typeWalker) resetStatement() {
	w.stmtWord = ""
	w.seenTable = false
	w.columnsOK = true
	w.inAdd = false
	w.alter = alterNone
	w.stack = w.stack[:0]
}

func (w *typeWalker) word(p int) string {
	if p < 0 || p >= len(w.sig) {
		return ""
	}
	t := w.toks[w.sig[p]]
	if t.kind != tokWord {
		return ""
	}
	return strings.ToUpper(t.text)
}

func (w *typeWalker) text(p int) string {
	if p < 0 || p >= len(w.sig) {
		return ""
	}
	return w.toks[w.sig[p]].text
}

func (w *typeWalker) isName(p int) bool {
	if p < 0 || p >= len(w.sig) {
		return false
	}
	k := w.toks[w.sig[p]].kind
	return k == tokWord || k == tokQuotedIdent
}

//nolint:gocyclo // a single pass over the token stream keeps the grammar rules in one place
func (w *typeWalker) walk() {
	w.resetStatement()
	for p := 0; p < len(w.sig); p++ {
		tok := w.text(p)
		upper := w.word(p)
		depth := len(w.stack)

		if w.stmtWord == "" && upper != "" {
			w.stmtWord = upper
		}

		switch tok {
		case ";":
			w.resetStatement()
			continue
		case "::":
			p = w.translateAt(p + 1)
			continue
		case "(":
			w.stack = append(w.stack, w.openParen(p, depth))
			continue
		case ")":
			if depth > 0 {
				w.stack = w.stack[:depth-1]
			}
			continue
		}

		if depth > 0 {
			top := &w.stack[depth-1]
			switch top.kind {
			case parenCast:
				if upper == "AS" {
					p = w.translateAt(p + 1)
				}
			case parenColumns:
				p = w.columnDefinition(p, top)
			}
			continue
		}

		// Depth 0
		if upper == "TABLE" {
			w.seenTable = true
		}
		if w.stmtWord == "CREATE" && upper == "AS" {
			w.columnsOK = false // CREATE TABLE ... AS SELECT
		}
		if w.stmtWord == "ALTER" && w.seenTable {
			p = w.alterTable(p, tok, upper)
		}
	}
}

func (w *typeWalker) openParen(p, depth int) parenFrame {
	prev := w.word(p - 1)
	if prev == "CAST" || prev == "TRY_CAST" {
		return parenFrame{kind: parenCast}
	}
	if depth == 0 && w.stmtWord == "CREATE" && w.seenTable && w.columnsOK {
		w.columnsOK = false
		return parenFrame{kind: parenColumns, column: columnExpectName}
	}
	return parenFrame{kind: parenOther}
}

// columnDefinition handles a token directly inside a CREATE TABLE column list.
func (w *typeWalker) columnDefinition(p int, top *parenFrame) int {
	if w.text(p) == "," {
		top.column = columnExpectName
		return p
	}
	switch top.column {
	case columnExpectName:
		if constraintKeywords[w.word(p)] || !w.isName(p) {
			top.column = columnDone
		} else {
			top.column = columnExpectType
		}
	case columnExpectType:
		top.column = columnDone
		return w.translateAt(p)
	case columnDone:
	}
	return p
}

// alterTable handles a depth-0 token of an ALTER TABLE statement.
func (w *typeWalker) alterTable(p int, tok, upper string) int {
	switch w.alter {
	case alterExpectName:
		w.alterAddName(p, upper)
		return p
	case alterExpectType:
		w.alter = alterNone
		return w.translateAt(p)
	case alterNone:
	}

	switch {
	case upper == "ADD":
		w.inAdd = true
		w.alter = alterExpectName
	case tok == "," && w.inAdd:
		w.alter = alterExpectName
	case upper == "TYPE":
		return w.translateAt(p + 1)
	case upper == "DROP" || upper == "ALTER" || upper == "MODIFY" || upper == "RENAME":
		w.inAdd = false
	}
	return p
}

// alterAddName handles a token where ALTER TABLE ... ADD expects a column name.
func (w *typeWalker) alterAddName(p int, upper string) {
	switch {
	case upper == "COLUMN" || upper == "IF" || upper == "NOT" || upper == "EXISTS":
		// optional keywords before the column name
	case constraintKeywords[upper] || !w.isName(p):
		w.alter = alterNone
		w.inAdd = false
	default:
		w.alter = alterExpectType
	}
}

// translateAt rewrites the data type starting at significant token p, if it is a
// Snowflake type that needs translation. It returns the index of the last
// significant token consumed by the type so the caller can continue after it.
func (w *typeWalker) translateAt(p int) int {
	name, args, hasArgs, last := w.parseType(p)
	rule, ok := snowflakeTypeRules[name]
	if !ok {
		return last
	}

	replacement := rule.to
	switch {
	case hasArgs && rule.keepArgs:
		replacement += "(" + strings.TrimSpace(args) + ")"
	case !hasArgs && rule.defaultArgs != "":
		replacement += "(" + rule.defaultArgs + ")"
	}

	start, end := w.sig[p], w.sig[last]+1
	if strings.EqualFold(w.joinTokens(start, end), replacement) {
		return last
	}
	if w.replacements == nil {
		w.replacements = make(map[int]typeReplacement)
	}
	w.replacements[start] = typeReplacement{end: end, text: replacement}
	return last
}

// parseType reads the data type starting at significant token p. It returns the
// uppercase type name (empty if p does not start a type), its arguments, and the index
// of the last significant token of the type.
func (w *typeWalker) parseType(p int) (name, args string, hasArgs bool, last int) {
	name = w.word(p)
	if name == "" {
		return "", "", false, p - 1
	}
	last = p

	// Multi-word type names
	next := w.word(p + 1)
	switch {
	case name == "DOUBLE" && next == "PRECISION",
		(name == "CHAR" || name == "CHARACTER" || name == "NCHAR") && next == "VARYING":
		name += " " + next
		last = p + 1
	case name == "TIMESTAMP" && (next == "WITH" || next == "WITHOUT"):
		return "", "", false, p // DuckDB understands TIMESTAMP WITH[OUT] TIME ZONE
	}

	// Optional type arguments, e.g. NUMBER(10, 2)
	if w.text(last+1) == "(" {
		closeP := w.matchingParen(last + 1)
		if closeP < 0 {
			return "", "", false, last
		}
		return name, w.joinSig(last+2, closeP), true, closeP
	}
	return name, "", false, last
}

// matchingParen returns the significant index of the ')' matching the '(' at p, or -1.
func (w *typeWalker) matchingParen(p int) int {
	depth := 0
	for i := p; i < len(w.sig); i++ {
		switch w.text(i) {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return i
			}
		case ";":
			return -1
		}
	}
	return -1
}

// joinSig returns the original text between significant tokens [from, to).
func (w *typeWalker) joinSig(from, to int) string {
	if from >= to {
		return ""
	}
	return w.joinTokens(w.sig[from], w.sig[to])
}

func (w *typeWalker) joinTokens(from, to int) string {
	var b strings.Builder
	for i := from; i < to; i++ {
		b.WriteString(w.toks[i].text)
	}
	return b.String()
}

// rewriteConvertToCast rewrites convert(expr, type) back to CAST(expr AS type).
// vitess-sqlparser serializes CAST(expr AS type) as MySQL-style convert(expr, type),
// which DuckDB does not support.
func rewriteConvertToCast(sql string) string {
	if !strings.Contains(strings.ToLower(sql), "convert(") {
		return sql
	}
	return renderConvertToCast(tokenizeSQL(sql))
}

func renderConvertToCast(toks []sqlToken) string {
	var b strings.Builder
	for i := 0; i < len(toks); i++ {
		if toks[i].kind == tokWord && strings.EqualFold(toks[i].text, "convert") &&
			i+1 < len(toks) && toks[i+1].text == "(" {
			if closeIdx, commaIdx := scanConvertArgs(toks, i+1); commaIdx > 0 {
				b.WriteString("CAST(")
				b.WriteString(strings.TrimSpace(renderConvertToCast(toks[i+2 : commaIdx])))
				b.WriteString(" AS ")
				b.WriteString(strings.TrimSpace(joinTokenTexts(toks[commaIdx+1 : closeIdx])))
				b.WriteString(")")
				i = closeIdx
				continue
			}
		}
		b.WriteString(toks[i].text)
	}
	return b.String()
}

// scanConvertArgs returns the index of the ')' matching the '(' at open, and the index of
// the first top-level comma inside it (or -1 if there is none, e.g. convert(x using utf8)).
func scanConvertArgs(toks []sqlToken, open int) (closeIdx, commaIdx int) {
	depth := 0
	commaIdx = -1
	for i := open; i < len(toks); i++ {
		switch toks[i].text {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return i, commaIdx
			}
		case ",":
			if depth == 1 && commaIdx < 0 {
				commaIdx = i
			}
		}
	}
	return -1, -1
}

func joinTokenTexts(toks []sqlToken) string {
	var b strings.Builder
	for _, t := range toks {
		b.WriteString(t.text)
	}
	return b.String()
}

type sqlTokenKind int

const (
	tokSpace       sqlTokenKind = iota // whitespace or comment
	tokWord                            // unquoted identifier or keyword
	tokQuotedIdent                     // "identifier"
	tokString                          // 'string' or $$string$$
	tokNumber
	tokPunct
)

type sqlToken struct {
	kind sqlTokenKind
	text string
}

// tokenizeSQL splits SQL into tokens. Concatenating the token texts yields the input.
// String literals, quoted identifiers and comments are kept as single tokens so that
// their contents are never mistaken for keywords or type names.
//
//nolint:gocyclo // a lexer is naturally one switch over the next character
func tokenizeSQL(sql string) []sqlToken {
	var toks []sqlToken
	i := 0
	for i < len(sql) {
		start := i
		kind := tokPunct
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			kind = tokSpace
			for i < len(sql) && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r') {
				i++
			}
		case strings.HasPrefix(sql[i:], "--") || strings.HasPrefix(sql[i:], "//"):
			kind = tokSpace
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case strings.HasPrefix(sql[i:], "/*"):
			kind = tokSpace
			if end := strings.Index(sql[i+2:], "*/"); end >= 0 {
				i += 2 + end + 2
			} else {
				i = len(sql)
			}
		case strings.HasPrefix(sql[i:], "$$"):
			kind = tokString
			if end := strings.Index(sql[i+2:], "$$"); end >= 0 {
				i += 2 + end + 2
			} else {
				i = len(sql)
			}
		case c == '\'':
			kind = tokString
			i = scanQuoted(sql, i, '\'', true)
		case c == '"':
			kind = tokQuotedIdent
			i = scanQuoted(sql, i, '"', false)
		case isIdentStart(c):
			kind = tokWord
			for i < len(sql) && (isIdentStart(sql[i]) || (sql[i] >= '0' && sql[i] <= '9') || sql[i] == '$') {
				i++
			}
		case c >= '0' && c <= '9':
			kind = tokNumber
			for i < len(sql) && ((sql[i] >= '0' && sql[i] <= '9') || sql[i] == '.') {
				i++
			}
		case strings.HasPrefix(sql[i:], "::"):
			i += 2
		default:
			i++
		}
		toks = append(toks, sqlToken{kind: kind, text: sql[start:i]})
	}
	return toks
}

// scanQuoted returns the index just past the quoted section starting at sql[start].
// A doubled quote is an escaped quote; backslash escapes are honored when requested.
func scanQuoted(sql string, start int, quote byte, backslash bool) int {
	i := start + 1
	for i < len(sql) {
		switch {
		case backslash && sql[i] == '\\':
			i += 2
		case sql[i] == quote:
			if i+1 < len(sql) && sql[i+1] == quote {
				i += 2
				continue
			}
			return i + 1
		default:
			i++
		}
	}
	return len(sql)
}

func isIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c >= 0x80
}
