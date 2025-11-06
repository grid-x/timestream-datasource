package validator

// A tolerant SQL validator for AWS Timestream queries.
// It ensures that every SELECT which directly reads from a base table
// (not just from a subquery/CTE alias) has (sensible) predicates for
// time and measure name in WHERE.
//
// Heuristics (no full SQL parse):
//   - We lex tokens, track parentheses depth, and find SELECT blocks.
//   - For each SELECT, we locate FROM and WHERE at the same depth.
//   - A SELECT is considered "hits DB" if the FROM source looks like a base
//     table name (db.table or "db"."table"). If it's just an alias (e.g. a),
//     or starts with '(' (subquery), we skip it at that level; inner SELECTs
//     are validated separately.
//   - Each such SELECT needs to have both a valid time and a valid measure_name filter.
//   - A valid time filter is any predicate in WHERE that references one of
//     the allowed time columns (default: time, measure_time) and uses BETWEEN
//     (with optional NOT) or comparison operators (=, <, <=, >, >=, <>, !=).
//   - For measure_name, we are more restrictive: all occurrences of it have to be valid
//     conditions (e.g., measure_name = 'foo' or regexp_like(measure_name, '...')).
//
// Note: This is intentionally heuristic and aims to be practical for Timestream.

import (
	"strings"
	"unicode"
)

type Issue struct {
	Snippet string
	Reason  string
	AtDepth int
}

// Validate returns true if every SELECT that directly reads from a table
// has a WHERE time filter; otherwise returns false and the list of issues.
func Validate(sql string) (bool, []Issue) {
	src := stripComments(sql)
	toks := lex(src)

	type sel struct {
		selIdx int
		depth  int
	}
	var selects []sel
	for i := 0; i < len(toks); i++ {
		if toks[i].kind == tkKeyword && toks[i].val == "select" {
			selects = append(selects, sel{selIdx: i, depth: toks[i].depth})
		}
	}

	var issues []Issue

	for _, s := range selects {
		// Find FROM at same depth after this SELECT.
		fromIdx := findNextKeywordAtDepth(toks, s.selIdx+1, s.depth, "from")
		if fromIdx == -1 {
			// SELECT without FROM (e.g., SELECT 1): ignore (doesn't hit DB).
			continue
		}

		// FROM clause ends at next clause keyword (excluding WHERE) or when depth drops.
		stopIdx := findNextTerminatorAtDepth(toks, fromIdx+1, s.depth)

		// Decide if this SELECT directly reads from a base table (not subquery or CTE alias).
		hitsDB := fromStartsWithBaseTable(toks, fromIdx+1, stopIdx, s.depth)
		if !hitsDB {
			// Outer SELECT over CTE/derived table — inner SELECTs will be validated separately.
			continue
		}

		// WHERE must be present at same depth between FROM and its terminator.
		whereIdx := findNextKeywordBetweenAtDepth(toks, fromIdx+1, stopIdx, s.depth, "where")
		if whereIdx == -1 {
			issues = append(issues, Issue{
				Snippet: snippetAroundTokens(toks, s.selIdx, stopIdx),
				Reason:  "missing WHERE clause",
				AtDepth: s.depth,
			})
			continue
		}

		// WHERE body ends at next clause (group/order/having/union/...) or on depth drop.
		whereStop := findNextTerminatorAtDepth(toks, whereIdx+1, s.depth)

		// Logic to handle top-level ORs
		branches := findTopLevelOrBranches(toks, whereIdx+1, whereStop, s.depth)

		hasMissingTime := false
		hasMissingMeasure := false
		hasInvalidOr := len(branches) > 1

		for _, branch := range branches {
			branchStart, branchStop := branch[0], branch[1]

			// Check for time predicate.
			if !whereHasTimePredicate(toks, branchStart, branchStop) {
				hasMissingTime = true
			}

			// Check for measure_name predicate
			if !whereHasMeasureNamePredicate(toks, branchStart, branchStop) {
				hasMissingMeasure = true
			}
		}

		// Report issues.
		if hasMissingTime {
			reason := "WHERE clause lacks a time predicate"
			if hasInvalidOr {
				reason = "an OR branch in WHERE clause lacks a time predicate"
			}
			issues = append(issues, Issue{
				Snippet: snippetAroundTokens(toks, s.selIdx, whereStop),
				Reason:  reason,
				AtDepth: s.depth,
			})
		}

		if hasMissingMeasure {
			reason := "WHERE clause lacks a valid measure_name predicate (requires = '...' or regexp_like)"
			if hasInvalidOr {
				reason = "an OR branch in WHERE clause lacks a valid measure_name predicate (requires = '...' or regexp_like)"
			}
			issues = append(issues, Issue{
				Snippet: snippetAroundTokens(toks, s.selIdx, whereStop),
				Reason:  reason,
				AtDepth: s.depth,
			})
		}
	}

	return len(issues) == 0, issues
}

// NEW FUNCTION: Splits a token range by top-level OR keywords.
func findTopLevelOrBranches(toks []token, start, stop, depth int) [][2]int {
	var branches [][2]int
	currentBranchStart := start

	if stop < 0 {
		stop = len(toks)
	}

	for i := start; i < stop && i < len(toks); i++ {
		// If we find an 'OR' at the same depth, it's a separator.
		if toks[i].depth == depth && toks[i].kind == tkKeyword && toks[i].val == "or" {
			// Add the branch ending just before this 'OR'
			branches = append(branches, [2]int{currentBranchStart, i})
			// Start the next branch just after this 'OR'
			currentBranchStart = i + 1
		}
	}
	// Add the final branch (or the only branch, if no 'OR' was found)
	branches = append(branches, [2]int{currentBranchStart, stop})

	return branches
}

/* -------------------- internal: lexer & helpers -------------------- */

type tokenKind int

const (
	tkIdent tokenKind = iota
	tkKeyword
	tkString
	tkNumber
	tkSymbol
)

type token struct {
	val   string
	kind  tokenKind
	depth int
}

var keywords = map[string]struct{}{
	"select": {}, "from": {}, "where": {}, "group": {}, "by": {}, "order": {}, "having": {},
	"union": {}, "intersect": {}, "except": {}, "join": {}, "left": {}, "right": {}, "full": {},
	"outer": {}, "inner": {}, "cross": {}, "on": {}, "as": {}, "with": {}, "lateral": {},
	"between": {}, "and": {}, "or": {}, "not": {}, "in": {}, "exists": {},
}

func stripComments(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inLine, inBlock := false, false
	for i := 0; i < len(s); i++ {
		if inLine {
			if s[i] == '\n' {
				inLine = false
				b.WriteByte(s[i])
			}
			continue
		}
		if inBlock {
			if s[i] == '*' && i+1 < len(s) && s[i+1] == '/' {
				inBlock = false
				i++
			}
			continue
		}
		if s[i] == '-' && i+1 < len(s) && s[i+1] == '-' {
			inLine = true
			i++
			continue
		}
		if s[i] == '/' && i+1 < len(s) && s[i+1] == '*' {
			inBlock = true
			i++
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func lex(s string) []token {
	var out []token
	depth := 0

	readString := func(i int, quote byte) (string, int) {
		j := i + 1
		for j < len(s) {
			if s[j] == quote {
				// handle escaped '' or "" inside literals/quoted idents
				if j+1 < len(s) && s[j+1] == quote {
					j += 2
					continue
				}
				return s[i : j+1], j + 1
			}
			j++
		}
		return s[i:], len(s)
	}

	for i := 0; i < len(s); {
		r := s[i]
		// whitespace
		if unicode.IsSpace(rune(r)) {
			i++
			continue
		}
		// parentheses adjust depth
		if r == '(' {
			out = append(out, token{val: "(", kind: tkSymbol, depth: depth})
			depth++
			i++
			continue
		}
		if r == ')' {
			depth--
			if depth < 0 {
				depth = 0
			}
			out = append(out, token{val: ")", kind: tkSymbol, depth: depth})
			i++
			continue
		}
		// strings / quoted identifiers
		if r == '\'' || r == '"' {
			str, nx := readString(i, r)
			if r == '"' {
				// treat "ident" as identifier (lowercased, quotes kept for context)
				out = append(out, token{val: strings.ToLower(str), kind: tkIdent, depth: depth})
			} else {
				out = append(out, token{val: str, kind: tkString, depth: depth})
			}
			i = nx
			continue
		}
		// numbers
		if isNumStart(r) {
			j := i + 1
			for j < len(s) && (isNum(s[j]) || s[j] == '.') {
				j++
			}
			out = append(out, token{val: s[i:j], kind: tkNumber, depth: depth})
			i = j
			continue
		}
		// identifiers / keywords
		if isIdentStart(r) {
			j := i + 1
			for j < len(s) && isIdentPart(s[j]) {
				j++
			}
			word := strings.ToLower(s[i:j])
			if _, ok := keywords[word]; ok {
				out = append(out, token{val: word, kind: tkKeyword, depth: depth})
			} else {
				out = append(out, token{val: word, kind: tkIdent, depth: depth})
			}
			i = j
			continue
		}
		// multi-char operators (>=, <=, <>, !=)
		if (r == '>' || r == '<' || r == '!') && i+1 < len(s) {
			n := s[i+1]
			if (r == '>' && n == '=') || (r == '<' && (n == '=' || n == '>')) || (r == '!' && n == '=') {
				out = append(out, token{val: strings.ToLower(s[i : i+2]), kind: tkSymbol, depth: depth})
				i += 2
				continue
			}
		}
		// single-char symbols
		out = append(out, token{val: strings.ToLower(string(r)), kind: tkSymbol, depth: depth})
		i++
	}
	return out
}

// identifiers start with letter, '_' or '$' (keeping '$' support harmless)
func isIdentStart(b byte) bool { return unicode.IsLetter(rune(b)) || b == '_' || b == '$' }
func isIdentPart(b byte) bool {
	return unicode.IsLetter(rune(b)) || unicode.IsDigit(rune(b)) || b == '_' || b == '.' || b == '$'
}
func isNumStart(b byte) bool { return unicode.IsDigit(rune(b)) }
func isNum(b byte) bool      { return unicode.IsDigit(rune(b)) }

func findNextKeywordAtDepth(toks []token, start, depth int, word string) int {
	for i := start; i < len(toks); i++ {
		// If we exited this block, abort.
		if toks[i].depth < depth {
			return -1
		}
		if toks[i].depth != depth {
			continue
		}
		if toks[i].kind == tkKeyword && toks[i].val == word {
			return i
		}
	}
	return -1
}

func findNextKeywordBetweenAtDepth(toks []token, start, stop, depth int, word string) int {
	if stop < 0 {
		stop = len(toks)
	}
	for i := start; i < stop && i < len(toks); i++ {
		if toks[i].depth != depth {
			continue
		}
		if toks[i].kind == tkKeyword && toks[i].val == word {
			return i
		}
	}
	return -1
}

// Do NOT treat WHERE as a terminator when scanning FROM.
// Terminate on other clause keywords at same depth or when the depth drops.
func findNextTerminatorAtDepth(toks []token, start, depth int) int {
	for i := start; i < len(toks); i++ {
		// Block ended (e.g., we hit a closing parenthesis).
		if toks[i].depth < depth {
			return i
		}
		// Clause terminators at the same depth.
		if toks[i].depth == depth && toks[i].kind == tkKeyword {
			switch toks[i].val {
			case "group", "order", "having", "union", "intersect", "except":
				return i
			}
		}
	}
	return len(toks)
}

// Returns true if FROM's first source at this depth looks like a base table:
//   - single identifier containing a dot (db.table) and not a function call
//   - pattern: ident '.' ident  (covers "db"."table" and unquoted db.table split into parts)
//
// Robust to stray symbol tokens (e.g., backslashes from \" in test strings).
// Returns false for '(' (subquery) or single-part identifier (likely CTE alias).
func fromStartsWithBaseTable(toks []token, start, stop, depth int) bool {
	i := start

	// Advance to first meaningful token at this depth
	for i < stop && i < len(toks) {
		if toks[i].depth != depth {
			i++
			continue
		}
		// Skip stray symbols; '(' indicates subquery/derived table.
		if toks[i].kind == tkSymbol {
			if toks[i].val == "(" {
				return false
			}
			i++
			continue
		}
		// If we see SELECT here, it's a subquery-ish form.
		if toks[i].kind == tkKeyword {
			if toks[i].val == "select" {
				return false
			}
			i++
			continue
		}
		break
	}

	if i >= stop || i >= len(toks) || toks[i].kind != tkIdent {
		return false
	}

	// ident containing '.' => qualified name (db.table)
	if strings.Contains(stripQuotes(toks[i].val), ".") {
		// Ensure it's not immediately a function call ident(...)
		j := i + 1
		for j < stop && j < len(toks) && toks[j].depth != depth {
			j++
		}
		if j < stop && j < len(toks) && toks[j].kind == tkSymbol && toks[j].val == "(" {
			return false
		}
		return true
	}

	// Otherwise, look for: ident (noise?) '.' (noise?) ident
	// Skip stray symbol tokens between parts (e.g., backslashes from \" in tests).
	j := i + 1
	for j < stop && j < len(toks) {
		if toks[j].depth != depth {
			j++
			continue
		}
		// Seek the dot
		if toks[j].kind == tkSymbol {
			if toks[j].val != "." {
				j++
				continue
			}
			// Found '.', now find the following identifier skipping noise
			k := j + 1
			for k < stop && k < len(toks) {
				if toks[k].depth != depth {
					k++
					continue
				}
				if toks[k].kind == tkSymbol {
					k++
					continue
				}
				return toks[k].kind == tkIdent
			}
			return false
		}
		// A non-symbol before '.' means it's not a qualified base name here (likely alias).
		return false
	}

	return false
}
func whereHasTimePredicate(toks []token, start, stop int) bool {
	if stop < 0 {
		stop = len(toks)
	}

	for i := start; i < stop && i < len(toks); i++ {
		// Simple comparisons: time [op] ...
		if isTimeIdentifierAt(toks, i) {
			// Look ahead for operator at same depth (optionally allow NOT before BETWEEN).
			depth := toks[i].depth
			j := i + 1
			for j < stop && j < len(toks) && toks[j].depth != depth {
				j++
			}
			// NOT BETWEEN pattern: time NOT BETWEEN ...
			if j < stop && j < len(toks) && toks[j].kind == tkKeyword && toks[j].val == "not" {
				k := j + 1
				for k < stop && k < len(toks) && toks[k].depth != depth {
					k++
				}
				if k < stop && k < len(toks) && toks[k].kind == tkKeyword && toks[k].val == "between" {
					return true
				}
			}
			// BETWEEN pattern: time BETWEEN ...
			if j < stop && j < len(toks) && toks[j].kind == tkKeyword && toks[j].val == "between" {
				return true
			}
			// Comparison operator pattern
			if j < stop && j < len(toks) && toks[j].kind == tkSymbol && isCompareOp(toks[j].val) {
				return true
			}
		}

		// Also handle encountering BETWEEN first, then look back for time column within a small window.
		if toks[i].kind == tkKeyword && toks[i].val == "between" {
			depth := toks[i].depth
			for k := i - 1; k >= start && k >= i-6; k-- {
				if toks[k].kind == tkKeyword && toks[k].val == "not" {
					continue
				}
				if isTimeIdentifierAt(toks, k) && toks[k].depth == depth {
					return true
				}
			}
		}
	}
	return false
}

// MODIFIED FUNCTION
func whereHasMeasureNamePredicate(toks []token, start, stop int) bool {
	if stop < 0 {
		stop = len(toks)
	}

	foundValid := false
	foundInvalid := false // Flag for any *unapproved* use of measure_name

	i := start
	for i < stop && i < len(toks) {

		// Check for Pattern 1: regexp_like(measure_name, 'string')
		// We check this *first* because it contains 'measure_name' and
		// we need to consume the whole block at once.
		if toks[i].kind == tkIdent && toks[i].val == "regexp_like" {
			// Check for regexp_like(measure_name, 'string')
			if i+5 < stop && i+5 < len(toks) &&
				toks[i+1].kind == tkSymbol && toks[i+1].val == "(" &&
				toks[i+2].kind == tkIdent && toks[i+2].val == "measure_name" &&
				toks[i+3].kind == tkSymbol && toks[i+3].val == "," &&
				toks[i+4].kind == tkString &&
				toks[i+5].kind == tkSymbol && toks[i+5].val == ")" {

				foundValid = true
				i += 6   // Skip past the ')'
				continue // Continue to next token
			}
			// If it's regexp_like but *not* this pattern (e.g., wrong args),
			// we just treat it as a normal identifier and let the
			// 'measure_name' check below catch it if it's used inside.
		}

		// Check for Pattern 2: measure_name = 'string'
		if toks[i].kind == tkIdent && toks[i].val == "measure_name" {
			// Check for valid: measure_name = 'string'
			if i+2 < stop && i+2 < len(toks) &&
				toks[i+1].kind == tkSymbol && toks[i+1].val == "=" &&
				toks[i+2].kind == tkString {

				foundValid = true
				i += 3   // Skip past the string
				continue // Continue to next token

			} else {
				// We found 'measure_name' but it was NOT part of
				// measure_name = 'string'.
				// And since we checked regexp_like *first*, we know it's
				// not the 'measure_name' *inside* a valid regexp_like.
				// This is an invalid use.
				foundInvalid = true
			}
		}

		// Move to the next token
		i++
	}
	// Must have at least one valid condition and NO invalid conditions.
	return foundValid && !foundInvalid
}

func isCompareOp(s string) bool {
	switch s {
	case "=", "<", ">", "<=", ">=", "<>", "!=":
		return true
	}
	return false
}

func stripQuotes(s string) string {
	if len(s) >= 2 && ((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'')) {
		return strings.ToLower(s[1 : len(s)-1])
	}
	return strings.ToLower(s)
}

func isTimeIdentifierAt(toks []token, i int) bool {
	if i < 0 || i >= len(toks) {
		return false
	}
	if toks[i].kind != tkIdent {
		return false
	}

	return toks[i].val == "time"
}

func snippetAroundTokens(toks []token, start, stop int) string {
	if start < 0 {
		start = 0
	}
	if stop < 0 || stop > len(toks) {
		stop = len(toks)
	}
	var b strings.Builder
	limit := 220
	for i := start; i < stop; i++ {
		if b.Len() > limit {
			b.WriteString(" ...")
			break
		}
		b.WriteString(toks[i].val)
		if i+1 < stop {
			b.WriteByte(' ')
		}
	}
	return strings.TrimSpace(b.String())
}
