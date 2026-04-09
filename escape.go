package clickhouselogs

import "strings"

// escapeString escapes a string for use in a ClickHouse SQL literal.
// ClickHouse treats backslash as an escape character ('\t' becomes tab, '\0' becomes null byte),
// so backslashes must be escaped FIRST to avoid double-escaping. Then single quotes are doubled.
func escapeString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `''`)

	return s
}

// quoteLiteral wraps a string in single quotes after escaping it for ClickHouse.
func quoteLiteral(s string) string {
	return "'" + escapeString(s) + "'"
}

// escapeForLike escapes LIKE special characters (%, _, \) in a string.
// Backslashes are escaped first to avoid double-escaping. The caller must
// include ESCAPE '\' in the LIKE expression.
func escapeForLike(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)

	return s
}

// formatIN formats a string slice as a ClickHouse IN tuple: ('a', 'b', 'c').
func formatIN(values []string) string {
	parts := make([]string, 0, len(values))

	for _, v := range values {
		parts = append(parts, quoteLiteral(v))
	}

	return "(" + strings.Join(parts, ", ") + ")"
}
