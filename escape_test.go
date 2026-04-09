package clickhouselogs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "plain string", input: "hello", want: "hello"},
		{name: "single quote", input: "O'Brien", want: "O''Brien"},
		{name: "multiple quotes", input: "it''s", want: "it''''s"},
		{name: "backslash", input: `C:\temp`, want: `C:\\temp`},
		{name: "backslash t not tab", input: `\t`, want: `\\t`},
		{name: "backslash 0 not null", input: `\0`, want: `\\0`},
		{name: "backslash n not newline", input: `\n`, want: `\\n`},
		{name: "backslash and quote", input: `path\'s`, want: `path\\''s`},
		{name: "empty string", input: "", want: ""},
		{name: "unicode", input: "日本語", want: "日本語"},
		{name: "null byte", input: "a\x00b", want: "a\x00b"},
		{name: "multiple backslashes", input: `a\\b`, want: `a\\\\b`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, escapeString(tt.input))
		})
	}
}

func TestQuoteLiteral(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "plain", input: "hello", want: "'hello'"},
		{name: "with quote", input: "O'Brien", want: "'O''Brien'"},
		{name: "with backslash", input: `C:\temp`, want: `'C:\\temp'`},
		{name: "empty", input: "", want: "''"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, quoteLiteral(tt.input))
		})
	}
}

func TestEscapeForLike(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "plain", input: "hello", want: "hello"},
		{name: "percent", input: "100%", want: `100\%`},
		{name: "underscore", input: "a_b", want: `a\_b`},
		{name: "backslash", input: `C:\temp`, want: `C:\\temp`},
		{name: "all special", input: `100%_\`, want: `100\%\_\\`},
		{name: "empty", input: "", want: ""},
		{name: "backslash then percent", input: `\%`, want: `\\\%`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, escapeForLike(tt.input))
		})
	}
}

func TestFormatIN(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{name: "single value", values: []string{"a"}, want: "('a')"},
		{name: "multiple values", values: []string{"a", "b", "c"}, want: "('a', 'b', 'c')"},
		{name: "with special chars", values: []string{"O'Brien", `C:\x`}, want: `('O''Brien', 'C:\\x')`},
		{name: "empty slice", values: []string{}, want: "()"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, formatIN(tt.values))
		})
	}
}
