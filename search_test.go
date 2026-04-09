package clickhouselogs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearch_Validate(t *testing.T) {
	tests := []struct {
		name    string
		search  MessageSearch
		wantErr string
	}{
		{name: "valid token", search: Token("ERROR")},
		{name: "empty token", search: Token(""), wantErr: "must not be empty"},
		{name: "token with space", search: Token("a b"), wantErr: "whitespace"},
		{name: "token with tab", search: Token("a\tb"), wantErr: "whitespace"},
		{name: "valid tokens", search: Tokens("a", "b")},
		{name: "empty tokens list", search: Tokens(), wantErr: "must not be empty"},
		{name: "token with empty element", search: Tokens("a", ""), wantErr: "index 1 must not be empty"},
		{name: "token with whitespace element", search: Tokens("a", "b c"), wantErr: "index 1 must not contain whitespace"},
		{name: "valid substring", search: Substring("hello")},
		{name: "empty substring", search: Substring(""), wantErr: "must not be empty"},
		{name: "valid regex", search: Regex("err.*")},
		{name: "empty regex", search: Regex(""), wantErr: "must not be empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.search.validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSearch_Clause(t *testing.T) {
	tests := []struct {
		name   string
		search MessageSearch
		want   string
	}{
		{
			name:   "regex with backslash",
			search: Regex(`err\d+`),
			want:   `match(Message, 'err\\d+')`,
		},
		{
			name:   "tokens with quotes",
			search: Tokens("it's", "O'Brien"),
			want:   "hasAllTokens(Message, 'it''s O''Brien')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.search.clause())
		})
	}
}
