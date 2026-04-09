package clickhouselogs

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testFrom = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testTo   = time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)
)

func TestSource_String(t *testing.T) {
	assert.Equal(t, "logs_internal.logs", Internal.String())
	assert.Equal(t, "logs_external.logs", External.String())
}

func TestQuery_Validate(t *testing.T) {
	tests := []struct {
		name    string
		query   *Query
		wantErr string
	}{
		{
			name:    "missing From",
			query:   NewQuery(Internal).To(testTo),
			wantErr: "From time is required",
		},
		{
			name:    "missing To",
			query:   NewQuery(Internal).From(testFrom),
			wantErr: "To time is required",
		},
		{
			name:  "valid minimal",
			query: NewQuery(Internal).From(testFrom).To(testTo),
		},
		{
			name:    "invalid search - empty token",
			query:   NewQuery(Internal).From(testFrom).To(testTo).Search(Token("")),
			wantErr: "search: token must not be empty",
		},
		{
			name:    "invalid search - token with whitespace",
			query:   NewQuery(Internal).From(testFrom).To(testTo).Search(Token("a b")),
			wantErr: "whitespace",
		},
		{
			name: "invalid search - too many tokens",
			query: NewQuery(Internal).From(testFrom).To(testTo).Search(
				Tokens(make([]string, 65)...),
			),
			wantErr: "too many tokens",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.query.validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestQuery_Build_MinimalQuery(t *testing.T) {
	q := NewQuery(Internal).From(testFrom).To(testTo)
	sql := q.build(orderNone)

	assert.Contains(t, sql, "FROM logs_internal.logs")
	assert.Contains(t, sql, "Timestamp >= toDateTime64('2025-01-01 00:00:00.000', 3)")
	assert.Contains(t, sql, "Timestamp < toDateTime64('2025-01-01 01:00:00.000', 3)")
	assert.NotContains(t, sql, "ORDER BY")
	assert.NotContains(t, sql, "LIMIT")
}

func TestQuery_Build_ExternalSource(t *testing.T) {
	q := NewQuery(External).From(testFrom).To(testTo)
	sql := q.build(orderNone)

	assert.Contains(t, sql, "FROM logs_external.logs")
}

func TestQuery_Build_AllFilters(t *testing.T) {
	q := NewQuery(Internal).
		From(testFrom).
		To(testTo).
		IngressUser("sigma").
		Namespace("mainnet", "testnet").
		Pod("my-pod-123").
		Container("reth", "lighthouse").
		Node("node-1", "node-2").
		Stream("stderr").
		Search(Token("ERROR")).
		Limit(100)

	sql := q.build(orderDesc)

	assert.Contains(t, sql, "IngressUser = 'sigma'")
	assert.Contains(t, sql, "Namespace IN ('mainnet', 'testnet')")
	assert.Contains(t, sql, "Pod = 'my-pod-123'")
	assert.Contains(t, sql, "Container IN ('reth', 'lighthouse')")
	assert.Contains(t, sql, "Node IN ('node-1', 'node-2')")
	assert.Contains(t, sql, "Stream IN ('stderr')")
	assert.Contains(t, sql, "hasToken(Message, 'ERROR')")
	assert.Contains(t, sql, "LIMIT 100")
	assert.Contains(t, sql, "ORDER BY Timestamp DESC")
}

func TestQuery_Build_PodLike(t *testing.T) {
	q := NewQuery(Internal).From(testFrom).To(testTo).PodLike("%tysm%")
	sql := q.build(orderNone)

	assert.Contains(t, sql, "Pod LIKE '%tysm%'")
}

func TestQuery_Build_SearchModes(t *testing.T) {
	tests := []struct {
		name   string
		search MessageSearch
		want   string
	}{
		{
			name:   "token",
			search: Token("ERROR"),
			want:   "hasToken(Message, 'ERROR')",
		},
		{
			name:   "tokens",
			search: Tokens("error", "timeout"),
			want:   "hasAllTokens(Message, 'error timeout')",
		},
		{
			name:   "substring",
			search: Substring("panic:"),
			want:   "Message LIKE '%panic:%'",
		},
		{
			name:   "regex",
			search: Regex("error.*timeout"),
			want:   "match(Message, 'error.*timeout')",
		},
		{
			name:   "token with quote",
			search: Token("O'Brien"),
			want:   "hasToken(Message, 'O''Brien')",
		},
		{
			name:   "token with backslash",
			search: Token(`C:\temp`),
			want:   `hasToken(Message, 'C:\\temp')`,
		},
		{
			name:   "substring with percent",
			search: Substring("100%"),
			want:   `Message LIKE '%100\\%%'`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(Internal).From(testFrom).To(testTo).Search(tt.search)
			sql := q.build(orderNone)

			assert.Contains(t, sql, tt.want)
		})
	}
}

func TestQuery_Build_Ordering(t *testing.T) {
	tests := []struct {
		name         string
		order        func(q *Query) *Query
		defaultOrder orderDirection
		wantOrder    string
		wantNoOrder  bool
	}{
		{
			name:         "unset with desc default",
			order:        func(q *Query) *Query { return q },
			defaultOrder: orderDesc,
			wantOrder:    "ORDER BY Timestamp DESC",
		},
		{
			name:         "unset with none default",
			order:        func(q *Query) *Query { return q },
			defaultOrder: orderNone,
			wantNoOrder:  true,
		},
		{
			name:         "explicit asc overrides default",
			order:        func(q *Query) *Query { return q.OrderAsc() },
			defaultOrder: orderDesc,
			wantOrder:    "ORDER BY Timestamp ASC",
		},
		{
			name:         "explicit desc",
			order:        func(q *Query) *Query { return q.OrderDesc() },
			defaultOrder: orderNone,
			wantOrder:    "ORDER BY Timestamp DESC",
		},
		{
			name:         "unordered overrides desc default",
			order:        func(q *Query) *Query { return q.Unordered() },
			defaultOrder: orderDesc,
			wantNoOrder:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.order(NewQuery(Internal).From(testFrom).To(testTo))
			sql := q.build(tt.defaultOrder)

			if tt.wantNoOrder {
				assert.NotContains(t, sql, "ORDER BY")
			} else {
				assert.Contains(t, sql, tt.wantOrder)
			}
		})
	}
}

func TestQuery_Build_EscapesSpecialChars(t *testing.T) {
	q := NewQuery(Internal).
		From(testFrom).
		To(testTo).
		IngressUser("user'with\"quotes").
		Pod(`pod\with\backslash`)

	sql := q.build(orderNone)

	assert.Contains(t, sql, "IngressUser = 'user''with\"quotes'")
	assert.Contains(t, sql, `Pod = 'pod\\with\\backslash'`)
}

func TestQuery_Build_TimezoneConversion(t *testing.T) {
	// Times in non-UTC timezone should be converted to UTC in the query
	loc := time.FixedZone("EST", -5*3600)
	from := time.Date(2025, 1, 1, 12, 0, 0, 0, loc) // 12:00 EST = 17:00 UTC
	to := time.Date(2025, 1, 1, 13, 0, 0, 0, loc)   // 13:00 EST = 18:00 UTC

	q := NewQuery(Internal).From(from).To(to)
	sql := q.build(orderNone)

	assert.Contains(t, sql, "17:00:00.000")
	assert.Contains(t, sql, "18:00:00.000")
}

func TestTokens_Validate_MaxTokens(t *testing.T) {
	tokens := make([]string, maxTokensCount, maxTokensCount+1)
	for i := range tokens {
		tokens[i] = "tok"
	}

	search := Tokens(tokens...)

	require.NoError(t, search.validate())

	tokens = append(tokens, "one-too-many")
	search = Tokens(tokens...)

	err := search.validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many tokens: 65")
}

func TestQuery_Build_SelectColumns(t *testing.T) {
	q := NewQuery(Internal).From(testFrom).To(testTo)
	sql := q.build(orderNone)

	assert.True(t, strings.HasPrefix(sql, "SELECT Timestamp, IngressUser, Namespace, Pod, Container, Node, Stream, Message FROM"))
}
