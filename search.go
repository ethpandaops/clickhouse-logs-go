package clickhouselogs

import (
	"fmt"
	"strings"
)

const maxTokensCount = 64

// MessageSearch defines a message filter that generates a WHERE clause fragment.
type MessageSearch interface {
	clause() string
	validate() error
}

// tokenSearch matches a single whole-word token using hasToken (full_text index accelerated).
type tokenSearch struct {
	token string
}

// Token creates a message search using hasToken(Message, 'token').
// Accelerated by the full_text index. Case-sensitive. Matches whole words only.
func Token(token string) MessageSearch {
	return &tokenSearch{token: token}
}

func (s *tokenSearch) clause() string {
	return fmt.Sprintf("hasToken(Message, %s)", quoteLiteral(s.token))
}

func (s *tokenSearch) validate() error {
	if s.token == "" {
		return fmt.Errorf("token must not be empty")
	}

	if strings.ContainsAny(s.token, " \t\n\r") {
		return fmt.Errorf("token must not contain whitespace; use Tokens() for multiple tokens")
	}

	return nil
}

// tokensSearch matches multiple tokens using hasAllTokens (full_text index accelerated).
type tokensSearch struct {
	tokens []string
}

// Tokens creates a message search using hasAllTokens(Message, 'token1 token2 ...').
// All tokens must be present. Accelerated by the full_text index. Case-sensitive.
// Rejects more than 64 tokens (ClickHouse hasAllTokens limit).
func Tokens(tokens ...string) MessageSearch {
	return &tokensSearch{tokens: tokens}
}

func (s *tokensSearch) clause() string {
	escaped := make([]string, 0, len(s.tokens))

	for _, tok := range s.tokens {
		escaped = append(escaped, escapeString(tok))
	}

	return fmt.Sprintf("hasAllTokens(Message, '%s')", strings.Join(escaped, " "))
}

func (s *tokensSearch) validate() error {
	if len(s.tokens) == 0 {
		return fmt.Errorf("tokens must not be empty")
	}

	if len(s.tokens) > maxTokensCount {
		return fmt.Errorf("too many tokens: %d (max %d)", len(s.tokens), maxTokensCount)
	}

	for i, tok := range s.tokens {
		if tok == "" {
			return fmt.Errorf("token at index %d must not be empty", i)
		}

		if strings.ContainsAny(tok, " \t\n\r") {
			return fmt.Errorf("token at index %d must not contain whitespace", i)
		}
	}

	return nil
}

// substringSearch matches a substring using LIKE (NOT accelerated by full_text index).
type substringSearch struct {
	substr string
}

// Substring creates a message search using Message LIKE '%%substr%%'.
// NOT accelerated by the full_text index. Case-sensitive.
func Substring(substr string) MessageSearch {
	return &substringSearch{substr: substr}
}

func (s *substringSearch) clause() string {
	// Two layers of escaping:
	// 1. escapeForLike: escape LIKE wildcards (% → \%, _ → \_)
	// 2. escapeString: escape for SQL string literal (\ → \\, ' → '')
	// ClickHouse processes the SQL string layer first (\\ → \), then LIKE uses the result.
	// ClickHouse uses \ as the default LIKE escape character (no ESCAPE clause needed).
	return fmt.Sprintf("Message LIKE '%%%s%%'", escapeString(escapeForLike(s.substr)))
}

func (s *substringSearch) validate() error {
	if s.substr == "" {
		return fmt.Errorf("substring must not be empty")
	}

	return nil
}

// regexSearch matches using match() (NOT accelerated by full_text index).
type regexSearch struct {
	pattern string
}

// Regex creates a message search using match(Message, 'pattern').
// NOT accelerated by the full_text index. Case-sensitive. Use sparingly.
func Regex(pattern string) MessageSearch {
	return &regexSearch{pattern: pattern}
}

func (s *regexSearch) clause() string {
	return fmt.Sprintf("match(Message, %s)", quoteLiteral(s.pattern))
}

func (s *regexSearch) validate() error {
	if s.pattern == "" {
		return fmt.Errorf("regex pattern must not be empty")
	}

	return nil
}
