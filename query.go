package clickhouselogs

import (
	"fmt"
	"strings"
	"time"
)

// Source identifies which ClickHouse log table to query.
type Source int

const (
	// Internal queries internal.otel_logs — operator platform and
	// infrastructure logs.
	Internal Source = iota
	// External queries external.otel_logs — devnet and testnet node
	// container logs.
	External
)

// String returns the fully-qualified table name.
func (s Source) String() string {
	switch s {
	case Internal:
		return "internal.otel_logs"
	case External:
		return "external.otel_logs"
	default:
		return "internal.otel_logs"
	}
}

// selectList projects the physical schema onto the logical column names, so
// the decoded block layout is the same regardless of how the underlying table
// spells its columns.
var selectList = strings.Join([]string{
	exprTimestamp + " AS " + columnTimestamp,
	exprIngressUser + " AS " + columnIngressUser,
	exprNamespace + " AS " + columnNamespace,
	exprPod + " AS " + columnPod,
	exprContainer + " AS " + columnContainer,
	exprNode + " AS " + columnNode,
	exprStream + " AS " + columnStream,
	exprMessage + " AS " + columnMessage,
}, ", ")

// orderDirection represents the ORDER BY direction for a query.
type orderDirection int

const (
	orderUnset orderDirection = iota // method applies its own default
	orderAsc
	orderDesc
	orderNone // explicitly no ORDER BY
)

// Query builds a ClickHouse log query with fluent filter methods.
type Query struct {
	source      Source
	from        time.Time
	to          time.Time
	ingressUser string
	namespaces  []string
	pod         string
	podLike     string
	containers  []string
	nodes       []string
	streams     []string
	search      MessageSearch
	limit       int
	order       orderDirection
}

// NewQuery creates a new query builder for the given source table.
func NewQuery(source Source) *Query {
	return &Query{source: source}
}

// From sets the start time (inclusive). Required.
func (q *Query) From(t time.Time) *Query {
	q.from = t

	return q
}

// To sets the end time (exclusive). Required.
func (q *Query) To(t time.Time) *Query {
	q.to = t

	return q
}

// IngressUser filters by ingress user (exact match).
func (q *Query) IngressUser(user string) *Query {
	q.ingressUser = user

	return q
}

// Namespace filters by one or more namespaces (IN clause).
func (q *Query) Namespace(ns ...string) *Query {
	q.namespaces = ns

	return q
}

// Pod filters by exact pod name.
func (q *Query) Pod(pod string) *Query {
	q.pod = pod

	return q
}

// PodLike filters by pod name pattern (LIKE clause).
func (q *Query) PodLike(pattern string) *Query {
	q.podLike = pattern

	return q
}

// Container filters by one or more container names (IN clause).
func (q *Query) Container(containers ...string) *Query {
	q.containers = containers

	return q
}

// Node filters by one or more node names (IN clause).
func (q *Query) Node(nodes ...string) *Query {
	q.nodes = nodes

	return q
}

// Stream filters by one or more stream names (IN clause), e.g. "stdout", "stderr".
func (q *Query) Stream(streams ...string) *Query {
	q.streams = streams

	return q
}

// Search sets the message search filter.
func (q *Query) Search(search MessageSearch) *Query {
	q.search = search

	return q
}

// Limit sets the maximum number of rows to return.
func (q *Query) Limit(n int) *Query {
	q.limit = n

	return q
}

// OrderAsc sets the query to ORDER BY Timestamp ASC.
func (q *Query) OrderAsc() *Query {
	q.order = orderAsc

	return q
}

// OrderDesc sets the query to ORDER BY Timestamp DESC.
func (q *Query) OrderDesc() *Query {
	q.order = orderDesc

	return q
}

// Unordered explicitly removes ORDER BY from the query.
func (q *Query) Unordered() *Query {
	q.order = orderNone

	return q
}

// validate checks that the query has all required fields.
func (q *Query) validate() error {
	if q.from.IsZero() {
		return fmt.Errorf("From time is required")
	}

	if q.to.IsZero() {
		return fmt.Errorf("To time is required")
	}

	if q.search != nil {
		if err := q.search.validate(); err != nil {
			return fmt.Errorf("search: %w", err)
		}
	}

	return nil
}

// build generates the SQL query string. The effectiveOrder parameter allows
// the caller (Fetch/Scan/Stream) to supply a default when q.order is orderUnset.
func (q *Query) build(defaultOrder orderDirection) string {
	var sb strings.Builder

	sb.WriteString("SELECT ")
	sb.WriteString(selectList)
	sb.WriteString(" FROM ")
	sb.WriteString(q.source.String())
	sb.WriteString(" WHERE ")
	sb.WriteString(q.whereClause())

	effectiveOrder := q.order
	if effectiveOrder == orderUnset {
		effectiveOrder = defaultOrder
	}

	switch effectiveOrder {
	case orderAsc:
		sb.WriteString(" ORDER BY Timestamp ASC")
	case orderDesc:
		sb.WriteString(" ORDER BY Timestamp DESC")
	case orderNone, orderUnset:
		// no ORDER BY
	}

	if q.limit > 0 {
		fmt.Fprintf(&sb, " LIMIT %d", q.limit)
	}

	return sb.String()
}

// buildDistinct generates a SELECT DISTINCT query for a single logical column,
// aliased back to that name so the caller decodes a predictable block.
func (q *Query) buildDistinct(column string) string {
	var sb strings.Builder

	expr := physicalColumn(column)

	sb.WriteString("SELECT DISTINCT ")
	sb.WriteString(expr)
	sb.WriteString(" AS ")
	sb.WriteString(column)
	sb.WriteString(" FROM ")
	sb.WriteString(q.source.String())
	sb.WriteString(" WHERE ")
	sb.WriteString(q.whereClause())
	sb.WriteString(" ORDER BY ")
	sb.WriteString(column)
	sb.WriteString(" ASC")

	return sb.String()
}

// whereClause generates the WHERE conditions without the WHERE keyword.
func (q *Query) whereClause() string {
	conditions := make([]string, 0, 8)

	conditions = append(conditions,
		fmt.Sprintf("Timestamp >= toDateTime64(%s, 3)", quoteLiteral(q.from.UTC().Format("2006-01-02 15:04:05.000"))),
		fmt.Sprintf("Timestamp < toDateTime64(%s, 3)", quoteLiteral(q.to.UTC().Format("2006-01-02 15:04:05.000"))),
	)

	if q.ingressUser != "" {
		conditions = append(conditions, fmt.Sprintf("%s = %s", exprIngressUser, quoteLiteral(q.ingressUser)))
	}

	if len(q.namespaces) > 0 {
		conditions = append(conditions, fmt.Sprintf("%s IN %s", exprNamespace, formatIN(q.namespaces)))
	}

	if q.pod != "" {
		conditions = append(conditions, fmt.Sprintf("%s = %s", exprPod, quoteLiteral(q.pod)))
	}

	if q.podLike != "" {
		conditions = append(conditions, fmt.Sprintf("%s LIKE %s", exprPod, quoteLiteral(q.podLike)))
	}

	if len(q.containers) > 0 {
		conditions = append(conditions, fmt.Sprintf("%s IN %s", exprContainer, formatIN(q.containers)))
	}

	if len(q.nodes) > 0 {
		conditions = append(conditions, fmt.Sprintf("%s IN %s", exprNode, formatIN(q.nodes)))
	}

	if len(q.streams) > 0 {
		conditions = append(conditions, fmt.Sprintf("%s IN %s", exprStream, formatIN(q.streams)))
	}

	if q.search != nil {
		conditions = append(conditions, q.search.clause())
	}

	return strings.Join(conditions, " AND ")
}
