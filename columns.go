package clickhouselogs

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// Logical column names. These are the names LogEntry exposes and the aliases
// every generated SELECT projects to, so the decoded block layout stays
// independent of the physical schema underneath.
const (
	columnTimestamp   = "Timestamp"
	columnIngressUser = "IngressUser"
	columnNamespace   = "Namespace"
	columnPod         = "Pod"
	columnContainer   = "Container"
	columnNode        = "Node"
	columnStream      = "Stream"
	columnMessage     = "Message"
)

// Physical expressions in the OpenTelemetry log schema. The k8s.* columns are
// materialized from ResourceAttributes rather than map lookups, so filtering
// on them stays index-friendly; they need backtick quoting because of the
// dots in their names. Stream has no promoted column and is read out of
// LogAttributes — populated on the external tenant, empty on internal.
const (
	exprTimestamp   = "Timestamp"
	exprIngressUser = "IngressUser"
	exprNamespace   = "`k8s.namespace.name`"
	exprPod         = "`k8s.pod.name`"
	exprContainer   = "`k8s.container.name`"
	exprNode        = "`k8s.node.name`"
	exprStream      = "LogAttributes['stream']"
	exprMessage     = "Body"
)

// physicalColumn maps a logical column name onto its physical expression.
// Unknown names pass through so callers can project columns this package
// does not model.
func physicalColumn(logical string) string {
	switch logical {
	case columnTimestamp:
		return exprTimestamp
	case columnIngressUser:
		return exprIngressUser
	case columnNamespace:
		return exprNamespace
	case columnPod:
		return exprPod
	case columnContainer:
		return exprContainer
	case columnNode:
		return exprNode
	case columnStream:
		return exprStream
	case columnMessage:
		return exprMessage
	default:
		return logical
	}
}

// isLowCardinality reports whether a logical column decodes as
// LowCardinality(String). Body is a plain String, and a map lookup yields the
// map's value type, which is also a plain String.
func isLowCardinality(logical string) bool {
	switch logical {
	case columnIngressUser, columnNamespace, columnPod, columnContainer, columnNode:
		return true
	default:
		return false
	}
}

// logColumns holds the ch-go column definitions for log queries.
// DateTime64 is explicitly set to UTC to avoid ch-go's default of time.Local.
// LowCardinality columns use ColLowCardinality[string] to match the schema.
type logColumns struct {
	timestamp   *proto.ColDateTime64
	ingressUser *proto.ColLowCardinality[string]
	namespace   *proto.ColLowCardinality[string]
	pod         *proto.ColLowCardinality[string]
	container   *proto.ColLowCardinality[string]
	node        *proto.ColLowCardinality[string]
	stream      *proto.ColStr
	message     *proto.ColStr
}

func newLogColumns() *logColumns {
	return &logColumns{
		timestamp:   new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano).WithLocation(time.UTC),
		ingressUser: new(proto.ColStr).LowCardinality(),
		namespace:   new(proto.ColStr).LowCardinality(),
		pod:         new(proto.ColStr).LowCardinality(),
		container:   new(proto.ColStr).LowCardinality(),
		node:        new(proto.ColStr).LowCardinality(),
		stream:      new(proto.ColStr),
		message:     new(proto.ColStr),
	}
}

func (c *logColumns) results() proto.Results {
	return proto.Results{
		{Name: columnTimestamp, Data: c.timestamp},
		{Name: columnIngressUser, Data: c.ingressUser},
		{Name: columnNamespace, Data: c.namespace},
		{Name: columnPod, Data: c.pod},
		{Name: columnContainer, Data: c.container},
		{Name: columnNode, Data: c.node},
		{Name: columnStream, Data: c.stream},
		{Name: columnMessage, Data: c.message},
	}
}

func (c *logColumns) reset() {
	c.timestamp.Reset()
	c.ingressUser.Reset()
	c.namespace.Reset()
	c.pod.Reset()
	c.container.Reset()
	c.node.Reset()
	c.stream.Reset()
	c.message.Reset()
}

func (c *logColumns) row(i int) LogEntry {
	return LogEntry{
		Timestamp:   c.timestamp.Row(i),
		IngressUser: c.ingressUser.Row(i),
		Namespace:   c.namespace.Row(i),
		Pod:         c.pod.Row(i),
		Container:   c.container.Row(i),
		Node:        c.node.Row(i),
		Stream:      c.stream.Row(i),
		Message:     c.message.Row(i),
	}
}

func (c *logColumns) rows() int {
	return c.timestamp.Rows()
}
