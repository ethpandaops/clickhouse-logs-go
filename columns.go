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

// Physical expressions in the OpenTelemetry log schema.
//
// The two tenants identify a workload differently and neither populates the
// other's columns, so each expression falls back rather than binding to one
// shape. k8s-hosted workloads (the internal tenant) carry the materialized
// k8s.* columns, promoted from ResourceAttributes and backtick-quoted here
// because of the dots. VM-hosted devnet nodes (the external tenant) leave
// those empty and describe themselves through ResourceAttributes instead:
//
//	Namespace -> network     (e.g. glamsterdam-devnet-7)
//	Pod       -> host.name   (e.g. prysm-geth-1)
//	Node      -> host.name   (the VM is the machine)
//	Container -> ServiceName (e.g. beacon)
//
// Unset values are empty strings rather than NULL on both sides, so a plain
// emptiness check picks the right one. None of these columns are in the sort
// key, so wrapping them costs no index usage.
const (
	exprTimestamp   = "Timestamp"
	exprIngressUser = "IngressUser"
	exprNamespace   = "if(`k8s.namespace.name` != '', `k8s.namespace.name`, ResourceAttributes['network'])"
	exprPod         = "if(`k8s.pod.name` != '', `k8s.pod.name`, ResourceAttributes['host.name'])"
	exprContainer   = "if(`k8s.container.name` != '', `k8s.container.name`, ServiceName)"
	exprNode        = "if(`k8s.node.name` != '', `k8s.node.name`, ResourceAttributes['host.name'])"
	exprStream      = "if(LogAttributes['stream'] != '', LogAttributes['stream'], LogAttributes['log.iostream'])"
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
// LowCardinality(String). Only IngressUser is read straight off the table:
// every other column goes through an if() fallback, and if() over a
// LowCardinality(String) and a String yields a plain String.
func isLowCardinality(logical string) bool {
	return logical == columnIngressUser
}

// logColumns holds the ch-go column definitions for log queries.
// DateTime64 is explicitly set to UTC to avoid ch-go's default of time.Local.
// LowCardinality columns use ColLowCardinality[string] to match the schema.
type logColumns struct {
	timestamp   *proto.ColDateTime64
	ingressUser *proto.ColLowCardinality[string]
	namespace   *proto.ColStr
	pod         *proto.ColStr
	container   *proto.ColStr
	node        *proto.ColStr
	stream      *proto.ColStr
	message     *proto.ColStr
}

func newLogColumns() *logColumns {
	return &logColumns{
		timestamp:   new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano).WithLocation(time.UTC),
		ingressUser: new(proto.ColStr).LowCardinality(),
		namespace:   new(proto.ColStr),
		pod:         new(proto.ColStr),
		container:   new(proto.ColStr),
		node:        new(proto.ColStr),
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
