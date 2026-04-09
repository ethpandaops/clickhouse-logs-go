package clickhouselogs

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// logColumns holds the ch-go column definitions for log queries.
// DateTime64 is explicitly set to UTC to avoid ch-go's default of time.Local.
// LowCardinality columns use ColLowCardinality[string] to match the schema.
type logColumns struct {
	timestamp   *proto.ColDateTime64
	ingressUser *proto.ColLowCardinality[string]
	namespace   *proto.ColLowCardinality[string]
	pod         *proto.ColStr
	container   *proto.ColLowCardinality[string]
	node        *proto.ColLowCardinality[string]
	stream      *proto.ColLowCardinality[string]
	message     *proto.ColStr
}

func newLogColumns() *logColumns {
	return &logColumns{
		timestamp:   new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC),
		ingressUser: new(proto.ColStr).LowCardinality(),
		namespace:   new(proto.ColStr).LowCardinality(),
		pod:         new(proto.ColStr),
		container:   new(proto.ColStr).LowCardinality(),
		node:        new(proto.ColStr).LowCardinality(),
		stream:      new(proto.ColStr).LowCardinality(),
		message:     new(proto.ColStr),
	}
}

func (c *logColumns) results() proto.Results {
	return proto.Results{
		{Name: "Timestamp", Data: c.timestamp},
		{Name: "IngressUser", Data: c.ingressUser},
		{Name: "Namespace", Data: c.namespace},
		{Name: "Pod", Data: c.pod},
		{Name: "Container", Data: c.container},
		{Name: "Node", Data: c.node},
		{Name: "Stream", Data: c.stream},
		{Name: "Message", Data: c.message},
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
