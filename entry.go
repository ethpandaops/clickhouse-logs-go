package clickhouselogs

import "time"

// LogEntry represents a single log row from ClickHouse.
type LogEntry struct {
	Timestamp   time.Time
	IngressUser string
	Namespace   string
	Pod         string
	Container   string
	Node        string
	Stream      string
	Message     string
}
