package clickhouselogs

import "context"

// MockCall represents a method call made to the mock.
type MockCall struct {
	Method string
	Args   []any
}

// MockClient is a mock implementation for testing consumers of this library.
type MockClient struct {
	FetchFunc  func(ctx context.Context, q *Query) ([]LogEntry, error)
	ScanFunc   func(ctx context.Context, q *Query, fn func(LogEntry) error) error
	StreamFunc func(ctx context.Context, q *Query) (*Iterator, error)
	StartFunc  func(ctx context.Context) error
	StopFunc   func() error

	Calls []MockCall
}

// NewMockClient creates a new mock client with default no-op implementations.
func NewMockClient() *MockClient {
	return &MockClient{
		FetchFunc: func(ctx context.Context, q *Query) ([]LogEntry, error) {
			return nil, nil
		},
		ScanFunc: func(ctx context.Context, q *Query, fn func(LogEntry) error) error {
			return nil
		},
		StreamFunc: func(ctx context.Context, q *Query) (*Iterator, error) {
			ch := make(chan iteratorEntry)

			close(ch)

			return &Iterator{ch: ch, cancel: func() {}, idx: -1}, nil
		},
		StartFunc: func(ctx context.Context) error {
			return nil
		},
		StopFunc: func() error {
			return nil
		},
		Calls: make([]MockCall, 0),
	}
}

// Fetch implements the Fetch method.
func (m *MockClient) Fetch(ctx context.Context, q *Query) ([]LogEntry, error) {
	m.Calls = append(m.Calls, MockCall{Method: "Fetch", Args: []any{ctx, q}})

	return m.FetchFunc(ctx, q)
}

// Scan implements the Scan method.
func (m *MockClient) Scan(ctx context.Context, q *Query, fn func(LogEntry) error) error {
	m.Calls = append(m.Calls, MockCall{Method: "Scan", Args: []any{ctx, q, fn}})

	return m.ScanFunc(ctx, q, fn)
}

// Stream implements the Stream method.
func (m *MockClient) Stream(ctx context.Context, q *Query) (*Iterator, error) {
	m.Calls = append(m.Calls, MockCall{Method: "Stream", Args: []any{ctx, q}})

	return m.StreamFunc(ctx, q)
}

// Start implements the Start method.
func (m *MockClient) Start(ctx context.Context) error {
	m.Calls = append(m.Calls, MockCall{Method: "Start", Args: []any{ctx}})

	return m.StartFunc(ctx)
}

// Stop implements the Stop method.
func (m *MockClient) Stop() error {
	m.Calls = append(m.Calls, MockCall{Method: "Stop"})

	return m.StopFunc()
}

// GetCallCount returns the number of times a method was called.
func (m *MockClient) GetCallCount(method string) int {
	count := 0

	for _, call := range m.Calls {
		if call.Method == method {
			count++
		}
	}

	return count
}

// WasCalled returns true if the specified method was called.
func (m *MockClient) WasCalled(method string) bool {
	return m.GetCallCount(method) > 0
}

// Reset clears all recorded calls.
func (m *MockClient) Reset() {
	m.Calls = make([]MockCall, 0)
}
