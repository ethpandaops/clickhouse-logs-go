package clickhouselogs

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
)

// Option configures a Client created with NewWithPool.
type Option func(*Client)

// WithQueryTimeout sets the per-attempt query timeout.
func WithQueryTimeout(d time.Duration) Option {
	return func(c *Client) {
		c.config.QueryTimeout = d
	}
}

// WithMaxBlockSize sets the max_block_size ClickHouse query setting,
// controlling the number of rows per block in streaming results.
func WithMaxBlockSize(n int) Option {
	return func(c *Client) {
		c.maxBlockSize = n
	}
}

// Client provides methods to query pod logs from ClickHouse.
type Client struct {
	pool         *chpool.Pool
	config       *Config
	log          *slog.Logger
	ownsPool     bool
	maxBlockSize int
	mu           sync.RWMutex
}

// New creates a new Client that manages its own connection pool.
// Call Start to connect and Stop to close.
func New(cfg *Config, log *slog.Logger) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	cfg.SetDefaults()

	return &Client{
		config:   cfg,
		log:      log.With(slog.String("component", "clickhouse-logs")),
		ownsPool: true,
	}, nil
}

// NewWithPool creates a Client using an existing connection pool.
// Start is a no-op and Stop does not close the pool.
func NewWithPool(pool *chpool.Pool, log *slog.Logger, opts ...Option) *Client {
	cfg := &Config{}
	cfg.SetDefaults()

	c := &Client{
		pool:     pool,
		config:   cfg,
		log:      log.With(slog.String("component", "clickhouse-logs")),
		ownsPool: false,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Start connects to ClickHouse with retry logic. For BYO pool clients this is a no-op.
func (c *Client) Start(ctx context.Context) error {
	if !c.ownsPool {
		return nil
	}

	c.mu.Lock()
	if c.pool != nil {
		c.mu.Unlock()

		return nil
	}

	c.mu.Unlock()

	compression := ch.CompressionLZ4

	switch c.config.Compression {
	case "zstd":
		compression = ch.CompressionZSTD
	case "none":
		compression = ch.CompressionDisabled
	}

	dialCtx, cancel := context.WithTimeout(ctx, c.config.DialTimeout*time.Duration(c.config.MaxRetries+1))
	defer cancel()

	var pool *chpool.Pool

	err := doWithRetry(dialCtx, c.config, "dial", func(attemptCtx context.Context) error {
		var dialErr error

		pool, dialErr = chpool.Dial(attemptCtx, chpool.Options{
			ClientOptions: ch.Options{
				Address:     c.config.Addr,
				Database:    "default",
				User:        c.config.Username,
				Password:    c.config.Password,
				Compression: compression,
				DialTimeout: c.config.DialTimeout,
			},
			MaxConns:          c.config.MaxConns,
			MinConns:          c.config.MinConns,
			MaxConnLifetime:   c.config.ConnMaxLifetime,
			MaxConnIdleTime:   c.config.ConnMaxIdleTime,
			HealthCheckPeriod: c.config.HealthCheckPeriod,
		})

		return dialErr
	})
	if err != nil {
		return fmt.Errorf("failed to dial clickhouse: %w", err)
	}

	c.mu.Lock()
	c.pool = pool
	c.mu.Unlock()

	c.log.InfoContext(ctx, "Connected to ClickHouse")

	return nil
}

// Stop closes the connection pool. For BYO pool clients, this is a no-op.
func (c *Client) Stop() error {
	if !c.ownsPool {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.pool != nil {
		c.pool.Close()
		c.log.Info("Closed ClickHouse connection pool")
	}

	return nil
}

// Fetch executes a bounded query and returns all matching log entries.
// The query must have a Limit set. Retries on transient errors.
func (c *Client) Fetch(ctx context.Context, q *Query) ([]LogEntry, error) {
	if err := q.validate(); err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}

	if q.limit <= 0 {
		return nil, fmt.Errorf("Fetch requires Limit to be set on the query")
	}

	sql := q.build(orderDesc)
	entries := make([]LogEntry, 0, q.limit)
	cols := newLogColumns()

	err := doWithRetry(ctx, c.config, "fetch", func(attemptCtx context.Context) error {
		cols.reset()

		entries = entries[:0]

		return c.pool.Do(attemptCtx, ch.Query{
			Body:     sql,
			Result:   cols.results(),
			Settings: c.querySettings(),
			OnResult: func(ctx context.Context, block proto.Block) error {
				for i := 0; i < cols.rows(); i++ {
					entries = append(entries, cols.row(i))
				}

				return nil
			},
		})
	})
	if err != nil {
		return nil, fmt.Errorf("fetch failed: %w", err)
	}

	return entries, nil
}

// Scan executes a query and calls fn for each log entry. Does not retry
// because the callback may have side effects. No Limit required.
func (c *Client) Scan(ctx context.Context, q *Query, fn func(LogEntry) error) error {
	if err := q.validate(); err != nil {
		return fmt.Errorf("invalid query: %w", err)
	}

	sql := q.build(orderNone)
	cols := newLogColumns()

	queryCtx, cancel := withQueryTimeout(ctx, c.config.QueryTimeout)
	defer cancel()

	err := c.pool.Do(queryCtx, ch.Query{
		Body:     sql,
		Result:   cols.results(),
		Settings: c.querySettings(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < cols.rows(); i++ {
				if err := fn(cols.row(i)); err != nil {
					return err
				}
			}

			return nil
		},
	})
	if err != nil {
		return fmt.Errorf("scan failed: %w", err)
	}

	return nil
}

// Stream executes a query and returns an Iterator for pull-based consumption.
// No Limit required. Does not retry. Always call Iterator.Close when done.
func (c *Client) Stream(ctx context.Context, q *Query) (*Iterator, error) {
	if err := q.validate(); err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}

	sql := q.build(orderNone)
	results := make(chan iteratorEntry, 1)

	queryCtx, cancel := context.WithCancel(ctx) //nolint:gosec // G118: cancel is passed to Iterator.Close

	go func() {
		defer close(results)

		cols := newLogColumns()

		err := c.pool.Do(queryCtx, ch.Query{
			Body:     sql,
			Result:   cols.results(),
			Settings: c.querySettings(),
			OnResult: func(ctx context.Context, block proto.Block) error {
				n := cols.rows()
				if n == 0 {
					return nil
				}

				entries := make([]LogEntry, 0, n)

				for i := 0; i < n; i++ {
					entries = append(entries, cols.row(i))
				}

				select {
				case results <- iteratorEntry{entries: entries}:
				case <-ctx.Done():
					return ctx.Err()
				}

				return nil
			},
		})
		if err != nil {
			select {
			case results <- iteratorEntry{err: err}:
			case <-queryCtx.Done():
			}
		}
	}()

	return &Iterator{
		ch:     results,
		cancel: cancel,
		idx:    -1,
	}, nil
}

func (c *Client) querySettings() []ch.Setting {
	if c.maxBlockSize <= 0 {
		return nil
	}

	return []ch.Setting{
		ch.SettingInt("max_block_size", c.maxBlockSize),
	}
}
