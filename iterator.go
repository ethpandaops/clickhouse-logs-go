package clickhouselogs

import "sync"

// iteratorEntry holds either a batch of log entries or an error from the query goroutine.
type iteratorEntry struct {
	entries []LogEntry
	err     error
}

// Iterator provides a pull-based, sql.Rows-style interface for streaming log entries.
// Call Next to advance, Entry to read the current entry, and Err to check for errors.
// Always call Close when done.
type Iterator struct {
	ch      <-chan iteratorEntry
	cancel  func()
	current LogEntry
	batch   []LogEntry
	idx     int
	err     error
	done    bool
	once    sync.Once
}

// Next advances to the next log entry. Returns false when iteration is complete
// or an error occurred.
func (it *Iterator) Next() bool {
	if it.done {
		return false
	}

	// Advance within the current batch.
	it.idx++
	if it.idx < len(it.batch) {
		it.current = it.batch[it.idx]

		return true
	}

	// Fetch the next batch from the channel.
	item, ok := <-it.ch
	if !ok {
		it.done = true

		return false
	}

	if item.err != nil {
		it.err = item.err
		it.done = true

		return false
	}

	if len(item.entries) == 0 {
		// Empty batch — try the next one.
		return it.Next()
	}

	it.batch = item.entries
	it.idx = 0
	it.current = it.batch[0]

	return true
}

// Entry returns the current log entry. Only valid after a successful call to Next.
func (it *Iterator) Entry() LogEntry {
	return it.current
}

// Err returns the first error encountered during iteration, if any.
func (it *Iterator) Err() error {
	return it.err
}

// Close cancels the underlying query and drains the channel. Safe to call multiple times.
func (it *Iterator) Close() {
	it.once.Do(func() {
		it.done = true
		it.cancel()

		// Drain channel to unblock the producer goroutine.
		for range it.ch {
		}
	})
}
