package runtime

import "sync/atomic"

// Subscriber receives frame reports asynchronously.
// Slow subscribers never block the frame loop: events are delivered through a
// bounded channel and dropped (with a counter) when the subscriber is full.
type Subscriber struct {
	name    string
	ch      chan FrameReport
	dropped uint64
	closed  atomic.Bool
}

// Name returns the subscriber name.
func (s *Subscriber) Name() string { return s.name }

// Channel returns the report channel.
func (s *Subscriber) Channel() <-chan FrameReport { return s.ch }

// Dropped returns the number of reports dropped due to a slow consumer.
func (s *Subscriber) Dropped() uint64 { return atomic.LoadUint64(&s.dropped) }

// Close closes the subscriber channel.
func (s *Subscriber) Close() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.ch)
	}
}

// deliver pushes a report without blocking; returns false if dropped.
func (s *Subscriber) deliver(r FrameReport) bool {
	if s.closed.Load() {
		return false
	}
	select {
	case s.ch <- r:
		return true
	default:
		atomic.AddUint64(&s.dropped, 1)
		return false
	}
}
