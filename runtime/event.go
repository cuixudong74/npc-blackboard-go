package runtime

import (
	"sort"

	"github.com/cuixudong74/npc-blackboard-go/behavior"
)

// Event is an input event delivered to behaviors in a frame.
type Event = behavior.Event

// pendingEvent is an event waiting for delivery.
type pendingEvent struct {
	ev        Event
	deliverAt int64
}

// EventQueue is a deterministic, capacity-bounded event queue.
// Events are delivered in (deliverAt, seq) order.
type EventQueue struct {
	capacity int
	nextSeq  uint64
	items    []pendingEvent
}

// NewEventQueue returns a queue with the given capacity.
func NewEventQueue(capacity int) *EventQueue {
	if capacity <= 0 {
		capacity = 1024
	}
	return &EventQueue{capacity: capacity, nextSeq: 1}
}

// Enqueue adds an event. DeliverTick == 0 means next frame.
// Returns the assigned seq, or false if the queue is full.
func (q *EventQueue) Enqueue(ev Event, currentTick int64) (uint64, bool) {
	if len(q.items) >= q.capacity {
		return 0, false
	}
	ev.Seq = q.nextSeq
	q.nextSeq++
	deliverAt := ev.DeliverTick
	if deliverAt <= currentTick {
		deliverAt = currentTick + 1
	}
	q.items = append(q.items, pendingEvent{ev: ev, deliverAt: deliverAt})
	return ev.Seq, true
}

// Due returns the events deliverable at tick, in (deliverAt, seq) order,
// WITHOUT removing them. Call Consume after a successful commit to drop them.
func (q *EventQueue) Due(tick int64) []Event {
	due := make([]pendingEvent, 0, len(q.items))
	for _, pe := range q.items {
		if pe.deliverAt <= tick {
			due = append(due, pe)
		}
	}
	sort.SliceStable(due, func(i, j int) bool {
		if due[i].deliverAt != due[j].deliverAt {
			return due[i].deliverAt < due[j].deliverAt
		}
		return due[i].ev.Seq < due[j].ev.Seq
	})
	out := make([]Event, len(due))
	for i, pe := range due {
		out[i] = pe.ev
	}
	return out
}

// Consume removes events delivered at or before tick (called after a
// successful commit).
func (q *EventQueue) Consume(tick int64) {
	rest := q.items[:0]
	for _, pe := range q.items {
		if pe.deliverAt > tick {
			rest = append(rest, pe)
		}
	}
	q.items = rest
}

// Pending returns the number of undelivered events.
func (q *EventQueue) Pending() int { return len(q.items) }

// PendingEvents returns the pending events in deterministic order (for
// checkpoints).
func (q *EventQueue) PendingEvents() []Event {
	out := make([]Event, len(q.items))
	for i, pe := range q.items {
		out[i] = pe.ev
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// Restore re-inserts a pending event (used by restart replay).
func (q *EventQueue) Restore(ev Event) {
	q.items = append(q.items, pendingEvent{ev: ev, deliverAt: ev.DeliverTick})
	if ev.Seq >= q.nextSeq {
		q.nextSeq = ev.Seq + 1
	}
}
