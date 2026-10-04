package runtime

import "time"

// Config holds the tunable parameters of a Runtime.
type Config struct {
	// Seed is the master random seed. Fixed seed + fixed input sequence
	// yields identical per-frame decisions and blackboard evolution.
	Seed int64
	// WorkerCount is the number of concurrent NPC evaluators. Concurrency
	// only affects speed, never results: outcomes are merged in NPC order.
	WorkerCount int
	// FrameTimeout is the wall-clock upper bound for one decision cycle.
	// Exceeding it cancels the cycle and rolls back all writes.
	FrameTimeout time.Duration
	// FrameInterval is the target gap between frames in Run. 0 means run
	// as fast as possible.
	FrameInterval time.Duration
	// DefaultTTL is the default fact validity in ticks when a WriteOp has
	// TTL == 0. 0 means "never expire".
	DefaultTTL int64
	// LatePolicy determines how writes with a SourceTick older than the
	// existing fact's WriteTick are handled.
	LatePolicy LatePolicy
	// QueueCapacity is the pending event queue size.
	QueueCapacity int
	// SubscribeBuffer is the per-subscriber channel buffer.
	SubscribeBuffer int
	// CheckpointInterval is the number of frames between WAL checkpoints.
	CheckpointInterval int
	// PersistPath is the directory for WAL/snapshot files. Empty means
	// in-memory only (no restart support).
	PersistPath string
}

// LatePolicy determines how out-of-order (late) writes are handled.
type LatePolicy uint8

const (
	// LateDrop silently drops late writes (recorded in the frame report).
	LateDrop LatePolicy = iota
	// LateQuarantine redirects late writes to a quarantine namespace.
	LateQuarantine
	// LateOverwrite lets the older write overwrite newer state (not
	// recommended; recorded in the frame report).
	LateOverwrite
)

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Seed:               1,
		WorkerCount:        4,
		FrameTimeout:       time.Second,
		DefaultTTL:         0,
		LatePolicy:         LateDrop,
		QueueCapacity:      1024,
		SubscribeBuffer:    256,
		CheckpointInterval: 100,
		PersistPath:        "",
	}
}

// Validate checks the configuration and fills zero values with defaults.
func (c *Config) Validate() error {
	if c.Seed == 0 {
		c.Seed = 1
	}
	if c.WorkerCount <= 0 {
		c.WorkerCount = 4
	}
	if c.FrameTimeout <= 0 {
		c.FrameTimeout = time.Second
	}
	if c.DefaultTTL < 0 {
		c.DefaultTTL = 0
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 1024
	}
	if c.SubscribeBuffer <= 0 {
		c.SubscribeBuffer = 256
	}
	if c.CheckpointInterval <= 0 {
		c.CheckpointInterval = 100
	}
	switch c.LatePolicy {
	case LateDrop, LateQuarantine, LateOverwrite:
	default:
		c.LatePolicy = LateDrop
	}
	return nil
}
