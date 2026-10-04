package persist

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/cuixudong74/npc-blackboard-go/behavior"
	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// Store persists committed frames so a runtime can resume after restart.
type Store interface {
	AppendFrame(tick int64, events []behavior.Event, writes []blackboard.Write) error
	AppendEnqueue(ev behavior.Event) error
	Checkpoint(tick int64, snap *blackboard.Snapshot, pending []behavior.Event) error
	Load() (tick int64, snap *blackboard.Snapshot, pending []behavior.Event, err error)
	Close() error
}

// FileStore is a JSON-lines WAL with a JSON snapshot checkpoint on disk.
type FileStore struct {
	dir string
	mu  sync.Mutex
	wal *os.File
	bw  *bufio.Writer
}

// Open opens (or creates) a store in dir.
func Open(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(walPath(dir), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileStore{dir: dir, wal: f, bw: bufio.NewWriter(f)}, nil
}

func walPath(dir string) string  { return filepath.Join(dir, "wal.log") }
func snapPath(dir string) string { return filepath.Join(dir, "snap.json") }

func (f *FileStore) append(rec walRecord) error {
	data, err := encode(rec)
	if err != nil {
		return err
	}
	if _, err := f.bw.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.bw.Flush()
}

// AppendFrame appends a committed frame (delivered events + writes).
func (f *FileStore) AppendFrame(tick int64, events []behavior.Event, writes []blackboard.Write) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec := walRecord{Type: "frame", Tick: tick}
	for _, ev := range events {
		rec.Events = append(rec.Events, eventToDTO(ev))
	}
	for _, w := range writes {
		rec.Writes = append(rec.Writes, writeToDTO(w))
	}
	return f.append(rec)
}

// AppendEnqueue appends an enqueued event (before delivery).
func (f *FileStore) AppendEnqueue(ev behavior.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.append(walRecord{Type: "enqueue", Tick: ev.DeliverTick, Event: ptr(eventToDTO(ev))})
}

// Checkpoint writes a compact snapshot and truncates older WAL records.
func (f *FileStore) Checkpoint(tick int64, snap *blackboard.Snapshot, pending []behavior.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 1. write snapshot atomically (tmp + rename)
	dto := snapshotDTO{}
	for _, ns := range snap.Namespaces() {
		for _, fact := range snap.All(ns) {
			dto.Facts = append(dto.Facts, factToDTO(fact))
		}
	}
	tmp := snapPath(f.dir) + ".tmp"
	data, err := json.Marshal(dto)
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, snapPath(f.dir)); err != nil {
		return err
	}

	// 2. truncate WAL: keep only records newer than the checkpoint tick
	if err := f.truncateLocked(tick); err != nil {
		return err
	}

	// 3. log the checkpoint + pending events for durability
	rec := walRecord{Type: "checkpoint", Tick: tick, Snapshot: &dto}
	for _, ev := range pending {
		rec.Pending = append(rec.Pending, eventToDTO(ev))
	}
	return f.append(rec)
}

// truncateLocked rewrites the WAL keeping only records with tick > keepTick.
func (f *FileStore) truncateLocked(keepTick int64) error {
	if err := f.bw.Flush(); err != nil {
		return err
	}
	if _, err := f.wal.Seek(0, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(f.wal)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	var keep [][]byte
	for scanner.Scan() {
		line := scanner.Bytes()
		rec, err := decode(line)
		if err != nil {
			continue
		}
		if rec.Tick > keepTick {
			keep = append(keep, append([]byte(nil), line...))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	// rewrite
	if err := f.wal.Truncate(0); err != nil {
		return err
	}
	if _, err := f.wal.Seek(0, 0); err != nil {
		return err
	}
	w := bufio.NewWriter(f.wal)
	for _, line := range keep {
		if _, err := w.Write(append(line, '\n')); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	f.bw.Reset(f.wal)
	return nil
}

// Load reconstructs the latest committed state.
func (f *FileStore) Load() (int64, *blackboard.Snapshot, []behavior.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	tick := int64(0)
	var snap *blackboard.Snapshot
	pending := map[uint64]behavior.Event{}

	// 1. snapshot checkpoint
	if data, err := os.ReadFile(snapPath(f.dir)); err == nil {
		var dto snapshotDTO
		if err := json.Unmarshal(data, &dto); err == nil {
			snap = snapshotFromDTO(dto)
			tick = snap.Tick()
		}
	}

	// 2. replay WAL
	if _, err := f.wal.Seek(0, 0); err != nil {
		return 0, nil, nil, err
	}
	scanner := bufio.NewScanner(f.wal)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for scanner.Scan() {
		rec, err := decode(scanner.Bytes())
		if err != nil {
			continue
		}
		switch rec.Type {
		case "enqueue":
			if rec.Event != nil {
				ev := rec.Event.toEvent()
				if _, ok := pending[ev.Seq]; !ok {
					pending[ev.Seq] = ev
				}
			}
		case "frame":
			if rec.Tick > tick {
				tick = rec.Tick
			}
			if snap == nil {
				snap = blackboard.NewBlackboard().Snapshot()
			}
			snap = applyFacts(snap, rec.Writes, rec.Tick)
			// remove delivered events from pending
			for _, evDTO := range rec.Events {
				delete(pending, evDTO.Seq)
			}
		case "checkpoint":
			if rec.Snapshot != nil {
				snap = snapshotFromDTO(*rec.Snapshot)
				tick = rec.Tick
				pending = map[uint64]behavior.Event{}
				for _, evDTO := range rec.Pending {
					pending[evDTO.Seq] = evDTO.toEvent()
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, nil, nil, err
	}

	out := make([]behavior.Event, 0, len(pending))
	for _, ev := range pending {
		out = append(out, ev)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return tick, snap, out, nil
}

// Close flushes and closes the store.
func (f *FileStore) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.bw.Flush(); err != nil {
		return err
	}
	return f.wal.Close()
}

func ptr[T any](v T) *T { return &v }

func snapshotFromDTO(dto snapshotDTO) *blackboard.Snapshot {
	data := map[blackboard.Namespace]map[string]blackboard.Fact{}
	tick := int64(0)
	for _, fd := range dto.Facts {
		f := fd.toFact()
		if f.WriteTick > tick {
			tick = f.WriteTick
		}
		m := data[f.Namespace]
		if m == nil {
			m = map[string]blackboard.Fact{}
			data[f.Namespace] = m
		}
		m[f.Key] = f
	}
	return blackboard.NewSnapshot(tick, data)
}

// applyFacts rebuilds a snapshot by applying stamped facts (WAL replay).
func applyFacts(base *blackboard.Snapshot, facts []factDTO, tick int64) *blackboard.Snapshot {
	data := map[blackboard.Namespace]map[string]blackboard.Fact{}
	if base != nil {
		for _, ns := range base.Namespaces() {
			m := map[string]blackboard.Fact{}
			for _, f := range base.All(ns) {
				m[f.Key] = f
			}
			data[ns] = m
		}
	}
	for _, fd := range facts {
		f := fd.toFact()
		m := data[f.Namespace]
		if m == nil {
			m = map[string]blackboard.Fact{}
			data[f.Namespace] = m
		}
		m[f.Key] = f
	}
	return blackboard.NewSnapshot(tick, data)
}
