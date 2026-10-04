package persist

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/cuixudong74/npc-blackboard-go/behavior"
	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// jval is a type-preserving JSON wrapper for blackboard fact values.
// Plain encoding/json turns ints into float64 on the round trip, which would
// make a behavior's type assertion fail after restart and could change
// decision outcomes. jval tags primitive types so they decode back exactly.
type jval struct {
	K string          `json:"k"`           // kind tag
	V json.RawMessage `json:"v,omitempty"` // value
}

// jmarshal encodes a value with its concrete type preserved.
func jmarshal(v any) (json.RawMessage, error) {
	var k string
	var val any
	switch x := v.(type) {
	case nil:
		k = "nil"
	case bool:
		k, val = "bool", x
	case int:
		k, val = "int", x
	case int64:
		k, val = "int64", x
	case float64:
		k, val = "float64", x
	case string:
		k, val = "string", x
	case map[string]any:
		k = "map"
		m := map[string]json.RawMessage{}
		for mk, mv := range x {
			raw, err := jmarshal(mv)
			if err != nil {
				return nil, err
			}
			m[mk] = raw
		}
		val = m
	case []any:
		k = "slice"
		arr := make([]json.RawMessage, 0, len(x))
		for _, iv := range x {
			raw, err := jmarshal(iv)
			if err != nil {
				return nil, err
			}
			arr = append(arr, raw)
		}
		val = arr
	default:
		// unknown type: best-effort plain JSON (numbers become float64)
		k = "json"
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		val = json.RawMessage(raw)
	}
	raw, err := json.Marshal(val)
	if err != nil {
		return nil, err
	}
	return json.Marshal(jval{K: k, V: json.RawMessage(raw)})
}

// junmarshal decodes a value encoded by jmarshal.
func junmarshal(raw json.RawMessage) (any, error) {
	var jv jval
	if err := json.Unmarshal(raw, &jv); err != nil {
		return nil, err
	}
	switch jv.K {
	case "nil":
		return nil, nil
	case "bool":
		var v bool
		return v, json.Unmarshal(jv.V, &v)
	case "int":
		var v int
		return v, json.Unmarshal(jv.V, &v)
	case "int64":
		var v int64
		return v, json.Unmarshal(jv.V, &v)
	case "float64":
		var v float64
		return v, json.Unmarshal(jv.V, &v)
	case "string":
		var v string
		return v, json.Unmarshal(jv.V, &v)
	case "map":
		var m map[string]json.RawMessage
		if err := json.Unmarshal(jv.V, &m); err != nil {
			return nil, err
		}
		out := map[string]any{}
		for mk, rv := range m {
			v, err := junmarshal(rv)
			if err != nil {
				return nil, err
			}
			out[mk] = v
		}
		return out, nil
	case "slice":
		var arr []json.RawMessage
		if err := json.Unmarshal(jv.V, &arr); err != nil {
			return nil, err
		}
		out := []any{}
		for _, rv := range arr {
			v, err := junmarshal(rv)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case "json":
		var v any
		dec := json.NewDecoder(bytes.NewReader(jv.V))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	default:
		return nil, fmt.Errorf("persist: unknown value kind %q", jv.K)
	}
}

// walRecord is one JSON-lines record in the write-ahead log.
type walRecord struct {
	Type     string       `json:"type"`
	Tick     int64        `json:"tick,omitempty"`
	Events   []eventDTO   `json:"events,omitempty"`
	Writes   []factDTO    `json:"writes,omitempty"`
	Event    *eventDTO    `json:"event,omitempty"`
	Snapshot *snapshotDTO `json:"snapshot,omitempty"`
	Pending  []eventDTO   `json:"pending,omitempty"`
}

type eventDTO struct {
	Seq         uint64          `json:"seq"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	SourceTick  int64           `json:"source_tick,omitempty"`
	DeliverTick int64           `json:"deliver_tick,omitempty"`
}

type factDTO struct {
	Namespace  string          `json:"namespace"`
	Key        string          `json:"key"`
	Value      json.RawMessage `json:"value,omitempty"`
	WriteTick  int64           `json:"write_tick"`
	ExpireTick int64           `json:"expire_tick,omitempty"`
	Writer     string          `json:"writer,omitempty"`
}

type snapshotDTO struct {
	Facts []factDTO `json:"facts"`
}

func eventToDTO(ev behavior.Event) eventDTO {
	raw, err := jmarshal(ev.Payload)
	if err != nil {
		raw = nil
	}
	return eventDTO{
		Seq: ev.Seq, Type: ev.Type, Payload: raw,
		SourceTick: ev.SourceTick, DeliverTick: ev.DeliverTick,
	}
}

func (d eventDTO) toEvent() behavior.Event {
	var payload any
	if len(d.Payload) > 0 {
		if v, err := junmarshal(d.Payload); err == nil {
			payload = v
		}
	}
	return behavior.Event{
		Seq: d.Seq, Type: d.Type, Payload: payload,
		SourceTick: d.SourceTick, DeliverTick: d.DeliverTick,
	}
}

func writeToDTO(w blackboard.Write) factDTO {
	raw, err := jmarshal(w.Op.Value)
	if err != nil {
		raw = nil
	}
	return factDTO{
		Namespace:  string(w.Op.Namespace),
		Key:        w.Op.Key,
		Value:      raw,
		WriteTick:  w.WriteTick,
		ExpireTick: w.ExpireTick,
		Writer:     w.Writer,
	}
}

func factToDTO(f blackboard.Fact) factDTO {
	raw, err := jmarshal(f.Value)
	if err != nil {
		raw = nil
	}
	return factDTO{
		Namespace:  string(f.Namespace),
		Key:        f.Key,
		Value:      raw,
		WriteTick:  f.WriteTick,
		ExpireTick: f.ExpireTick,
		Writer:     f.Writer,
	}
}

func (d factDTO) toFact() blackboard.Fact {
	var val any
	if len(d.Value) > 0 {
		v, err := junmarshal(d.Value)
		if err == nil {
			val = v
		}
	}
	return blackboard.Fact{
		Namespace:  blackboard.Namespace(d.Namespace),
		Key:        d.Key,
		Value:      val,
		WriteTick:  d.WriteTick,
		ExpireTick: d.ExpireTick,
		Writer:     d.Writer,
	}
}

func encode(rec walRecord) ([]byte, error) {
	return json.Marshal(rec)
}

func decode(line []byte) (walRecord, error) {
	var rec walRecord
	err := json.Unmarshal(line, &rec)
	return rec, err
}
