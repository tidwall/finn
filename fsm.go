package nikolai

import (
	"io"

	"github.com/tidwall/raft"
)

type command struct {
}

type fsm Node

func (f *fsm) Node() *Node {
	return (*Node)(f)
}

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	/*
		var c command
		if err := json.Unmarshal(l.Data, &c); err != nil {
			panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
		}

		switch c.Op {
		case "set":
			return f.applySet(c.Key, c.Value)
		case "delete":
			return f.applyDelete(c.Key)
		default:
			panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
		}
	*/
	return nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	/*
		o := make(map[string]string)
		if err := json.NewDecoder(rc).Decode(&o); err != nil {
			return err
		}

		// Set the state from the snapshot, no lock required according to
		// Hashicorp docs.
		f.m = o
	*/
	return nil
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	/*
		f.mu.Lock()
		defer f.mu.Unlock()

		// Clone the map.
		o := make(map[string]string)
		for k, v := range f.m {
			o[k] = v
		}
		return &fsmSnapshot{store: o}, nil
	*/
	return nil, nil
}
