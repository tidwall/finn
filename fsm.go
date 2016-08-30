package nikolai

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/hashicorp/raft"
	"github.com/tidwall/buntdb"
)

type fsm Node

func (f *fsm) Node() *Node {
	return (*Node)(f)
}

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	c, err := decodeCommand(l.Data)
	if err != nil {
		return err
	}
	var val interface{}
	switch c.op {
	default:
		return errInvalidCommand
	case opGet:
		val, err = f.Node().applyGet(c.args, c.multi)
	case opSet:
		val, err = f.Node().applySet(c.args, c.multi)
	case opDel:
		val, err = f.Node().applyDel(c.args, c.multi)
	}
	if err != nil {
		return err
	}
	return val
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	n := f.Node()
	n.mu.Lock()
	defer n.mu.Unlock()
	// close the current database and open a blank one.
	n.ddb.Close()
	n.ddb, _ = buntdb.Open(":memory:")
	if err := n.ddb.Load(rc); err != nil {
		// there was an error, so close the db, open a blank one,
		// and return the error.
		n.ddb.Close()
		n.ddb, _ = buntdb.Open(":memory:")
		return err
	}
	// all good, moving on
	return nil
}

type fsmSnapshot struct {
	path string
}

// Persist writes the snapshot to the given sink.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	file, err := os.Open(f.path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(sink, file)
	return err
}

// Release deletes the temp file
func (f *fsmSnapshot) Release() {
	os.Remove(f.path)
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	n := f.Node()

	n.mu.Lock()
	defer n.mu.Unlock()

	// create a temp file that will hold the snapshot.
	file, err := ioutil.TempFile("", "nikolai-snap-")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// save the database to the snapshot file.
	if err := n.ddb.Save(file); err != nil {
		// there was an error. delete the temp file and return error.
		os.Remove(file.Name())
		return nil, err
	}
	return &fsmSnapshot{file.Name()}, nil
}
