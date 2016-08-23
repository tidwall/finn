package nikolai

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/hashicorp/raft"
	"github.com/tidwall/buntdb"
)

const (
	opGet = 1
	opSet = 2
	opDel = 3
)

var errInvalidCommand = errors.New("invalid command")
var errInvalidConsistencyLevel = errors.New("invalid consistency level")

type command struct {
	op         byte
	key, value string
}

func encodeCommand(c command) []byte {
	nb := make([]byte, 8)
	var buf bytes.Buffer
	buf.WriteByte(c.op)
	binary.LittleEndian.PutUint64(nb, uint64(len(c.key)))
	buf.Write(nb)
	buf.WriteString(c.key)
	if c.op == opSet {
		binary.LittleEndian.PutUint64(nb, uint64(len(c.value)))
		buf.Write(nb)
		buf.WriteString(c.value)
	}
	return buf.Bytes()
}

func decodeCommand(b []byte) (command, error) {
	var n uint64
	var c command
	if len(b) < 9 {
		return c, errInvalidCommand
	}
	// read op
	c.op = b[0]
	// read key
	n = binary.LittleEndian.Uint64(b[1:])
	if len(b) < 9+int(n) {
		return c, errInvalidCommand
	}
	c.key = string(b[9 : 9+int(n)])
	switch b[0] {
	default:
		return c, errInvalidCommand
	case opGet, opDel:
		// nothing to do
	case opSet:
		// read value
		if len(b) < 9+len(c.key)+8 {
			return c, errInvalidCommand
		}
		n = binary.LittleEndian.Uint64(b[9+len(c.key):])
		c.value = string(b[9+len(c.key)+8 : 9+len(c.key)+8+int(n)])
	}
	return c, nil
}

func (n *Node) doDelete(key string) error {
	c := command{op: opDel, key: key}
	f := n.raft.Apply(encodeCommand(c), raftTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	if err, ok := f.Response().(error); ok {
		return err
	}
	return nil
}

func (n *Node) doSet(key, value string) error {
	c := command{op: opSet, key: key, value: value}
	f := n.raft.Apply(encodeCommand(c), raftTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	if err, ok := f.Response().(error); ok {
		return err
	}
	return nil
}

func (n *Node) doGet(key string, level Level) (string, error) {
	if level == Low {
		return n.applyGet(key)
	} else if level == Medium {
		if n.raft.State() != raft.Leader {
			return "", raft.ErrNotLeader
		}
		return n.applyGet(key)
	} else if level == High {
		c := command{op: opGet, key: key}
		n.raft.AppliedIndex()
		f := n.raft.Apply(encodeCommand(c), raftTimeout)
		if err := f.Error(); err != nil {
			return "", err
		}
		v := f.Response()
		switch v := v.(type) {
		case string:
			return v, nil
		case error:
			return "", v
		}
		return "", errors.New("invalid response")
	}
	return "", errInvalidConsistencyLevel
}

func (n *Node) applyGet(key string) (string, error) {
	var value string
	err := n.ddb.View(func(tx *buntdb.Tx) error {
		v, err := tx.Get(key)
		if err != nil {
			return err
		}
		value = v
		return nil
	})
	if err != nil {
		return "", err
	}
	return value, nil
}

func (n *Node) applySet(key, value string) error {
	return n.ddb.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, nil)
		return err
	})
}

func (n *Node) applyDelete(key string) error {
	return n.ddb.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(key)
		return err
	})
}
