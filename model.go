package nikolai

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/hashicorp/raft"
	"github.com/tidwall/buntdb"
)

const (
	opGet  = 1
	opSet  = 2
	opDel  = 3
	opMSet = 4
	opMGet = 5
)

var errInvalidCommand = errors.New("invalid command")
var errInvalidConsistencyLevel = errors.New("invalid consistency level")

type command struct {
	op    byte
	multi bool
	args  []string
}

func encodeCommand(c command) []byte {
	nb := make([]byte, 8)
	var buf bytes.Buffer
	buf.WriteByte(c.op)
	for _, arg := range c.args {
		binary.LittleEndian.PutUint64(nb, uint64(len(arg)))
		buf.Write(nb)
		buf.WriteString(arg)
	}
	return buf.Bytes()
}

func decodeCommand(b []byte) (command, error) {
	var n int
	var c command
	if len(b) < 1 {
		return c, errInvalidCommand
	}
	// read op
	c.op = b[0]
	b = b[1:]
	for len(b) > 0 {
		if len(b) < 8 {
			return c, errInvalidCommand
		}
		n = int(binary.LittleEndian.Uint64(b))
		b = b[8:]
		if len(b) < n {
			return c, errInvalidCommand
		}
		c.args = append(c.args, string(b[:n]))
		b = b[n:]
	}
	return c, nil
}

func (n *Node) doDel(keys []string, multi bool) (int, error) {
	c := command{op: opDel, args: keys, multi: multi}
	f := n.raft.Apply(encodeCommand(c), raftTimeout)
	if err := f.Error(); err != nil {
		return 0, err
	}
	v := f.Response()
	switch v := v.(type) {
	case int:
		return v, nil
	case error:
		return 0, v
	}
	return 0, errors.New("invalid response")
}

func (n *Node) doSet(pairs []string, multi bool) (int, error) {
	if len(pairs)%2 == 1 {
		return 0, errInvalidCommand
	}
	c := command{op: opSet, args: pairs, multi: multi}
	f := n.raft.Apply(encodeCommand(c), raftTimeout)
	if err := f.Error(); err != nil {
		return 0, err
	}
	v := f.Response()
	switch v := v.(type) {
	case int:
		return v, nil
	case error:
		return 0, v
	}
	return 0, errors.New("invalid response")
}

func (n *Node) doGet(keys []string, multi bool, level Level) ([]*string, error) {
	if level == Low {
		return n.applyGet(keys, multi)
	} else if level == Medium {
		if n.raft.State() != raft.Leader {
			return nil, raft.ErrNotLeader
		}
		return n.applyGet(keys, multi)
	} else if level == High {
		c := command{op: opGet, args: keys, multi: multi}
		n.raft.AppliedIndex()
		f := n.raft.Apply(encodeCommand(c), raftTimeout)
		if err := f.Error(); err != nil {
			return nil, err
		}
		v := f.Response()
		switch v := v.(type) {
		case []*string:
			return v, nil
		case error:
			return nil, v
		}
		return nil, errors.New("invalid response")
	}
	return nil, errInvalidConsistencyLevel
}

func (n *Node) applyGet(keys []string, multi bool) ([]*string, error) {
	var vals []*string
	err := n.ddb.View(func(tx *buntdb.Tx) error {
		for _, key := range keys {
			v, err := tx.Get(key)
			if err == buntdb.ErrNotFound {
				vals = append(vals, nil)
			} else if err != nil {
				return err
			} else {
				vals = append(vals, &v)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return vals, nil
}

func (n *Node) applySet(pairs []string, multi bool) (int, error) {
	if len(pairs)%2 == 1 {
		return 0, errInvalidCommand
	}
	var count int
	err := n.ddb.Update(func(tx *buntdb.Tx) error {
		for i := 0; i < len(pairs); i += 2 {
			_, _, err := tx.Set(pairs[i+0], pairs[i+1], nil)
			if err != nil {
				return err
			}
			count++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (n *Node) applyDel(keys []string, multi bool) (int, error) {
	var count int
	err := n.ddb.Update(func(tx *buntdb.Tx) error {
		for _, key := range keys {
			_, err := tx.Delete(key)
			if err != buntdb.ErrNotFound {
				if err != nil {
					return err
				}
				count++
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
