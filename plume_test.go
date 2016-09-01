package plume

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
)

type KeyValueMachine struct {
	mu   sync.RWMutex
	keys map[string]string
}

func NewKeyValueMachine() *KeyValueMachine {
	return &KeyValueMachine{
		keys: make(map[string]string),
	}
}
func (kvm *KeyValueMachine) Command(m Applier, conn redcon.Conn, args []string) (interface{}, error) {
	switch strings.ToLower(args[0]) {
	default:
		return nil, ErrUnknownCommand
	case "set":
		if len(args) != 3 {
			return nil, ErrInvalidNumberOfArgs
		}
		return m.Apply(conn, args,
			func() (interface{}, error) {
				kvm.mu.Lock()
				defer kvm.mu.Unlock()
				kvm.keys[args[1]] = args[2]
				return nil, nil
			},
			func(v interface{}) (interface{}, error) {
				conn.WriteString("OK")
				return nil, nil
			},
		)
	case "get":
		if len(args) != 2 {
			return nil, ErrInvalidNumberOfArgs
		}
		return m.Apply(conn, args,
			nil,
			func(interface{}) (interface{}, error) {
				kvm.mu.RLock()
				defer kvm.mu.RUnlock()
				if val, ok := kvm.keys[args[1]]; !ok {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
				return nil, nil
			},
		)
	case "del":
		if len(args) < 2 {
			return nil, ErrInvalidNumberOfArgs
		}
		return m.Apply(conn, args,
			func() (interface{}, error) {
				kvm.mu.Lock()
				defer kvm.mu.Unlock()
				var n int
				for i := 1; i < len(args); i++ {
					key := args[i]
					if _, ok := kvm.keys[key]; ok {
						delete(kvm.keys, key)
						n++
					}
				}
				return n, nil
			},
			func(v interface{}) (interface{}, error) {
				n := v.(int)
				conn.WriteInt(n)
				return nil, nil
			},
		)
	case "keys":
		if len(args) != 2 {
			return nil, ErrInvalidNumberOfArgs
		}
		return m.Apply(conn, args,
			nil,
			func(v interface{}) (interface{}, error) {
				kvm.mu.RLock()
				defer kvm.mu.RUnlock()
				var keys []string
				for key := range kvm.keys {
					if match.Match(key, args[1]) {
						keys = append(keys, key)
					}
				}
				conn.WriteArray(len(keys))
				for _, key := range keys {
					conn.WriteBulk(key)
				}
				return nil, nil
			},
		)
	}
}

func (kvm *KeyValueMachine) Restore(rd io.Reader) error {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	data, err := ioutil.ReadAll(rd)
	if err != nil {
		return err
	}
	var keys map[string]string
	if err := json.Unmarshal(data, &keys); err != nil {
		return err
	}
	kvm.keys = keys
	return nil
}

func (kvm *KeyValueMachine) Snapshot(wr io.Writer) error {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	data, err := json.Marshal(kvm.keys)
	if err != nil {
		return err
	}
	if _, err := wr.Write(data); err != nil {
		return err
	}
	return nil
}

func TestServer(t *testing.T) {
	//os.RemoveAll("data")
	opts := Options{
		Consistency: Low,
	}
	node := os.Getenv("NODE")
	join := ""
	if node == "" {
		node = "0"
	}
	if node != "0" {
		join = ":7840"
	}
	n, err := Open("data"+node, ":784"+node, join, NewKeyValueMachine(), &opts)
	if err != nil {
		t.Fatal(err)
	}
	//defer os.RemoveAll("data")
	defer n.Close()
	select {}
}
