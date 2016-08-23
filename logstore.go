package nikolai

import (
	"encoding/binary"
	"errors"
	"strconv"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/tidwall/buntdb"
)

const logPrefix = "log:"

type logStore struct {
	db *buntdb.DB
}

func (ls *logStore) FirstIndex() (uint64, error) {
	var num string
	err := ls.db.View(func(tx *buntdb.Tx) error {
		return tx.Ascend("",
			func(key, val string) bool {
				if strings.HasPrefix(key, logPrefix) {
					num = key[len(logPrefix):]
					return false
				}
				return true
			},
		)
	})
	if err != nil || num == "" {
		return 0, err
	}
	return stringToUint64(num), nil
}

func (ls *logStore) LastIndex() (uint64, error) {
	var num string
	err := ls.db.View(func(tx *buntdb.Tx) error {
		return tx.Descend("",
			func(key, val string) bool {
				if strings.HasPrefix(key, logPrefix) {
					num = key[len(logPrefix):]
					return false
				}
				return true
			},
		)
	})
	if err != nil || num == "" {
		return 0, err
	}
	return stringToUint64(num), nil
}

func (ls *logStore) GetLog(index uint64, log *raft.Log) error {
	var val string
	var verr error
	err := ls.db.View(func(tx *buntdb.Tx) error {
		val, verr = tx.Get(logPrefix + uint64ToString(index))
		return verr
	})
	if err != nil {
		if err == buntdb.ErrNotFound {
			return raft.ErrLogNotFound
		}
		return err
	}
	return decodeLog([]byte(val), log)
}

func (ls *logStore) StoreLog(log *raft.Log) error {
	return ls.StoreLogs([]*raft.Log{log})
}

func (ls *logStore) StoreLogs(logs []*raft.Log) error {
	err := ls.db.Update(func(tx *buntdb.Tx) error {
		for _, log := range logs {
			idx := uint64ToString(log.Index)
			key := make([]byte, 0, len(logPrefix)+len(idx))
			key = append(key, logPrefix...)
			key = append(key, idx...)
			val, err := encodeLog(log)
			if err != nil {
				return err
			}
			if _, _, err := tx.Set(string(key), string(val), nil); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (ls *logStore) DeleteRange(min, max uint64) error {
	return ls.db.Update(func(tx *buntdb.Tx) error {
		for i := min; i <= max; i++ {
			if _, err := tx.Delete(logPrefix + uint64ToString(i)); err != nil {
				if err != buntdb.ErrNotFound {
					return err
				}
			}
		}
		return nil
	})
}

// Decode reverses the encode operation on a byte slice input
func decodeLog(buf []byte, in *raft.Log) error {
	if len(buf) < 17 {
		return errors.New("invalid buffer")
	}
	in.Index = binary.LittleEndian.Uint64(buf[0:8])
	in.Term = binary.LittleEndian.Uint64(buf[8:16])
	in.Type = raft.LogType(buf[16])
	in.Data = buf[17:]
	return nil
}

// Encode writes an encoded object to a new bytes buffer
func encodeLog(in *raft.Log) ([]byte, error) {
	buf := make([]byte, 17+len(in.Data))
	binary.LittleEndian.PutUint64(buf[0:8], in.Index)
	binary.LittleEndian.PutUint64(buf[8:16], in.Term)
	buf[16] = byte(in.Type)
	copy(buf[17:], in.Data)
	return buf, nil
}

// Converts string to an integer
func stringToUint64(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

// Converts a uint to a string
func uint64ToString(u uint64) string {
	s := ("00000000000000000000" + strconv.FormatUint(u, 10))
	return s[len(s)-20:]
}
