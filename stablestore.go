package nikolai

import (
	"strconv"

	"github.com/tidwall/buntdb"
)

type stableStore buntdb.DB

func (ss *stableStore) DB() *buntdb.DB {
	return (*buntdb.DB)(ss)
}

func (ss *stableStore) Set(key []byte, val []byte) error {
	return ss.DB().Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set("r:stable:"+string(key), string(val), nil)
		return err
	})
}

func (ss *stableStore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := ss.DB().View(func(tx *buntdb.Tx) error {
		sval, err := tx.Get("r:stable:" + string(key))
		if err != nil {
			return err
		}
		val = []byte(sval)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (ss *stableStore) SetUint64(key []byte, val uint64) error {
	return ss.Set(key, []byte(strconv.FormatUint(val, 10)))
}

func (ss *stableStore) GetUint64(key []byte) (uint64, error) {
	val, err := ss.Get(key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(val), 10, 64)
}
