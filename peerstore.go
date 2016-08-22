package nikolai

import (
	"encoding/json"

	"github.com/tidwall/buntdb"
)

type peerStore buntdb.DB

func (ps *peerStore) DB() *buntdb.DB {
	return (*buntdb.DB)(ps)
}

func (ps *peerStore) Peers() ([]string, error) {
	var peers []string
	err := ps.DB().View(func(tx *buntdb.Tx) error {
		val, err := tx.Get("r:peers")
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}
		return json.Unmarshal([]byte(val), &peers)
	})
	if err != nil {
		return nil, err
	}
	return peers, nil
}

func (ps *peerStore) SetPeers(peers []string) error {
	data, err := json.Marshal(peers)
	if err != nil {
		return err
	}
	return ps.DB().Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set("r:peers", string(data), nil)
		return err
	})
}
