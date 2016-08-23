package nikolai

import (
	"encoding/json"

	"github.com/tidwall/buntdb"
)

type peerStore struct {
	db *buntdb.DB
}

func (ps *peerStore) Peers() ([]string, error) {
	var peers []string
	err := ps.db.View(func(tx *buntdb.Tx) error {
		val, err := tx.Get("r:peers")
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}
		if len(val) == 0 {
			return nil
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
	return ps.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set("r:peers", string(data), nil)
		return err
	})
}
