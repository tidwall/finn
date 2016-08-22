package nikolai

import (
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/raft"
	"github.com/tidwall/redcon"
)

const (
	retainSnapshotCount = 2
)

type Node struct {
	clientAddr   string
	raftAddr     string
	mu           sync.Mutex
	clientServer *redcon.Server
	db           *buntdb.DB
	ps           *peerStore
	ss           *stableStore
	ls           *logStore
	snaps        raft.SnapshotStore
	trans        raft.Transport
	raft         *raft.Raft
}

func Open(dir, addr, join string) (*Node, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	db, err := buntdb.Open(filepath.Join(dir, "data.db"))
	if err != nil {
		return nil, err
	}

	// create a node and assign it some fields
	n := &Node{
		db: db,
		ps: (*peerStore)(db),
		ss: (*stableStore)(db),
		ls: (*logStore)(db),
	}

	// get the peer list
	peers, err := n.ps.Peers()
	if err != nil {
		n.Close()
		return nil, err
	}

	// Setup Raft configuration.
	config := raft.DefaultConfig()

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if join == "" && len(peers) <= 1 {
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// create the snapshot store. This allows the Raft to truncate the log.
	n.snaps, err = raft.NewFileSnapshotStore(dir, retainSnapshotCount, os.Stderr)
	if err != nil {
		n.Close()
		return nil, err
	}

	// verify the syntax of the address.
	taddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		n.Close()
		return nil, err
	}
	n.clientAddr = taddr.String()

	// derive the raftAddr from the clientAddr by adding 10000 to the port.
	taddr.Port += 10000
	n.raftAddr = taddr.String()

	// start the raft server

	n.trans, err = raft.NewTCPTransport(n.raftAddr, taddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		n.Close()
		return nil, err
	}

	// Instantiate the Raft systems.
	n.raft, err = raft.NewRaft(config, (*fsm)(n), n.ls, n.ss, n.snaps, n.ps, n.trans)
	if err != nil {
		n.Close()
		return nil, err
	}

	// create new redcon server and bind the handlers.
	csrv := redcon.NewServer(n.clientAddr,
		func(conn redcon.Conn, cmds [][]string) {

		}, func(conn redcon.Conn) bool {
			return true
		}, func(conn redcon.Conn, err error) {

		},
	)

	// start the redcon server
	signal := make(chan error)
	go func() {
		var err error
		defer func() {
			csrv.Close()
			if err != nil {
				n.signalCritical(err)
			}
		}()
		err = csrv.ListenServeAndSignal(signal)
	}()
	err = <-signal
	if err != nil {
		n.Close()
		return nil, err
	}
	n.clientServer = csrv

	return n, nil
}

func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.clientServer != nil {
		n.clientServer.Close()
	}

	if n.db != nil {
		n.db.Close()
	}
	return nil
}

func (n *Node) signalCritical(err error) {
	println("critical " + err.Error())
}
