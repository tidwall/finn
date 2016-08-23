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

type Options struct {
	// Logger is a custom Nikolai Logger
	Logger *Logger
}

func fillOptions(opts *Options) *Options {
	if opts == nil {
		opts = &Options{}
	}
	// copy and reassign the options
	nopts := *opts
	if nopts.Logger == nil {
		nopts.Logger = NewLogger(os.Stderr)
	}
	return &nopts
}

type Node struct {
	mu           sync.RWMutex
	clientAddr   string
	raftAddr     string
	clientServer *redcon.Server
	db           *buntdb.DB
	peers        *peerStore
	stable       *stableStore
	logs         *logStore
	snapshot     raft.SnapshotStore
	trans        raft.Transport
	raft         *raft.Raft
	log          *Logger
}

func Open(dir, addr, join string, opts *Options) (node *Node, err error) {
	opts = fillOptions(opts)

	//
	defer func() {
		if err != nil {
			opts.Logger.Logf('!', "%v", err)
		}
	}()

	// create the directory
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	db, err := buntdb.Open(filepath.Join(dir, "data.db"))
	if err != nil {
		return nil, err
	}

	// create a node and assign it some fields
	n := &Node{
		db:     db,
		peers:  &peerStore{db: db},
		stable: &stableStore{db: db},
		logs:   &logStore{db: db},
		log:    opts.Logger,
	}

	// get the peer list
	peers, err := n.peers.Peers()
	if err != nil {
		n.Close()
		return nil, err
	}

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LogOutput = n.log

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if join == "" && len(peers) <= 1 {
		n.log.Logf('*', "Enable single node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// create the snapshot store. This allows the Raft to truncate the log.
	n.snapshot, err = raft.NewFileSnapshotStore(dir, retainSnapshotCount, n.log)
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
	n.trans, err = raft.NewTCPTransport(n.raftAddr, taddr, 3, 10*time.Second, n.log)
	if err != nil {
		n.Close()
		return nil, err
	}

	// Instantiate the Raft systems.
	n.raft, err = raft.NewRaft(config, (*fsm)(n), n.logs, n.stable, n.snapshot, n.peers, n.trans)
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
	n.log.Logf('!', "Critial error: %v", err)
}
