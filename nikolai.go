package nikolai

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/redcon"
)

type Level int

func (l Level) String() string {
	switch l {
	default:
		return "unknown"
	case Low:
		return "low"
	case Medium:
		return "medium"
	case High:
		return "high"
	}
}

const (
	Low    Level = -1
	Medium Level = 0
	High   Level = 1
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

// Options are used to provide a Node with optional funtionality.
type Options struct {
	// Logger is a custom Logger
	Logger *Logger
	// Consistency is the raft consistency level for reads.
	// Default is Medium
	Consistency Level
	// Durability is the fsync durability for disk writes.
	// Default is Medium
	Durability Level
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

// Node represents a Raft server node.
type Node struct {
	mu           sync.RWMutex
	clientAddr   string
	raftAddr     string
	clientServer *redcon.Server
	rdb          *buntdb.DB
	ddb          *buntdb.DB
	peers        *peerStore
	stable       *stableStore
	logs         *logStore
	snapshot     raft.SnapshotStore
	trans        raft.Transport
	raft         *raft.Raft
	log          *Logger
	closed       bool
	opts         *Options
	clevel       Level
}

// Open opens a Raft node and returns the Node to the caller.
func Open(dir, addr, join string, opts *Options) (node *Node, err error) {
	opts = fillOptions(opts)

	opts.Logger.Debugf('N', "Consistency: %s, Durability: %s", opts.Consistency, opts.Durability)

	// if this function fails then write the error to the logger
	defer func() {
		if err != nil {
			opts.Logger.Warningf('N', "%v", err)
		}
	}()

	// create the directory
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	// open the database. this is used for most everything
	ddb, err := buntdb.Open(":memory:")
	if err != nil {
		return nil, err
	}

	// open the database. this is used for most everything
	rdb, err := buntdb.Open(filepath.Join(dir, "raft.db"))
	if err != nil {
		return nil, err
	}

	// set the database configuration
	var cfg buntdb.Config
	if err := rdb.ReadConfig(&cfg); err != nil {
		return nil, err
	}
	switch opts.Durability {
	case Medium:
		cfg.SyncPolicy = buntdb.EverySecond
	case Low:
		cfg.SyncPolicy = buntdb.Never
	case High:
		cfg.SyncPolicy = buntdb.Always
	}
	if err := rdb.SetConfig(cfg); err != nil {
		return nil, err
	}
	// create a node and assign it some fields
	n := &Node{
		rdb:    rdb,
		ddb:    ddb,
		peers:  &peerStore{db: rdb},
		stable: &stableStore{db: rdb},
		logs:   &logStore{db: rdb},
		log:    opts.Logger,
		opts:   opts,
		clevel: opts.Consistency,
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
		n.log.Noticef('N', "Enable single node")
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
	n.trans, err = raft.NewTCPTransport(n.raftAddr, taddr, 3, raftTimeout, n.log)
	if err != nil {
		n.Close()
		return nil, err
	}

	// Instantiate the Raft systems.
	n.raft, err = raft.NewRaft(config, (*fsm)(n),
		n.logs, n.stable, n.snapshot, n.peers, n.trans)
	if err != nil {
		n.Close()
		return nil, err
	}

	var csrv = redcon.NewServer(n.clientAddr,
		func(conn redcon.Conn, cmds [][]string) {
			for _, args := range cmds {
				n.handleRedcon(conn, args)
			}
		}, nil, nil)

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

	// If join was specified, make the join request.
	if join != "" && len(peers) == 0 {
		if err := tryJoin(join, n.raftAddr); err != nil {
			return nil, fmt.Errorf("failed to join node at %v: %v", join, err)
		}
	}
	return n, nil
}

// Close closes the node
func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	// shutdown the clientServer first.
	// we do not want to accept any more connections.
	if n.clientServer != nil {
		n.clientServer.Close()
	}
	// shutdown the raft, but do not handle the future error.
	if n.raft != nil {
		n.raft.Shutdown()
	}
	// close the data database
	if n.rdb != nil {
		n.rdb.Close()
	}
	// close the raft database
	if n.rdb != nil {
		n.rdb.Close()
	}
	n.closed = true
	return nil
}

func (n *Node) signalCritical(err error) {
	n.log.Warningf('N', "Critial error: %v", err)
}
func writeWrongArgs(conn redcon.Conn, cmd string) {
	conn.WriteError("ERR wrong number of arguments for '" + cmd + "' command")
}

func (n *Node) leader() string {
	addr, err := net.ResolveTCPAddr("tcp", n.raft.Leader())
	if err != nil {
		return ""
	}
	addr.Port -= 10000
	return addr.String()
}

func (n *Node) handleRedcon(conn redcon.Conn, args []string) {
	switch strings.ToLower(args[0]) {
	default:
		// serialize this command and pass it back to the caller
		conn.WriteError("ERR unknown command '" + args[0] + "'")
	case "raft.join":
		switch len(args) {
		default:
			writeWrongArgs(conn, args[0])
		case 2:
			n.log.Noticef('N', "Received join request from %v", args[1])
			f := n.raft.AddPeer(args[1])
			if f.Error() != nil {
				conn.WriteError(f.Error().Error())
				return
			}
			n.log.Noticef('N', "Node %v joined successfully", args[1])
			conn.WriteString("OK")
		}
	case "raft.leader":
		switch len(args) {
		default:
			writeWrongArgs(conn, args[0])
		case 1:
			conn.WriteString(n.leader())
		}
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "ping":
		switch len(args) {
		default:
			writeWrongArgs(conn, args[0])
		case 1:
			conn.WriteString("PONG")
		case 2:
			conn.WriteString(args[1])
		}
	case "get":
		switch len(args) {
		default:
			writeWrongArgs(conn, args[0])
		case 2:
			level := n.clevel
			// TODO: read the level param
			val, err := n.doGet(args[1], level)
			if err != nil {
				if err == buntdb.ErrNotFound {
					conn.WriteNull()
				} else if err == raft.ErrNotLeader {
					conn.WriteError(n.notLeaderMessage())
				} else {
					conn.WriteError(err.Error())
				}
				return
			}
			conn.WriteBulk(val)
		}
	case "set":
		switch len(args) {
		default:
			writeWrongArgs(conn, args[0])
		case 3:
			if err := n.doSet(args[1], args[2]); err != nil {
				if err == raft.ErrNotLeader {
					conn.WriteError(n.notLeaderMessage())
				} else {
					conn.WriteError(err.Error())
				}
				return
			}
			conn.WriteString("OK")
		}
	case "del":
		switch len(args) {
		default:
			writeWrongArgs(conn, args[0])
		case 2:
			if err := n.doDelete(args[1]); err != nil {
				if err == buntdb.ErrNotFound {
					conn.WriteInt(0)
				} else if err == raft.ErrNotLeader {
					conn.WriteError(n.notLeaderMessage())
				} else {
					conn.WriteError(err.Error())
				}
				return
			}
			conn.WriteInt(1)
		}
	}
}
func (n *Node) notLeaderMessage() string {
	return fmt.Sprintf("%v: try %v", raft.ErrNotLeader, n.leader())
}
func buildCommand(args ...string) string {
	var b = make([]byte, 0, 32)
	b = append(b, '*')
	b = append(b, []byte(strconv.FormatInt(int64(len(args)), 10))...)
	b = append(b, '\r', '\n')
	for _, arg := range args {
		b = append(b, '$')
		b = append(b, []byte(strconv.FormatInt(int64(len(arg)), 10))...)
		b = append(b, '\r', '\n')
		b = append(b, []byte(arg)...)
		b = append(b, '\r', '\n')
	}
	return string(b)
}
func do(addr string, cmd string) (string, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_, err = io.WriteString(conn, cmd)
	if err != nil {
		return "", err
	}
	bytes := make([]byte, 32)
	n, err := conn.Read(bytes)
	if err != nil {
		return "", err
	}
	resp := string(bytes[:n])
	if strings.HasPrefix(resp, "-") {
		return "", errors.New(strings.TrimSpace(resp[1:]))
	}
	if strings.HasPrefix(resp, "+") {
		return strings.TrimSpace(resp[1:]), nil
	}
	return "", errors.New("invalid response")
}

func tryJoin(join, raftAddr string) error {
	resp, err := do(join, buildCommand("raft.join", raftAddr))
	if err != nil {
		return err
	}
	if resp != "OK" {
		return errors.New("invalid response")
	}
	return nil
}
func getLeader(addr string) (string, error) {
	return do(addr, buildCommand("raft.leader"))
}
