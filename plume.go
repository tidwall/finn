package plume

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tidwall/raft-boltdb"
	"github.com/tidwall/raft-buntdb"
	"github.com/tidwall/redcon"
	"github.com/tidwall/redlog"
)

var (
	ErrUnknownCommand          = errors.New("unknown command")
	ErrInvalidNumberOfArgs     = errors.New("invalid number or arguments")
	ErrInvalidCommand          = errors.New("invalid command")
	ErrInvalidConsistencyLevel = errors.New("invalid consistency level")
	ErrSyntaxError             = errors.New("syntax error")
	ErrInvalidResponse         = errors.New("invalid response")
	ErrDisabled                = errors.New("disabled")
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

// Level is for defining the raft consistency level.
type Level int

// String returns a string representation of Level.
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
	// Low is "low" consistency. All readonly commands will can processed by
	// any node. Very fast but may have stale reads.
	Low Level = -1
	// Medium is "medium" consistency. All readonly commands can only be
	// processed by the leader. The command is not processed through the
	// raft log, therefore a very small (microseconds) chance for a stale
	// read is possible when a leader change occurs. Fast but only the leader
	// handles all reads and writes.
	Medium Level = 0
	// High is "high" consistency. All commands go through the raft log.
	// Not as fast because all commands must pass through the raft log.
	High Level = 1
)

// Backend is a raft log database type.
type Backend int

const (
	// Bunt is for using BuntDB for the raft log.
	Bunt Backend = 0
	// Bolt is for using BoltDB for the raft log.
	Bolt Backend = 1
)

// String returns a string representation of the Backend
func (b Backend) String() string {
	switch b {
	default:
		return "unknown"
	case Bunt:
		return "bunt"
	case Bolt:
		return "bolt"
	}
}

// LogLevel is used to define the verbosity of the log outputs
type LogLevel int

const (
	// Debug prints everything
	Debug LogLevel = -2
	// Verbose prints extra detail
	Verbose LogLevel = -1
	// Notice is the standard level
	Notice LogLevel = 0
	// Warning only prints warnings
	Warning LogLevel = 1
)

// Options are used to provide a Node with optional funtionality.
type Options struct {
	// Consistency is the raft consistency level for reads.
	// Default is Medium
	Consistency Level
	// Durability is the fsync durability for disk writes.
	// Default is Medium
	Durability Level
	// Backend is the database backend.
	// Default is Bunt
	Backend Backend
	// LogLevel is the log verbosity
	// Default is Notice
	LogLevel LogLevel
	// LogOutput is the log writer
	// Default is os.Stderr
	LogOutput io.Writer
}

// fillOptions fills in default options
func fillOptions(opts *Options) *Options {
	if opts == nil {
		opts = &Options{}
	}
	// copy and reassign the options
	nopts := *opts
	if nopts.LogOutput == nil {
		nopts.LogOutput = os.Stderr
	}
	return &nopts
}

// Logger is a logger
type Logger interface {
	// Printf write notice messages
	Printf(format string, args ...interface{})
	// Verbosef writes verbose messages
	Verbosef(format string, args ...interface{})
	// Noticef writes notice messages
	Noticef(format string, args ...interface{})
	// Warningf write warning messages
	Warningf(format string, args ...interface{})
	// Debugf writes debug messages
	Debugf(format string, args ...interface{})
}

// Applier is used to apply raft commands.
type Applier interface {
	// Apply applies a command
	Apply(conn redcon.Conn, args []string,
		mutate func() (interface{}, error),
		respond func(interface{}) (interface{}, error),
	) (interface{}, error)
	Log() Logger
}

// Machine handles raft commands and raft snapshotting.
type Machine interface {
	// Command is called by the Node for incoming commands.
	Command(a Applier, conn redcon.Conn, args []string) (interface{}, error)
	// Restore is used to restore data from a snapshot.
	Restore(rd io.Reader) error
	// Snapshot is used to support log compaction. This call should write a
	// snapshot to the provided writer.
	Snapshot(wr io.Writer) error
}

// Node represents a Raft server node.
type Node struct {
	mu           sync.RWMutex
	clientAddr   string
	raftAddr     string
	clientServer *redcon.Server
	snapshot     raft.SnapshotStore
	trans        raft.Transport
	raft         *raft.Raft
	log          *redlog.Logger // the node logger
	mlog         *redlog.Logger // the machine logger
	closed       bool
	opts         *Options
	level        Level
	handler      Machine
	buntstore    *raftbuntdb.BuntStore
	boltstore    *raftboltdb.BoltStore
	store        bigStore
}

// bigStore represents a raft store that conforms to
// raft.PeerStore, raft.LogStore, and raft.StableStore.
type bigStore interface {
	Close() error
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	GetLog(idx uint64, log *raft.Log) error
	StoreLog(log *raft.Log) error
	StoreLogs(logs []*raft.Log) error
	DeleteRange(min, max uint64) error
	Set(k, v []byte) error
	Get(k []byte) ([]byte, error)
	SetUint64(key []byte, val uint64) error
	GetUint64(key []byte) (uint64, error)
	Peers() ([]string, error)
	SetPeers(peers []string) error
}

// Open opens a Raft node and returns the Node to the caller.
func Open(dir, addr, join string, handler Machine, opts *Options) (node *Node, err error) {
	opts = fillOptions(opts)
	log := redlog.New(opts.LogOutput).Sub('N')
	log.SetFilter(redlog.HashicorpRaftFilter)

	// if this function fails then write the error to the logger
	defer func() {
		if err != nil {
			log.Warningf("%v", err)
		}
	}()

	// create the directory
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	var store bigStore
	if opts.Backend == Bolt {
		opts.Durability = High
		store, err = raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
		if err != nil {
			return nil, err
		}
	} else {
		opts.Backend = Bunt
		var dur raftbuntdb.Level
		switch opts.Durability {
		default:
			dur = raftbuntdb.Medium
			opts.Durability = Medium
		case High:
			dur = raftbuntdb.High
		case Low:
			dur = raftbuntdb.Low
		}
		store, err = raftbuntdb.NewBuntStore(filepath.Join(dir, "raft.db"), dur)
		if err != nil {
			return nil, err
		}
	}
	// create a node and assign it some fields
	n := &Node{
		store:   store,
		log:     log,
		mlog:    log.Sub('C'),
		opts:    opts,
		level:   opts.Consistency,
		handler: handler,
	}

	n.log.Debugf("Consistency: %s, Durability: %s, Backend: %s", opts.Consistency, opts.Durability, opts.Backend)

	// get the peer list
	peers, err := n.store.Peers()
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
		n.log.Noticef("Enable single node")
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

	n.trans, err = NewRedconTransport(n.raftAddr, taddr, 3, raftTimeout, n.log)
	if err != nil {
		n.Close()
		return nil, err
	}

	// start the raft server
	if true {
		n.trans, err = raft.NewTCPTransport(n.raftAddr, taddr, 3, raftTimeout, n.log)
		if err != nil {
			n.Close()
			return nil, err
		}
	}
	// Instantiate the Raft systems.
	n.raft, err = raft.NewRaft(config, (*nodeFSM)(n),
		n.store, n.store, n.snapshot, n.store, n.trans)
	if err != nil {
		n.Close()
		return nil, err
	}
	var csrv *redcon.Server
	csrv = redcon.NewServer(n.clientAddr,
		func(conn redcon.Conn, cmds [][]string) {
			for _, args := range cmds {
				n.doCommand(conn, args)
			}
		}, nil, nil)

	// start the redcon server
	signal := make(chan error)
	go func() {
		var err error
		defer func() {
			csrv.Close()
			if err != nil {
				n.log.Warningf("Critial error: %v", err)
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
		if err := reqRaftJoin(join, n.raftAddr); err != nil {
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
	// shutdown the raft, but do not handle the future error. :PPA:
	if n.raft != nil {
		n.raft.Shutdown()
	}
	// close the raft database
	if n.store != nil {
		n.store.Close()
	}
	n.closed = true
	return nil
}

// leader returns the client address for the leader
func (n *Node) leader() string {
	addr, err := net.ResolveTCPAddr("tcp", n.raft.Leader())
	if err != nil {
		return ""
	}
	if addr.Port == 0 {
		return ""
	}
	addr.Port -= 10000
	return addr.String()
}

// buildCommand builds a valid redis command.
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

// req makes a very simple remote request with the specified commands.
// The command should be a valid redis array. The response is a plain
// string or an error.
func req(addr string, cmd string) (string, error) {
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

// reqRaftJoin does a remote "RAFTJOIN" command at the specified address.
func reqRaftJoin(join, raftAddr string) error {
	resp, err := req(join, buildCommand("raftjoin", raftAddr))
	if err != nil {
		return err
	}
	if resp != "OK" {
		return errors.New("invalid response")
	}
	return nil
}

// doCommand executes a client command, which will process through
// the raft log pipeline.
func (n *Node) doCommand(conn redcon.Conn, args []string) (interface{}, error) {
	if len(args) == 0 {
		return nil, nil
	}
	var val interface{}
	var err error
	switch strings.ToLower(args[0]) {
	default:
		val, err = n.handler.Command((*nodeApplier)(n), conn, args)
		if err == ErrDisabled {
			err = ErrUnknownCommand
		}
	case "raftjoin":
		val, err = n.doRaftJoin(conn, args)
	case "raftleader":
		val, err = n.doRaftLeader(conn, args)
	case "raftsnapshot":
		val, err = n.doRaftSnapshot(conn, args)
	case "raftstate":
		val, err = n.doRaftState(conn, args)
	case "raftstats":
		val, err = n.doRaftStats(conn, args)
	case "quit":
		val, err = n.doQuit(conn, args)
	case "ping":
		val, err = n.doPing(conn, args)
	}
	if conn != nil {
		if err != nil {
			if err == ErrUnknownCommand {
				conn.WriteError("ERR unknown command '" + args[0] + "'")
			} else if err == ErrInvalidNumberOfArgs {
				conn.WriteError("ERR wrong number of arguments for '" + args[0] + "' command")
			} else if err == raft.ErrNotLeader {
				conn.WriteError(fmt.Sprintf("%v, try %v", raft.ErrNotLeader, n.leader()))
			} else {
				conn.WriteError("ERR " + strings.TrimSpace(strings.Split(err.Error(), "\n")[0]))
			}
		}
	}
	return val, err
}

// doPing handles a "PING" client command.
func (n *Node) doPing(conn redcon.Conn, args []string) (interface{}, error) {
	switch len(args) {
	default:
		return nil, ErrInvalidNumberOfArgs
	case 1:
		conn.WriteString("PONG")
	case 2:
		conn.WriteBulk(args[1])
	}
	return nil, nil
}

// doRaftLeader handles a "RAFTLEADER" client command.
func (n *Node) doRaftLeader(conn redcon.Conn, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, ErrInvalidNumberOfArgs
	}
	conn.WriteString(n.leader())
	return nil, nil
}

// doRaftSnapshot handles a "RAFTSNAPSHOT" client command.
func (n *Node) doRaftSnapshot(conn redcon.Conn, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, ErrInvalidNumberOfArgs
	}
	f := n.raft.Snapshot()
	err := f.Error()
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return nil, nil
	}
	conn.WriteString("OK")
	return nil, nil
}

// doRaftState handles a "RAFTSTATE" client command.
func (n *Node) doRaftState(conn redcon.Conn, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, ErrInvalidNumberOfArgs
	}
	conn.WriteBulk(n.raft.State().String())
	return nil, nil
}

// doRaftStatus handles a "RAFTSTATUS" client command.
func (n *Node) doRaftStats(conn redcon.Conn, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, ErrInvalidNumberOfArgs
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	stats := n.raft.Stats()
	keys := make([]string, 0, len(stats))
	for key := range stats {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	conn.WriteArray(len(keys) * 2)
	for _, key := range keys {
		conn.WriteBulk(key)
		conn.WriteBulk(stats[key])
	}
	return nil, nil
}

// doQuit handles a "QUIT" client command.
func (n *Node) doQuit(conn redcon.Conn, args []string) (interface{}, error) {
	conn.WriteString("OK")
	conn.Close()
	return nil, nil
}

// doRaftJoin handles a "RAFTJOIN address" client command.
func (n *Node) doRaftJoin(conn redcon.Conn, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, ErrInvalidNumberOfArgs
	}
	n.log.Noticef("Received join request from %v", args[0])
	f := n.raft.AddPeer(args[1])
	if f.Error() != nil {
		return nil, f.Error()
	}
	n.log.Noticef("Node %v joined successfully", args[0])
	conn.WriteString("OK")
	return nil, nil
}

// encodeArgs converts a []string into []byte
func encodeArgs(args []string) []byte {
	var count int
	for _, arg := range args {
		count += len(arg) + 8
	}
	res := make([]byte, 0, count)
	num := make([]byte, 8)
	for _, arg := range args {
		binary.LittleEndian.PutUint64(num, uint64(len(arg)))
		res = append(res, num...)
		res = append(res, []byte(arg)...)
	}
	return res
}

/// decodeArgs converts a []byte into []string
func decodeArgs(data []byte) ([]string, error) {
	var args []string
	for len(data) > 0 {
		if len(data) < 8 {
			return nil, ErrInvalidCommand
		}
		num := int(binary.LittleEndian.Uint64(data))
		data = data[8:]
		if len(data) < num {
			return nil, ErrInvalidCommand
		}
		args = append(args, string(data[:num]))
		data = data[num:]
	}
	return args, nil
}

// raftApplyCommand encodes a series of args into a raft command and
// applies it to the index.
func (n *Node) raftApplyCommand(args []string) (interface{}, error) {
	f := n.raft.Apply(encodeArgs(args), raftTimeout)
	if err := f.Error(); err != nil {
		return nil, err
	}
	// we check for the response to be an error and return it as such.
	switch v := f.Response().(type) {
	default:
		return v, nil
	case error:
		return nil, v
	}
}

// raftLevelGuard is used to process readonly commands depending on the
// consistency readonly level.
// It either:
// - low consistency: just processes the command without concern about
//   leadership or cluster state.
// - medium consistency: makes sure that the node is the leader first.
// - high consistency: sends a blank command through the raft pipeline to
// ensure that the node is thel leader, the raft index is incremented, and
// that the cluster is sane before processing the readonly command.
func (n *Node) raftLevelGuard() error {
	switch n.level {
	default:
		// a valid level is required
		return ErrInvalidConsistencyLevel
	case Low:
		// anything goes.
		return nil
	case Medium:
		// must be the leader
		if n.raft.State() != raft.Leader {
			return raft.ErrNotLeader
		}
		return nil
	case High:
		// process a blank command. this will update the raft log index
		// and allow for readonly commands to process in order without
		// serializing the actual command.
		f := n.raft.Apply(nil, raftTimeout)
		if err := f.Error(); err != nil {
			return err
		}
		// the blank command succeeded.
		v := f.Response()
		// check if response was an error and return that.
		switch v := v.(type) {
		case nil:
			return nil
		case error:
			return v
		}
		return ErrInvalidResponse
	}
}

// nodeApplier exposes the Applier interface of the Node type
type nodeApplier Node

// Apply executes a command through raft.
// The mutate param should be set to nil for readonly commands.
// The repsond param is required and any response to conn happens here.
// The return value from mutate will be passed into the respond param.
func (m *nodeApplier) Apply(
	conn redcon.Conn,
	args []string,
	mutate func() (interface{}, error),
	respond func(interface{}) (interface{}, error),
) (interface{}, error) {
	var val interface{}
	var err error
	if mutate == nil {
		// no apply, just do a level guard.
		if err := (*Node)(m).raftLevelGuard(); err != nil {
			return nil, err
		}
	} else if conn == nil {
		// this is happening on a follower node.
		return mutate()
	} else {
		// this is happening on the leader node.
		// apply the command to the raft log.
		val, err = (*Node)(m).raftApplyCommand(args)
	}
	if err != nil {
		return nil, err
	}
	// responde
	return respond(val)
}

// Log returns the active logger for printing messages
func (m *nodeApplier) Log() Logger {
	return (*Node)(m).mlog
}

// nodeFSM exposes the raft.FSM interface of the Node type
type nodeFSM Node

// Apply applies a Raft log entry to the key-value store.
func (m *nodeFSM) Apply(l *raft.Log) interface{} {
	args, err := decodeArgs(l.Data)
	if err != nil {
		return err
	}
	val, err := (*Node)(m).doCommand(nil, args)
	if err != nil {
		return err
	}
	return val
}

// Restore stores the key-value store to a previous state.
func (m *nodeFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return (*Node)(m).handler.Restore(rc)
}

// Persist writes the snapshot to the given sink.
func (m *nodeFSM) Persist(sink raft.SnapshotSink) error {
	if err := (*Node)(m).handler.Snapshot(sink); err != nil {
		sink.Cancel()
		return err
	}
	sink.Close()
	return nil
}

// Release deletes the temp file
func (m *nodeFSM) Release() {}

// Snapshot returns a snapshot of the key-value store.
func (m *nodeFSM) Snapshot() (raft.FSMSnapshot, error) {
	return m, nil
}
