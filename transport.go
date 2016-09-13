package plume

import (
	"encoding/json"
	"io"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tidwall/redcon"
)

type RedconTransport struct {
	bindAddr  string
	consumer  chan raft.RPC
	doCommand func(conn redcon.Conn, args []string)
}

func NewRedconTransport(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
	doCommand func(conn redcon.Conn, args []string),
) (*RedconTransport, error) {
	t := &RedconTransport{
		bindAddr:  bindAddr,
		consumer:  make(chan raft.RPC),
		doCommand: doCommand,
	}
	server := redcon.NewServer(bindAddr, func(conn redcon.Conn, cmds [][]string) {
		for _, args := range cmds {
			t.handle(conn, args)
		}
	}, nil, nil)
	signal := make(chan error)
	go server.ListenServeAndSignal(signal)
	err := <-signal
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (t *RedconTransport) AppendEntriesPipeline(target string) (raft.AppendPipeline, error) {
	return nil, raft.ErrPipelineReplicationNotSupported
}
func (t *RedconTransport) AppendEntries(target string, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	data, err := json.Marshal(args)
	if err != nil {
		return err
	}
	val, err := req(target, buildCommand("raftappendentries", string(data)))
	if err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(val), resp); err != nil {
		return err
	}
	return nil
}

func (t *RedconTransport) handleAppendEntries(args []string) (string, error) {
	if len(args) != 2 {
		return "", ErrInvalidNumberOfArgs
	}
	var rpc raft.RPC
	rpc.Command = &raft.AppendEntriesRequest{}
	if err := json.Unmarshal([]byte(args[1]), &rpc.Command); err != nil {
		return "", err
	}
	respChan := make(chan raft.RPCResponse)
	rpc.RespChan = respChan
	t.consumer <- rpc
	resp := <-respChan
	if resp.Error != nil {
		return "", resp.Error
	}
	data, err := json.Marshal(resp.Response)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// RequestVote sends the appropriate RPC to the target node.
func (t *RedconTransport) RequestVote(target string, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	data, err := json.Marshal(args)
	if err != nil {
		return err
	}
	val, err := req(target, buildCommand("raftrequestvote", string(data)))
	if err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(val), resp); err != nil {
		return err
	}
	return nil
}

func (t *RedconTransport) handleRequestVote(args []string) (string, error) {
	if len(args) != 2 {
		return "", ErrInvalidNumberOfArgs
	}
	var rpc raft.RPC
	rpc.Command = &raft.RequestVoteRequest{}
	if err := json.Unmarshal([]byte(args[1]), &rpc.Command); err != nil {
		return "", err
	}
	respChan := make(chan raft.RPCResponse)
	rpc.RespChan = respChan
	t.consumer <- rpc
	resp := <-respChan
	if resp.Error != nil {
		return "", resp.Error
	}
	data, err := json.Marshal(resp.Response)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (t *RedconTransport) InstallSnapshot(target string, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	panic("unsupported")
	return nil
}

func (t *RedconTransport) handle(conn redcon.Conn, args []string) {
	var err error
	var res string
	switch strings.ToLower(args[0]) {
	default:
		t.doCommand(conn, args)
		return
	case "raftrequestvote":
		res, err = t.handleRequestVote(args)
	case "raftappendentries":
		res, err = t.handleAppendEntries(args)
	}
	if err != nil {
		if err == ErrInvalidNumberOfArgs {
			conn.WriteError("ERR wrong number of arguments for '" + args[0] + "' command")
		} else {
			conn.WriteError("ERR " + err.Error())
		}
	} else {
		conn.WriteBulk(res)
	}
}

func (t *RedconTransport) Consumer() <-chan raft.RPC                 { return t.consumer }
func (t *RedconTransport) LocalAddr() string                         { return t.bindAddr }
func (t *RedconTransport) EncodePeer(peer string) []byte             { return []byte(peer) }
func (t *RedconTransport) DecodePeer(peer []byte) string             { return string(peer) }
func (t *RedconTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) { println("set heartbeat handler") }
