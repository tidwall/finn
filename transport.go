package plume

import (
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

type RedconTransport struct {
	bindAddr string
	consumer chan raft.RPC
}

func NewRedconTransport(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) (*RedconTransport, error) {
	return &RedconTransport{
		bindAddr: bindAddr,
		consumer: make(chan raft.RPC),
	}, nil
}
func (t *RedconTransport) Consumer() <-chan raft.RPC {
	return t.consumer
}

// LocalAddr is used to return our local address to distinguish from our peers.
func (t *RedconTransport) LocalAddr() string {
	return t.bindAddr
}
func (t *RedconTransport) AppendEntriesPipeline(target string) (raft.AppendPipeline, error) {
	panic("unsupported")
	return nil, nil
}
func (t *RedconTransport) AppendEntries(target string, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	panic("unsupported")
	return nil
}

// RequestVote sends the appropriate RPC to the target node.
func (t *RedconTransport) RequestVote(target string, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	panic("unsupported")
	return nil
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (t *RedconTransport) InstallSnapshot(target string, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	panic("unsupported")
	return nil
}

func (t *RedconTransport) EncodePeer(peer string) []byte             { return []byte(peer) }
func (t *RedconTransport) DecodePeer(peer []byte) string             { return string(peer) }
func (t *RedconTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) { println("set heartbeat handler") }
