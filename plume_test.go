package plume

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	raftredcon "github.com/tidwall/raft-redcon"
	"github.com/tidwall/redcon"
)

type KVM struct {
	mu   sync.RWMutex
	keys map[string][]byte
}

func NewKVM() *KVM {
	return &KVM{
		keys: make(map[string][]byte),
	}
}
func (kvm *KVM) Command(m Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		return nil, ErrUnknownCommand
	case "set":
		if len(cmd.Args) != 3 {
			return nil, ErrWrongNumberOfArguments
		}
		return m.Apply(conn, cmd,
			func() (interface{}, error) {
				kvm.mu.Lock()
				defer kvm.mu.Unlock()
				kvm.keys[string(cmd.Args[1])] = cmd.Args[2]
				return nil, nil
			},
			func(v interface{}) (interface{}, error) {
				conn.WriteString("OK")
				return nil, nil
			},
		)
	case "get":
		if len(cmd.Args) != 2 {
			return nil, ErrWrongNumberOfArguments
		}
		return m.Apply(conn, cmd,
			nil,
			func(interface{}) (interface{}, error) {
				kvm.mu.RLock()
				defer kvm.mu.RUnlock()
				if val, ok := kvm.keys[string(cmd.Args[1])]; !ok {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
				return nil, nil
			},
		)
	}
}

func (kvm *KVM) Restore(rd io.Reader) error {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	data, err := ioutil.ReadAll(rd)
	if err != nil {
		return err
	}
	var keys map[string][]byte
	if err := json.Unmarshal(data, &keys); err != nil {
		return err
	}
	kvm.keys = keys
	return nil
}

func (kvm *KVM) Snapshot(wr io.Writer) error {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	data, err := json.Marshal(kvm.keys)
	if err != nil {
		return err
	}
	if _, err := wr.Write(data); err != nil {
		return err
	}
	return nil
}
func startTestNode(t testing.TB, num int, logger bool) {
	node := fmt.Sprintf("%d", num)
	if err := os.MkdirAll("data/"+node, 0700); err != nil {
		t.Fatal(err)
	}
	var opts Options
	if !logger {
		opts.LogOutput = ioutil.Discard
	} else {
		opts.LogLevel = Debug
	}
	join := ""
	if node == "" {
		node = "0"
	}
	if node != "0" {
		join = ":7480"
	}
	n, err := Open("data/"+node, ":748"+node, join, NewKVM(), &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	select {}
}
func waitFor(t testing.TB, num int) {
	start := time.Now()
	for {
		if time.Now().Sub(start) > time.Second*10 {
			t.Fatal("timeout looking for leader")
		}
		time.Sleep(time.Second / 4)
		resp, _, err := raftredcon.Do(fmt.Sprintf(":748%d", num), nil, []byte("raftleader"))
		if err != nil {
			continue
		}
		if len(resp) != 0 {
			return
		}
	}
}

func testDo(t testing.TB, node int, expect string, args ...string) {
	var bargs [][]byte
	for _, arg := range args {
		bargs = append(bargs, []byte(arg))
	}
	resp, _, err := raftredcon.Do(fmt.Sprintf(":748%d", node), nil, bargs...)
	if err != nil {
		if err.Error() == expect {
			return
		}
		t.Fatalf("node %d: %v", node, err)
	}
	if string(resp) != expect {
		t.Fatalf("node %d: expected '%v', got '%v'", node, expect, string(resp))
	}
}

func TestCluster(t *testing.T) {
	os.RemoveAll("data")
	defer os.RemoveAll("data")
	for i := 0; i < 3; i++ {
		go startTestNode(t, i, os.Getenv("LOG") == "1")
		waitFor(t, i)
	}
	t.Run("Leader", SubTestLeader)
	t.Run("Set", SubTestSet)
	t.Run("Get", SubTestGet)
	t.Run("Snapshot", SubTestSnapshot)
	t.Run("AddPeer", SubTestAddPeer)
}

func SubTestLeader(t *testing.T) {
	testDo(t, 0, ":7480", "raftleader")
	testDo(t, 1, ":7480", "raftleader")
	testDo(t, 2, ":7480", "raftleader")
}

func SubTestSet(t *testing.T) {
	testDo(t, 0, "OK", "set", "hello", "world")
	testDo(t, 1, "TRY :7480", "set", "hello", "world")
	testDo(t, 2, "TRY :7480", "set", "hello", "world")
}

func SubTestGet(t *testing.T) {
	testDo(t, 0, "world", "get", "hello")
	testDo(t, 1, "TRY :7480", "set", "hello", "world")
	testDo(t, 2, "TRY :7480", "set", "hello", "world")
}

func SubTestSnapshot(t *testing.T) {
	// insert 1000 items
	for i := 0; i < 1000; i++ {
		testDo(t, 0, "OK", "set", fmt.Sprintf("key:%d", i), fmt.Sprintf("val:%d", i))
	}
	testDo(t, 0, "OK", "raftsnapshot")
	testDo(t, 1, "OK", "raftsnapshot")
	testDo(t, 2, "OK", "raftsnapshot")
}
func SubTestAddPeer(t *testing.T) {
	go startTestNode(t, 3, os.Getenv("LOG") == "1")
	waitFor(t, 3)
	testDo(t, 3, ":7480", "raftleader")
	testDo(t, 3, "TRY :7480", "set", "hello", "world")
	testDo(t, 3, "OK", "raftsnapshot")
}

func BenchmarkCluster(t *testing.B) {
	os.RemoveAll("data")
	defer os.RemoveAll("data")
	for i := 0; i < 3; i++ {
		go startTestNode(t, i, false)
		waitFor(t, i)
	}
	t.Run("PL", func(t *testing.B) {
		pl := []int{1, 4, 16, 64}
		for i := 0; i < len(pl); i++ {
			func(pl int) {
				t.Run(fmt.Sprintf("%d", pl), func(t *testing.B) {
					t.Run("Ping", func(t *testing.B) { SubBenchmarkPing(t, pl) })
					t.Run("Set", func(t *testing.B) { SubBenchmarkSet(t, pl) })
					t.Run("Get", func(t *testing.B) { SubBenchmarkGet(t, pl) })
				})
			}(pl[i])
		}
	})
}
func testDial(t testing.TB, node int) (net.Conn, *bufio.ReadWriter) {
	conn, err := net.Dial("tcp", fmt.Sprintf(":748%d", node))
	if err != nil {
		t.Fatal(err)
	}
	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
}
func buildCommand(args ...string) []byte {
	var buf []byte
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(len(args)), 10)...)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		buf = append(buf, '$')
		buf = append(buf, strconv.FormatInt(int64(len(arg)), 10)...)
		buf = append(buf, '\r', '\n')
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

func testConnDo(t testing.TB, rw *bufio.ReadWriter, pl int, expect string, cmd []byte) {
	for i := 0; i < pl; i++ {
		rw.Write(cmd)
	}
	if err := rw.Flush(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(expect))
	for i := 0; i < pl; i++ {
		if _, err := io.ReadFull(rw, buf); err != nil {
			t.Fatal(err)
		}
		if string(buf) != expect {
			t.Fatalf("expected '%v', got '%v'", expect, string(buf))
		}
	}
}

func SubBenchmarkPing(t *testing.B, pipeline int) {
	conn, rw := testDial(t, 0)
	defer conn.Close()
	t.ResetTimer()
	for i := 0; i < t.N; i += pipeline {
		n := pipeline
		if t.N-i < pipeline {
			n = t.N - i
		}
		testConnDo(t, rw, n, "+PONG\r\n", []byte("*1\r\n$4\r\nPING\r\n"))
	}
}

func SubBenchmarkSet(t *testing.B, pipeline int) {
	conn, rw := testDial(t, 0)
	defer conn.Close()
	t.ResetTimer()
	for i := 0; i < t.N; i += pipeline {
		n := pipeline
		if t.N-i < pipeline {
			n = t.N - i
		}
		testConnDo(t, rw, n, "+OK\r\n", buildCommand("set", fmt.Sprintf("key:%d", i), fmt.Sprintf("val:%d", i)))
	}
}

func SubBenchmarkGet(t *testing.B, pipeline int) {
	conn, rw := testDial(t, 0)
	defer conn.Close()
	t.ResetTimer()
	for i := 0; i < t.N; i += pipeline {
		n := pipeline
		if t.N-i < pipeline {
			n = t.N - i
		}
		testConnDo(t, rw, n, "$-1\r\n", buildCommand("get", "key:na"))
	}
}
