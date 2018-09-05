package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/kvdb"
)

const (
	retainSnapshotCount = 2
	kvFile              = "kv.db"
	kvbkFile            = "kvbk.db"
	tmpSnapFile         = "snap.tmp.db"
)

// RStoreOp for op of RStore
type RStoreOp uint16

const (
	// SAdd for set add
	SAdd RStoreOp = iota
	// SRem for set remove
	SRem
	// SMembers for all set elements
	SMembers
)

// RStoreConfig configures raft store
type RStoreConfig struct {
	RaftBind  string
	DataDir   string
	LocalID   string
	Bootstrap bool
}

// RStore is a simple key-value store, where all changes are made via Raft consensus.
type RStore struct {
	StoreConfig RStoreConfig

	kvdb *kvdb.KVDB // for store key/value and meta data

	raft *raft.Raft // The consensus mechanism

	snapshoting int32 // used for snapshot
}

// New returns a RStore instance
func New(conf RStoreConfig) (*RStore, error) {
	rs := &RStore{StoreConfig: conf}

	kvdb, err := kvdb.New(filepath.Join(conf.DataDir, kvFile))
	if err != nil {
		return nil, err
	}
	rs.kvdb = kvdb

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(conf.LocalID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", conf.RaftBind)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(conf.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	raftDir := filepath.Join(conf.DataDir, "raft")
	snapshots, err := raft.NewFileSnapshotStore(raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %s", err)
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	logStore = boltDB
	stableStore = boltDB

	ra, err := raft.NewRaft(config, (*fsm)(rs), logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %s", err)
	}

	if conf.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	rs.raft = ra

	return rs, nil
}

// Command is for kv op
type Command struct {
	Op    RStoreOp
	Key   string
	Value string
}

type fsm RStore

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c Command
	if err := bson.FromBytes(l.Data, &c); err != nil {
		panic(fmt.Sprintf("bson.FromBytes failed: %s data: %v", err.Error(), l.Data))
	}

	switch c.Op {
	case SAdd:
	case SRem:
	}
	return nil
}

func (f *fsm) snapshotFile() string {
	return filepath.Join(f.StoreConfig.DataDir, tmpSnapFile)
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (snapshot raft.FSMSnapshot, err error) {
	swapped := atomic.CompareAndSwapInt32(&f.snapshoting, 0, 1)
	if !swapped {
		return nil, fmt.Errorf("previous snapshot hasn't finished yet")
	}

	defer func() {
		if err != nil {
			atomic.StoreInt32(&f.snapshoting, 0)
		}
	}()

	snapFile := f.snapshotFile()
	file, err := os.Create(snapFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	err = f.kvdb.Snapshot(w)
	if err != nil {
		return nil, err
	}
	err = w.Flush()
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{f: f}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	f.kvdb.Close()

	kvdbFile := filepath.Join(f.StoreConfig.DataDir, kvFile)
	kvbkFile := filepath.Join(f.StoreConfig.DataDir, kvbkFile)
	os.Rename(kvdbFile, kvbkFile)

	file, err := os.Create(kvdbFile)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	_, err = io.Copy(w, rc)
	if err != nil {
		return err
	}

	return w.Flush()
}

type fsmSnapshot struct {
	f *fsm
}

func (snapshot *fsmSnapshot) Persist(sink raft.SnapshotSink) error {

	snapFile := snapshot.f.snapshotFile()
	r, err := os.Open(snapFile)
	if err != nil {
		return err
	}
	defer r.Close()

	_, err = io.Copy(sink, r)
	if err != nil {
		return err
	}

	return sink.Close()
}

func (snapshot *fsmSnapshot) Release() {

	os.Remove(snapshot.f.snapshotFile())
	atomic.StoreInt32(&snapshot.f.snapshoting, 0)

}
