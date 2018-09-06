package rkv

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second

	raftDir     = "raft"
	raftSnapDir = "raft_snapshot" // snapshots in raft_snapshot/
	raftFile    = "raft.db"       // lives in raft/raft.db

	kvFile = "kv.db"
)

var (
	// ErrNotLeader when mutate on non-leader
	ErrNotLeader = errors.New("not leader")
)

// KV for di, all mutations should happen via raft
type KV interface {
	// data ops
	SAdd(key, value []byte) error
	SRem(key, value []byte) error

	// meta ops
	SetAPIAddr(key, value []byte) error

	// raft ops
	SnapShot() (raft.FSMSnapshot, error)
	Restore(io.ReadCloser) error
}

// RKV is a simple raft key-value store, where all changes are made via Raft consensus.
// currently only set value type is supported
type RKV struct {
	Config Config

	kv KV

	raft *raft.Raft // The consensus mechanism

}

// New returns a RKV instance
func New(conf Config) (*RKV, error) {

	rs := &RKV{Config: conf}

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(conf.LocalID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", conf.LocalRaftAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(conf.LocalRaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(conf.DataDir, raftSnapDir), retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %s", err)
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(conf.DataDir, raftDir, raftFile))
	logStore = boltDB
	stableStore = boltDB

	ra, err := raft.NewRaft(config, (*fsm)(rs), logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %s", err)
	}

	if conf.RemoteAPIAddr == "" {
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

// SAdd to rkv
func (rkv *RKV) SAdd(key []byte, val ...interface{}) error {
	if rkv.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	bytes, err := bson.VarToBytes(val...)
	if err != nil {
		return err
	}

	c := &Command{
		Op:    SAdd,
		Key:   key,
		Value: bytes,
	}

	cbytes, err := bson.ToBytes(c)
	if err != nil {
		return err
	}

	f := rkv.raft.Apply(cbytes, raftTimeout)
	return f.Error()
}

// SRem to rkv
func (rkv *RKV) SRem(key []byte, val ...interface{}) error {
	bytes, err := bson.VarToBytes(val...)
	if err != nil {
		return err
	}

	c := &Command{
		Op:    SRem,
		Key:   key,
		Value: bytes,
	}

	cbytes, err := bson.ToBytes(c)
	if err != nil {
		return err
	}

	f := rkv.raft.Apply(cbytes, raftTimeout)
	return f.Error()
}

// SetAPIAddr set apiAddr for node
func (rkv *RKV) SetAPIAddr(key, val []byte) error {
	c := &Command{
		Op:    SetAPIAddr,
		Key:   key,
		Value: val,
	}

	cbytes, err := bson.ToBytes(c)
	if err != nil {
		return err
	}

	f := rkv.raft.Apply(cbytes, raftTimeout)
	return f.Error()
}
