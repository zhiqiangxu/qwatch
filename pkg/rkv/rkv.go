package rkv

import (
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

	raftDirPerm = 0700
	raftDir     = "raft"
	raftSnapDir = "raft_snapshot" // snapshots in raft_snapshot/
	raftFile    = "raft.db"       // lives in raft/raft.db

	kvFile = "kv.db"
)

// KV for di, all mutations should happen via raft
type KV interface {
	// data ops
	SAdd(key, value []byte) error
	SRem(key, value []byte) error

	// meta ops
	SetAPIAddr(key, value []byte) error
	GetAPIAddr(nodeID string) string

	Expire(value []byte) error
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
func New(kv KV, conf Config) (*RKV, error) {

	rs := &RKV{Config: conf, kv: kv}

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
	// NewBoltStore won't create directory automatically, so do it here
	boltDir := filepath.Join(conf.DataDir, raftDir)
	os.MkdirAll(boltDir, raftDirPerm)

	boltDB, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(boltDir, raftFile), NoSync: false})
	if err != nil {
		return nil, err
	}
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

// IsLeader tells if the node is leader
func (rkv *RKV) IsLeader() bool {
	return rkv.raft.State() == raft.Leader
}

// LeaderAPIAddr returns the apiAddr for leader
func (rkv *RKV) LeaderAPIAddr() string {
	leaderRaftAddr := rkv.raft.Leader()
	return rkv.kv.GetAPIAddr(string(leaderRaftAddr))
}

// Join joins a node, identified by nodeID and located at raftAddr, to the cluster.
// The node must be ready to respond to Raft communications at that address.
func (rkv *RKV) Join(nodeID, raftAddr string) error {

	configFuture := rkv.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(raftAddr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(raftAddr) && srv.ID == raft.ServerID(nodeID) {
				return nil
			}

			future := rkv.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return err
			}
		}
	}

	f := rkv.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, 0)

	return f.Error()
}

// SAdd to rkv
func (rkv *RKV) SAdd(key []byte, val ...interface{}) error {

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

// Expire ExpiredEndPointTTLsInKey
func (rkv *RKV) Expire(expired interface{}) error {
	bytes, err := bson.ToBytes(expired)
	if err != nil {
		return err
	}

	c := &Command{
		Op:    Expire,
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

// Shutdown rkv
func (rkv *RKV) Shutdown() error {
	f := rkv.raft.Shutdown()
	return f.Error()
}

// LeaderCh for notify leader change
func (rkv *RKV) LeaderCh() <-chan bool {
	return rkv.raft.LeaderCh()
}

// GetServerList returns current raft servers
func (rkv *RKV) GetServerList() ([]raft.Server, error) {
	f := rkv.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}

	return f.Configuration().Servers, nil
}
