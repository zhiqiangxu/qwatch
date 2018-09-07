package store

import (
	"context"
	"fmt"
	"time"

	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/client"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/logger"
	"github.com/zhiqiangxu/qwatch/pkg/rkv"
	"github.com/zhiqiangxu/qwatch/server"
)

const (
	ttl = time.Second * 60
)

// Store provide read/write kv ops
type Store struct {
	localAPIAddr string
	kv           *KV
	rkv          *rkv.RKV
}

// New returns a store
func New(config rkv.Config, localAPIAddr string) (*Store, error) {

	kv := NewKV()
	rkv, err := rkv.New(kv, config)
	if err != nil {
		return nil, err
	}
	return &Store{localAPIAddr: localAPIAddr, kv: kv, rkv: rkv}, nil
}

// mutation ops

// SAdd add endpoints to service
func (s *Store) SAdd(key []byte, val []NetworkEndPoint) error {
	ttl := TTL{NodeID: s.rkv.Config.LocalID, LastUpdate: time.Now()}
	var entries []interface{}
	for _, networkEndPoint := range val {
		entry := NetworkEndPointTTL{NetworkEndPoint: networkEndPoint, TTL: ttl}
		entries = append(entries, entry)
	}
	return s.rkv.SAdd(key, entries...)
}

// SRem remove nodes from set
func (s *Store) SRem(key []byte, val []NetworkEndPoint) error {
	ttl := TTL{NodeID: s.rkv.Config.LocalID, LastUpdate: time.Time{}}
	var entries []interface{}
	for _, networkEndPoint := range val {
		entry := NetworkEndPointTTL{NetworkEndPoint: networkEndPoint, TTL: ttl}
		entries = append(entries, entry)
	}
	return s.rkv.SRem(key, entries...)
}

// SetAPIAddr set apiAddr for node
func (s *Store) SetAPIAddr(nodeID []byte, apiAddr []byte) error {
	return s.rkv.SetAPIAddr(nodeID, apiAddr)
}

// read ops

// GetEndPoints returns nodes for specified service
func (s *Store) GetEndPoints(service, networkID string) []EndPoint {
	return s.kv.GetEndPoints(service, networkID)
}

// GetAPIAddr returns the apiAddr for node
func (s *Store) GetAPIAddr(nodeID []byte) string {
	return s.kv.GetAPIAddr(string(nodeID))
}

// raft related

// IsLeader tells if current node is leader
func (s *Store) IsLeader() bool {
	return s.rkv.IsLeader()
}

// LeaderAPIAddr returns the apiAddr for leader
func (s *Store) LeaderAPIAddr() string {
	return s.rkv.LeaderAPIAddr()
}

// Join a node to raft cluster
func (s *Store) Join(nodeID, raftAddr string) error {
	return s.rkv.Join(nodeID, raftAddr)
}

// JoinByQrpc tries to join self to raft cluster by qrpc
func (s *Store) JoinByQrpc(remoteAPIAddr string) error {

	return JoinPeerByQrpc(remoteAPIAddr, s.rkv.Config.LocalID, s.rkv.Config.LocalRaftAddr)
}

// UpdateAPIAddr update localAPIAddr for itself
func (s *Store) UpdateAPIAddr() {
	for {
		logger.Info("UpdateAPIAddr")
		if s.rkv.IsLeader() {
			err := s.SetAPIAddr([]byte(s.rkv.Config.LocalID), []byte(s.localAPIAddr))
			if err == nil {
				return
			}
			logger.Error("SetAPIAddr", err)
		} else {
			leaderAPIAddr := s.rkv.LeaderAPIAddr()
			if leaderAPIAddr != "" {
				err := SetAPIAddrByQrpc(leaderAPIAddr, s.rkv.Config.LocalID, s.localAPIAddr)
				if err == nil {
					return
				}
				logger.Error("SetAPIAddrByQrpc", err)
			} else {
				logger.Info("leader NA")
			}
		}
		time.Sleep(time.Second)
	}
}

// SetAPIAddrByQrpc tries to set apiAddr via qrpc
func SetAPIAddrByQrpc(remoteAPIAddr, nodeID, apiAddr string) error {
	api := qrpc.NewAPI([]string{remoteAPIAddr}, qrpc.ConnectionConfig{}, nil)
	defer api.Close()

	payload, err := bson.ToBytes(client.SetAPIAddrCmd{NodeID: nodeID, APIAddr: apiAddr})
	if err != nil {
		return err
	}
	frame, err := api.Call(context.Background(), server.SetAPIAddrCmd, payload)
	if err != nil {
		return err
	}

	var resp client.SetAPIAddrResp
	err = bson.FromBytes(frame.Payload, &resp)
	if err != nil {
		return err
	}

	if !resp.OK {
		return fmt.Errorf("%s", resp.Msg)
	}

	return nil
}

// JoinPeerByQrpc tries to join other to raft cluster by qrpc
func JoinPeerByQrpc(remoteAPIAddr, nodeID, raftAddr string) error {
	api := qrpc.NewAPI([]string{remoteAPIAddr}, qrpc.ConnectionConfig{}, nil)
	defer api.Close()

	payload, err := bson.ToBytes(client.JoinCmd{NodeID: nodeID, RaftAddr: raftAddr})
	if err != nil {
		return err
	}
	frame, err := api.Call(context.Background(), server.JoinCmd, payload)
	if err != nil {
		return err
	}

	var resp client.JoinResp
	err = bson.FromBytes(frame.Payload, &resp)
	if err != nil {
		return err
	}

	if !resp.OK {
		return fmt.Errorf("%s", resp.Msg)
	}

	return nil
}
