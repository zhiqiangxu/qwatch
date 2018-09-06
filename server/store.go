package server

import (
	"github.com/zhiqiangxu/qwatch/pkg/rkv"
	"github.com/zhiqiangxu/qwatch/server/store"
)

// Store provide read/write kv ops
type Store struct {
	kv  *store.KV
	rkv *rkv.RKV
}

// NewStore returns a store
func NewStore(config rkv.Config) (*Store, error) {

	kv := &store.KV{}
	config.KV = kv
	rkv, err := rkv.New(config)
	if err != nil {
		return nil, err
	}
	return &Store{kv: kv, rkv: rkv}, nil
}

// mutation ops

// SAdd add nodes to set
func (s *Store) SAdd(key []byte, val []store.Node) error {
	var nodes []interface{}
	for _, node := range val {
		nodes = append(nodes, node)
	}
	return s.rkv.SAdd(key, nodes...)
}

// SRem remove nodes from set
func (s *Store) SRem(key []byte, val []store.Node) error {
	var nodes []interface{}
	for _, node := range val {
		nodes = append(nodes, node)
	}
	return s.rkv.SRem(key, nodes...)
}

// SetAPIAddr set apiAddr for node
func (s *Store) SetAPIAddr(nodeID []byte, apiAddr []byte) error {
	return s.rkv.SetAPIAddr(nodeID, apiAddr)
}

// read ops

// GetNodes returns nodes for specified service
func (s *Store) GetNodes(service string) []store.Node {
	return s.kv.GetNodes(service)
}

// GetAPIAddr returns the apiAddr for node
func (s *Store) GetAPIAddr(nodeID []byte) string {
	return s.kv.GetAPIAddr(string(nodeID))
}
