package store

import (
	"io"
	"sync"

	"reflect"

	"github.com/hashicorp/raft"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
)

// KV is bundled with rkv
type KV struct {
	meta sync.Map
	data sync.Map
}

// EndPoint for each node
type EndPoint struct {
	IP   string
	Port uint16
}

// Node contains all EndPoint
type Node struct {
	M map[string]EndPoint
}

// Clone returns a copy
func (s *Node) Clone() Node {

	m := make(map[string]EndPoint)
	for k, v := range s.M {
		m[k] = v
	}
	return Node{M: m}
}

// NodeSet for all nodes of service
type NodeSet struct {
	L     sync.RWMutex
	Nodes []Node
}

// Add the node to set
func (s *NodeSet) Add(node Node) {
	s.L.Lock()
	defer s.L.Unlock()

	for _, n := range s.Nodes {
		if reflect.DeepEqual(n, node) {
			return
		}
	}

	s.Nodes = append(s.Nodes, node.Clone())
}

// Remove a node from set
func (s *NodeSet) Remove(node Node) {
	s.L.Lock()
	defer s.L.Unlock()

	for i, n := range s.Nodes {
		if reflect.DeepEqual(n, node) {
			s.Nodes[len(s.Nodes)-1], s.Nodes[i] = s.Nodes[i], s.Nodes[len(s.Nodes)-1]
			s.Nodes = s.Nodes[:len(s.Nodes)-1]
			return
		}
	}
}

// Members returns all nodes in set
func (s *NodeSet) Members() []Node {

	s.L.RLock()
	defer s.L.RUnlock()

	var nodes []Node
	for _, n := range s.Nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// data ops

// SAdd will decode bytes then sadd
func (kv *KV) SAdd(key, value []byte) error {

	var nodes []Node
	err := bson.SliceFromBytes(value, &nodes)
	if err != nil {
		return err
	}

	val, _ := kv.data.LoadOrStore(string(key), NodeSet{})
	nodeset := val.(*NodeSet)

	for _, node := range nodes {
		nodeset.Add(node)
	}

	return nil
}

// SRem will decode bytes then srem
func (kv *KV) SRem(key, value []byte) error {
	var nodes []Node
	err := bson.SliceFromBytes(value, &nodes)
	if err != nil {
		return err
	}

	val, _ := kv.data.LoadOrStore(string(key), NodeSet{})
	nodeset := val.(*NodeSet)

	for _, node := range nodes {
		nodeset.Remove(node)
	}

	return nil
}

// meta ops

// SetAPIAddr will store api addr
func (kv *KV) SetAPIAddr(key, value []byte) error {
	kv.meta.Store(string(key), string(value))
	return nil
}

// raft ops

// SnapShot will do snapshot
func (kv *KV) SnapShot() (raft.FSMSnapshot, error) {

	return nil, nil
}

// Restore will do restore
func (kv *KV) Restore(io.ReadCloser) error {
	return nil
}
