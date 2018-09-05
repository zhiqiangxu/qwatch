package server

import (
	"sync"

	"github.com/hashicorp/raft"
)

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	RaftDir  string
	RaftBind string

	m    sync.Map // The key-value store for the system.
	meta sync.Map // the meta info for nodes

	raft *raft.Raft // The consensus mechanism
}
