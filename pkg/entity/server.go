package entity

import (
	"github.com/hashicorp/raft"
)

// Server for node in cluster
type Server struct {
	RaftInfo raft.Server
	APIAddr  string
}
