package rkv

// Config configures raft store
type Config struct {
	DataDir       string // root dir to store data
	LocalID       string // raftID for local
	LocalRaftAddr string // should be accessable from peers
	Bootstrap     bool   // whether bootstrap
	Recover       bool   // whether to recover (TODO implement it)
}
