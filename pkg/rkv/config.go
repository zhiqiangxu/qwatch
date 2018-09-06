package rkv

// Config configures raft store
type Config struct {
	KV            KV
	DataDir       string // root dir to store data
	LocalID       string // raftID for local
	LocalRaftAddr string // should be accessable from peers
	LocalAPIAddr  string // should be accessable from peers
	RemoteAPIAddr string // remote api addr to join the cluster, empty means bootstrap
}
