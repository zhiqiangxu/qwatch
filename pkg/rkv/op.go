package rkv

// Op for op of RStore
type Op uint16

const (
	// data ops

	// SAdd for set add
	SAdd Op = iota
	// SRem for set remove
	SRem

	// meta ops

	// SetAPIAddr sets the apiAddr for a raft node
	// only called when join a cluster
	SetAPIAddr
)
