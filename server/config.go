package server

import "github.com/zhiqiangxu/qrpc"

// Config contains config for Server
type Config struct {
	// QrpcBindings is for qrpc server
	QrpcBindings []qrpc.ServerBinding
	// RaftBind is for raft
	RaftBind string
	// RaftDir stores raft data
	RaftDir string
	// Join specifies a node
	Join string
}
