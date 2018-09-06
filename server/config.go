package server

import (
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/pkg/rkv"
)

// Config contains config for Server
type Config struct {
	// QrpcBindings is for qrpc server
	QrpcBindings []qrpc.ServerBinding
	RKVConfig    rkv.Config
}
