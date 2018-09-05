package server

import (
	"github.com/zhiqiangxu/qrpc"
)

const (
	// RegCmd for register
	RegCmd qrpc.Cmd = iota
	// LWCmd for list and watch
	LWCmd
)
