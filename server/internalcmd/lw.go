package internalcmd

import (
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatcher/client"
	"github.com/zhiqiangxu/qwatcher/pkg/gob"
)

// LWCmd do list and watch
type LWCmd struct {
}

// ServeQRPC implements qrpc.Handler
func (cmd *LWCmd) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var lwCmd client.LWCmd
	err := gob.FromBytes(frame.Payload, &lwCmd)
	if err != nil {
		frame.Close()
		return
	}
}
