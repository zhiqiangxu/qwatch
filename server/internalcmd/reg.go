package internalcmd

import (
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatcher/client"
	"github.com/zhiqiangxu/qwatcher/pkg/gob"
)

// RegCmd do register
type RegCmd struct {
}

// ServeQRPC implements qrpc.Handler
func (cmd *RegCmd) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var regCmd client.RegCmd
	err := gob.FromBytes(frame.Payload, &regCmd)
	if err != nil {
		frame.Close()
		return
	}

}
