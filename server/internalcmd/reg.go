package internalcmd

import (
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/client"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
)

// RegCmd do register
type RegCmd struct {
}

// ServeQRPC implements qrpc.Handler
func (cmd *RegCmd) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var regCmd client.RegCmd
	err := bson.FromBytes(frame.Payload, &regCmd)
	if err != nil {
		frame.Close()
		return
	}

}
