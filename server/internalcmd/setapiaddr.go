package internalcmd

import (
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/client"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/logger"
	"github.com/zhiqiangxu/qwatch/server"
	"github.com/zhiqiangxu/qwatch/store"
)

// SetAPIAddrCmd will set apiAddr for node if it's leader
type SetAPIAddrCmd struct {
	store *store.Store
}

// NewSetAPIAddrCmd returns a SetAPIAddrCmd
func NewSetAPIAddrCmd(store *store.Store) *SetAPIAddrCmd {
	return &SetAPIAddrCmd{store: store}
}

// ServeQRPC implements qrpc.Handler
func (cmd *SetAPIAddrCmd) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var setAPIAddrCmd client.SetAPIAddrCmd
	err := bson.FromBytes(frame.Payload, &setAPIAddrCmd)
	if err != nil {
		frame.Close()
		return
	}

	if !cmd.store.IsLeader() {
		cmd.writeResp(writer, frame, server.ErrNotLeader)
	}

	err = cmd.store.SetAPIAddr([]byte(setAPIAddrCmd.NodeID), []byte(setAPIAddrCmd.APIAddr))
	cmd.writeResp(writer, frame, err)
}

func (cmd *SetAPIAddrCmd) writeResp(writer qrpc.FrameWriter, frame *qrpc.RequestFrame, err error) {
	var resp client.JoinResp
	if err == nil {
		resp.OK = true
	} else {
		resp.OK = false
		resp.Msg = err.Error()
	}

	bytes, err := bson.ToBytes(resp)
	if err != nil {
		logger.Error("ToBytes", err)
		frame.Close()
		return
	}

	writer.StartWrite(frame.RequestID, server.JoinRespCmd, 0)
	writer.WriteBytes(bytes)
	writer.EndWrite()
}
