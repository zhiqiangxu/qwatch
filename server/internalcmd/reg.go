package internalcmd

import (
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/entity"
	"github.com/zhiqiangxu/qwatch/pkg/logger"
	"github.com/zhiqiangxu/qwatch/server"
	"github.com/zhiqiangxu/qwatch/store"
)

// RegCmd do register
type RegCmd struct {
	store *store.Store
}

// NewRegCmd returns a RegCmd
func NewRegCmd(store *store.Store) *RegCmd {

	cmd := &RegCmd{store: store}

	return cmd
}

// ServeQRPC implements qrpc.Handler
func (cmd *RegCmd) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var regCmd entity.RegCmd
	err := bson.FromBytes(frame.Payload, &regCmd)
	if err != nil {
		frame.Close()
		return
	}

	if cmd.store.IsLeader() {
		err = cmd.store.SAdd([]byte(regCmd.Service), regCmd.NetworkEndPoints)
		if err != nil {
			logger.Error("cmd.store.SAdd", err)
		}
		cmd.writeResp(writer, frame, err)
		return
	}

	leaderAPIAddr := cmd.store.LeaderAPIAddr()
	if leaderAPIAddr == "" {
		cmd.writeResp(writer, frame, server.ErrLeaderAPINA)
		return
	}
	err = store.RegServiceByQrpc(leaderAPIAddr, regCmd)
	cmd.writeResp(writer, frame, err)
}

func (cmd *RegCmd) writeResp(writer qrpc.FrameWriter, frame *qrpc.RequestFrame, err error) {
	var resp entity.RegResp
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

	writer.StartWrite(frame.RequestID, server.RegRespCmd, 0)
	writer.WriteBytes(bytes)
	writer.EndWrite()
}
