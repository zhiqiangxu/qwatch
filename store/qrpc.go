package store

import (
	"context"
	"fmt"

	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/client"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/server"
)

// SetAPIAddrByQrpc tries to set apiAddr via qrpc
func SetAPIAddrByQrpc(remoteAPIAddr, nodeID, apiAddr string) error {

	payload, err := bson.ToBytes(client.SetAPIAddrCmd{NodeID: nodeID, APIAddr: apiAddr})
	if err != nil {
		return err
	}

	api := qrpc.NewAPI([]string{remoteAPIAddr}, qrpc.ConnectionConfig{}, nil)
	defer api.Close()

	frame, err := api.Call(context.Background(), server.SetAPIAddrCmd, payload)
	if err != nil {
		return err
	}

	var resp client.SetAPIAddrResp
	err = bson.FromBytes(frame.Payload, &resp)
	if err != nil {
		return err
	}

	if !resp.OK {
		return fmt.Errorf("%s", resp.Msg)
	}

	return nil
}

// JoinPeerByQrpc tries to join other to raft cluster by qrpc
func JoinPeerByQrpc(remoteAPIAddr, nodeID, raftAddr string) error {
	api := qrpc.NewAPI([]string{remoteAPIAddr}, qrpc.ConnectionConfig{}, nil)
	defer api.Close()

	payload, err := bson.ToBytes(client.JoinCmd{NodeID: nodeID, RaftAddr: raftAddr})
	if err != nil {
		return err
	}
	frame, err := api.Call(context.Background(), server.JoinCmd, payload)
	if err != nil {
		return err
	}

	var resp client.JoinResp
	err = bson.FromBytes(frame.Payload, &resp)
	if err != nil {
		return err
	}

	if !resp.OK {
		return fmt.Errorf("%s", resp.Msg)
	}

	return nil
}
