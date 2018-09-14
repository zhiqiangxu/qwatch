package main

import (
	"context"
	"testing"

	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/client"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/entity"
	"github.com/zhiqiangxu/qwatch/server"
)

func BenchmarkRegPerformance(b *testing.B) {

	regCmd := entity.RegCmd{Service: "test service", NetworkEndPoints: []entity.NetworkEndPoint{
		entity.NetworkEndPoint{NetworkID: client.DefaultNetworkID, EndPoint: entity.EndPoint("localhost:9999")}}}
	bytes, _ := bson.ToBytes(regCmd)

	ctx := context.Background()
	api := qrpc.NewAPI([]string{"localhost:8878"}, qrpc.ConnectionConfig{}, nil)
	b.SetParallelism(500)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := api.Call(ctx, server.RegCmd, bytes)
			if err != nil {
				b.Fatalf("RegCmd fail:%v", err)
			}
		}
	})
}
