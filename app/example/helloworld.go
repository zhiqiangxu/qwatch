package main

import (
	"fmt"
	"time"

	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/client"
	"github.com/zhiqiangxu/qwatch/pkg/entity"
)

const (
	serviceName = "hello world"
)

func main() {
	servers := []string{"localhost:8878"}
	qwatchClient := client.New(servers, qrpc.ConnectionConfig{})
	regInfo := entity.RegCmd{
		Service: serviceName,
		NetworkEndPoints: []entity.NetworkEndPoint{
			entity.NetworkEndPoint{
				NetworkID: client.DefaultNetworkID,
				EndPoint:  entity.EndPoint("localhost:8878")}}}
	qwatchClient.RegisterService(regInfo, func(resp client.RegResponse) {
		fmt.Println("RegisterService", resp)
	})

	lwInfo := entity.LWCmd{
		entity.ServiceNetwork{Service: serviceName, NetworkID: client.DefaultNetworkID},
	}
	qwatchClient.ListWatch(lwInfo, func(lwResp client.LWResponse) {
		fmt.Println("ListWatch lwResp", lwResp)
	}, func(lwPushResp entity.LWPushResp) {
		fmt.Println("ListWatch lwPushResp", lwPushResp)
	})

	for {
		endpoints := qwatchClient.EndPoints(entity.ServiceNetwork{Service: serviceName, NetworkID: client.DefaultNetworkID})
		fmt.Println("EndPoints", endpoints)
		time.Sleep(time.Second * 2)
	}
}
