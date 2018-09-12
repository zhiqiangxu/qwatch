package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/client"
	"github.com/zhiqiangxu/qwatch/pkg/config"
	"github.com/zhiqiangxu/qwatch/pkg/entity"
)

const (
	serviceName = "hello world"
)

func init() {
	_, err := config.Load("dev")
	if err != nil {
		panic(err)
	}
}

func main() {

	qwatchServers := []string{"localhost:8878"}
	qwatchClient := client.New(qwatchServers, qrpc.ConnectionConfig{})
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

func startHTTPServer(qwatchClient *client.Client) {
	go func() {
		srv := &http.Server{Addr: "0.0.0.0:8081"}
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			servers, _ := store.GetServerList()
			data := store.GetAllData()
			result := make(map[string]interface{})
			result["servers"] = servers
			result["data"] = data

			bytes, _ := json.Marshal(result)
			io.WriteString(w, string(bytes))
		})

		fmt.Println(srv.ListenAndServe())
	}()
}
