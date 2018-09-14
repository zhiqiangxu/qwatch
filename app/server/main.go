package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/zhiqiangxu/qwatch/pkg/rkv"

	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/pkg/config"
	"github.com/zhiqiangxu/qwatch/pkg/logger"
	"github.com/zhiqiangxu/qwatch/server"
	"github.com/zhiqiangxu/qwatch/server/internalcmd"
	"github.com/zhiqiangxu/qwatch/store"
)

const (
	// DefaultAPIAddress is for internal
	DefaultAPIAddress = "localhost:8878"
	// DefaultRaftAddress is for raft
	DefaultRaftAddress = "localhost:8879"
	// DataDir for data storage
	DataDir = "/tmp/qwatch"
)

var (
	env     string
	join    string
	recover bool
)

func main() {

	var rootCmd = &cobra.Command{
		Use:   "qwatch [api address] [raft address]",
		Short: "listen and server at specified address",
		Args:  cobra.MaximumNArgs(2),
		Run: func(cobraCmd *cobra.Command, args []string) {
			// read params
			apiAddr, raftAddr := DefaultAPIAddress, DefaultRaftAddress
			slice := []*string{&apiAddr, &raftAddr}
			for i, arg := range args {
				*slice[i] = arg
			}

			// start rkv store
			var bootstrap bool
			if join == "" && !recover {
				bootstrap = true
			}
			rkvConf := rkv.Config{DataDir: DataDir, LocalID: raftAddr, LocalRaftAddr: raftAddr, Bootstrap: bootstrap, Recover: recover}
			store, err := store.New(rkvConf, apiAddr)
			if err != nil {
				panic(fmt.Sprintf("NewStore fail:%v", err))
			}
			// do join by any node in cluster if required
			if join != "" {
				err = store.JoinByQrpc(join)
				if err != nil {
					panic(fmt.Sprintf("JoinByQrpc fail:%v", err))
				}
			}
			// update apiAddr for self
			// it will keep trying until succeed, if possible
			store.UpdateAPIAddr()

			// startHTTPServer for test
			startHTTPServer(store)

			// start qrpc server after rkv is ready

			// qrpc request count
			requestCountMetric := kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
				Namespace: "qwatch",
				Subsystem: "server",
				Name:      "request_count",
				Help:      "The counter result per app.",
			}, []string{"method", "error"})
			// qrpc request latency
			requestLatencyMetric := kitprometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
				Namespace: "qwatch",
				Subsystem: "server",
				Name:      "request_latency",
				Help:      "request latency.",
			}, []string{"method", "error"})

			handler := qrpc.NewServeMux()
			handler.Handle(server.JoinCmd, internalcmd.NewJoinCmd(store))
			handler.Handle(server.SetAPIAddrCmd, internalcmd.NewSetAPIAddrCmd(store))
			handler.Handle(server.RegCmd, internalcmd.NewRegCmd(store))
			lwCmd := internalcmd.NewLWCmd(store)
			handler.Handle(server.LWCmd, lwCmd)

			bindings := []qrpc.ServerBinding{
				qrpc.ServerBinding{Addr: apiAddr, Handler: handler, LatencyMetric: requestLatencyMetric, CounterMetric: requestCountMetric}}

			qserver := qrpc.NewServer(bindings)
			lwCmd.SetServer(qserver)
			lwCmd.StartWatch()

			var wg sync.WaitGroup
			qrpc.GoFunc(&wg, func() {
				qserver.ListenAndServe()
			})

			quitChan := make(chan os.Signal, 1)
			signal.Notify(quitChan, os.Interrupt, os.Kill, syscall.SIGTERM)

			<-quitChan
			logger.Info("Shutdown")
			err = qserver.Shutdown()
			if err != nil {
				logger.Error("Shutdown", err)
			}
			lwCmd.StopWatch()
			logger.Info("store.Close")
			err = store.Close()
			if err != nil {
				logger.Error("store.Close", err)
			}
		}}

	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&env, "env", "", "environment")
	rootCmd.PersistentFlags().StringVar(&join, "join", "", "node to join")
	rootCmd.PersistentFlags().BoolVar(&recover, "recover", false, "whether to recover")
	rootCmd.Execute()

}

func initConfig() {
	_, err := config.Load(env)
	if err != nil {
		panic(fmt.Sprintf("failed to load config file: %s", env))
	}
}

func startHTTPServer(store *store.Store) {
	go func() {
		srv := &http.Server{Addr: "0.0.0.0:8080"}
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
