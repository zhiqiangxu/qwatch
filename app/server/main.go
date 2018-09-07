package main

import (
	"fmt"
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
)

const (
	// DefaultPublicAddress is for public
	DefaultPublicAddress = "localhost:8877"
	// DefaultInternalAddress is for internal
	DefaultInternalAddress = "localhost:8878"
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
		Use:   "qwatch [public address] [internal address] [raft address]",
		Short: "listen and server at specified address",
		Args:  cobra.MaximumNArgs(3),
		Run: func(cobraCmd *cobra.Command, args []string) {
			// read params
			publicAddr, internalAddr, raftAddr := DefaultPublicAddress, DefaultInternalAddress, DefaultRaftAddress
			slice := []*string{&publicAddr, &internalAddr, &raftAddr}
			for i, arg := range args {
				*slice[i] = arg
			}

			// start rkv store
			var bootstrap bool
			if join == "" && !recover {
				bootstrap = true
			}
			rkvConf := rkv.Config{DataDir: DataDir, LocalID: raftAddr, LocalRaftAddr: raftAddr, Bootstrap: bootstrap, Recover: recover}
			_, err := server.NewStore(rkvConf)
			if err != nil {
				panic(fmt.Sprintf("NewStore fail:%v", err))
			}
			if join != "" {
			}

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
			internalHandler := qrpc.NewServeMux()

			bindings := []qrpc.ServerBinding{
				qrpc.ServerBinding{Addr: publicAddr, Handler: handler, DefaultReadTimeout: 10 /*second*/, LatencyMetric: requestLatencyMetric, CounterMetric: requestCountMetric},
				qrpc.ServerBinding{Addr: internalAddr, Handler: internalHandler, LatencyMetric: requestLatencyMetric, CounterMetric: requestCountMetric}}

			qserver := qrpc.NewServer(bindings)

			var wg sync.WaitGroup
			qrpc.GoFunc(&wg, func() {
				qserver.ListenAndServe()
			})

			quitChan := make(chan os.Signal, 1)
			signal.Notify(quitChan, os.Interrupt, os.Kill, syscall.SIGTERM)

			<-quitChan
			err = qserver.Shutdown()
			logger.Info("Shutdown")
			if err != nil {
				logger.Error("Shutdown", err)
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
