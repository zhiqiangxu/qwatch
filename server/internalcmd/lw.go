package internalcmd

import (
	"sync"

	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/entity"
	"github.com/zhiqiangxu/qwatch/pkg/logger"
	"github.com/zhiqiangxu/qwatch/server"
	"github.com/zhiqiangxu/qwatch/store"
)

// LWCmd do list and watch
type LWCmd struct {
	mu          sync.RWMutex
	keyWatchers map[string]map[*qrpc.ConnectionInfo]struct{}
	ciWatchMap  map[*qrpc.ConnectionInfo]map[string]struct{}
	store       *store.Store
	server      *qrpc.Server
	ch          <-chan []entity.EndPointsInKey
}

// NewLWCmd returns a LWCmd
func NewLWCmd(store *store.Store) *LWCmd {

	cmd := &LWCmd{store: store, keyWatchers: make(map[string]map[*qrpc.ConnectionInfo]struct{}), ciWatchMap: make(map[*qrpc.ConnectionInfo]map[string]struct{})}

	return cmd
}

// SetServer sets qrpc server
func (cmd *LWCmd) SetServer(server *qrpc.Server) {
	cmd.server = server
}

// StartWatch start watch changes
func (cmd *LWCmd) StartWatch() {
	if cmd.ch != nil {
		panic("StartWatch called twice")
	}
	cmd.ch = cmd.store.Watch()
	go cmd.fire()
}

// StopWatch stop watch changes
func (cmd *LWCmd) StopWatch() {
	cmd.store.Unwatch(cmd.ch)
}

func (cmd *LWCmd) fire() {

	qserver := cmd.server
	for {
		select {
		case changes, ok := <-cmd.ch:

			// logger.Info("changes", ok, changes)

			if !ok {
				return
			}
			for _, endPointsInKey := range changes {

				serviceNetwork, err := store.Key2ServiceNetwork(endPointsInKey.Key)
				if err != nil {
					logger.Error("invalid key spot", endPointsInKey.Key)
					continue
				}
				pushResp := entity.LWPushResp{ServiceNetwork: *serviceNetwork, EndPoints: endPointsInKey.EndPoints}

				var wg sync.WaitGroup
				bytes, err := bson.ToBytes(pushResp)
				if err != nil {
					logger.Error("pushResp ToBytes", err)
					continue
				}

				pushID := qserver.GetPushID()

				cmd.mu.RLock()
				ciMap := cmd.keyWatchers[endPointsInKey.Key]
				for ci := range ciMap {
					writer := ci.SC.GetWriter()
					qrpc.GoFunc(&wg, func() {
						writer.StartWrite(pushID, server.LWPushRespCmd, qrpc.PushFlag)
						writer.WriteBytes(bytes)
						err := writer.EndWrite()
						if err != nil {
							logger.Error("push failed", pushID)
						}
					})
				}
				cmd.mu.RUnlock()

				wg.Wait()
			}
		}
	}
}

// ServeQRPC implements qrpc.Handler
func (cmd *LWCmd) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	logger.Debug("LWCmd called")

	var lwCmd entity.LWCmd
	err := bson.SliceFromBytes(frame.Payload, &lwCmd)
	if err != nil {
		logger.Error("LWCmd FromBytes", err)
		frame.Close()
		return
	}

	ci := frame.ConnectionInfo()

	// first watch

	cmd.mu.Lock()
	for _, serviceNetwork := range lwCmd {
		key := store.KeyForServiceNetwork(serviceNetwork.Service, serviceNetwork.NetworkID)
		ciMap := cmd.keyWatchers[key]
		if ciMap == nil {
			ciMap = make(map[*qrpc.ConnectionInfo]struct{})
			cmd.keyWatchers[key] = ciMap
		}
		ciMap[ci] = struct{}{}

		keyMap := cmd.ciWatchMap[ci]
		if keyMap == nil {
			keyMap = make(map[string]struct{})
			cmd.ciWatchMap[ci] = keyMap
		}
		keyMap[key] = struct{}{}
	}
	cmd.mu.Unlock()

	ci.NotifyWhenClose(func() {
		cmd.mu.Lock()

		keyMap := cmd.ciWatchMap[ci]
		for k := range keyMap {
			delete(cmd.keyWatchers[k], ci)
		}
		delete(cmd.ciWatchMap, ci)

		cmd.mu.Unlock()
	})

	// second list
	var resp entity.LWResp
	for _, serviceNetwork := range lwCmd {
		endpoints := cmd.store.GetEndPoints(serviceNetwork.Service, serviceNetwork.NetworkID)
		resp = append(resp, entity.ServiceNetworkEndPoints{ServiceNetwork: serviceNetwork, EndPoints: endpoints})
	}
	bytes, err := bson.ToBytes(resp)
	if err != nil {
		logger.Error("LWCmd ToBytes", err)
		frame.Close()
		return
	}
	writer.StartWrite(frame.RequestID, server.LWRespCmd, qrpc.StreamFlag)
	writer.WriteBytes(bytes)
	err = writer.EndWrite()
	if err != nil {
		logger.Error("LWCmd EndWrite bytes", err)
		return
	}

}
