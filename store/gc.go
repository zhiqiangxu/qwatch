package store

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/logger"
)

const (
	batchCount   = 10
	batchTimeout = 60 * time.Second
	clearTimeout = 5 * time.Minute
)

// GcRequest for request key to gc
type GcRequest struct {
	NetworkID string
	Service   string
}

// GC will batch GcRequest
type GC struct {
	mu    sync.Mutex
	reqs  map[GcRequest]interface{}
	reqed atomic.Value // *sync.Map clear interval clearTimeout
	done  int32
	wg    sync.WaitGroup
	bell  chan struct{}
}

// NewGC constructs a GC
func NewGC() *GC {
	gc := &GC{reqs: make(map[GcRequest]interface{}), bell: make(chan struct{})}
	gc.reqed.Store(&sync.Map{})
	qrpc.GoFunc(&gc.wg, gc.work)
	return gc
}

// Put a GcRequest
func (g *GC) Put(req GcRequest) {
	if atomic.LoadInt32(&g.done) != 0 {
		return
	}

	g.mu.Lock()
	g.reqs[req] = struct{}{}
	count := len(g.reqs)
	g.mu.Unlock()

	g.reqed.Load().(*sync.Map).Store(req, struct{}{})

	// ring the bell if batch met
	if count > batchCount {
		select {
		case g.bell <- struct{}{}:
		default:
		}
	}

}

func (g *GC) work() {
	for {
		select {
		case <-g.bell:
		case <-time.After(batchTimeout):
		case <-time.After(clearTimeout):
			g.reqed.Store(&sync.Map{})
			continue
		}

		g.mu.Lock()
		reqs := g.reqs
		g.reqs = make(map[GcRequest]interface{})
		g.mu.Unlock()

		if len(reqs) == 0 {
			continue
		}

		reqed := g.reqed.Load().(*sync.Map)

		var slice []GcRequest
		for req := range reqs {
			_, ok := reqed.Load(req)
			if !ok {
				slice = append(slice, req)
			}
		}

		if len(slice) == 0 {
			continue
		}

		_, err := bson.ToBytes(slice)
		if err != nil {
			logger.Error("work ToBytes", err)
			continue
		}
	}
}

// End closes GC
func (g *GC) End() {
	swapped := atomic.CompareAndSwapInt32(&g.done, 0, 1)
	if !swapped {
		return
	}

	g.wg.Wait()
}
