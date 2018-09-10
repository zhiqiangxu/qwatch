package store

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/entity"
	"github.com/zhiqiangxu/qwatch/pkg/logger"
	"github.com/zhiqiangxu/qwatch/pkg/rkv"
)

const (
	ttlTimeout = time.Second * 60
	gcInterval = time.Minute * 2
)

// Store provide read/write kv ops
type Store struct {
	localAPIAddr string
	kv           *KV
	rkv          *rkv.RKV
	closed       int32
	closeCh      chan struct{}
	wg           sync.WaitGroup

	mu       sync.Mutex
	watchers []chan []entity.EndPointTTLsInKey
}

// New returns a store
func New(config rkv.Config, localAPIAddr string) (*Store, error) {

	store := &Store{localAPIAddr: localAPIAddr, closeCh: make(chan struct{})}
	kv := NewKV(store)
	store.kv = kv
	rkv, err := rkv.New(kv, config)
	if err != nil {
		return nil, err
	}
	store.rkv = rkv

	qrpc.GoFunc(&store.wg, store.Expire)

	return store, nil
}

// mutation ops

// SAdd add endpoints to service
func (s *Store) SAdd(key []byte, val []entity.NetworkEndPoint) error {
	ttl := entity.TTL{NodeID: s.rkv.Config.LocalID, LastUpdate: bson.Now()}
	var entries []interface{}
	for _, networkEndPoint := range val {
		entry := entity.NetworkEndPointTTL{NetworkEndPoint: networkEndPoint, TTL: ttl}
		entries = append(entries, entry)
	}
	return s.rkv.SAdd(key, entries...)
}

// SRem remove nodes from set
func (s *Store) SRem(key []byte, val []entity.NetworkEndPoint) error {
	ttl := entity.TTL{NodeID: s.rkv.Config.LocalID, LastUpdate: time.Time{}}
	var entries []interface{}
	for _, networkEndPoint := range val {
		entry := entity.NetworkEndPointTTL{NetworkEndPoint: networkEndPoint, TTL: ttl}
		entries = append(entries, entry)
	}
	return s.rkv.SRem(key, entries...)
}

// SetAPIAddr set apiAddr for node
func (s *Store) SetAPIAddr(nodeID []byte, apiAddr []byte) error {
	return s.rkv.SetAPIAddr(nodeID, apiAddr)
}

// read ops

// GetEndPoints returns nodes for specified service
func (s *Store) GetEndPoints(service, networkID string) []entity.EndPoint {
	return s.kv.GetEndPoints(service, networkID)
}

// GetAPIAddr returns the apiAddr for node
func (s *Store) GetAPIAddr(nodeID []byte) string {
	return s.kv.GetAPIAddr(string(nodeID))
}

// raft related

// IsLeader tells if current node is leader
func (s *Store) IsLeader() bool {
	return s.rkv.IsLeader()
}

// LeaderAPIAddr returns the apiAddr for leader
func (s *Store) LeaderAPIAddr() string {
	return s.rkv.LeaderAPIAddr()
}

// Join a node to raft cluster
func (s *Store) Join(nodeID, raftAddr string) error {
	return s.rkv.Join(nodeID, raftAddr)
}

// JoinByQrpc tries to join self to raft cluster by qrpc
func (s *Store) JoinByQrpc(remoteAPIAddr string) error {

	return JoinPeerByQrpc(remoteAPIAddr, s.rkv.Config.LocalID, s.rkv.Config.LocalRaftAddr)
}

// UpdateAPIAddr update localAPIAddr for itself
func (s *Store) UpdateAPIAddr() {
	defer logger.Info("UpdateAPIAddr done")
	for {
		logger.Info("UpdateAPIAddr")
		if s.rkv.IsLeader() {
			err := s.SetAPIAddr([]byte(s.rkv.Config.LocalID), []byte(s.localAPIAddr))
			if err == nil {
				return
			}
			logger.Error("SetAPIAddr", err)
		} else {
			leaderAPIAddr := s.rkv.LeaderAPIAddr()
			if leaderAPIAddr != "" {
				err := SetAPIAddrByQrpc(leaderAPIAddr, s.rkv.Config.LocalID, s.localAPIAddr)
				if err == nil {
					return
				}
				logger.Error("SetAPIAddrByQrpc", err)
			} else {
				logger.Info("leader NA")
			}
		}
		time.Sleep(time.Second)
	}
}

// Expire actively expire endpoints
func (s *Store) Expire() {
	var (
		ctx        context.Context
		cancelFunc context.CancelFunc
	)
	for {
		select {
		case leader := <-s.rkv.LeaderCh():
			if cancelFunc != nil {
				cancelFunc()
				cancelFunc = nil
			}
			if leader {
				ctx, cancelFunc = context.WithCancel(context.Background())
				qrpc.GoFunc(&s.wg, func(ctx context.Context) func() {
					return func() {
						for {
							select {
							case <-ctx.Done():
								return
							case <-time.After(gcInterval):
								// do gc
								expired := s.kv.ScanExpired()
								if len(expired) > 0 {
									s.rkv.Expire(expired)
								}

							}
						}
					}
				}(ctx))
			}
		case <-s.closeCh:
			if cancelFunc != nil {
				cancelFunc()
				cancelFunc = nil
			}
			return
		}
	}

}

// Close the Store
func (s *Store) Close() error {
	swapped := atomic.CompareAndSwapInt32(&s.closed, 0, 1)
	if !swapped {
		return nil
	}

	close(s.closeCh)
	s.wg.Wait()

	err := s.rkv.Shutdown()
	if err != nil {
		logger.Error("rkv.Shutdown", err)
		return err
	}

	return nil
}

// Watch for key change
func (s *Store) Watch() <-chan []entity.EndPointTTLsInKey {
	ch := make(chan []entity.EndPointTTLsInKey)
	s.mu.Lock()
	s.watchers = append(s.watchers, ch)
	s.mu.Unlock()

	return ch
}

// Unwatch for key change
func (s *Store) Unwatch(ch <-chan []entity.EndPointTTLsInKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, c := range s.watchers {
		if ch == c {
			s.watchers = append(s.watchers[:i], s.watchers[i+1:]...)
			break
		}
	}
}

func (s *Store) fire(mutatedKeys map[string]*AliveEndPoints) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, c := range s.watchers {
		var change []entity.EndPointTTLsInKey
		for k, v := range mutatedKeys {
			change = append(change, entity.EndPointTTLsInKey{Key: k, EndPointTTLs: v.EndPointTTLs()})
		}
		c <- change
	}
}
