package store

import (
	"io"
	"io/ioutil"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/entity"
	"github.com/zhiqiangxu/qwatch/pkg/logger"
)

// KV is bundled with rkv
type KV struct {
	store *Store
	mu    sync.RWMutex
	meta  map[string]string          // nodeID -> apiAddr
	data  map[string]*AliveEndPoints // service:networkID -> *AliveEndPoints
}

// NewKV constructs a KV
func NewKV(store *Store) *KV {
	return &KV{store: store, meta: make(map[string]string), data: make(map[string]*AliveEndPoints)}
}

// AliveEndPoints contains alive endpoints for service:networkID
type AliveEndPoints struct {
	mu           sync.Mutex
	endpointInfo map[entity.EndPoint]*entity.TTL
}

// NewAliveEndPoints constructs an AliveEndPoints
func NewAliveEndPoints() *AliveEndPoints {
	return &AliveEndPoints{endpointInfo: make(map[entity.EndPoint]*entity.TTL)}
}

// Add an entry
func (a *AliveEndPoints) Add(endpoint entity.EndPoint, ttl entity.TTL) {
	a.mu.Lock()
	a.endpointInfo[endpoint] = &ttl
	a.mu.Unlock()
}

// Delete an entry
func (a *AliveEndPoints) Delete(endpoint entity.EndPoint, nodeID string) {
	a.mu.Lock()

	ttl, ok := a.endpointInfo[endpoint]
	if ok {
		if ttl.NodeID == nodeID {
			delete(a.endpointInfo, endpoint)
		}
	}

	a.mu.Unlock()
}

// EndPoints returns all EndPoints
func (a *AliveEndPoints) EndPoints() (ret []entity.EndPoint) {

	now := bson.Now()
	a.mu.Lock()
	defer a.mu.Unlock()

	for endpoint, ttl := range a.endpointInfo {
		if ttl.LastUpdate.Add(ttlTimeout).After(now) {
			ret = append(ret, endpoint)
		}
	}
	return
}

// EndPointTTLs returns all EndPointTTLs
func (a *AliveEndPoints) EndPointTTLs() (ret []entity.EndPointTTL) {
	now := bson.Now()
	a.mu.Lock()
	defer a.mu.Unlock()

	for endpoint, ttl := range a.endpointInfo {
		if ttl.LastUpdate.Add(ttlTimeout).After(now) {
			ret = append(ret, entity.EndPointTTL{EndPoint: endpoint, TTL: *ttl})
		}
	}
	return
}

// Expire pre-filtered expiredEndPointTTLs
func (a *AliveEndPoints) Expire(expiredEndPointTTLs []entity.EndPointTTL) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, endpointTTL := range expiredEndPointTTLs {
		if *a.endpointInfo[endpointTTL.EndPoint] == endpointTTL.TTL {
			delete(a.endpointInfo, endpointTTL.EndPoint)
		}
	}

}

// ExpiredEndPointTTLs returns expred EndPointTTL
func (a *AliveEndPoints) ExpiredEndPointTTLs(target time.Time) (ret []entity.EndPointTTL) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for k, v := range a.endpointInfo {
		if v.LastUpdate.Add(ttlTimeout).Before(target) {
			ret = append(ret, entity.EndPointTTL{TTL: *v, EndPoint: k})
		}
	}
	return nil
}

// Clone returns a copy of AliveEndPoints
func (a *AliveEndPoints) Clone() (clone *AliveEndPoints) {
	clone = NewAliveEndPoints()
	a.mu.Lock()
	defer a.mu.Unlock()

	for k, v := range a.endpointInfo {
		clone.endpointInfo[k] = &*v
	}
	return
}

// data ops

// SAdd will decode bytes then sadd
func (kv *KV) SAdd(key, value []byte) error {

	var networkEndPointTTLs []entity.NetworkEndPointTTL
	err := bson.SliceFromBytes(value, &networkEndPointTTLs)
	if err != nil {
		return err
	}

	mutatedKeys := make(map[string]*AliveEndPoints)
	kv.mu.Lock()
	for _, networkEndPointTTL := range networkEndPointTTLs {
		key := KeyForServiceNetwork(string(key), networkEndPointTTL.NetworkEndPoint.NetworkID)
		val, ok := kv.data[key]
		if !ok {
			val = NewAliveEndPoints()
			kv.data[key] = val
		}

		val.Add(networkEndPointTTL.NetworkEndPoint.EndPoint, networkEndPointTTL.TTL)
		mutatedKeys[string(key)] = val.Clone()
	}
	kv.mu.Unlock()

	if len(mutatedKeys) > 0 {
		kv.store.fire(mutatedKeys)
	}
	return nil
}

// SRem will decode bytes then srem
func (kv *KV) SRem(key, value []byte) error {
	var networkEndPointTTLs []entity.NetworkEndPointTTL
	err := bson.SliceFromBytes(value, &networkEndPointTTLs)
	if err != nil {
		return err
	}

	mutatedKeys := make(map[string]*AliveEndPoints)
	kv.mu.Lock()
	for _, networkEndPointTTL := range networkEndPointTTLs {
		key := KeyForServiceNetwork(string(key), networkEndPointTTL.NetworkEndPoint.NetworkID)
		val, ok := kv.data[key]
		if !ok {
			continue
		}

		val.Delete(networkEndPointTTL.NetworkEndPoint.EndPoint, networkEndPointTTL.TTL.NodeID)
		mutatedKeys[string(key)] = val.Clone()
	}
	kv.mu.Unlock()

	if len(mutatedKeys) > 0 {
		kv.store.fire(mutatedKeys)
	}
	return nil
}

// Expire will remove ExpiredEndPointTTLsInKey
func (kv *KV) Expire(value []byte) error {
	var expiredEndPointTTLsInKeys []entity.ExpiredEndPointTTLsInKey
	err := bson.SliceFromBytes(value, &expiredEndPointTTLsInKeys)
	if err != nil {
		return err
	}

	mutatedKeys := make(map[string]*AliveEndPoints)
	kv.mu.Lock()
	for _, expiredEndPointTTLsInKey := range expiredEndPointTTLsInKeys {
		aliveEndPoints, ok := kv.data[expiredEndPointTTLsInKey.Key]
		if !ok {
			continue
		}
		aliveEndPoints.Expire(expiredEndPointTTLsInKey.EndPointTTLs)
		mutatedKeys[expiredEndPointTTLsInKey.Key] = aliveEndPoints.Clone()
	}
	kv.mu.Unlock()

	if len(mutatedKeys) > 0 {
		kv.store.fire(mutatedKeys)
	}
	return nil
}

// meta ops

// SetAPIAddr will store api addr
func (kv *KV) SetAPIAddr(key, value []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.meta[string(key)] = string(value)
	return nil
}

// raft ops

// SnapShot will do snapshot
func (kv *KV) SnapShot() (raft.FSMSnapshot, error) {

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	meta := make(map[string]string)
	data := make(map[string]*AliveEndPoints)

	for k, v := range kv.meta {
		meta[k] = v
	}
	for k, v := range kv.data {
		data[k] = v.Clone()
	}
	return &kvSnapshot{meta: meta, data: data}, nil
}

// Restore will do restore
func (kv *KV) Restore(rc io.ReadCloser) error {

	bytes, err := ioutil.ReadAll(rc)
	if err != nil {
		logger.Error("Restore ReadAll", err)
		return err
	}
	var snapshot kvSnapshot
	err = bson.FromBytes(bytes, &snapshot)
	if err != nil {
		return err
	}
	kv.mu.Lock()
	kv.meta = snapshot.meta
	kv.data = snapshot.data
	if kv.meta == nil {
		kv.meta = make(map[string]string)
	}
	if kv.data == nil {
		kv.data = make(map[string]*AliveEndPoints)
	}
	kv.mu.Unlock()
	snapshot.meta = nil
	snapshot.data = nil

	return nil
}

type kvSnapshot struct {
	meta map[string]string
	data map[string]*AliveEndPoints
}

func (kvs *kvSnapshot) Persist(sink raft.SnapshotSink) error {

	err := func() error {
		// Encode data.
		bytes, err := bson.ToBytes(kvs)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(bytes); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (kvs *kvSnapshot) Release() {

}
