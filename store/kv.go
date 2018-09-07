package store

import (
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
)

// KV is bundled with rkv
type KV struct {
	mu   sync.RWMutex
	meta map[string]string          // nodeID -> apiAddr
	data map[string]*AliveEndPoints // service:networkID -> *AliveEndPoints
	gc   *GC
}

// NewKV constructs a KV
func NewKV() *KV {
	return &KV{meta: make(map[string]string), data: make(map[string]*AliveEndPoints), gc: NewGC()}
}

// AliveEndPoints contains alive endpoints for service:networkID
type AliveEndPoints struct {
	mu           sync.Mutex
	endpointInfo map[EndPoint]*TTL
}

// Add an entry
func (a *AliveEndPoints) Add(endpoint EndPoint, ttl TTL) {
	a.mu.Lock()
	if a.endpointInfo == nil {
		a.endpointInfo = make(map[EndPoint]*TTL)
	}
	a.endpointInfo[endpoint] = &ttl
	a.mu.Unlock()
}

// Delete an entry
func (a *AliveEndPoints) Delete(endpoint EndPoint, nodeID string) {
	a.mu.Lock()

	if a.endpointInfo != nil {
		ttl, ok := a.endpointInfo[endpoint]
		if ok {
			if ttl.NodeID == nodeID {
				delete(a.endpointInfo, endpoint)
			}
		}
	}

	a.mu.Unlock()
}

// EndPoints returns all EndPoints
func (a *AliveEndPoints) EndPoints() (ret []EndPoint, shouldGC bool) {

	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.endpointInfo == nil {
		return
	}

	for endpoint, ttl := range a.endpointInfo {
		if ttl.LastUpdate.Add(ttlTimeout).Before(now) {
			shouldGC = true
		} else {
			ret = append(ret, endpoint)
		}
	}
	return
}

// EndPointTTLs returns all EndPointTTLs
func (a *AliveEndPoints) EndPointTTLs() (ret []EndPointTTL, shouldGC bool) {
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.endpointInfo == nil {
		return
	}

	for endpoint, ttl := range a.endpointInfo {
		if ttl.LastUpdate.Add(ttlTimeout).Before(now) {
			shouldGC = true
		} else {
			ret = append(ret, EndPointTTL{EndPoint: endpoint, TTL: *ttl})
		}
	}
	return
}

// TTL contains heartbeat info for endpoint
type TTL struct {
	NodeID     string
	LastUpdate time.Time
}

// NetworkEndPointTTL contains both NetworkEndPoint and TTL
type NetworkEndPointTTL struct {
	TTL             TTL
	NetworkEndPoint NetworkEndPoint
}

// NetworkEndPoint includes NetworkID
type NetworkEndPoint struct {
	NetworkID string
	EndPoint  EndPoint
}

// EndPointTTL is for resp
type EndPointTTL struct {
	TTL      TTL
	EndPoint EndPoint
}

// EndPoint for IP:Port
type EndPoint struct {
	Addr string
	Port uint16
}

// data ops

// SAdd will decode bytes then sadd
func (kv *KV) SAdd(key, value []byte) error {

	var networkEndPointTTLs []NetworkEndPointTTL
	err := bson.SliceFromBytes(value, &networkEndPointTTLs)
	if err != nil {
		return err
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, networkEndPointTTL := range networkEndPointTTLs {
		key := KeyForServiceNetwork(string(key), networkEndPointTTL.NetworkEndPoint.NetworkID)
		val, ok := kv.data[key]
		if !ok {
			val = &AliveEndPoints{}
			kv.data[key] = val
		}

		val.Add(networkEndPointTTL.NetworkEndPoint.EndPoint, networkEndPointTTL.TTL)
	}

	return nil
}

// SRem will decode bytes then srem
func (kv *KV) SRem(key, value []byte) error {
	var networkEndPointTTLs []NetworkEndPointTTL
	err := bson.SliceFromBytes(value, &networkEndPointTTLs)
	if err != nil {
		return err
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, networkEndPointTTL := range networkEndPointTTLs {
		key := KeyForServiceNetwork(string(key), networkEndPointTTL.NetworkEndPoint.NetworkID)
		val, ok := kv.data[key]
		if !ok {
			continue
		}

		val.mu.Lock()
		ttl, ok := val.endpointInfo[networkEndPointTTL.NetworkEndPoint.EndPoint]
		if ok {
			if ttl.NodeID == networkEndPointTTL.TTL.NodeID {
				delete(val.endpointInfo, networkEndPointTTL.NetworkEndPoint.EndPoint)
			}
		}
		val.mu.Unlock()
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

	return nil, nil
}

// Restore will do restore
func (kv *KV) Restore(io.ReadCloser) error {
	return nil
}
