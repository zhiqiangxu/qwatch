package store

import (
	"errors"
	"fmt"
	"strings"

	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/entity"
)

var (
	// ErrInvalidKey for invalid key
	ErrInvalidKey = errors.New("invalid key")
)

// KeyForServiceNetwork generate key for (service, networkID)
func KeyForServiceNetwork(service, networkID string) string {
	return fmt.Sprintf("%s:%s", service, networkID)
}

// Key2ServiceNetwork converts key to ServiceNetwork
func Key2ServiceNetwork(key string) (*entity.ServiceNetwork, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 2 {
		return nil, ErrInvalidKey
	}
	return &entity.ServiceNetwork{Service: parts[0], NetworkID: parts[1]}, nil
}

// GetAPIAddr returns the apiAddr for specified node
func (kv *KV) GetAPIAddr(nodeID string) string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.meta[nodeID]
}

// GetEndPoints returns all EndPoint for service:networkID
func (kv *KV) GetEndPoints(service, networkID string) []entity.EndPoint {
	key := KeyForServiceNetwork(service, networkID)
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, ok := kv.data[key]
	if !ok {
		return nil
	}
	return val.EndPoints()
}

// GetEndPointTTLs returns all EndPointTTL for service:networkID
func (kv *KV) GetEndPointTTLs(service, networkID string) []entity.EndPointTTL {
	key := KeyForServiceNetwork(service, networkID)
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, ok := kv.data[key]
	if !ok {
		return nil
	}
	return val.EndPointTTLs()
}

// ScanExpired scans for ExpiredEndPointTTLsInKey
func (kv *KV) ScanExpired() (expired []entity.ExpiredEndPointTTLsInKey) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	now := bson.Now()
	for k, v := range kv.data {
		endpointTTLs := v.ExpiredEndPointTTLs(now)
		if len(endpointTTLs) > 0 {
			expired = append(expired, entity.ExpiredEndPointTTLsInKey{Key: k, EndPointTTLs: endpointTTLs})
		}
	}

	return
}
