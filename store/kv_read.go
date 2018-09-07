package store

import "fmt"

// KeyForServiceNetwork generate key for (service, networkID)
func KeyForServiceNetwork(service, networkID string) string {
	return fmt.Sprintf("%s:%s", service, networkID)
}

// GetAPIAddr returns the apiAddr for specified node
func (kv *KV) GetAPIAddr(nodeID string) string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.meta[nodeID]
}

// GetEndPoints returns all EndPoint for service:networkID
func (kv *KV) GetEndPoints(service, networkID string) []EndPoint {
	key := KeyForServiceNetwork(service, networkID)
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, ok := kv.data[key]
	if !ok {
		return nil
	}
	ret, shouldGC := val.EndPoints()
	if shouldGC {
		kv.gc.Put(GcRequest{NetworkID: networkID, Service: service})
	}
	return ret
}

// GetEndPointTTLs returns all EndPointTTL for service:networkID
func (kv *KV) GetEndPointTTLs(service, networkID string) []EndPointTTL {
	key := KeyForServiceNetwork(service, networkID)
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, ok := kv.data[key]
	if !ok {
		return nil
	}
	ret, shouldGC := val.EndPointTTLs()
	if shouldGC {
		kv.gc.Put(GcRequest{NetworkID: networkID, Service: service})
	}
	return ret
}
