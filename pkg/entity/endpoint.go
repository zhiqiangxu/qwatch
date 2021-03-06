package entity

import "time"

// TTL contains heartbeat info for endpoint
type TTL struct {
	NodeID     string
	LastUpdate time.Time
}

// ServiceNetwork contains (Service, NetworkID)
type ServiceNetwork struct {
	Service   string
	NetworkID string
}

// ServiceNetworkEndPoints contains EndPoints per ServiceNetwork
type ServiceNetworkEndPoints struct {
	ServiceNetwork ServiceNetwork
	EndPoints      []EndPoint
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

// ExpiredEndPointTTLsInKey is what the name indicates
type ExpiredEndPointTTLsInKey struct {
	Key          string
	EndPointTTLs []EndPointTTL
}

// EndPointsInKey contains all stuff under a key
type EndPointsInKey struct {
	Key       string
	EndPoints []EndPoint
}

// EndPointTTL is for resp
type EndPointTTL struct {
	TTL      TTL
	EndPoint EndPoint
}

// EndPoint for IP:Port
type EndPoint string
