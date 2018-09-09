package entity

import "time"

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

// ExpiredEndPointTTLsInKey is what the name indicates
type ExpiredEndPointTTLsInKey struct {
	Key          string
	EndPointTTLs []EndPointTTL
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
