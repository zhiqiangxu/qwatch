package entity

// RegCmd is struct for register
type RegCmd struct {
	Service          []byte
	NetworkEndPoints []NetworkEndPoint
}

// RegResp is resp for RegCmd
type RegResp struct {
	OK  bool
	Msg string
}

// LWCmd is struct for list and watch
type LWCmd []ServiceNetwork

// LWResp is resp for LWCmd
type LWResp []ServiceNetworkEndPoints

// LWPushResp is pushed, once for each ServiceNetwork
type LWPushResp ServiceNetworkEndPoints

// JoinCmd tries to add a new node to raft cluster
type JoinCmd struct {
	NodeID   string
	RaftAddr string
}

// JoinResp is resp for join
type JoinResp struct {
	OK  bool
	Msg string
}

// SetAPIAddrCmd set apiAddr for node
type SetAPIAddrCmd struct {
	NodeID  string
	APIAddr string
}

// SetAPIAddrResp is resp for SetAPIAddrCmd
type SetAPIAddrResp struct {
	OK  bool
	Msg string
}
