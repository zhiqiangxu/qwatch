package client

// RegCmd is struct for register
type RegCmd struct {
	Service string
	Addrs   []string
}

// LWCmd is struct for list and watch
type LWCmd struct {
	Services []string
}

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

// NotifyLeaderGCResp is resp for NotifyLeaderGCCmd
type NotifyLeaderGCResp struct {
	OK  bool
	Msg string
}
