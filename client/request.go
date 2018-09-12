package client

import "github.com/zhiqiangxu/qwatch/pkg/entity"

// RegRequest for RegCmd
type RegRequest struct {
	Data   entity.RegCmd
	RespCh chan RegResponse
}

// RegResponse for RegRequest
type RegResponse struct {
	Data entity.RegResp
	Err  error
}

// LWRequest for LWCmd
type LWRequest struct {
	Data   entity.LWCmd
	RespCh chan LWResponse
}

// LWResponse for LWRequest
type LWResponse struct {
	Data entity.LWResp
	Err  error
}

// LWPushResponse for pushed response
type LWPushResponse struct {
	Data entity.LWPushResp
	Err  error
}
