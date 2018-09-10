package client

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/entity"
	"github.com/zhiqiangxu/qwatch/server"
)

// Client for qwatch
type Client struct {
	endpoints []entity.EndPoint
	conf      qrpc.ConnectionConfig

	wg   sync.WaitGroup
	done chan struct{}
	mu   sync.Mutex
	idx  int
	conn *qrpc.Connection
}

// New creates a qwatch client
func New(endpoints []entity.EndPoint, conf qrpc.ConnectionConfig) *Client {
	var copy []entity.EndPoint
	for _, endpoint := range endpoints {
		copy = append(copy, endpoint)
	}
	rand.Shuffle(len(copy), func(i, j int) {
		copy[i], copy[j] = copy[j], copy[i]
	})

	return &Client{conf: conf, endpoints: copy, done: make(chan struct{})}
}

func (c *Client) getConn() *qrpc.Connection {
	return nil
}

// RegisterService do registeration for service
func (c *Client) RegisterService(regCmd entity.RegCmd) error {

	bytes, err := bson.ToBytes(regCmd)
	if err != nil {
		return err
	}
	conn := c.getConn()
	_, resp, err := conn.Request(server.RegCmd, 0, bytes)
	if err != nil {
		return err
	}
	frame, err := resp.GetFrame()
	if err != nil {
		return err
	}
	var regResp entity.RegResp
	err = bson.FromBytes(frame.Payload, &regResp)
	if err != nil {
		return err
	}
	if regResp.OK {
		return nil
	}

	return fmt.Errorf(regResp.Msg)
}

// ListWatch for service
func (c *Client) ListWatch(lwCmd entity.LWCmd) (resp entity.LWResp, pushCh <-chan entity.LWPushResp, err error) {
	return
}
