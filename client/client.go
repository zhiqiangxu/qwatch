package client

import (
	"errors"
	"sync"
	"time"

	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
	"github.com/zhiqiangxu/qwatch/pkg/entity"
	"github.com/zhiqiangxu/qwatch/pkg/logger"
	"github.com/zhiqiangxu/qwatch/server"
)

const (
	defaultRegInterval = time.Minute
	defaultLWInterval  = time.Second
	// DefaultNetworkID for default NetworkID
	DefaultNetworkID = "qw"
)

var (
	// ErrClosed when operate on closed client
	ErrClosed = errors.New("Client closed")
)

// Client for qwatch
type Client struct {
	wg           sync.WaitGroup
	regInterlval time.Duration
	lwInterlval  time.Duration
	conn         *qrpc.Connection

	mu         sync.RWMutex
	lwInfo     map[entity.ServiceNetwork]map[entity.EndPoint]struct{}
	pushInfo   map[entity.ServiceNetwork]map[entity.EndPoint]struct{}
	pushNotify map[entity.ServiceNetwork][]func(entity.LWPushResp)
}

// New creates a qwatch client
func New(servers []string, conf qrpc.ConnectionConfig) *Client {
	client := &Client{
		regInterlval: defaultRegInterval, lwInterlval: defaultLWInterval,
		lwInfo:     make(map[entity.ServiceNetwork]map[entity.EndPoint]struct{}),
		pushInfo:   make(map[entity.ServiceNetwork]map[entity.EndPoint]struct{}),
		pushNotify: make(map[entity.ServiceNetwork][]func(entity.LWPushResp))}
	conn := qrpc.NewConnectionWithReconnect(servers, conf, client.subFunc)
	client.conn = conn

	return client
}

// called for pushed result
// always treat pushes result as newer than list result
func (c *Client) subFunc(conn *qrpc.Connection, frame *qrpc.Frame) {
	var lwPushResp entity.LWPushResp
	err := bson.FromBytes(frame.Payload, &lwPushResp)
	if err != nil {
		logger.Error("lwPushResp FromBytes", err)
		return
	}

	c.handleLWPushResp(lwPushResp)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range c.pushNotify[lwPushResp.ServiceNetwork] {
		f(lwPushResp)
	}
}

// RegisterService do registeration for service
func (c *Client) RegisterService(regCmd entity.RegCmd, f func(RegResponse)) {
	qrpc.GoFunc(&c.wg, func() {
		c.registerService(&regCmd, f)
		for {
			select {
			case <-c.conn.Done():
				return
			case <-time.After(c.regInterlval):
				c.registerService(&regCmd, f)
			}
		}
	})
}

func (c *Client) registerService(regCmd *entity.RegCmd, f func(RegResponse)) {
	logger.Debug("registerService1")
	bytes, err := bson.ToBytes(regCmd)
	if err != nil {
		f(RegResponse{Err: err})
		return
	}

	logger.Debug("registerService2")
	_, resp, err := c.conn.Request(server.RegCmd, 0, bytes)
	if err != nil {
		f(RegResponse{Err: err})
		return
	}

	logger.Debug("registerService3")
	frame, err := resp.GetFrame()
	if err != nil {
		f(RegResponse{Err: err})
		return
	}

	logger.Debug("registerService4")
	var regResp entity.RegResp
	err = bson.FromBytes(frame.Payload, &regResp)
	if err != nil {
		f(RegResponse{Err: err})
		return
	}

	f(RegResponse{Data: regResp})

}

// ListWatch for service
func (c *Client) ListWatch(lwCmd entity.LWCmd, f func(LWResponse), fp func(entity.LWPushResp)) {
	c.mu.Lock()
	for _, serviceNetwork := range lwCmd {
		sn := entity.ServiceNetwork{Service: serviceNetwork.Service, NetworkID: serviceNetwork.NetworkID}
		c.pushNotify[sn] = append(c.pushNotify[sn], fp)
	}
	c.mu.Unlock()

	qrpc.GoFunc(&c.wg, func() {
		c.listWatch(&lwCmd, f, fp)
		for {
			select {
			case <-c.conn.Done():
				return
			case <-time.After(c.lwInterlval):
				c.listWatch(&lwCmd, f, fp)
			}
		}
	})
}

func (c *Client) handleLWResp(lwResp entity.LWResp) {
	c.mu.Lock()
	for _, serviceNetworkEndpoints := range lwResp {
		sn := serviceNetworkEndpoints.ServiceNetwork
		endpointMap := make(map[entity.EndPoint]struct{})
		for _, endpoint := range serviceNetworkEndpoints.EndPoints {
			endpointMap[endpoint] = struct{}{}
		}
		c.lwInfo[sn] = endpointMap
	}
	c.mu.Unlock()
}

func (c *Client) handleLWPushResp(lwPushResp entity.LWPushResp) {
	endpointMap := make(map[entity.EndPoint]struct{})
	c.mu.Lock()
	for _, endpoint := range lwPushResp.EndPoints {
		endpointMap[endpoint] = struct{}{}
	}
	c.pushInfo[lwPushResp.ServiceNetwork] = endpointMap
	c.mu.Unlock()
}

// EndPoints returns endpoints for ServiceNetwork
func (c *Client) EndPoints(sn entity.ServiceNetwork) (ret []entity.EndPoint) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	pushInfo := c.pushInfo[sn]
	if pushInfo != nil {
		for k := range pushInfo {
			ret = append(ret, k)
		}
		return
	}

	lwInfo := c.lwInfo[sn]
	if lwInfo == nil {
		return
	}

	for k := range lwInfo {
		ret = append(ret, k)
	}
	return
}

func (c *Client) listWatch(lwCmd *entity.LWCmd, f func(LWResponse), fp func(entity.LWPushResp)) {
	bytes, err := bson.ToBytes(lwCmd)
	if err != nil {
		f(LWResponse{Err: err})
		return
	}

	_, resp, err := c.conn.Request(server.LWCmd, 0, bytes)
	if err != nil {
		f(LWResponse{Err: err})
		return
	}

	frame, err := resp.GetFrame()
	if err != nil {
		f(LWResponse{Err: err})
		return
	}

	var lwResp entity.LWResp
	err = bson.FromBytes(frame.Payload, &lwResp)
	if err != nil {
		f(LWResponse{Err: err})
		return
	}

	c.handleLWResp(lwResp)
	f(LWResponse{Data: lwResp})

	// wait for connection close
	<-frame.FrameCh()
}

// Close client
func (c *Client) Close() error {
	return c.conn.Close()
}

// Wait for all goroutines to quit
func (c *Client) Wait() {
	c.wg.Wait()
}
