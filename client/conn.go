package client

import (
	"fmt"

	"github.com/zhiqiangxu/qrpc"
)

func (c *Client) handleConnectionClose(connDone <-chan struct{}) {
	qrpc.GoFunc(&c.wg, func() {
		for {
			select {
			case <-connDone:
				c.connect()
			case <-c.done:
				return
			}
		}
	})
}

func (c *Client) closeCurrentConn() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return
	}

	c.conn.Close()
	c.idx = (c.idx + 1) % len(c.endpoints)
}

func (c *Client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}

	endpoint := c.endpoints[c.idx]
	conn, err := qrpc.NewConnection(fmt.Sprintf("%s:%d", endpoint.Addr, endpoint.Port), c.conf, c.subscribe)
	if err != nil {
		return err
	}

	c.conn = conn
	return nil
}

func (c *Client) subscribe(*qrpc.Connection, *qrpc.Frame) {

}
