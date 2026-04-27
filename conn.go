package zws

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"nhooyr.io/websocket"
)

// Conn 包装 WebSocket 连接
type Conn struct {
	id     string
	ws     *websocket.Conn
	hub    *Hub
	codec  MessageCodec
	ctx    context.Context
	cancel context.CancelFunc
	send   chan []byte
	once   sync.Once
	mu     sync.RWMutex
	closed atomic.Bool
	// 用户自定义数据
	metadata map[string]any
}

// NewConn 创建新的连接包装
func NewConn(id string, ws *websocket.Conn, hub *Hub, codec MessageCodec) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &Conn{
		id:       id,
		ws:       ws,
		hub:      hub,
		codec:    codec,
		ctx:      ctx,
		cancel:   cancel,
		send:     make(chan []byte, DefaultSendBufferSize),
		metadata: make(map[string]any),
	}
}

// ID 返回连接 ID
func (c *Conn) ID() string {
	return c.id
}

// Context 返回连接上下文
func (c *Conn) Context() context.Context {
	return c.ctx
}

// Set 设置元数据
func (c *Conn) Set(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metadata[key] = value
}

// Get 获取元数据
func (c *Conn) Get(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.metadata[key]
	return v, ok
}

// Send 发送原始字节数据
func (c *Conn) Send(data []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrConnClosed
		}
	}()

	if c.closed.Load() {
		return ErrConnClosed
	}

	// 设置 5 秒超时，防止永久阻塞
	sendTimeout := 5 * time.Second
	if c.hub != nil && c.hub.config.WriteBufferSize > 0 {
		// 可以根据配置调整，这里暂时使用固定超时
	}

	select {
	case c.send <- data:
		return nil
	case <-time.After(sendTimeout):
		return fmt.Errorf("send timeout: channel full")
	case <-c.ctx.Done():
		return ErrConnClosed
	}
}

// JSON 发送 JSON 数据
func (c *Conn) JSON(v any) error {
	data, err := c.codec.Encode(v)
	if err != nil {
		return err
	}
	return c.Send(data)
}

// Close 关闭连接
func (c *Conn) Close() error {
	if c.closed.Load() {
		return ErrConnClosed
	}

	c.once.Do(func() {
		c.closed.Store(true)
		c.cancel()
		if c.hub != nil {
			c.hub.Unregister(c)
		}
		if c.ws != nil {
			c.ws.Close(websocket.StatusNormalClosure, "")
		}
	})

	return nil
}

// IsClosed 检查连接是否已关闭
func (c *Conn) IsClosed() bool {
	return c.closed.Load()
}

func (c *Conn) reportError(err error) {
	if c.hub != nil {
		c.hub.reportError(c, err)
	}
}

// writePump 写入循环
func (c *Conn) writePump() {
	interval := DefaultPingInterval
	pingWait := DefaultPingWait
	if c.hub != nil && c.hub.config != nil {
		interval = c.hub.config.PingInterval
		pingWait = c.hub.config.PingWait
	}

	var tickerC <-chan time.Time
	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		tickerC = ticker.C
	}
	defer c.Close()

	for {
		select {
		case data := <-c.send:
			if err := c.write(data); err != nil {
				c.reportError(err)
				return
			}
		case <-tickerC:
			// 心跳 ping
			if c.ws != nil {
				ctx, cancel := context.WithTimeout(c.ctx, pingWait)
				err := c.ws.Ping(ctx)
				cancel()
				if err != nil {
					c.reportError(err)
					return
				}
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// write 写入数据
func (c *Conn) write(data []byte) error {
	if c.ws == nil {
		return fmt.Errorf("websocket connection is nil")
	}
	ctx, cancel := context.WithTimeout(c.ctx, DefaultWriteTimeout)
	defer cancel()
	return c.ws.Write(ctx, websocket.MessageText, data)
}

// readPump 读取循环
func (c *Conn) readPump(handler func(*Conn, []byte)) {
	defer c.Close()

	if c.ws == nil {
		c.reportError(fmt.Errorf("websocket connection is nil"))
		return
	}

	for {
		_, data, err := c.ws.Read(c.ctx)
		if err != nil {
			c.reportError(err)
			break
		}
		if handler != nil {
			handler(c, data)
		}
	}
}
