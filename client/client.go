package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sohaha/zws"
	"nhooyr.io/websocket"
)

// Client WebSocket 客户端
type Client struct {
	id       string
	url      string
	header   http.Header
	config   *zws.ClientConfig
	conn     *websocket.Conn
	hub      *clientHub
	once     sync.Once
	closed   atomic.Bool
	ctx      context.Context
	cancel   context.CancelFunc
	handlers *Handlers
	mu       sync.RWMutex
}

// Handlers 客户端事件回调
type Handlers struct {
	// OnConnect 连接建立时回调
	OnConnect func(*Client)
	// OnMessage 收到消息时回调
	OnMessage func(*Client, []byte)
	// OnDisconnect 连接断开时回调
	OnDisconnect func(*Client)
	// OnError 发生错误时回调
	OnError func(*Client, error)
}

// NewClient 创建新的客户端
func NewClient(url string, config *zws.ClientConfig) (*Client, error) {
	config = zws.NormalizeClientConfig(config)

	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		url:      url,
		config:   config,
		ctx:      ctx,
		cancel:   cancel,
		handlers: &Handlers{},
		hub:      newClientHub(),
	}

	return c, nil
}

// Connect 连接到服务器
func (c *Client) Connect() error {
	if c.closed.Load() {
		return zws.ErrConnClosed
	}

	_, err := url.Parse(c.url)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	ctx, cancel := context.WithTimeout(c.ctx, c.config.HandshakeTimeout)
	defer cancel()

	c.mu.RLock()
	header := c.header.Clone()
	c.mu.RUnlock()

	opts := &websocket.DialOptions{
		HTTPHeader: header,
	}

	// 注意: nhooyr.io/websocket 默认会验证 TLS 证书
	// 对于 wss:// 连接，它将使用标准库的 net/http 并自动验证证书
	// 不需要手动设置 TLSConfig

	conn, _, err := websocket.Dial(ctx, c.url, opts)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	// 启动心跳
	if c.config.PingInterval > 0 {
		go c.startPing()
	}

	// 启动读取循环
	go c.readPump()

	// 启动写入循环
	go c.writePump()

	c.mu.RLock()
	onConnect := c.handlers.OnConnect
	c.mu.RUnlock()
	if onConnect != nil {
		onConnect(c)
	}

	return nil
}

// Close 关闭连接
func (c *Client) Close() error {
	if c.closed.Load() {
		return zws.ErrConnClosed
	}

	c.once.Do(func() {
		c.closed.Store(true)
		c.cancel()
		c.hub.close()

		c.mu.Lock()
		if c.conn != nil {
			c.conn.Close(websocket.StatusNormalClosure, "")
		}
		c.mu.Unlock()

		onDisconnect := c.onDisconnect()
		if onDisconnect != nil {
			onDisconnect(c)
		}
	})

	return nil
}

// IsClosed 检查连接是否已关闭
func (c *Client) IsClosed() bool {
	return c.closed.Load()
}

// Send 发送消息
func (c *Client) Send(data []byte) error {
	if c.closed.Load() {
		return zws.ErrConnClosed
	}
	return c.hub.send(data)
}

// JSON 发送 JSON 消息
func (c *Client) JSON(v any) error {
	data, err := c.config.Codec.Encode(v)
	if err != nil {
		return err
	}
	return c.Send(data)
}

// SetHeader 设置请求头
func (c *Client) SetHeader(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.header == nil {
		c.header = make(http.Header)
	}
	c.header.Set(key, value)
}

// OnConnect 设置连接回调
func (c *Client) OnConnect(fn func(*Client)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers.OnConnect = fn
}

// OnMessage 设置消息回调
func (c *Client) OnMessage(fn func(*Client, []byte)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers.OnMessage = fn
}

// OnDisconnect 设置断开回调
func (c *Client) OnDisconnect(fn func(*Client)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers.OnDisconnect = fn
}

// OnError 设置错误回调
func (c *Client) OnError(fn func(*Client, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers.OnError = fn
}

func (c *Client) onMessage() func(*Client, []byte) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.handlers.OnMessage
}

func (c *Client) onDisconnect() func(*Client) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.handlers.OnDisconnect
}

func (c *Client) onError() func(*Client, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.handlers.OnError
}

// readPump 读取循环
func (c *Client) readPump() {
	defer c.Close()

	for {
		_, data, err := c.conn.Read(c.ctx)
		if err != nil {
			onError := c.onError()
			if onError != nil {
				onError(c, err)
			}
			break
		}

		onMessage := c.onMessage()
		if onMessage != nil {
			onMessage(c, data)
		}
	}
}

// writePump 写入循环
func (c *Client) writePump() {
	for {
		select {
		case data, ok := <-c.hub.outbox:
			if !ok {
				return
			}
			if err := c.write(data); err != nil {
				onError := c.onError()
				if onError != nil {
					onError(c, err)
				}
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// write 写入数据
func (c *Client) write(data []byte) error {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	return c.conn.Write(ctx, websocket.MessageText, data)
}

// startPing 启动心跳
func (c *Client) startPing() {
	ticker := time.NewTicker(c.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(c.ctx, c.config.PingWait)
			if err := c.conn.Ping(ctx); err != nil {
				cancel()
				c.Close()
				return
			}
			cancel()
		case <-c.ctx.Done():
			return
		}
	}
}
