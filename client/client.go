package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zlsgo/zws"
	"nhooyr.io/websocket"
)

// Client WebSocket 客户端
type Client struct {
	id         string
	url        string
	header     http.Header
	config     *zws.ClientConfig
	conn       *websocket.Conn
	hub        *clientHub
	once       sync.Once
	closed     atomic.Bool
	ctx        context.Context
	cancel     context.CancelFunc
	connCtx    context.Context    // 连接级别的上下文
	connCancel context.CancelFunc // 取消连接级别的上下文
	handlers   *Handlers
	mu         sync.RWMutex
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
	// OnConnectionLoss 连接丢失时回调（可恢复的断线）
	OnConnectionLoss func(*Client, error)
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

	// 清理旧连接资源
	c.mu.Lock()
	if c.connCancel != nil {
		c.connCancel()
	}
	if c.conn != nil {
		c.conn.Close(websocket.StatusGoingAway, "reconnecting")
	}

	// 创建新的连接级别上下文
	connCtx, connCancel := context.WithCancel(c.ctx)
	c.conn = conn
	c.connCtx = connCtx
	c.connCancel = connCancel
	c.mu.Unlock()

	// 启动读取循环
	go c.readPump()

	// 启动写入循环
	go c.writePump()

	// 启动心跳
	if c.config.PingInterval > 0 {
		go c.startPing()
	}

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
		// 清理连接级别的上下文
		if c.connCancel != nil {
			c.connCancel()
			c.connCancel = nil
		}
		if c.conn != nil {
			c.conn.Close(websocket.StatusNormalClosure, "")
			c.conn = nil
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

// OnConnectionLoss 设置连接丢失回调
func (c *Client) OnConnectionLoss(fn func(*Client, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers.OnConnectionLoss = fn
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

func (c *Client) onConnectionLoss() func(*Client, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.handlers.OnConnectionLoss
}

// readPump 读取循环
func (c *Client) readPump() {
	// 获取当前连接的上下文
	c.mu.RLock()
	connCtx := c.connCtx
	c.mu.RUnlock()

	if connCtx == nil {
		return
	}

	for {
		select {
		case <-connCtx.Done():
			// 连接级别上下文被取消，退出读取循环
			return
		default:
		}

		_, data, err := c.conn.Read(connCtx)
		if err != nil {
			// 检查是否是连接级别的上下文取消
			if connCtx.Err() != nil {
				return
			}

			// 检查是否是 Logical Session 被关闭
			if c.closed.Load() {
				return
			}

			// 这是 Connection Loss
			onConnectionLoss := c.onConnectionLoss()
			if onConnectionLoss != nil {
				onConnectionLoss(c, err)
			}

			// 检查是否应该重连
			if c.config.Reconnect && IsRecoverableError(err) {
				policy := NewDefaultReconnectPolicy(c.config)
				go c.reconnectLoop(c.ctx, policy, err)
			} else {
				// 不重连，触发 OnError 并关闭
				onError := c.onError()
				if onError != nil {
					onError(c, err)
				}
				c.Close()
			}
			return
		}

		onMessage := c.onMessage()
		if onMessage != nil {
			onMessage(c, data)
		}
	}
}

// writePump 写入循环
func (c *Client) writePump() {
	// 获取当前连接的上下文
	c.mu.RLock()
	connCtx := c.connCtx
	c.mu.RUnlock()

	if connCtx == nil {
		return
	}

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
		case <-connCtx.Done():
			// 连接级别上下文被取消，退出写入循环
			return
		case <-c.ctx.Done():
			// Logical Session 被关闭
			return
		}
	}
}

// write 写入数据
func (c *Client) write(data []byte) error {
	ctx, cancel := context.WithTimeout(c.connCtx, 5*time.Second)
	defer cancel()
	return c.conn.Write(ctx, websocket.MessageText, data)
}

// startPing 启动心跳
func (c *Client) startPing() {
	// 获取当前连接的上下文
	c.mu.RLock()
	connCtx := c.connCtx
	c.mu.RUnlock()

	if connCtx == nil {
		return
	}

	ticker := time.NewTicker(c.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(connCtx, c.config.PingWait)
			if err := c.conn.Ping(ctx); err != nil {
				cancel()
				c.Close()
				return
			}
			cancel()
		case <-connCtx.Done():
			// 连接级别上下文被取消，退出心跳
			return
		case <-c.ctx.Done():
			// Logical Session 被关闭
			return
		}
	}
}
