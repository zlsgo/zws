package zws

import (
	"context"
	"time"

	"nhooyr.io/websocket"
)

// WebSocketReader 定义读取操作的接口
type WebSocketReader interface {
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
}

// WebSocketWriter 定义写入操作的接口
type WebSocketWriter interface {
	Write(ctx context.Context, typ websocket.MessageType, p []byte) error
	Ping(ctx context.Context) error
}

// WebSocketConn 组合读写接口
type WebSocketConn interface {
	WebSocketReader
	WebSocketWriter
}

// ConnectionAdapter 读写循环适配器
type ConnectionAdapter struct {
	conn      WebSocketConn
	ctx       context.Context
	onRead    func([]byte)    // 读取到数据时的回调
	onReadErr func(error)     // 读取错误时的回调
	onWrite   func([]byte) bool // 写入数据时的回调，返回 true 表示继续，false 表示停止
	onWriteErr func(error)    // 写入错误时的回调
	onClose   func()          // 关闭时的回调

	// Ping 配置
	pingInterval time.Duration
	pingWait     time.Duration
	onPing       func(error) // ping 错误时的回调
}

// NewConnectionAdapter 创建新的适配器
func NewConnectionAdapter(conn WebSocketConn, ctx context.Context) *ConnectionAdapter {
	return &ConnectionAdapter{
		conn: conn,
		ctx:  ctx,
	}
}

// SetReadHandler 设置读取相关回调
func (a *ConnectionAdapter) SetReadHandler(onRead func([]byte), onReadErr func(error)) {
	a.onRead = onRead
	a.onReadErr = onReadErr
}

// SetWriteHandler 设置写入相关回调
func (a *ConnectionAdapter) SetWriteHandler(onWrite func([]byte) bool, onWriteErr func(error)) {
	a.onWrite = onWrite
	a.onWriteErr = onWriteErr
}

// SetCloseHandler 设置关闭回调
func (a *ConnectionAdapter) SetCloseHandler(onClose func()) {
	a.onClose = onClose
}

// SetPingConfig 设置心跳配置
func (a *ConnectionAdapter) SetPingConfig(interval, wait time.Duration, onPing func(error)) {
	a.pingInterval = interval
	a.pingWait = wait
	a.onPing = onPing
}

// ReadPump 启动读取循环
func (a *ConnectionAdapter) ReadPump() {
	if a.onClose != nil {
		defer a.onClose()
	}

	for {
		_, data, err := a.conn.Read(a.ctx)
		if err != nil {
			if a.onReadErr != nil {
				a.onReadErr(err)
			}
			return
		}

		if a.onRead != nil {
			a.onRead(data)
		}
	}
}

// WritePumpWithChannel 带有 channel 的写入循环
func (a *ConnectionAdapter) WritePumpWithChannel(send <-chan []byte) {
	if a.onClose != nil {
		defer a.onClose()
	}

	var tickerC <-chan time.Time
	if a.pingInterval > 0 {
		ticker := time.NewTicker(a.pingInterval)
		defer ticker.Stop()
		tickerC = ticker.C
	}

	for {
		select {
		case data := <-send:
			if err := a.writeData(data); err != nil {
				if a.onWriteErr != nil {
					a.onWriteErr(err)
				}
				return
			}
		case <-tickerC:
			// 心跳 ping
			if a.conn != nil && a.onPing != nil {
				ctx, cancel := context.WithTimeout(a.ctx, a.pingWait)
				err := a.conn.Ping(ctx)
				cancel()
				a.onPing(err)
				if err != nil {
					return
				}
			}
		case <-a.ctx.Done():
			return
		}
	}
}

// writeData 执行实际的写入操作
func (a *ConnectionAdapter) writeData(data []byte) error {
	ctx, cancel := context.WithTimeout(a.ctx, DefaultWriteTimeout)
	defer cancel()
	return a.conn.Write(ctx, websocket.MessageText, data)
}
