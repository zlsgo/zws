package zws

import (
	"context"
	"sync"
	"time"
)

// PingManager 管理心跳检测
type PingManager struct {
	hub       *Hub
	interval  time.Duration
	wait      time.Duration
	pingMsg   any
	stop      chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	mu        sync.RWMutex
	conns     map[string]context.CancelFunc
}

// NewPingManager 创建心跳管理器
func NewPingManager(hub *Hub, interval, wait time.Duration, pingMsg any) *PingManager {
	return &PingManager{
		hub:      hub,
		interval: interval,
		wait:     wait,
		pingMsg:  pingMsg,
		stop:     make(chan struct{}),
		conns:    make(map[string]context.CancelFunc),
	}
}

// Start 启动心跳检测
func (p *PingManager) Start() {
	if p.interval <= 0 {
		return // 心跳禁用
	}

	p.startOnce.Do(func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.pingAll()
			case <-p.stop:
				return
			}
		}
	})
}

// Stop 停止心跳检测
func (p *PingManager) Stop() {
	p.stopOnce.Do(func() {
		close(p.stop)
		p.mu.Lock()
		for id, cancel := range p.conns {
			cancel()
			delete(p.conns, id)
		}
		p.mu.Unlock()
	})
}

// Add 添加连接到心跳管理
func (p *PingManager) Add(conn *Conn) {
	if p.interval <= 0 {
		return
	}

	p.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	p.conns[conn.ID()] = cancel
	p.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-conn.Context().Done():
			p.Remove(conn.ID())
		case <-p.stop:
			return
		}
	}()
}

// Remove 移除连接
func (p *PingManager) Remove(connID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if cancel, ok := p.conns[connID]; ok {
		cancel()
		delete(p.conns, connID)
	}
}

// Reset 重置连接的心跳计时器
func (p *PingManager) Reset(conn *Conn) {
	p.Remove(conn.ID())
	p.Add(conn)
}

// pingAll 向所有连接发送心跳
func (p *PingManager) pingAll() {
	// 通过 Hub 的安全方法获取连接列表
	conns := p.hub.GetAllConns()

	// 使用原生 WebSocket ping
	for _, conn := range conns {
		if conn.ws != nil && !conn.IsClosed() {
			ctx, cancel := context.WithTimeout(conn.Context(), p.wait)
			if err := conn.ws.Ping(ctx); err != nil {
				conn.Close()
			}
			cancel()
		}
	}
}

// PingData 返回心跳消息数据
func (p *PingManager) PingData() ([]byte, error) {
	if p.pingMsg == nil {
		return nil, nil
	}
	return p.hub.config.Codec.Encode(p.pingMsg)
}
