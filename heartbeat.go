package zws

import (
	"context"
	"sync"
	"time"
)

// HeartbeatController 心跳控制器接口
// 定义了心跳管理的标准接口，可被服务端和客户端实现
type HeartbeatController interface {
	// Start 启动心跳检测
	Start()

	// Stop 停止心跳检测
	Stop()

	// Add 添加连接到心跳管理
	Add(conn *Conn)

	// Remove 从心跳管理中移除连接
	Remove(connID string)

	// Reset 重置连接的心跳计时器
	Reset(conn *Conn)
}

// HeartbeatManager 心跳管理器
// 使用单一 goroutine 监控所有连接，避免为每个连接创建独立 goroutine
// 解决了原 PingManager 的资源泄漏问题（P0 问题 #2）
type HeartbeatManager struct {
	mu        sync.RWMutex
	config    *ServerConfig
	conns     map[string]*heartbeatConn
	stopCh    chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	interval  time.Duration
}

// heartbeatConn 单个连接的心跳状态
type heartbeatConn struct {
	conn       *Conn
	lastActive time.Time
}

// NewHeartbeatManager 创建心跳管理器
func NewHeartbeatManager(config *ServerConfig) *HeartbeatManager {
	config = normalizeServerConfig(config)
	interval := config.PingInterval

	return &HeartbeatManager{
		config:   config,
		conns:    make(map[string]*heartbeatConn),
		stopCh:   make(chan struct{}),
		interval: interval,
	}
}

// Start 启动心跳监控
// 使用单一 goroutine 轮询所有连接，而非为每个连接创建 goroutine
func (h *HeartbeatManager) Start() {
	if h.interval <= 0 {
		return // 心跳禁用
	}

	h.startOnce.Do(func() {
		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				h.checkAll()
			case <-h.stopCh:
				return
			}
		}
	})
}

// Stop 停止心跳监控
func (h *HeartbeatManager) Stop() {
	h.stopOnce.Do(func() {
		close(h.stopCh)

		h.mu.Lock()
		defer h.mu.Unlock()

		h.conns = make(map[string]*heartbeatConn)
	})
}

// Add 添加连接到心跳管理
func (h *HeartbeatManager) Add(conn *Conn) {
	if h.interval <= 0 {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.conns[conn.ID()] = &heartbeatConn{
		conn:       conn,
		lastActive: time.Now(),
	}
}

// Remove 从心跳管理中移除连接
func (h *HeartbeatManager) Remove(connID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.conns[connID]; ok {
		delete(h.conns, connID)
	}
}

// Reset 重置连接的心跳计时器
func (h *HeartbeatManager) Reset(conn *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if hc, ok := h.conns[conn.ID()]; ok {
		hc.lastActive = time.Now()
	}
}

// checkAll 检查所有连接的活跃状态
// 在锁外执行 Ping 操作，避免长时间持锁（P1 问题 #5）
func (h *HeartbeatManager) checkAll() {
	// 复制连接列表，避免持锁执行网络操作
	h.mu.RLock()
	conns := make([]*heartbeatConn, 0, len(h.conns))
	for _, hc := range h.conns {
		conns = append(conns, hc)
	}
	h.mu.RUnlock()

	// 在锁外执行 Ping
	for _, hc := range conns {
		if hc.conn.IsClosed() {
			h.Remove(hc.conn.ID())
			continue
		}
		if hc.conn.ws == nil {
			h.Remove(hc.conn.ID())
			continue
		}

		ctx, cancel := context.WithTimeout(hc.conn.Context(), h.config.PingWait)
		if err := hc.conn.ws.Ping(ctx); err != nil {
			cancel()
			hc.conn.Close()
			continue
		}
		cancel()
	}
}

// watchConn 监控连接关闭
func (h *HeartbeatManager) watchConn(ctx context.Context, conn *Conn) {
	select {
	case <-ctx.Done():
		h.Remove(conn.ID())
	case <-conn.Context().Done():
		h.Remove(conn.ID())
	}
}

// PingData 返回心跳消息数据
func (h *HeartbeatManager) PingData() ([]byte, error) {
	if h.config.PingMessage == nil {
		return nil, nil
	}
	return h.config.Codec.Encode(h.config.PingMessage)
}
