package zws

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestNewHeartbeatManager 测试创建心跳管理器
func TestNewHeartbeatManager(t *testing.T) {
	config := DefaultServerConfig()
	hm := NewHeartbeatManager(config)

	if hm == nil {
		t.Fatal("NewHeartbeatManager returned nil")
	}

	if hm.config != config {
		t.Error("Config not set correctly")
	}

	if hm.conns == nil {
		t.Error("Connections map not initialized")
	}

	if hm.stopCh == nil {
		t.Error("Stop channel not initialized")
	}
}

// TestHeartbeatManager_Start 测试启动心跳监控
func TestHeartbeatManager_Start(t *testing.T) {
	config := DefaultServerConfig()
	config.PingInterval = 100 * time.Millisecond
	hm := NewHeartbeatManager(config)

	// 启动心跳（在后台运行）
	done := make(chan struct{})
	go func() {
		hm.Start()
		close(done)
	}()

	// 等待一段时间确认它正在运行
	time.Sleep(150 * time.Millisecond)

	// 停止心跳
	hm.Stop()

	// 确认 Start 已完成
	select {
	case <-done:
		// 正常退出
	case <-time.After(time.Second):
		t.Error("Start did not exit after Stop")
	}
}

// TestHeartbeatManager_StartDisabled 测试禁用心跳时的启动
func TestHeartbeatManager_StartDisabled(t *testing.T) {
	config := DefaultServerConfig()
	config.PingInterval = 0 // 禁用心跳
	hm := NewHeartbeatManager(config)

	// Start 应该立即返回
	hm.Start()

	// 不应该 panic 或阻塞
}

// TestHeartbeatManager_Stop 测试停止心跳监控
func TestHeartbeatManager_Stop(t *testing.T) {
	config := DefaultServerConfig()
	config.PingInterval = 100 * time.Millisecond
	hm := NewHeartbeatManager(config)

	// 启动心跳
	go hm.Start()

	// 停止应该安全且只生效一次
	hm.Stop()
	hm.Stop() // 第二次调用应该安全

	// 验证连接已清理
	hm.mu.Lock()
	if len(hm.conns) != 0 {
		t.Error("Connections not cleared after stop")
	}
	hm.mu.Unlock()
}

// TestHeartbeatManager_Add 测试添加连接
func TestHeartbeatManager_Add(t *testing.T) {
	config := DefaultServerConfig()
	config.PingInterval = 100 * time.Millisecond
	config.PingWait = 200 * time.Millisecond
	hm := NewHeartbeatManager(config)

	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 添加连接
	hm.Add(conn)

	// 验证连接已添加
	hm.mu.Lock()
	_, exists := hm.conns[conn.ID()]
	hm.mu.Unlock()

	if !exists {
		t.Error("Connection not added to heartbeat manager")
	}

	// 清理
	hm.Stop()
}

// TestHeartbeatManager_AddDisabled 测试禁用心跳时的添加
func TestHeartbeatManager_AddDisabled(t *testing.T) {
	config := DefaultServerConfig()
	config.PingInterval = 0 // 禁用心跳
	hm := NewHeartbeatManager(config)

	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 添加连接应该直接返回
	hm.Add(conn)

	// 验证连接未添加
	hm.mu.Lock()
	len := len(hm.conns)
	hm.mu.Unlock()

	if len != 0 {
		t.Error("Connection should not be added when heartbeat is disabled")
	}
}

// TestHeartbeatManager_Remove 测试移除连接
func TestHeartbeatManager_Remove(t *testing.T) {
	config := DefaultServerConfig()
	config.PingInterval = 100 * time.Millisecond
	hm := NewHeartbeatManager(config)

	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 添加连接
	hm.Add(conn)

	// 移除连接
	hm.Remove(conn.ID())

	// 验证连接已移除
	hm.mu.Lock()
	_, exists := hm.conns[conn.ID()]
	hm.mu.Unlock()

	if exists {
		t.Error("Connection not removed from heartbeat manager")
	}

	// 移除不存在的连接应该安全
	hm.Remove("non-existent")

	hm.Stop()
}

// TestHeartbeatManager_Reset 测试重置连接
func TestHeartbeatManager_Reset(t *testing.T) {
	config := DefaultServerConfig()
	config.PingInterval = 100 * time.Millisecond
	config.PingWait = 200 * time.Millisecond
	hm := NewHeartbeatManager(config)

	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 添加连接
	hm.Add(conn)

	hm.mu.Lock()
	hc := hm.conns[conn.ID()]
	hm.mu.Unlock()

	if hc == nil {
		t.Fatal("Connection not found")
	}

	oldActive := hc.lastActive
	time.Sleep(10 * time.Millisecond)

	// 重置连接
	hm.Reset(conn)

	hm.mu.Lock()
	newActive := hm.conns[conn.ID()].lastActive
	hm.mu.Unlock()

	if newActive.Before(oldActive) || newActive.Equal(oldActive) {
		t.Error("Last active time not updated")
	}

	// 重置不存在的连接应该安全
	hm.Reset(NewConn("unknown", nil, hub, JSONCodec{}))

	hm.Stop()
}

// TestHeartbeatManager_PingData 测试获取心跳数据
func TestHeartbeatManager_PingData(t *testing.T) {
	config := DefaultServerConfig()
	hm := NewHeartbeatManager(config)

	// 无自定义心跳消息
	data, err := hm.PingData()
	if err != nil {
		t.Errorf("PingData returned error: %v", err)
	}
	if data != nil {
		t.Error("PingData should return nil when no custom message")
	}

	// 有自定义心跳消息
	config.PingMessage = map[string]string{"type": "ping"}
	hm2 := NewHeartbeatManager(config)

	data, err = hm2.PingData()
	if err != nil {
		t.Errorf("PingData with message returned error: %v", err)
	}
	if data == nil {
		t.Error("PingData should return data when custom message is set")
	}
}

// TestHeartbeatManager_watchConn 测试监控连接
func TestHeartbeatManager_watchConn(t *testing.T) {
	config := DefaultServerConfig()
	config.PingWait = 50 * time.Millisecond
	hm := NewHeartbeatManager(config)

	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 启动监控
	ctx, cancel := context.WithTimeout(context.Background(), config.PingWait)
	defer cancel()

	done := make(chan struct{})
	go func() {
		hm.watchConn(ctx, conn)
		close(done)
	}()

	// 等待超时
	select {
	case <-done:
		// watchConn 已完成
	case <-time.After(200 * time.Millisecond):
		t.Error("watchConn did not complete on timeout")
	}

	// 监控超时只清理管理器状态，不主动关闭仍可用的连接。
	if conn.IsClosed() {
		t.Error("Connection should not be closed by watch timeout")
	}
}

// TestHeartbeatManager_ConcurrentOperations 测试并发操作
func TestHeartbeatManager_ConcurrentOperations(t *testing.T) {
	config := DefaultServerConfig()
	config.PingInterval = 50 * time.Millisecond
	hm := NewHeartbeatManager(config)

	hub := NewHub(nil)
	var wg sync.WaitGroup

	// 启动心跳
	go hm.Start()

	// 并发添加连接
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn := NewConn(string(rune(idx)), nil, hub, JSONCodec{})
			hm.Add(conn)
			time.Sleep(10 * time.Millisecond)
			hm.Remove(conn.ID())
		}(i)
	}

	wg.Wait()
	hm.Stop()

	// 验证没有 panic 或死锁
	t.Log("Concurrent operations completed successfully")
}

// BenchmarkHeartbeatManager_AddRemove 性能基准测试
func BenchmarkHeartbeatManager_AddRemove(b *testing.B) {
	config := DefaultServerConfig()
	hm := NewHeartbeatManager(config)
	hub := NewHub(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := NewConn("", nil, hub, JSONCodec{})
		hm.Add(conn)
		hm.Remove(conn.ID())
	}
}
