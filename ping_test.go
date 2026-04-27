package zws

import (
	"sync"
	"testing"
	"time"
)

// TestNewPingManager 测试创建 PingManager
func TestNewPingManager(t *testing.T) {
	hub := NewHub(nil)
	interval := 30 * time.Second
	wait := 60 * time.Second
	pingMsg := map[string]string{"type": "ping"}

	pm := NewPingManager(hub, interval, wait, pingMsg)

	if pm == nil {
		t.Fatal("NewPingManager returned nil")
	}

	if pm.hub != hub {
		t.Error("Hub not set correctly")
	}

	if pm.interval != interval {
		t.Error("Interval not set correctly")
	}

	if pm.wait != wait {
		t.Error("Wait not set correctly")
	}

	if pm.pingMsg == nil {
		t.Error("PingMsg should be set")
	}

	if pm.conns == nil {
		t.Error("Connections map not initialized")
	}

	if pm.stop == nil {
		t.Error("Stop channel not initialized")
	}
}

// TestPingManager_Start 测试启动 PingManager
func TestPingManager_Start(t *testing.T) {
	hub := NewHub(nil)
	interval := 100 * time.Millisecond
	pm := NewPingManager(hub, interval, 200*time.Millisecond, nil)

	// 启动 PingManager（在后台运行）
	done := make(chan struct{})
	go func() {
		pm.Start()
		close(done)
	}()

	// 等待一段时间确认它正在运行
	time.Sleep(150 * time.Millisecond)

	// 停止
	pm.Stop()

	// 确认 Start 已完成
	select {
	case <-done:
		// 正常退出
	case <-time.After(time.Second):
		t.Error("Start did not exit after Stop")
	}
}

// TestPingManager_StartDisabled 测试禁用心跳时的启动
func TestPingManager_StartDisabled(t *testing.T) {
	hub := NewHub(nil)
	pm := NewPingManager(hub, 0, 60*time.Second, nil)

	// Start 应该立即返回
	pm.Start()

	// 不应该 panic 或阻塞
}

// TestPingManager_Stop 测试停止 PingManager
func TestPingManager_Stop(t *testing.T) {
	hub := NewHub(nil)
	pm := NewPingManager(hub, 100*time.Millisecond, 200*time.Second, nil)

	// 启动
	go pm.Start()

	// 等待一下
	time.Sleep(50 * time.Millisecond)

	// 停止应该安全且只生效一次
	pm.Stop()
	pm.Stop() // 第二次调用应该安全
}

// TestPingManager_Add 测试添加连接
func TestPingManager_Add(t *testing.T) {
	hub := NewHub(nil)
	pm := NewPingManager(hub, 100*time.Millisecond, 200*time.Millisecond, nil)

	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 添加连接
	pm.Add(conn)

	// 验证连接已添加
	pm.mu.RLock()
	_, exists := pm.conns[conn.ID()]
	pm.mu.RUnlock()

	if !exists {
		t.Error("Connection not added to PingManager")
	}

	// 清理
	pm.Stop()
}

// TestPingManager_AddDisabled 测试禁用心跳时的添加
func TestPingManager_AddDisabled(t *testing.T) {
	hub := NewHub(nil)
	pm := NewPingManager(hub, 0, 60*time.Second, nil)

	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 添加连接应该直接返回
	pm.Add(conn)

	// 验证连接未添加
	pm.mu.RLock()
	len := len(pm.conns)
	pm.mu.RUnlock()

	if len != 0 {
		t.Error("Connection should not be added when heartbeat is disabled")
	}
}

// TestPingManager_Remove 测试移除连接
func TestPingManager_Remove(t *testing.T) {
	hub := NewHub(nil)
	pm := NewPingManager(hub, 100*time.Millisecond, 200*time.Millisecond, nil)

	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 添加连接
	pm.Add(conn)

	// 移除连接
	pm.Remove(conn.ID())

	// 验证连接已移除
	pm.mu.RLock()
	_, exists := pm.conns[conn.ID()]
	pm.mu.RUnlock()

	if exists {
		t.Error("Connection not removed from PingManager")
	}

	// 移除不存在的连接应该安全
	pm.Remove("non-existent")
}

// TestPingManager_Reset 测试重置连接
func TestPingManager_Reset(t *testing.T) {
	hub := NewHub(nil)
	pm := NewPingManager(hub, 100*time.Millisecond, 200*time.Millisecond, nil)

	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 添加连接
	pm.Add(conn)

	pm.mu.RLock()
	_, exists := pm.conns[conn.ID()]
	pm.mu.RUnlock()

	if !exists {
		t.Fatal("Connection not found")
	}

	// 重置连接
	pm.Reset(conn)

	// 验证连接仍然存在
	pm.mu.RLock()
	_, exists = pm.conns[conn.ID()]
	pm.mu.RUnlock()

	if !exists {
		t.Error("Connection not found after reset")
	}

	// 重置不存在的连接应该安全
	pm.Reset(NewConn("unknown", nil, hub, JSONCodec{}))

	pm.Stop()
}

// TestPingManager_PingData 测试获取心跳数据
func TestPingManager_PingData(t *testing.T) {
	hub := NewHub(nil)

	// 无自定义心跳消息
	pm1 := NewPingManager(hub, 30*time.Second, 60*time.Second, nil)

	data, err := pm1.PingData()
	if err != nil {
		t.Errorf("PingData returned error: %v", err)
	}
	if data != nil {
		t.Error("PingData should return nil when no custom message")
	}

	// 有自定义心跳消息
	pingMsg := map[string]string{"type": "ping"}
	pm2 := NewPingManager(hub, 30*time.Second, 60*time.Second, pingMsg)

	data, err = pm2.PingData()
	if err != nil {
		t.Errorf("PingData with message returned error: %v", err)
	}
	if data == nil {
		t.Error("PingData should return data when custom message is set")
	}
}

// TestPingManager_ConcurrentOperations 测试并发操作
func TestPingManager_ConcurrentOperations(t *testing.T) {
	hub := NewHub(nil)
	pm := NewPingManager(hub, 50*time.Millisecond, 100*time.Millisecond, nil)

	var wg sync.WaitGroup

	// 启动 PingManager
	go pm.Start()

	// 并发添加和移除连接
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn := NewConn(string(rune(idx)), nil, hub, JSONCodec{})
			pm.Add(conn)
			time.Sleep(10 * time.Millisecond)
			pm.Remove(conn.ID())
		}(i)
	}

	wg.Wait()
	pm.Stop()

	// 验证没有 panic 或死锁
	t.Log("Concurrent operations completed successfully")
}

// TestPingManager_pingAll 测试向所有连接发送 ping
func TestPingManager_pingAll(t *testing.T) {
	hub := NewHub(nil)
	pm := NewPingManager(hub, 100*time.Millisecond, 200*time.Millisecond, nil)

	// 添加一些连接（没有实际的 ws，所以 ping 会失败但不会 panic）
	for i := 0; i < 3; i++ {
		conn := NewConn(string(rune(i)), nil, hub, JSONCodec{})
		pm.Add(conn)
	}

	// pingAll 应该安全处理 nil ws
	pm.pingAll()

	pm.Stop()
}

// BenchmarkPingManager_AddRemove 性能基准测试
func BenchmarkPingManager_AddRemove(b *testing.B) {
	hub := NewHub(nil)
	pm := NewPingManager(hub, 30*time.Second, 60*time.Second, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := NewConn("", nil, hub, JSONCodec{})
		pm.Add(conn)
		pm.Remove(conn.ID())
	}
}

// BenchmarkPingManager_Reset 性能基准测试
func BenchmarkPingManager_Reset(b *testing.B) {
	hub := NewHub(nil)
	pm := NewPingManager(hub, 30*time.Second, 60*time.Second, nil)
	conn := NewConn("bench-conn", nil, hub, JSONCodec{})
	pm.Add(conn)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pm.Reset(conn)
	}
}
