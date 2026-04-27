package zws

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestHub_OnConnect 测试 OnConnect 回调
func TestHub_OnConnect(t *testing.T) {
	hub := NewHub(nil)

	var called bool
	var receivedConn *Conn

	hub.OnConnect(func(conn *Conn) {
		called = true
		receivedConn = conn
	})

	// 注册一个连接
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	hub.Register(conn)

	// 验证回调被调用
	if !called {
		t.Error("OnConnect callback was not called")
	}

	if receivedConn != conn {
		t.Error("OnConnect did not receive the correct connection")
	}
}

func TestHub_OnConnectCanUseHub(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	done := make(chan struct{})

	hub.OnConnect(func(conn *Conn) {
		hub.JoinRoom("lobby", conn)
		close(done)
	})

	go hub.Register(conn)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("OnConnect callback deadlocked when using Hub")
	}

	if size := hub.GetRoomSize("lobby"); size != 1 {
		t.Fatalf("Expected lobby size 1, got %d", size)
	}
}

// TestHub_OnConnect_NilHandler 测试 nil OnConnect 处理器
func TestHub_OnConnect_NilHandler(t *testing.T) {
	hub := NewHub(nil)

	// 不设置 OnConnect 处理器
	// 注册连接应该不会 panic
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	hub.Register(conn)

	// 如果没有 panic，测试通过
}

// TestHub_OnDisconnect 测试 OnDisconnect 回调
func TestHub_OnDisconnect(t *testing.T) {
	hub := NewHub(nil)

	var called bool
	var receivedConn *Conn

	hub.OnDisconnect(func(conn *Conn) {
		called = true
		receivedConn = conn
	})

	// 注册然后注销连接
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	hub.Register(conn)
	hub.Unregister(conn)

	// 验证回调被调用
	if !called {
		t.Error("OnDisconnect callback was not called")
	}

	if receivedConn != conn {
		t.Error("OnDisconnect did not receive the correct connection")
	}
}

// TestHub_OnDisconnect_NilHandler 测试 nil OnDisconnect 处理器
func TestHub_OnDisconnect_NilHandler(t *testing.T) {
	hub := NewHub(nil)

	// 不设置 OnDisconnect 处理器
	// 注销连接应该不会 panic
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	hub.Register(conn)
	hub.Unregister(conn)

	// 如果没有 panic，测试通过
}

// TestHub_OnMessage 测试 OnMessage 回调
func TestHub_OnMessage(t *testing.T) {
	hub := NewHub(nil)

	var called bool
	var receivedConn *Conn
	var receivedData []byte

	hub.OnMessage(func(conn *Conn, data []byte) {
		called = true
		receivedConn = conn
		receivedData = data
	})

	// 模拟消息处理
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	testData := []byte("test message")

	if hub.handlers.OnMessage != nil {
		hub.handlers.OnMessage(conn, testData)
	}

	// 验证回调被调用
	if !called {
		t.Error("OnMessage callback was not called")
	}

	if receivedConn != conn {
		t.Error("OnMessage did not receive the correct connection")
	}

	if string(receivedData) != string(testData) {
		t.Errorf("OnMessage did not receive the correct data, got %v", receivedData)
	}
}

// TestHub_OnMessage_NilHandler 测试 nil OnMessage 处理器
func TestHub_OnMessage_NilHandler(t *testing.T) {
	hub := NewHub(nil)

	// 不设置 OnMessage 处理器
	// 调用消息处理应该不会 panic
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	if hub.handlers.OnMessage != nil {
		hub.handlers.OnMessage(conn, []byte("test"))
	}

	// 如果没有 panic，测试通过
	_ = conn // 避免未使用变量警告
}

// TestHub_OnError 测试 OnError 回调
func TestHub_OnError(t *testing.T) {
	hub := NewHub(nil)

	var called bool
	var receivedConn *Conn
	var receivedError error

	hub.OnError(func(conn *Conn, err error) {
		called = true
		receivedConn = conn
		receivedError = err
	})

	// 模拟错误处理
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	testError := ErrConnClosed

	if hub.handlers.OnError != nil {
		hub.handlers.OnError(conn, testError)
	}

	// 验证回调被调用
	if !called {
		t.Error("OnError callback was not called")
	}

	if receivedConn != conn {
		t.Error("OnError did not receive the correct connection")
	}

	if receivedError != testError {
		t.Errorf("OnError did not receive the correct error, got %v", receivedError)
	}
}

// TestHub_AllHandlers 测试所有事件处理器
func TestHub_AllHandlers(t *testing.T) {
	hub := NewHub(nil)

	var connectCount atomic.Int32
	var disconnectCount atomic.Int32
	var messageCount atomic.Int32
	var errorCount atomic.Int32

	// 设置所有处理器
	hub.OnConnect(func(conn *Conn) {
		connectCount.Add(1)
	})

	hub.OnDisconnect(func(conn *Conn) {
		disconnectCount.Add(1)
	})

	hub.OnMessage(func(conn *Conn, data []byte) {
		messageCount.Add(1)
	})

	hub.OnError(func(conn *Conn, err error) {
		errorCount.Add(1)
	})

	// 测试连接和断开
	conn1 := NewConn("conn1", nil, hub, JSONCodec{})
	hub.Register(conn1)

	conn2 := NewConn("conn2", nil, hub, JSONCodec{})
	hub.Register(conn2)

	hub.Unregister(conn1)

	// 测试消息
	if hub.handlers.OnMessage != nil {
		hub.handlers.OnMessage(conn1, []byte("msg1"))
		hub.handlers.OnMessage(conn2, []byte("msg2"))
	}

	// 测试错误
	if hub.handlers.OnError != nil {
		hub.handlers.OnError(conn1, ErrConnClosed)
		hub.handlers.OnError(conn2, ErrClientNotFound)
	}

	// 验证计数
	if connectCount.Load() != 2 {
		t.Errorf("Expected 2 connect callbacks, got %d", connectCount.Load())
	}

	if disconnectCount.Load() != 1 {
		t.Errorf("Expected 1 disconnect callback, got %d", disconnectCount.Load())
	}

	if messageCount.Load() != 2 {
		t.Errorf("Expected 2 message callbacks, got %d", messageCount.Load())
	}

	if errorCount.Load() != 2 {
		t.Errorf("Expected 2 error callbacks, got %d", errorCount.Load())
	}
}

// TestHub_HandlerOverwrite 测试处理器覆盖
func TestHub_HandlerOverwrite(t *testing.T) {
	hub := NewHub(nil)

	var firstCalled bool
	var secondCalled bool

	// 设置第一个处理器
	hub.OnConnect(func(conn *Conn) {
		firstCalled = true
	})

	// 覆盖为第二个处理器
	hub.OnConnect(func(conn *Conn) {
		secondCalled = true
	})

	// 触发回调
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	hub.Register(conn)

	// 只有第二个处理器应该被调用
	if firstCalled {
		t.Error("First handler should not be called after overwrite")
	}

	if !secondCalled {
		t.Error("Second handler should be called")
	}
}

// TestHub_ConcurrentHandlerExecution 测试并发执行处理器
func TestHub_ConcurrentHandlerExecution(t *testing.T) {
	hub := NewHub(nil)

	var count atomic.Int32

	hub.OnConnect(func(conn *Conn) {
		count.Add(1)
		time.Sleep(10 * time.Millisecond) // 模拟耗时操作
	})

	// 并发注册多个连接
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			defer func() { done <- true }()
			conn := NewConn("conn", nil, hub, JSONCodec{})
			hub.Register(conn)
		}(i)
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Concurrent handler test timeout")
		}
	}

	// 验证所有处理器都被调用
	if count.Load() != 10 {
		t.Errorf("Expected 10 handler calls, got %d", count.Load())
	}
}

// TestHub_HandlerPanicRecovery 测试处理器 panic 时的处理
func TestHub_HandlerPanicRecovery(t *testing.T) {
	hub := NewHub(nil)

	// 设置一个会 panic 的处理器
	hub.OnConnect(func(conn *Conn) {
		panic("test panic")
	})

	// 测试 panic 是否会被传播
	defer func() {
		if r := recover(); r != nil {
			// panic 被捕获，这是预期行为
			t.Log("Panic was propagated as expected:", r)
		}
	}()

	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	hub.Register(conn)

	// 如果代码执行到这里，说明 panic 被处理了
	// 在实际应用中，可能需要添加 panic recovery
}

// TestHub_HandlerWithNilConn 测试处理器接收 nil 连接
func TestHub_HandlerWithNilConn(t *testing.T) {
	hub := NewHub(nil)

	var receivedNilConn bool

	hub.OnConnect(func(conn *Conn) {
		if conn == nil {
			receivedNilConn = true
		}
	})

	// 直接调用处理器（不通过 Register）
	if hub.handlers.OnConnect != nil {
		hub.handlers.OnConnect(nil)
	}

	// 验证处理器接收到了 nil 连接
	if !receivedNilConn {
		t.Error("Handler should handle nil connection")
	}
}

// TestHub_MultipleConnectionsSameID 测试相同 ID 的连接
func TestHub_MultipleConnectionsSameID(t *testing.T) {
	hub := NewHub(nil)

	var registerCount atomic.Int32

	hub.OnConnect(func(conn *Conn) {
		registerCount.Add(1)
	})

	// 注册两个相同 ID 的连接
	conn1 := NewConn("same-id", nil, hub, JSONCodec{})
	conn2 := NewConn("same-id", nil, hub, JSONCodec{})

	hub.Register(conn1)
	hub.Register(conn2) // 这会覆盖第一个连接

	// OnConnect 应该被调用两次
	if registerCount.Load() != 2 {
		t.Errorf("Expected 2 register callbacks, got %d", registerCount.Load())
	}

	// 验证只有最后一个连接存在
	if conn, ok := hub.Get("same-id"); !ok || conn != conn2 {
		t.Error("Second connection should replace the first")
	}
}

// BenchmarkHub_HandlerCall 性能基准测试
func BenchmarkHub_HandlerCall(b *testing.B) {
	hub := NewHub(nil)

	hub.OnConnect(func(conn *Conn) {
		// 空处理器
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 每次都创建新连接以触发 OnConnect
		testConn := NewConn("", nil, hub, JSONCodec{})
		hub.Register(testConn)
	}
}

// BenchmarkHub_HandlerWithData 性能基准测试（带数据）
func BenchmarkHub_HandlerWithData(b *testing.B) {
	hub := NewHub(nil)

	hub.OnMessage(func(conn *Conn, data []byte) {
		// 处理消息
	})

	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	testData := []byte("test message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if hub.handlers.OnMessage != nil {
			hub.handlers.OnMessage(conn, testData)
		}
	}
}
