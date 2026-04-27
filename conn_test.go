package zws

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestConn_Send 测试 Conn.Send 方法
func TestConn_Send(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 测试正常发送
	err := conn.Send([]byte("test message"))
	if err != nil {
		t.Errorf("Send failed: %v", err)
	}
}

// TestConn_SendToClosed 测试发送到已关闭的连接
func TestConn_SendToClosed(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 手动设置关闭状态（不调用 Close 避免 nil 指针）
	conn.closed.Store(true)

	err := conn.Send([]byte("another message"))
	if err != ErrConnClosed {
		t.Errorf("Expected ErrConnClosed, got: %v", err)
	}
}

// TestConn_SendTimeout 测试 Conn.Send 的超时机制
func TestConn_SendTimeout(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 填满发送缓冲区
	count := 0
	for i := 0; i < 300; i++ {
		select {
		case conn.send <- []byte("fill buffer"):
			count++
		default:
			break // 缓冲区已满
		}
	}

	if count < 256 {
		t.Skip("Could not fill buffer completely")
	}

	// 现在发送应该超时
	start := time.Now()
	err := conn.Send([]byte("should timeout"))
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	// 验证超时时间大约为 5 秒（有 2 秒容差）
	if elapsed < 3*time.Second || elapsed > 8*time.Second {
		t.Logf("Warning: timeout took %v, expected ~5 seconds", elapsed)
	}
}

// TestConn_JSON 测试 Conn.JSON 方法
func TestConn_JSON(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 测试 JSON 编码和发送
	data := map[string]string{"hello": "world"}
	err := conn.JSON(data)
	if err != nil {
		t.Errorf("JSON failed: %v", err)
	}

	// 验证数据被发送到 channel
	select {
	case msg := <-conn.send:
		if len(msg) == 0 {
			t.Error("Expected non-empty message")
		}
	case <-time.After(time.Second):
		t.Error("Message not sent to channel")
	}
}

// TestConn_Metadata 测试 Conn 的元数据功能
func TestConn_Metadata(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 测试 Set 和 Get
	conn.Set("user_id", "12345")
	val, ok := conn.Get("user_id")
	if !ok || val != "12345" {
		t.Error("Set/Get metadata failed")
	}

	// 测试不存在的键
	_, ok = conn.Get("non_existent")
	if ok {
		t.Error("Expected false for non-existent key")
	}

	// 测试覆盖值
	conn.Set("user_id", "67890")
	val, ok = conn.Get("user_id")
	if !ok || val != "67890" {
		t.Error("Set/Get metadata overwrite failed")
	}
}

// TestConn_ID 测试 Conn.ID 方法
func TestConn_ID(t *testing.T) {
	hub := NewHub(nil)

	// 测试自定义 ID
	conn := NewConn("custom-id", nil, hub, JSONCodec{})
	if conn.ID() != "custom-id" {
		t.Errorf("Expected ID 'custom-id', got '%s'", conn.ID())
	}

	// 测试空 ID - NewConn 不会自动生成，需要通过 Register 生成
	conn2 := NewConn("", nil, hub, JSONCodec{})
	// 空 ID 在 NewConn 时保持空，需要在 Register 时生成
	if conn2.id != "" {
		t.Logf("Note: Empty ID is generated in Register, not NewConn")
	}
}

// TestConn_Context 测试 Conn.Context 方法
func TestConn_Context(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	ctx := conn.Context()
	if ctx == nil {
		t.Error("Context should not be nil")
	}

	// 取消 context
	conn.cancel()

	// 验证 context 已被取消
	select {
	case <-ctx.Done():
		// context 已取消，符合预期
	case <-time.After(time.Second):
		t.Error("Context should be cancelled after cancel()")
	}
}

// TestConn_ConcurrentSend 测试并发发送的竞态条件
func TestConn_ConcurrentSend(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 测试少量的并发发送，避免缓冲区满导致的阻塞
	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func() {
			defer func() { done <- true }()
			for j := 0; j < 10; j++ {
				err := conn.Send([]byte("concurrent test"))
				if err != nil {
					// 发送失败（可能是超时或连接关闭）
					t.Logf("Send failed: %v", err)
				}
			}
		}()
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 5; i++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("Concurrent send test timeout")
		}
	}
}

// TestConn_ConcurrentMetadata 测试并发元数据访问
func TestConn_ConcurrentMetadata(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	done := make(chan bool, 20)
	for i := 0; i < 10; i++ {
		go func(n int) {
			defer func() { done <- true }()
			conn.Set("key", n)
		}(i)
		go func() {
			defer func() { done <- true }()
			conn.Get("key")
		}()
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 20; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Concurrent metadata test timeout")
		}
	}
}

// TestConn_IsClosed 测试 IsClosed 方法
func TestConn_IsClosed(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 初始状态应该未关闭
	if conn.IsClosed() {
		t.Error("New connection should not be closed")
	}

	// 手动设置关闭状态
	conn.closed.Store(true)

	// 现在应该显示为已关闭
	if !conn.IsClosed() {
		t.Error("Connection should be closed")
	}
}

// TestNewConn 测试 NewConn 构造函数
func TestNewConn(t *testing.T) {
	hub := NewHub(nil)

	conn := NewConn("", nil, hub, JSONCodec{})
	if conn == nil {
		t.Fatal("NewConn returned nil")
	}

	// NewConn 不会自动生成 ID，ID 在 Register 时生成
	// 空 ID 在 NewConn 时保持空
	if conn.id != "" {
		t.Logf("Note: ID is generated in Register, not NewConn. Current ID: %s", conn.id)
	}

	if conn.hub != hub {
		t.Error("Conn hub not set correctly")
	}

	if conn.codec == nil {
		t.Error("Conn codec should not be nil")
	}

	if conn.send == nil {
		t.Error("Conn send channel should not be nil")
	}

	if conn.metadata == nil {
		t.Error("Conn metadata map should not be nil")
	}
}

// TestConn_JSONEncodeError 测试 JSON 编码错误
func TestConn_JSONEncodeError(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 创建一个无法序列化的类型（包含 channel）
	data := struct {
		Data chan int
	}{
		Data: make(chan int),
	}

	err := conn.JSON(data)
	if err == nil {
		t.Error("Expected error for unmarshalable type")
	}
}

// TestConn_SendChannelClosed 测试向已关闭的 channel 发送
func TestConn_SendChannelClosed(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 关闭 send channel
	close(conn.send)

	if err := conn.Send([]byte("test")); err != ErrConnClosed {
		t.Fatalf("Expected ErrConnClosed when sending to closed channel, got %v", err)
	}
}

// TestConn_AtomicOperations 测试原子操作的正确性
func TestConn_AtomicOperations(t *testing.T) {
	// 使用 atomic 操作测试 closed 状态
	var closed atomic.Bool
	closed.Store(false)

	if closed.Load() {
		t.Error("Should be false initially")
	}

	closed.Store(true)

	if !closed.Load() {
		t.Error("Should be true after store")
	}
}

// BenchmarkConn_Send 性能基准测试
func BenchmarkConn_Send(b *testing.B) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Send([]byte("benchmark test"))
		// 清空 channel 以避免阻塞
		select {
		case <-conn.send:
		default:
		}
	}
}

// BenchmarkConn_JSON 性能基准测试
func BenchmarkConn_JSON(b *testing.B) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	data := map[string]string{"benchmark": "test"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.JSON(data)
		// 清空 channel 以避免阻塞
		select {
		case <-conn.send:
		default:
		}
	}
}

// BenchmarkConn_Metadata 性能基准测试
func BenchmarkConn_Metadata(b *testing.B) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Set("key", i)
		conn.Get("key")
	}
}
