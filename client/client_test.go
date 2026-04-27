package client

import (
	"testing"
	"time"

	"github.com/zlsgo/zws"
)

// TestNewClient 测试 NewClient 构造函数
func TestNewClient(t *testing.T) {
	// 测试默认配置
	c1, err := NewClient("ws://localhost:8080/ws", nil)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if c1 == nil {
		t.Fatal("NewClient returned nil")
	}
	if c1.url != "ws://localhost:8080/ws" {
		t.Errorf("Expected URL 'ws://localhost:8080/ws', got '%s'", c1.url)
	}

	// 测试自定义配置
	config := &zws.ClientConfig{
		HandshakeTimeout: 10 * time.Second,
	}
	c2, err := NewClient("ws://example.com/socket", config)
	if err != nil {
		t.Fatalf("NewClient with config failed: %v", err)
	}
	if c2.config.HandshakeTimeout != 10*time.Second {
		t.Errorf("Expected HandshakeTimeout 10s, got %v", c2.config.HandshakeTimeout)
	}
}

// TestClient_Send 测试 Client.Send 方法
func TestClient_Send(t *testing.T) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)

	// 测试发送消息
	err := c.Send([]byte("test message"))
	if err != nil {
		t.Errorf("Send failed: %v", err)
	}

	// 验证消息被发送到 hub
	select {
	case msg := <-c.hub.outbox:
		if string(msg) != "test message" {
			t.Errorf("Expected 'test message', got '%s'", string(msg))
		}
	case <-time.After(time.Second):
		t.Error("Message not sent to hub outbox")
	}
}

// TestClient_SendTimeout 测试发送超时
func TestClient_SendTimeout(t *testing.T) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)

	// 填满 outbox 缓冲区
	for i := 0; i < 300; i++ {
		select {
		case c.hub.outbox <- []byte("fill"):
		default:
			break
		}
	}

	// 现在发送应该超时
	start := time.Now()
	err := c.Send([]byte("should timeout"))
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	// 验证超时时间大约为 5 秒
	if elapsed < 4*time.Second || elapsed > 7*time.Second {
		t.Logf("Warning: timeout took %v, expected ~5 seconds", elapsed)
	}
}

// TestClient_JSON 测试 Client.JSON 方法
func TestClient_JSON(t *testing.T) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)

	// 测试 JSON 编码和发送
	data := map[string]string{"hello": "world"}
	err := c.JSON(data)
	if err != nil {
		t.Errorf("JSON failed: %v", err)
	}

	// 验证数据被发送到 hub
	select {
	case msg := <-c.hub.outbox:
		if len(msg) == 0 {
			t.Error("Expected non-empty JSON message")
		}
	case <-time.After(time.Second):
		t.Error("JSON message not sent to hub outbox")
	}
}

// TestClient_Close 测试 Client.Close 方法
func TestClient_Close(t *testing.T) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)

	// 第一次关闭
	err := c.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// 第二次关闭应该返回 ErrConnClosed（已关闭）
	err = c.Close()
	if err != zws.ErrConnClosed {
		t.Errorf("Expected ErrConnClosed on second close, got: %v", err)
	}

	// 验证连接已关闭
	if !c.IsClosed() {
		t.Error("Client should be closed")
	}
}

// TestClient_IsClosed 测试 IsClosed 方法
func TestClient_IsClosed(t *testing.T) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)

	// 初始状态应该未关闭
	if c.IsClosed() {
		t.Error("New client should not be closed")
	}

	// 关闭后应该显示为已关闭
	c.Close()
	if !c.IsClosed() {
		t.Error("Client should be closed after Close()")
	}
}

// TestClient_SendToClosed 测试向已关闭的客户端发送
func TestClient_SendToClosed(t *testing.T) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)

	// 先关闭
	c.Close()

	// 再发送应该失败
	err := c.Send([]byte("test"))
	if err == nil {
		t.Error("Expected error when sending to closed client")
	}
}

// TestClient_SetHeader 测试 SetHeader 方法
func TestClient_SetHeader(t *testing.T) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)

	// 设置请求头
	c.SetHeader("Authorization", "Bearer token123")
	c.SetHeader("X-Custom-Header", "custom-value")

	if c.header.Get("Authorization") != "Bearer token123" {
		t.Error("Authorization header not set correctly")
	}

	if c.header.Get("X-Custom-Header") != "custom-value" {
		t.Error("X-Custom-Header not set correctly")
	}
}

// TestClient_Handlers 测试事件处理器设置
func TestClient_Handlers(t *testing.T) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)

	// 测试 OnConnect
	c.OnConnect(func(client *Client) {
		t.Log("OnConnect called")
	})
	if c.handlers.OnConnect == nil {
		t.Error("OnConnect handler not set")
	}

	// 测试 OnMessage
	c.OnMessage(func(client *Client, data []byte) {
		t.Log("OnMessage called:", string(data))
	})
	if c.handlers.OnMessage == nil {
		t.Error("OnMessage handler not set")
	}

	// 测试 OnDisconnect
	c.OnDisconnect(func(client *Client) {
		t.Log("OnDisconnect called")
	})
	if c.handlers.OnDisconnect == nil {
		t.Error("OnDisconnect handler not set")
	}

	// 测试 OnError
	c.OnError(func(client *Client, err error) {
		t.Log("OnError called:", err)
	})
	if c.handlers.OnError == nil {
		t.Error("OnError handler not set")
	}
}

// TestClient_ConcurrentSend 测试并发发送
func TestClient_ConcurrentSend(t *testing.T) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)

	// 并发发送
	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func() {
			defer func() { done <- true }()
			for j := 0; j < 10; j++ {
				c.Send([]byte("concurrent test"))
				time.Sleep(2 * time.Millisecond)
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

// TestClientHub_send 测试 clientHub.send 方法
func TestClientHub_send(t *testing.T) {
	hub := newClientHub()

	// 测试正常发送
	err := hub.send([]byte("test message"))
	if err != nil {
		t.Errorf("send failed: %v", err)
	}

	// 验证消息在 outbox 中
	select {
	case msg := <-hub.outbox:
		if string(msg) != "test message" {
			t.Errorf("Expected 'test message', got '%s'", string(msg))
		}
	case <-time.After(time.Second):
		t.Error("Message not in outbox")
	}
}

// TestClientHub_sendTimeout 测试 clientHub.send 超时
func TestClientHub_sendTimeout(t *testing.T) {
	hub := newClientHub()

	// 填满 outbox 缓冲区
	for i := 0; i < 300; i++ {
		select {
		case hub.outbox <- []byte("fill"):
		default:
			break
		}
	}

	// 现在发送应该超时
	err := hub.send([]byte("should timeout"))
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}
}

// TestClientHub_close 测试 clientHub.close 方法
func TestClientHub_close(t *testing.T) {
	hub := newClientHub()

	hub.close()

	if err := hub.send([]byte("test")); err != nil {
		t.Fatalf("close should not make outbox unsafe for concurrent send: %v", err)
	}
}

// TestDefaultClientConfig 测试默认客户端配置
func TestDefaultClientConfig(t *testing.T) {
	config := zws.DefaultClientConfig()

	if config == nil {
		t.Fatal("DefaultClientConfig returned nil")
	}

	if config.HandshakeTimeout != 45*time.Second {
		t.Errorf("Expected HandshakeTimeout 45s, got %v", config.HandshakeTimeout)
	}

	if config.Reconnect != false {
		t.Error("Expected Reconnect to be false by default")
	}

	if config.ReconnectDelay != 5*time.Second {
		t.Errorf("Expected ReconnectDelay 5s, got %v", config.ReconnectDelay)
	}

	if config.MaxReconnectAttempts != -1 {
		t.Errorf("Expected MaxReconnectAttempts -1, got %d", config.MaxReconnectAttempts)
	}

	if config.PingInterval != 30*time.Second {
		t.Errorf("Expected PingInterval 30s, got %v", config.PingInterval)
	}

	if config.PingWait != 60*time.Second {
		t.Errorf("Expected PingWait 60s, got %v", config.PingWait)
	}
}

// TestClient_JSONEncodeError 测试 JSON 编码错误
func TestClient_JSONEncodeError(t *testing.T) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)

	// 创建一个无法序列化的类型
	data := struct {
		Data chan int
	}{
		Data: make(chan int),
	}

	err := c.JSON(data)
	if err == nil {
		t.Error("Expected error for unmarshalable type")
	}
}

// BenchmarkClient_Send 性能基准测试
func BenchmarkClient_Send(b *testing.B) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Send([]byte("benchmark test"))
		// 清空 channel 以避免阻塞
		select {
		case <-c.hub.outbox:
		default:
		}
	}
}

// BenchmarkClient_JSON 性能基准测试
func BenchmarkClient_JSON(b *testing.B) {
	c, _ := NewClient("ws://localhost:8080/ws", nil)
	data := map[string]string{"benchmark": "test"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.JSON(data)
		// 清空 channel 以避免阻塞
		select {
		case <-c.hub.outbox:
		default:
		}
	}
}
