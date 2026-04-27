package zws

import (
	"testing"
	"time"
)

// TestNewServer 测试创建服务端
func TestNewServer(t *testing.T) {
	// 使用默认配置
	server := NewServer(nil)
	if server == nil {
		t.Fatal("NewServer returned nil")
	}

	if server.hub == nil {
		t.Error("Hub not initialized")
	}

	if server.config == nil {
		t.Error("Config not initialized")
	}

	// 使用自定义配置
	config := DefaultServerConfig()
	config.PingInterval = 45 * time.Second
	server2 := NewServer(config)

	if server2.config.PingInterval != 45*time.Second {
		t.Error("Custom config not applied")
	}
}

// TestServer_Hub 测试获取 Hub
func TestServer_Hub(t *testing.T) {
	server := NewServer(nil)

	hub := server.Hub()
	if hub == nil {
		t.Error("Hub() returned nil")
	}

	if hub != server.hub {
		t.Error("Hub() returned different instance")
	}
}

// TestServer_Config 测试获取配置
func TestServer_Config(t *testing.T) {
	config := DefaultServerConfig()
	server := NewServer(config)

	retrievedConfig := server.Config()
	if retrievedConfig == nil {
		t.Error("Config() returned nil")
	}

	if retrievedConfig != server.config {
		t.Error("Config() returned different instance")
	}
}

// TestServer_OnConnect 测试设置连接回调
func TestServer_OnConnect(t *testing.T) {
	server := NewServer(nil)

	server.OnConnect(func(*Conn) {
		// 回调函数
	})

	// 验证回调已设置（通过 Hub）
	if server.hub.handlers.OnConnect == nil {
		t.Error("OnConnect handler not set")
	}
}

// TestServer_OnMessage 测试设置消息回调
func TestServer_OnMessage(t *testing.T) {
	server := NewServer(nil)

	server.OnMessage(func(*Conn, []byte) {
		// 回调函数
	})

	// 验证回调已设置（通过 Hub）
	if server.hub.handlers.OnMessage == nil {
		t.Error("OnMessage handler not set")
	}
}

// TestServer_OnDisconnect 测试设置断开回调
func TestServer_OnDisconnect(t *testing.T) {
	server := NewServer(nil)

	server.OnDisconnect(func(*Conn) {
		// 回调函数
	})

	// 验证回调已设置（通过 Hub）
	if server.hub.handlers.OnDisconnect == nil {
		t.Error("OnDisconnect handler not set")
	}
}

// TestServer_OnError 测试设置错误回调
func TestServer_OnError(t *testing.T) {
	server := NewServer(nil)

	server.OnError(func(*Conn, error) {
		// 回调函数
	})

	// 验证回调已设置（通过 Hub）
	if server.hub.handlers.OnError == nil {
		t.Error("OnError handler not set")
	}
}

// TestServer_Handler 测试获取处理器
func TestServer_Handler(t *testing.T) {
	server := NewServer(nil)

	handler := server.Handler()
	if handler == nil {
		t.Error("Handler() returned nil")
	}

	// Handler 函数应该不为 nil
	// 注意：实际执行 handler 需要 WebSocketContext，这需要完整的集成测试
}

// BenchmarkServer_NewServer 性能基准测试
func BenchmarkServer_NewServer(b *testing.B) {
	config := DefaultServerConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewServer(config)
	}
}
