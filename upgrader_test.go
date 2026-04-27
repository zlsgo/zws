package zws

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestNewUpgrader 测试 NewUpgrader 构造函数
func TestNewUpgrader(t *testing.T) {
	hub := NewHub(nil)
	upgrader := NewUpgrader(hub)

	if upgrader == nil {
		t.Fatal("NewUpgrader returned nil")
	}

	if upgrader.hub != hub {
		t.Error("Upgrader hub not set correctly")
	}
}

// TestIsAllowedOrigin 测试 Origin 验证逻辑
func TestIsAllowedOrigin(t *testing.T) {
	tests := []struct {
		name     string
		origin   string
		allowed  []string
		expected bool
	}{
		{
			name:     "空允许列表拒绝所有",
			origin:   "http://example.com",
			allowed:  []string{},
			expected: false,
		},
		{
			name:     "星号允许所有",
			origin:   "http://example.com",
			allowed:  []string{"*"},
			expected: true,
		},
		{
			name:     "精确匹配允许",
			origin:   "http://example.com",
			allowed:  []string{"http://example.com"},
			expected: true,
		},
		{
			name:     "不匹配拒绝",
			origin:   "http://evil.com",
			allowed:  []string{"http://example.com"},
			expected: false,
		},
		{
			name:     "多个允许列表中的一个匹配",
			origin:   "http://example.com",
			allowed:  []string{"http://trusted.com", "http://example.com"},
			expected: true,
		},
		{
			name:     "空 origin 被拒绝",
			origin:   "",
			allowed:  []string{"http://example.com"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isAllowedOrigin(tt.origin, tt.allowed)
			if result != tt.expected {
				t.Errorf("isAllowedOrigin(%q, %v) = %v, want %v",
					tt.origin, tt.allowed, result, tt.expected)
			}
		})
	}
}

// TestUpgrader_Accept_InvalidOrigin 测试无效 Origin 被拒绝
func TestUpgrader_Accept_InvalidOrigin(t *testing.T) {
	hub := NewHub(&ServerConfig{
		AllowedOrigins: []string{"http://example.com"},
	})
	upgrader := NewUpgrader(hub)

	// 创建测试请求
	req := httptest.NewRequest("GET", "http://localhost:8080/ws", nil)
	req.Header.Set("Origin", "http://evil.com")

	w := httptest.NewRecorder()

	// 尝试接受连接
	conn, err := upgrader.Accept(w, req)

	// 应该返回错误
	if err == nil {
		t.Error("Expected error for invalid origin")
	}

	if conn != nil {
		t.Error("Expected nil connection for invalid origin")
	}

	// 验证返回了 403 Forbidden
	if w.Code != http.StatusForbidden {
		t.Errorf("Expected status 403, got %d", w.Code)
	}
}

// TestUpgrader_Accept_ValidOrigin 测试有效 Origin 被接受
func TestUpgrader_Accept_ValidOrigin(t *testing.T) {
	hub := NewHub(&ServerConfig{
		AllowedOrigins: []string{"http://example.com"},
	})
	upgrader := NewUpgrader(hub)

	// 创建测试请求 - 注意：这不是一个真实的 WebSocket 升级请求
	// 所以 Accept 会失败，但不是因为 Origin 验证
	req := httptest.NewRequest("GET", "http://localhost:8080/ws", nil)
	req.Header.Set("Origin", "http://example.com")

	w := httptest.NewRecorder()

	// 尝试接受连接
	_, err := upgrader.Accept(w, req)

	// 不应该因为 Origin 验证而失败
	if err != nil && strings.Contains(err.Error(), "origin not allowed") {
		t.Errorf("Origin should be allowed: %v", err)
	}
}

// TestUpgrader_Accept_AllowAll 测试允许所有来源
func TestUpgrader_Accept_AllowAll(t *testing.T) {
	hub := NewHub(&ServerConfig{
		AllowedOrigins: []string{"*"},
	})
	upgrader := NewUpgrader(hub)

	// 创建测试请求
	req := httptest.NewRequest("GET", "http://localhost:8080/ws", nil)
	req.Header.Set("Origin", "http://any-origin.com")

	w := httptest.NewRecorder()

	// 尝试接受连接
	_, err := upgrader.Accept(w, req)

	// 不应该因为 Origin 验证而失败
	if err != nil && strings.Contains(err.Error(), "origin not allowed") {
		t.Errorf("Origin should be allowed with wildcard: %v", err)
	}
}

// TestUpgrader_Accept_NoOriginHeader 测试没有 Origin 头的请求
func TestUpgrader_Accept_NoOriginHeader(t *testing.T) {
	hub := NewHub(&ServerConfig{
		AllowedOrigins: []string{}, // 空列表拒绝所有
	})
	upgrader := NewUpgrader(hub)

	// 创建测试请求（不设置 Origin 头）
	req := httptest.NewRequest("GET", "http://localhost:8080/ws", nil)

	w := httptest.NewRecorder()

	// 尝试接受连接
	conn, err := upgrader.Accept(w, req)

	// 应该返回错误（因为空 origin 不在允许列表中）
	if err == nil {
		t.Error("Expected error when origin header is missing")
	}

	if conn != nil {
		t.Error("Expected nil connection when origin header is missing")
	}

	if w.Code != http.StatusForbidden {
		t.Errorf("Expected status 403, got %d", w.Code)
	}
}

// TestUpgrader_Accept_EmptyOriginAllowed 测试空 origin 在某些情况下被允许
func TestUpgrader_Accept_EmptyOriginAllowed(t *testing.T) {
	hub := NewHub(&ServerConfig{
		AllowedOrigins: []string{"*"}, // 星号应该匹配所有，包括空
	})
	upgrader := NewUpgrader(hub)

	// 创建测试请求（不设置 Origin 头）
	req := httptest.NewRequest("GET", "http://localhost:8080/ws", nil)

	w := httptest.NewRecorder()

	// 尝试接受连接
	_, err := upgrader.Accept(w, req)

	// 使用星号时，空 origin 应该被允许
	if err != nil && strings.Contains(err.Error(), "origin not allowed") {
		t.Errorf("Empty origin should be allowed with wildcard: %v", err)
	}
}

// TestUpgrader_MaxMessageSize 测试消息大小限制被应用
func TestUpgrader_MaxMessageSize(t *testing.T) {
	hub := NewHub(&ServerConfig{
		BaseConfig: BaseConfig{
			MaxMessageSize: 1024, // 1KB
		},
		AllowedOrigins: []string{"*"},
	})
	upgrader := NewUpgrader(hub)

	// 验证配置被正确设置
	if hub.config.MaxMessageSize != 1024 {
		t.Errorf("Expected MaxMessageSize 1024, got %d", hub.config.MaxMessageSize)
	}

	// 注意：实际的消息大小限制在 Accept 时应用
	// 这里我们只能验证配置被正确设置
	if upgrader.hub.config.MaxMessageSize != 1024 {
		t.Error("MaxMessageSize not propagated to upgrader")
	}
}

// TestUpgrader_NilServerConfig 测试 nil 配置使用默认值
func TestUpgrader_NilServerConfig(t *testing.T) {
	hub := NewHub(nil) // 使用默认配置
	upgrader := NewUpgrader(hub)

	if upgrader.hub.config == nil {
		t.Fatal("Hub config should not be nil")
	}

	// 验证默认值
	if upgrader.hub.config.AllowedOrigins == nil {
		t.Error("AllowedOrigins should be initialized with default value")
	}

	if upgrader.hub.config.MaxMessageSize == 0 {
		t.Error("MaxMessageSize should have default value")
	}
}

// BenchmarkIsAllowedOrigin 性能基准测试
func BenchmarkIsAllowedOrigin(b *testing.B) {
	allowed := []string{"http://example.com", "https://example.com", "http://localhost:8080"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isAllowedOrigin("http://example.com", allowed)
	}
}

// BenchmarkIsAllowedOrigin_Wildcard 性能基准测试（通配符）
func BenchmarkIsAllowedOrigin_Wildcard(b *testing.B) {
	allowed := []string{"*"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isAllowedOrigin("http://example.com", allowed)
	}
}
