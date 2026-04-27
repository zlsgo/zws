package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zlsgo/zws"
)

// TestReconnectPolicy 测试重连策略
func TestReconnectPolicy(t *testing.T) {
	tests := []struct {
		name        string
		reconnect   bool
		maxAttempts int
		attempt     int
		want        bool
	}{
		{
			name:        "重连未启用",
			reconnect:   false,
			maxAttempts: -1,
			attempt:     0,
			want:        false,
		},
		{
			name:        "无限重试",
			reconnect:   true,
			maxAttempts: -1,
			attempt:     100,
			want:        true,
		},
		{
			name:        "达到最大尝试次数",
			reconnect:   true,
			maxAttempts: 3,
			attempt:     3,
			want:        false,
		},
		{
			name:        "未达到最大尝试次数",
			reconnect:   true,
			maxAttempts: 3,
			attempt:     2,
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &zws.ClientConfig{
				Reconnect:            tt.reconnect,
				MaxReconnectAttempts: tt.maxAttempts,
			}
			policy := NewDefaultReconnectPolicy(config)
			got := policy.ShouldReconnect(errors.New("test error"), tt.attempt)
			if got != tt.want {
				t.Errorf("ShouldReconnect() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestReconnectDelay 测试重连延迟
func TestReconnectDelay(t *testing.T) {
	config := &zws.ClientConfig{
		ReconnectDelay:       100 * time.Millisecond,
		MaxReconnectAttempts: 3,
	}
	policy := NewDefaultReconnectPolicy(config)

	// 测试不同尝试次数的延迟
	for i := 0; i < 5; i++ {
		delay := policy.NextDelay(i)
		if delay != 100*time.Millisecond {
			t.Errorf("NextDelay(%d) = %v, want %v", i, delay, 100*time.Millisecond)
		}
	}
}

// TestReconnectMaxAttempts 测试最大尝试次数
func TestReconnectMaxAttempts(t *testing.T) {
	config := &zws.ClientConfig{
		MaxReconnectAttempts: 5,
	}
	policy := NewDefaultReconnectPolicy(config)

	if got := policy.MaxAttempts(); got != 5 {
		t.Errorf("MaxAttempts() = %v, want %v", got, 5)
	}
}

// TestIsRecoverableError 测试错误可恢复性判断
func TestIsRecoverableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "generic error",
			err:  errors.New("connection lost"),
			want: true,
		},
		{
			name: "context canceled",
			err:  context.Canceled,
			want: true, // 当前实现认为所有非显式关闭的错误都是可恢复的
		},
		{
			name: "context deadline exceeded",
			err:  context.DeadlineExceeded,
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsRecoverableError(tt.err)
			if got != tt.want {
				t.Errorf("IsRecoverableError() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestClientWithReconnectConfig 测试创建启用重连的客户端
func TestClientWithReconnectConfig(t *testing.T) {
	config := &zws.ClientConfig{
		Reconnect:            true,
		ReconnectDelay:       1 * time.Second,
		MaxReconnectAttempts: 10,
	}

	client, err := NewClient("ws://example.com", config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	if client.config.Reconnect != true {
		t.Error("Expected client to have Reconnect enabled")
	}

	if client.config.ReconnectDelay != 1*time.Second {
		t.Errorf("Expected ReconnectDelay to be 1s, got %v", client.config.ReconnectDelay)
	}

	if client.config.MaxReconnectAttempts != 10 {
		t.Errorf("Expected MaxReconnectAttempts to be 10, got %d", client.config.MaxReconnectAttempts)
	}
}

// TestClientOnConnectionLoss 测试设置 OnConnectionLoss 回调
func TestClientOnConnectionLoss(t *testing.T) {
	config := &zws.ClientConfig{}
	client, err := NewClient("ws://example.com", config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	client.OnConnectionLoss(func(c *Client, err error) {
		// 回调已设置
	})

	// 验证回调已设置
	if client.onConnectionLoss() == nil {
		t.Error("Expected OnConnectionLoss callback to be set")
	}

	// 注意：我们无法轻易触发真实的 Connection Loss 事件
	// 因为那需要实际的网络连接和服务器
}

// TestReconnectPolicyNegativeAttempts 测试负数最大尝试次数（无限重试）
func TestReconnectPolicyNegativeAttempts(t *testing.T) {
	config := &zws.ClientConfig{
		Reconnect:            true,
		MaxReconnectAttempts: -1, // -1 表示无限重试
	}
	policy := NewDefaultReconnectPolicy(config)

	// 无限重试时，即使尝试次数很大也应该返回 true
	for i := 0; i < 1000; i++ {
		if !policy.ShouldReconnect(errors.New("test"), i) {
			t.Errorf("ShouldReconnect() should return true for all attempts when MaxAttempts is -1, failed at attempt %d", i)
		}
	}
}
