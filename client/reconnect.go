package client

import (
	"context"
	"time"

	"github.com/zlsgo/zws"
)

// ReconnectPolicy 定义重连策略接口
type ReconnectPolicy interface {
	// ShouldReconnect 判断是否应该重连
	// err: 导致断开的错误
	// attempt: 当前重连尝试次数（从 0 开始）
	ShouldReconnect(err error, attempt int) bool

	// NextDelay 返回下次重连的延迟时间
	NextDelay(attempt int) time.Duration

	// MaxAttempts 返回最大重连尝试次数
	// -1 表示无限重试
	MaxAttempts() int
}

// DefaultReconnectPolicy 默认重连策略
type DefaultReconnectPolicy struct {
	delay       time.Duration
	maxAttempts int
	config      *zws.ClientConfig
}

// NewDefaultReconnectPolicy 创建默认重连策略
func NewDefaultReconnectPolicy(config *zws.ClientConfig) *DefaultReconnectPolicy {
	return &DefaultReconnectPolicy{
		delay:       config.ReconnectDelay,
		maxAttempts: config.MaxReconnectAttempts,
		config:      config,
	}
}

// ShouldReconnect 判断是否应该重连
func (p *DefaultReconnectPolicy) ShouldReconnect(err error, attempt int) bool {
	// 检查是否启用了重连
	if !p.config.Reconnect {
		return false
	}

	// 检查是否超过最大尝试次数
	if p.maxAttempts >= 0 && attempt >= p.maxAttempts {
		return false
	}

	// 检查上下文是否已取消
	// 注意：这里需要外部传入 context 或者在调用方检查
	return true
}

// NextDelay 返回下次重连的延迟时间
func (p *DefaultReconnectPolicy) NextDelay(attempt int) time.Duration {
	// 简单的固定延迟策略
	// 未来可以扩展为指数退避
	return p.delay
}

// MaxAttempts 返回最大重连尝试次数
func (p *DefaultReconnectPolicy) MaxAttempts() int {
	return p.maxAttempts
}

// IsRecoverableError 判断错误是否可恢复（属于 Connection Loss）
// 返回 true 表示可重连，false 表示应该结束 Logical Session
func IsRecoverableError(err error) bool {
	if err == nil {
		return false
	}

	// 将来可以根据错误类型更精细地判断
	// 目前认为所有非显式关闭的错误都是可恢复的
	// 显式关闭通常通过 context.cancel() 触发，不会到达这里
	return true
}

// reconnectLoop 重连循环
// 在连接断开后尝试重连
func (c *Client) reconnectLoop(ctx context.Context, policy ReconnectPolicy, initialErr error) {
	attempt := 0

	for {
		// 检查是否应该重连
		if !policy.ShouldReconnect(initialErr, attempt) {
			// 不应该重连，结束 Logical Session
			c.Close()
			return
		}

		// 等待延迟
		delay := policy.NextDelay(attempt)
		select {
		case <-time.After(delay):
			// 继续重连
		case <-ctx.Done():
			// 上下文已取消，不再重连
			c.Close()
			return
		}

		// 尝试重连
		err := c.Connect()
		if err == nil {
			// 重连成功
			return
		}

		// 重连失败，记录并继续尝试
		attempt++
		if onError := c.onError(); onError != nil {
			onError(c, err)
		}
	}
}
