package zws

import "time"

// 默认配置常量
const (
	DefaultPingInterval    = 30 * time.Second
	DefaultPingWait        = 60 * time.Second
	DefaultMaxMessageSize  = 10 << 20 // 10MB
	DefaultWriteTimeout    = 5 * time.Second
	DefaultSendBufferSize  = 256
	DefaultReadBufferSize  = 4096
	DefaultWriteBufferSize = 4096
)

// BaseConfig 基础配置，包含服务端和客户端的共用配置项
type BaseConfig struct {
	// Codec 消息编解码器，默认使用 JSONCodec
	Codec MessageCodec

	// PingInterval 心跳检测间隔，默认 30 秒
	// 设置为 0 禁用心跳
	PingInterval time.Duration

	// PingWait 等待 pong 响应的超时时间，默认 60 秒
	PingWait time.Duration

	// PingMessage 自定义心跳消息内容，默认为 nil
	PingMessage any

	// MaxMessageSize 最大消息大小（字节），默认 10MB
	MaxMessageSize int64
}

// ServerConfig 服务端配置
type ServerConfig struct {
	BaseConfig

	// ReadBufferSize 读取缓冲区大小，默认 4096
	ReadBufferSize int

	// WriteBufferSize 写入缓冲区大小，默认 4096
	WriteBufferSize int

	// AllowedOrigins 允许的跨域来源列表
	// 空列表表示不允许任何跨域请求（安全默认值）
	// ["*"] 表示允许所有来源（不推荐，仅用于开发）
	// ["https://example.com"] 表示只允许该来源
	AllowedOrigins []string
}

// DefaultServerConfig 返回默认服务端配置
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		BaseConfig: BaseConfig{
			Codec:          JSONCodec{},
			PingInterval:   DefaultPingInterval,
			PingWait:       DefaultPingWait,
			MaxMessageSize: DefaultMaxMessageSize,
		},
		ReadBufferSize:  DefaultReadBufferSize,
		WriteBufferSize: DefaultWriteBufferSize,
		AllowedOrigins:  []string{}, // 默认不允许跨域
	}
}

func normalizeServerConfig(config *ServerConfig) *ServerConfig {
	defaults := DefaultServerConfig()
	if config == nil {
		return defaults
	}
	if config.Codec == nil {
		config.Codec = defaults.Codec
	}
	if config.PingWait == 0 {
		config.PingWait = defaults.PingWait
	}
	if config.MaxMessageSize == 0 {
		config.MaxMessageSize = defaults.MaxMessageSize
	}
	if config.ReadBufferSize == 0 {
		config.ReadBufferSize = defaults.ReadBufferSize
	}
	if config.WriteBufferSize == 0 {
		config.WriteBufferSize = defaults.WriteBufferSize
	}
	if config.AllowedOrigins == nil {
		config.AllowedOrigins = defaults.AllowedOrigins
	}
	return config
}

// ClientConfig 客户端配置
type ClientConfig struct {
	BaseConfig

	// HandshakeTimeout 握手超时时间，默认 45 秒
	HandshakeTimeout time.Duration

	// Reconnect 是否启用自动重连，默认 false
	Reconnect bool

	// ReconnectDelay 重连延迟，默认 5 秒
	ReconnectDelay time.Duration

	// MaxReconnectAttempts 最大重连尝试次数，默认 -1（无限重试）
	MaxReconnectAttempts int
}

// DefaultClientConfig 返回默认客户端配置
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		BaseConfig: BaseConfig{
			Codec:          JSONCodec{},
			PingInterval:   DefaultPingInterval,
			PingWait:       DefaultPingWait,
			MaxMessageSize: DefaultMaxMessageSize,
		},
		HandshakeTimeout:     45 * time.Second,
		Reconnect:            false,
		ReconnectDelay:       5 * time.Second,
		MaxReconnectAttempts: -1,
	}
}

func NormalizeClientConfig(config *ClientConfig) *ClientConfig {
	defaults := DefaultClientConfig()
	if config == nil {
		return defaults
	}
	if config.Codec == nil {
		config.Codec = defaults.Codec
	}
	if config.PingWait == 0 {
		config.PingWait = defaults.PingWait
	}
	if config.MaxMessageSize == 0 {
		config.MaxMessageSize = defaults.MaxMessageSize
	}
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = defaults.HandshakeTimeout
	}
	if config.ReconnectDelay == 0 {
		config.ReconnectDelay = defaults.ReconnectDelay
	}
	if config.MaxReconnectAttempts == 0 {
		config.MaxReconnectAttempts = defaults.MaxReconnectAttempts
	}
	return config
}
