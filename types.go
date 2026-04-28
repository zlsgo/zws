package zws

import (
	"nhooyr.io/websocket"
)

// MessageType 表示 WebSocket 消息类型
type MessageType = websocket.MessageType

// WebSocket 消息类型常量
const (
	MessageText   MessageType = websocket.MessageText   // 文本消息
	MessageBinary MessageType = websocket.MessageBinary // 二进制消息
)

// StatusCode 表示 WebSocket 关闭状态码
// https://tools.ietf.org/html/rfc6455#section-7.4
type StatusCode = websocket.StatusCode

// 常用状态码常量
const (
	StatusNormalClosure           StatusCode = websocket.StatusNormalClosure           // 1000 正常关闭
	StatusGoingAway               StatusCode = websocket.StatusGoingAway               // 1001 端点离开
	StatusProtocolError           StatusCode = websocket.StatusProtocolError           // 1002 协议错误
	StatusUnsupportedData         StatusCode = websocket.StatusUnsupportedData         // 1003 不支持的数据类型
	StatusNoStatusRcvd            StatusCode = websocket.StatusNoStatusRcvd            // 1005 未收到状态码
	StatusAbnormalClosure         StatusCode = websocket.StatusAbnormalClosure         // 1006 异常关闭
	StatusInvalidFramePayloadData StatusCode = websocket.StatusInvalidFramePayloadData // 1007 无效帧数据
	StatusPolicyViolation         StatusCode = websocket.StatusPolicyViolation         // 1008 策略违规
	StatusMessageTooBig           StatusCode = websocket.StatusMessageTooBig           // 1009 消息过大
	StatusMandatoryExtension      StatusCode = websocket.StatusMandatoryExtension      // 1010 缺少必需扩展
	StatusInternalError           StatusCode = websocket.StatusInternalError           // 1011 内部错误
	StatusServiceRestart          StatusCode = websocket.StatusServiceRestart          // 1012 服务重启
	StatusTryAgainLater           StatusCode = websocket.StatusTryAgainLater           // 1013 稍后重试
	StatusBadGateway              StatusCode = websocket.StatusBadGateway              // 1014 网关错误
	StatusTLSHandshake            StatusCode = websocket.StatusTLSHandshake            // 1015 TLS 握手失败
)

// CloseError 表示 WebSocket 关闭错误
type CloseError = websocket.CloseError

// CloseStatus 从错误中提取关闭状态码
// 如果错误不是 CloseError 类型，返回 -1
func CloseStatus(err error) StatusCode {
	return websocket.CloseStatus(err)
}

// CompressionMode 表示压缩模式
type CompressionMode = websocket.CompressionMode

const (
	CompressionDisabled           CompressionMode = websocket.CompressionDisabled            // 禁用压缩
	CompressionContextTakeover    CompressionMode = websocket.CompressionContextTakeover     // 启用压缩，带上下文接管
	CompressionNoContextTakeover  CompressionMode = websocket.CompressionNoContextTakeover   // 启用压缩，无上下文接管
)

// AcceptOptions WebSocket 升级选项
type AcceptOptions = websocket.AcceptOptions

// DialOptions WebSocket 客户端拨号选项
type DialOptions = websocket.DialOptions
