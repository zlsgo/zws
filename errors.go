package zws

import "errors"

var (
	// ErrInvalidRawMessage 原始消息类型错误
	ErrInvalidRawMessage = errors.New("invalid raw message type, expected []byte")

	// ErrConnClosed 连接已关闭
	ErrConnClosed = errors.New("connection closed")

	// ErrRoomNotFound 房间不存在
	ErrRoomNotFound = errors.New("room not found")

	// ErrClientNotFound 客户端不存在
	ErrClientNotFound = errors.New("client not found")
)
