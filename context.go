package zws

import (
	"github.com/sohaha/zlsgo/znet"
)

// WebSocketContext 扩展 znet.Context，添加 WebSocket 功能
type WebSocketContext struct {
	*znet.Context
	conn *Conn
	hub  *Hub
}

// Conn 返回当前 WebSocket 连接
func (c *WebSocketContext) Conn() *Conn {
	return c.conn
}

// Hub 返回 Hub 实例
func (c *WebSocketContext) Hub() *Hub {
	return c.hub
}

// Send 发送消息
func (c *WebSocketContext) Send(data []byte) error {
	return c.conn.Send(data)
}

// JSON 发送 JSON 消息
func (c *WebSocketContext) JSON(v any) error {
	return c.conn.JSON(v)
}

// Emit 向指定房间发送消息
func (c *WebSocketContext) Emit(roomID string, v any) error {
	return c.hub.SendJSONToRoom(roomID, v)
}

// Broadcast 广播消息到所有连接
func (c *WebSocketContext) Broadcast(v any) error {
	return c.hub.BroadcastJSON(v)
}

// JoinRoom 加入房间
func (c *WebSocketContext) JoinRoom(roomID string) {
	c.hub.JoinRoom(roomID, c.conn)
}

// LeaveRoom 离开房间
func (c *WebSocketContext) LeaveRoom(roomID string) {
	c.hub.LeaveRoom(roomID, c.conn)
}

// GetRoomSize 获取房间大小
func (c *WebSocketContext) GetRoomSize(roomID string) int {
	return c.hub.GetRoomSize(roomID)
}

// NewWebSocketContext 创建 WebSocket 上下文
func NewWebSocketContext(ctx *znet.Context, conn *Conn, hub *Hub) *WebSocketContext {
	return &WebSocketContext{
		Context: ctx,
		conn:    conn,
		hub:     hub,
	}
}
