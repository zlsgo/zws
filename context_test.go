package zws

import (
	"testing"
)

// TestWebSocketContext_Conn 测试获取连接
func TestWebSocketContext_Conn(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	wsCtx := &WebSocketContext{
		conn: conn,
		hub:  hub,
	}

	retrievedConn := wsCtx.Conn()
	if retrievedConn != conn {
		t.Error("Conn() returned different connection")
	}
}

// TestWebSocketContext_Hub 测试获取 Hub
func TestWebSocketContext_Hub(t *testing.T) {
	hub := NewHub(nil)

	wsCtx := &WebSocketContext{
		hub: hub,
	}

	retrievedHub := wsCtx.Hub()
	if retrievedHub != hub {
		t.Error("Hub() returned different hub")
	}
}

// TestWebSocketContext_Send 测试发送消息
func TestWebSocketContext_Send(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	wsCtx := &WebSocketContext{
		conn: conn,
		hub:  hub,
	}

	data := []byte("test message")
	err := wsCtx.Send(data)
	if err != nil {
		t.Errorf("Send() returned error: %v", err)
	}

	// 验证消息在 channel 中
	select {
	case msg := <-conn.send:
		if string(msg) != string(data) {
			t.Error("Message mismatch")
		}
	default:
		t.Error("Message not in send channel")
	}
}

// TestWebSocketContext_JSON 测试发送 JSON 消息
func TestWebSocketContext_JSON(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	wsCtx := &WebSocketContext{
		conn: conn,
		hub:  hub,
	}

	data := map[string]string{"hello": "world"}
	err := wsCtx.JSON(data)
	if err != nil {
		t.Errorf("JSON() returned error: %v", err)
	}

	// 验证消息被发送
	select {
	case msg := <-conn.send:
		if len(msg) == 0 {
			t.Error("Empty message sent")
		}
	default:
		t.Error("Message not in send channel")
	}
}

// TestWebSocketContext_Emit 测试向房间发送消息
func TestWebSocketContext_Emit(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	wsCtx := &WebSocketContext{
		conn: conn,
		hub:  hub,
	}

	// 加入房间
	hub.Register(conn)
	hub.JoinRoom("test-room", conn)

	data := map[string]string{"event": "test"}
	err := wsCtx.Emit("test-room", data)
	if err != nil {
		t.Errorf("Emit() returned error: %v", err)
	}

	// 清空 send channel
	for len(conn.send) > 0 {
		<-conn.send
	}
}

// TestWebSocketContext_Broadcast 测试广播消息
func TestWebSocketContext_Broadcast(t *testing.T) {
	hub := NewHub(nil)
	conn1 := NewConn("conn-1", nil, hub, JSONCodec{})
	conn2 := NewConn("conn-2", nil, hub, JSONCodec{})

	hub.Register(conn1)
	hub.Register(conn2)

	wsCtx := &WebSocketContext{
		conn: conn1,
		hub:  hub,
	}

	data := map[string]string{"msg": "broadcast"}
	err := wsCtx.Broadcast(data)
	if err != nil {
		t.Errorf("Broadcast() returned error: %v", err)
	}

	// 清空 send channels
	for len(conn1.send) > 0 {
		<-conn1.send
	}
	for len(conn2.send) > 0 {
		<-conn2.send
	}
}

// TestWebSocketContext_JoinRoom 测试加入房间
func TestWebSocketContext_JoinRoom(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	hub.Register(conn)

	wsCtx := &WebSocketContext{
		conn: conn,
		hub:  hub,
	}

	wsCtx.JoinRoom("test-room")

	size := hub.GetRoomSize("test-room")
	if size != 1 {
		t.Errorf("Expected room size 1, got %d", size)
	}
}

// TestWebSocketContext_LeaveRoom 测试离开房间
func TestWebSocketContext_LeaveRoom(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	hub.Register(conn)
	hub.JoinRoom("test-room", conn)

	wsCtx := &WebSocketContext{
		conn: conn,
		hub:  hub,
	}

	wsCtx.LeaveRoom("test-room")

	size := hub.GetRoomSize("test-room")
	if size != 0 {
		t.Errorf("Expected room size 0, got %d", size)
	}
}

// TestWebSocketContext_GetRoomSize 测试获取房间大小
func TestWebSocketContext_GetRoomSize(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	hub.Register(conn)
	hub.JoinRoom("test-room", conn)

	wsCtx := &WebSocketContext{
		conn: conn,
		hub:  hub,
	}

	size := wsCtx.GetRoomSize("test-room")
	if size != 1 {
		t.Errorf("Expected room size 1, got %d", size)
	}

	// 测试不存在的房间
	size = wsCtx.GetRoomSize("non-existent")
	if size != 0 {
		t.Errorf("Expected room size 0 for non-existent room, got %d", size)
	}
}

// TestNewWebSocketContext 测试创建 WebSocket 上下文
func TestNewWebSocketContext(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 创建 WebSocket 上下文（不使用 znet.Context）
	wsCtx := &WebSocketContext{
		conn: conn,
		hub:  hub,
	}

	if wsCtx == nil {
		t.Fatal("WebSocketContext creation failed")
	}

	if wsCtx.conn != conn {
		t.Error("Connection not set correctly")
	}

	if wsCtx.hub != hub {
		t.Error("Hub not set correctly")
	}
}

// BenchmarkWebSocketContext_Send 性能基准测试
func BenchmarkWebSocketContext_Send(b *testing.B) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	wsCtx := &WebSocketContext{
		conn: conn,
		hub:  hub,
	}
	data := []byte("test")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wsCtx.Send(data)
		// 清空 channel 避免阻塞
		select {
		case <-conn.send:
		default:
		}
	}
}

// BenchmarkWebSocketContext_JSON 性能基准测试
func BenchmarkWebSocketContext_JSON(b *testing.B) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})
	wsCtx := &WebSocketContext{
		conn: conn,
		hub:  hub,
	}
	data := map[string]string{"test": "data"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wsCtx.JSON(data)
		// 清空 channel 避免阻塞
		select {
		case <-conn.send:
		default:
		}
	}
}
