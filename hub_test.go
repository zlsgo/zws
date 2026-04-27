package zws

import (
	"context"
	"testing"
)

func TestHub(t *testing.T) {
	config := DefaultServerConfig()
	hub := NewHub(config)

	if hub.Count() != 0 {
		t.Errorf("Expected 0 connections, got %d", hub.Count())
	}

	// 创建测试连接
	conn := &Conn{id: "test1"}
	hub.Register(conn)

	if hub.Count() != 1 {
		t.Errorf("Expected 1 connection, got %d", hub.Count())
	}

	// 测试获取连接
	c, ok := hub.Get("test1")
	if !ok {
		t.Error("Expected to find connection")
	}
	if c.id != "test1" {
		t.Errorf("Expected 'test1', got '%s'", c.id)
	}

	// 测试注销
	hub.Unregister(conn)
	if hub.Count() != 0 {
		t.Errorf("Expected 0 connections, got %d", hub.Count())
	}
}

func TestConnCloseUnregistersFromHub(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-close", nil, hub, JSONCodec{})
	hub.Register(conn)
	hub.JoinRoom("room", conn)

	if err := conn.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if hub.Count() != 0 {
		t.Fatalf("Expected connection to be unregistered, got count %d", hub.Count())
	}
	if size := hub.GetRoomSize("room"); size != 0 {
		t.Fatalf("Expected room membership to be removed, got size %d", size)
	}
}

func TestRoom(t *testing.T) {
	config := DefaultServerConfig()
	hub := NewHub(config)

	// 创建测试连接
	conn1 := &Conn{id: "conn1"}
	conn2 := &Conn{id: "conn2"}

	// 加入房间
	hub.JoinRoom("test", conn1)
	hub.JoinRoom("test", conn2)

	// 检查房间大小
	if size := hub.GetRoomSize("test"); size != 2 {
		t.Errorf("Expected room size 2, got %d", size)
	}

	// 离开房间
	hub.LeaveRoom("test", conn1)
	if size := hub.GetRoomSize("test"); size != 1 {
		t.Errorf("Expected room size 1, got %d", size)
	}

	// 最后一个离开后房间应该被删除
	hub.LeaveRoom("test", conn2)
	if size := hub.GetRoomSize("test"); size != 0 {
		t.Errorf("Expected room size 0, got %d", size)
	}
}

func TestBroadcast(t *testing.T) {
	config := DefaultServerConfig()
	hub := NewHub(config)

	// 创建测试连接（需要初始化必要的字段）
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn1 := &Conn{
		id:     "conn1",
		send:   make(chan []byte, 10),
		ctx:    ctx,
		cancel: cancel,
	}
	conn2 := &Conn{
		id:     "conn2",
		send:   make(chan []byte, 10),
		ctx:    ctx,
		cancel: cancel,
	}

	hub.Register(conn1)
	hub.Register(conn2)

	// 广播消息
	data := []byte("test message")
	hub.Broadcast(data)

	// 检查两个连接都收到消息
	if len(conn1.send) != 1 {
		t.Errorf("Expected conn1 to receive 1 message, got %d", len(conn1.send))
	}
	if len(conn2.send) != 1 {
		t.Errorf("Expected conn2 to receive 1 message, got %d", len(conn2.send))
	}
}
