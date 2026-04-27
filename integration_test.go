package zws

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestIntegration_Conn_SendChannel 测试 Conn 的 send channel 功能
func TestIntegration_Conn_SendChannel(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 测试发送消息到 channel
	testMessage := []byte("Test message")
	err := conn.Send(testMessage)
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	// 验证消息在 channel 中
	select {
	case msg := <-conn.send:
		if string(msg) != string(testMessage) {
			t.Errorf("Expected %s, got %s", testMessage, msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Message not in send channel")
	}
}

// TestIntegration_Conn_MultipleSend 测试多次发送
func TestIntegration_Conn_MultipleSend(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 发送多条消息
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("Message %d", i))
		err := conn.Send(msg)
		if err != nil {
			t.Errorf("Failed to send message %d: %v", i, err)
		}
	}

	// 验证所有消息都在 channel 中
	receivedCount := 0
	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-conn.send:
			receivedCount++
		case <-timeout:
			if receivedCount != 10 {
				t.Errorf("Expected 10 messages, got %d", receivedCount)
			}
			return
		}
	}
}

// TestIntegration_Hub_ConnManagement 测试 Hub 的连接管理
func TestIntegration_Hub_ConnManagement(t *testing.T) {
	hub := NewHub(nil)

	// 创建连接
	conn1 := NewConn("conn-1", nil, hub, JSONCodec{})
	conn2 := NewConn("conn-2", nil, hub, JSONCodec{})

	// 注册连接
	hub.Register(conn1)
	hub.Register(conn2)

	// 验证连接数
	if hub.Count() != 2 {
		t.Errorf("Expected 2 connections, got %d", hub.Count())
	}

	// 获取连接
	retrievedConn, ok := hub.Get("conn-1")
	if !ok {
		t.Error("Failed to get connection")
	}
	if retrievedConn != conn1 {
		t.Error("Retrieved wrong connection")
	}

	// 注销连接
	hub.Unregister(conn1)

	// 验证连接数减少
	if hub.Count() != 1 {
		t.Errorf("Expected 1 connection after unregister, got %d", hub.Count())
	}

	// 验证连接已注销
	_, ok = hub.Get("conn-1")
	if ok {
		t.Error("Connection should be unregistered")
	}
}

// TestIntegration_Hub_Broadcast 测试 Hub.Broadcast 功能
func TestIntegration_Hub_Broadcast(t *testing.T) {
	hub := NewHub(nil)

	// 创建多个连接
	connCount := 5
	connections := make([]*Conn, connCount)
	receivedCounts := make([]atomic.Int32, connCount)
	done := make(chan struct{}, connCount)

	for i := 0; i < connCount; i++ {
		conn := NewConn(fmt.Sprintf("conn-%d", i), nil, hub, JSONCodec{})
		connections[i] = conn
		hub.Register(conn)

		// 启动接收器
		go func(idx int) {
			defer func() { done <- struct{}{} }()
			for {
				select {
				case data := <-conn.send:
					if len(data) > 0 {
						receivedCounts[idx].Add(1)
					}
					return
				case <-time.After(2 * time.Second):
					return
				}
			}
		}(i)
	}

	// 等待接收器就绪
	time.Sleep(100 * time.Millisecond)

	// 广播消息
	broadcastMsg := []byte("Broadcast test")
	hub.Broadcast(broadcastMsg)

	// 等待所有接收器完成
	for i := 0; i < connCount; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("Timeout waiting for receivers to complete")
		}
	}

	// 验证至少有一些连接收到了消息
	totalReceived := int32(0)
	for i := range receivedCounts {
		totalReceived += receivedCounts[i].Load()
	}

	if totalReceived == 0 {
		t.Error("No connections received broadcast message")
	}

	t.Logf("Total messages received: %d out of %d connections", totalReceived, connCount)
}

// TestIntegration_ConcurrentOperations 测试并发操作
func TestIntegration_ConcurrentOperations(t *testing.T) {
	hub := NewHub(nil)

	// 并发创建多个连接
	connCount := 20
	var wg sync.WaitGroup

	for i := 0; i < connCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			conn := NewConn(fmt.Sprintf("conn-%d", idx), nil, hub, JSONCodec{})
			hub.Register(conn)

			// 发送一些消息
			for j := 0; j < 10; j++ {
				conn.Send([]byte(fmt.Sprintf("Msg %d", j)))
			}

			// 清空 channel
			for len(conn.send) > 0 {
				<-conn.send
			}
		}(i)
	}

	wg.Wait()

	// 验证所有连接都已注册
	if hub.Count() != connCount {
		t.Errorf("Expected %d connections, got %d", connCount, hub.Count())
	}
}

// TestIntegration_Room_JoinLeave 测试房间加入和离开
func TestIntegration_Room_JoinLeave(t *testing.T) {
	hub := NewHub(nil)

	// 创建房间
	room := NewRoom("test-room")

	// 创建连接
	conn1 := NewConn("conn-1", nil, hub, JSONCodec{})
	conn2 := NewConn("conn-2", nil, hub, JSONCodec{})

	hub.Register(conn1)
	hub.Register(conn2)

	// 加入房间
	room.Join(conn1)
	room.Join(conn2)

	// 验证房间大小
	if room.Size() != 2 {
		t.Errorf("Expected room size 2, got %d", room.Size())
	}

	// 广播到房间
	room.Broadcast([]byte("room message"))

	// 清空 send channel
	for len(conn1.send) > 0 {
		<-conn1.send
	}
	for len(conn2.send) > 0 {
		<-conn2.send
	}

	// 离开房间
	room.Leave(conn1)

	// 验证房间大小减少
	if room.Size() != 1 {
		t.Errorf("Expected room size 1 after leave, got %d", room.Size())
	}

	// 验证房间 ID
	if room.ID() != "test-room" {
		t.Errorf("Expected room ID 'test-room', got %s", room.ID())
	}
}

// TestIntegration_Hub_RoomOperations 测试 Hub 的房间操作
func TestIntegration_Hub_RoomOperations(t *testing.T) {
	hub := NewHub(nil)

	// 创建连接
	conn1 := NewConn("conn-1", nil, hub, JSONCodec{})
	conn2 := NewConn("conn-2", nil, hub, JSONCodec{})

	hub.Register(conn1)
	hub.Register(conn2)

	// 加入房间
	hub.JoinRoom("test-room", conn1)
	hub.JoinRoom("test-room", conn2)

	// 验证房间大小
	size := hub.GetRoomSize("test-room")
	if size != 2 {
		t.Errorf("Expected room size 2, got %d", size)
	}

	// 发送消息到房间
	err := hub.SendToRoom("test-room", []byte("room message"))
	if err != nil {
		t.Errorf("Failed to send to room: %v", err)
	}

	// 清空 send channel
	for len(conn1.send) > 0 {
		<-conn1.send
	}
	for len(conn2.send) > 0 {
		<-conn2.send
	}

	// 离开房间
	hub.LeaveRoom("test-room", conn1)

	// 验证房间大小减少
	size = hub.GetRoomSize("test-room")
	if size != 1 {
		t.Errorf("Expected room size 1 after leave, got %d", size)
	}

	// 再次离开
	hub.LeaveRoom("test-room", conn2)

	// 验证房间为空
	size = hub.GetRoomSize("test-room")
	if size != 0 {
		t.Errorf("Expected room size 0, got %d", size)
	}
}

// TestIntegration_Metadata_ConcurrentAccess 测试元数据并发访问
func TestIntegration_Metadata_ConcurrentAccess(t *testing.T) {
	hub := NewHub(nil)
	conn := NewConn("test-conn", nil, hub, JSONCodec{})

	// 并发读写元数据
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				conn.Set(fmt.Sprintf("key-%d", idx), fmt.Sprintf("value-%d", j))
				conn.Get(fmt.Sprintf("key-%d", idx))
			}
		}(i)
	}

	wg.Wait()

	// 验证没有 panic 或死锁
	t.Log("Concurrent metadata access completed successfully")
}

// BenchmarkIntegration_Conn_Lifecycle 连接生命周期性能基准测试
func BenchmarkIntegration_Conn_Lifecycle(b *testing.B) {
	hub := NewHub(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := NewConn("", nil, hub, JSONCodec{})
		hub.Register(conn)
		conn.Send([]byte("test"))
		// 清空 channel
		for len(conn.send) > 0 {
			<-conn.send
		}
		hub.Unregister(conn)
	}
}

// BenchmarkIntegration_MultipleConnections 多连接性能基准测试
func BenchmarkIntegration_MultipleConnections(b *testing.B) {
	hub := NewHub(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for j := 0; j < 10; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn := NewConn("", nil, hub, JSONCodec{})
				hub.Register(conn)
				conn.Send([]byte("test"))
				// 清空 channel
				for len(conn.send) > 0 {
					<-conn.send
				}
			}()
		}
		wg.Wait()
	}
}

// BenchmarkIntegration_Hub_Broadcast Hub 广播性能基准测试
func BenchmarkIntegration_Hub_Broadcast(b *testing.B) {
	hub := NewHub(nil)

	// 创建多个连接
	connections := make([]*Conn, 100)
	for i := 0; i < 100; i++ {
		connections[i] = NewConn(fmt.Sprintf("conn-%d", i), nil, hub, JSONCodec{})
		hub.Register(connections[i])

		// 启动接收器
		go func(conn *Conn) {
			for {
				select {
				case <-conn.send:
				case <-time.After(time.Microsecond):
					return
				}
			}
		}(connections[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hub.Broadcast([]byte("broadcast message"))
	}
}
