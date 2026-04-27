package zws

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

// Hub 管理 WebSocket 连接池
type Hub struct {
	mu       sync.RWMutex
	conns    map[string]*Conn
	rooms    map[string]*Room
	handlers *Handlers
	config   *ServerConfig
}

// Handlers 定义事件回调
type Handlers struct {
	// OnConnect 连接建立时回调
	OnConnect func(*Conn)
	// OnMessage 收到消息时回调
	OnMessage func(*Conn, []byte)
	// OnDisconnect 连接断开时回调
	OnDisconnect func(*Conn)
	// OnError 发生错误时回调
	OnError func(*Conn, error)
}

// NewHub 创建新的 Hub
func NewHub(config *ServerConfig) *Hub {
	config = normalizeServerConfig(config)
	return &Hub{
		conns:    make(map[string]*Conn),
		rooms:    make(map[string]*Room),
		handlers: &Handlers{},
		config:   config,
	}
}

// Register 注册新连接
func (h *Hub) Register(conn *Conn) {
	h.mu.Lock()
	if conn.id == "" {
		conn.id = h.generateID()
	}
	conn.hub = h

	h.conns[conn.id] = conn
	onConnect := h.handlers.OnConnect
	h.mu.Unlock()

	if onConnect != nil {
		onConnect(conn)
	}
}

// Unregister 注销连接
func (h *Hub) Unregister(conn *Conn) {
	h.mu.Lock()

	if _, ok := h.conns[conn.id]; ok {
		delete(h.conns, conn.id)

		type roomEntry struct {
			id   string
			room *Room
		}

		// 复制房间列表，在锁外执行 Leave 操作
		rooms := make([]roomEntry, 0, len(h.rooms))
		for id, room := range h.rooms {
			rooms = append(rooms, roomEntry{id: id, room: room})
		}
		onDisconnect := h.handlers.OnDisconnect
		h.mu.Unlock()

		// 在锁外执行 Leave 操作，并在必要时回收空房间
		for _, entry := range rooms {
			entry.room.Leave(conn)
			if entry.room.IsEmpty() {
				h.mu.Lock()
				if current, ok := h.rooms[entry.id]; ok && current == entry.room && current.IsEmpty() {
					delete(h.rooms, entry.id)
				}
				h.mu.Unlock()
			}
		}

		if onDisconnect != nil {
			onDisconnect(conn)
		}
		return
	}
	h.mu.Unlock()
}

// Get 获取连接
func (h *Hub) Get(id string) (*Conn, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	conn, ok := h.conns[id]
	return conn, ok
}

// Count 返回连接数
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.conns)
}

// GetAllConns 安全地获取所有连接的副本
func (h *Hub) GetAllConns() []*Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()

	conns := make([]*Conn, 0, len(h.conns))
	for _, conn := range h.conns {
		conns = append(conns, conn)
	}
	return conns
}

// Broadcast 广播消息到所有连接
func (h *Hub) Broadcast(data []byte) {
	h.mu.RLock()
	// 复制连接列表，避免在锁外访问
	conns := make([]*Conn, 0, len(h.conns))
	for _, conn := range h.conns {
		conns = append(conns, conn)
	}
	h.mu.RUnlock()

	// 在锁外执行发送，避免持锁阻塞
	for _, conn := range conns {
		if err := conn.Send(data); err != nil {
			_ = conn.Close()
		}
	}
}

// BroadcastJSON 广播 JSON 消息到所有连接
func (h *Hub) BroadcastJSON(v any) error {
	data, err := h.config.Codec.Encode(v)
	if err != nil {
		return err
	}
	h.Broadcast(data)
	return nil
}

// Send 发送消息到指定连接
func (h *Hub) Send(id string, data []byte) error {
	conn, ok := h.Get(id)
	if !ok {
		return ErrClientNotFound
	}
	return conn.Send(data)
}

// SendJSON 发送 JSON 消息到指定连接
func (h *Hub) SendJSON(id string, v any) error {
	conn, ok := h.Get(id)
	if !ok {
		return ErrClientNotFound
	}
	return conn.JSON(v)
}

// JoinRoom 加入房间
func (h *Hub) JoinRoom(roomID string, conn *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	room, ok := h.rooms[roomID]
	if !ok {
		room = NewRoom(roomID)
		h.rooms[roomID] = room
	}
	room.Join(conn)
}

// LeaveRoom 离开房间
func (h *Hub) LeaveRoom(roomID string, conn *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if room, ok := h.rooms[roomID]; ok {
		room.Leave(conn)
		if room.IsEmpty() {
			delete(h.rooms, roomID)
		}
	}
}

// SendToRoom 发送消息到指定房间
func (h *Hub) SendToRoom(roomID string, data []byte) error {
	h.mu.RLock()
	room, ok := h.rooms[roomID]
	h.mu.RUnlock()
	if !ok {
		return ErrRoomNotFound
	}
	room.Broadcast(data)
	return nil
}

// SendJSONToRoom 发送 JSON 消息到指定房间
func (h *Hub) SendJSONToRoom(roomID string, v any) error {
	data, err := h.config.Codec.Encode(v)
	if err != nil {
		return err
	}
	return h.SendToRoom(roomID, data)
}

// GetRoomSize 获取房间大小
func (h *Hub) GetRoomSize(roomID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if room, ok := h.rooms[roomID]; ok {
		return room.Size()
	}
	return 0
}

// OnConnect 设置连接回调
func (h *Hub) OnConnect(fn func(*Conn)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlers.OnConnect = fn
}

// OnMessage 设置消息回调
func (h *Hub) OnMessage(fn func(*Conn, []byte)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlers.OnMessage = fn
}

// OnDisconnect 设置断开回调
func (h *Hub) OnDisconnect(fn func(*Conn)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlers.OnDisconnect = fn
}

// OnError 设置错误回调
func (h *Hub) OnError(fn func(*Conn, error)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlers.OnError = fn
}

func (h *Hub) messageHandler() func(*Conn, []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.handlers.OnMessage
}

func (h *Hub) reportError(conn *Conn, err error) {
	if err == nil {
		return
	}

	h.mu.RLock()
	onError := h.handlers.OnError
	h.mu.RUnlock()

	if onError != nil {
		onError(conn, err)
	}
}

// generateID 生成唯一连接 ID
// UUID v4 冲突概率极低（1/2^122），无需循环检查
func (h *Hub) generateID() string {
	return uuid.New().String()
}

// Room 管理房间内的连接
type Room struct {
	id    string
	conns map[string]*Conn
	mu    sync.RWMutex
}

// NewRoom 创建新房间
func NewRoom(id string) *Room {
	return &Room{
		id:    id,
		conns: make(map[string]*Conn),
	}
}

// ID 返回房间 ID
func (r *Room) ID() string {
	return r.id
}

// Join 加入房间
func (r *Room) Join(conn *Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.conns[conn.id] = conn
}

// Leave 离开房间
func (r *Room) Leave(conn *Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.conns, conn.id)
}

// Broadcast 广播消息到房间内所有连接
func (r *Room) Broadcast(data []byte) {
	r.mu.RLock()
	conns := make([]*Conn, 0, len(r.conns))
	for _, conn := range r.conns {
		conns = append(conns, conn)
	}
	r.mu.RUnlock()

	for _, conn := range conns {
		if err := conn.Send(data); err != nil {
			_ = conn.Close()
		}
	}
}

// Size 返回房间大小
func (r *Room) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.conns)
}

// IsEmpty 检查房间是否为空
func (r *Room) IsEmpty() bool {
	return r.Size() == 0
}

// String 返回房间信息
func (r *Room) String() string {
	return fmt.Sprintf("Room(%s, %d connections)", r.id, r.Size())
}
