package zws

// Server WebSocket 服务端
type Server struct {
	hub    *Hub
	config *ServerConfig
}

// NewServer 创建新的服务端
func NewServer(config *ServerConfig) *Server {
	config = normalizeServerConfig(config)
	return &Server{
		hub:    NewHub(config),
		config: config,
	}
}

// Hub 返回 Hub 实例
func (s *Server) Hub() *Hub {
	return s.hub
}

// Config 返回配置
func (s *Server) Config() *ServerConfig {
	return s.config
}

// OnConnect 设置连接回调
func (s *Server) OnConnect(fn func(*Conn)) {
	s.hub.OnConnect(fn)
}

// OnMessage 设置消息回调
func (s *Server) OnMessage(fn func(*Conn, []byte)) {
	s.hub.OnMessage(fn)
}

// OnDisconnect 设置断开回调
func (s *Server) OnDisconnect(fn func(*Conn)) {
	s.hub.OnDisconnect(fn)
}

// OnError 设置错误回调
func (s *Server) OnError(fn func(*Conn, error)) {
	s.hub.OnError(fn)
}

// HandlerFunc 定义 WebSocket 处理函数类型
type HandlerFunc func(*WebSocketContext)

func (s *Server) serve(wsCtx *WebSocketContext, handlers ...HandlerFunc) {
	for _, h := range handlers {
		h(wsCtx)
	}

	conn := wsCtx.Conn()
	go conn.writePump()
	go conn.readPump(func(conn *Conn, data []byte) {
		if onMessage := s.hub.messageHandler(); onMessage != nil {
			onMessage(conn, data)
		}
	})
}

// Handler 返回 znet 处理器
func (s *Server) Handler() HandlerFunc {
	return func(wsCtx *WebSocketContext) {
		s.serve(wsCtx)
	}
}
