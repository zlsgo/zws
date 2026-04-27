package zws

import (
	"fmt"
	"net/http"

	"github.com/sohaha/zlsgo/znet"
	"nhooyr.io/websocket"
)

// Upgrader HTTP 到 WebSocket 升级器
type Upgrader struct {
	server *Server
}

// NewUpgrader 创建升级器
func NewUpgrader(server *Server) *Upgrader {
	return &Upgrader{server: server}
}

// Accept 接受 WebSocket 连接
func (u *Upgrader) Accept(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	// 验证 Origin 头
	origin := r.Header.Get("Origin")
	if !isAllowedOrigin(origin, u.server.config.AllowedOrigins) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return nil, fmt.Errorf("origin not allowed: %s", origin)
	}

	opts := &websocket.AcceptOptions{
		OriginPatterns: u.server.config.AllowedOrigins,
	}

	ws, err := websocket.Accept(w, r, opts)
	if err != nil {
		return nil, err
	}

	// 应用消息大小限制
	ws.SetReadLimit(u.server.config.MaxMessageSize)

	conn := NewConn("", ws, u.server.hub, u.server.config.Codec)
	u.server.hub.Register(conn)

	return conn, nil
}

// isAllowedOrigin 检查 origin 是否在允许列表中
func isAllowedOrigin(origin string, allowed []string) bool {
	if len(allowed) == 0 {
		return false // 默认拒绝所有跨域请求
	}
	for _, pattern := range allowed {
		if pattern == "*" || origin == pattern {
			return true
		}
	}
	return false
}

// WS 为 znet.Engine 添加 WebSocket 路由支持
//
// 使用示例：
//
//	server := zws.NewServer(nil)
//	server.OnMessage(func(conn *zws.Conn, data []byte) {
//	    conn.JSON(map[string]string{"echo": string(data)})
//	})
//
//	zws.WS(engine, "/ws", server, func(wsCtx *zws.WebSocketContext) {
//	    wsCtx.Log().Info("Client connected:", wsCtx.Conn().ID())
//	})
func WS(engine *znet.Engine, pattern string, server *Server, handlers ...HandlerFunc) {
	if server == nil {
		server = NewServer(nil)
	}

	engine.GET(pattern, func(c *znet.Context) {
		upgrader := NewUpgrader(server)
		conn, err := upgrader.Accept(c.Writer, c.Request)
		if err != nil {
			server.hub.reportError(nil, err)
			c.Log.Error("WebSocket upgrade failed:", err)
			return
		}

		// 创建 WebSocket 上下文
		wsCtx := NewWebSocketContext(c, conn, server.hub)
		server.serve(wsCtx, handlers...)
	})
}
