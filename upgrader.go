package zws

import (
	"net/http"

	"github.com/sohaha/zlsgo/znet"
)

// Upgrader HTTP 到 WebSocket 升级器
type Upgrader struct {
	hub    *Hub
}

// NewUpgrader 创建升级器
func NewUpgrader(hub *Hub) *Upgrader {
	return &Upgrader{hub: hub}
}

// Accept 接受 WebSocket 连接
func (u *Upgrader) Accept(w http.ResponseWriter, r *http.Request) (*Conn, error) {





	return u.hub.Accept(w, r)
}

// WS 为 znet.Engine 添加 WebSocket 路由支持
//
// 使用示例：
//
//	hub := zws.NewHub(nil)
//	hub.OnMessage(func(conn *zws.Conn, data []byte) {
//	    conn.JSON(map[string]string{"echo": string(data)})
//	})
//
//	zws.WS(engine, "/ws", hub, func(wsCtx *zws.WebSocketContext) {
//	    wsCtx.Log().Info("Client connected:", wsCtx.Conn().ID())
//	})
func WS(engine *znet.Engine, pattern string, hub *Hub, handlers ...HandlerFunc) {
	if hub == nil {
		hub = NewHub(nil)
	}

	engine.GET(pattern, func(c *znet.Context) {
		upgrader := NewUpgrader(hub)
		conn, err := upgrader.Accept(c.Writer, c.Request)
		if err != nil {
			hub.reportError(nil, err)
			c.Log.Error("WebSocket upgrade failed:", err)
			return
		}

		// 创建 WebSocket 上下文
		wsCtx := NewWebSocketContext(c, conn, hub)
		hub.serve(wsCtx, handlers...)
	})
}
