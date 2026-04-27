package main

import (
	"github.com/sohaha/zlsgo/znet"
	"github.com/zlsgo/zws"
)

func main() {
	// 创建 znet 引擎
	engine := znet.New()

	// 创建 WebSocket Hub
	hub := zws.NewHub(&zws.ServerConfig{
		AllowedOrigins: []string{"*"},
	})

	// 设置消息处理
	hub.OnMessage(func(conn *zws.Conn, data []byte) {
		// 广播消息到所有房间
		hub.BroadcastJSON(map[string]interface{}{
			"from":    conn.ID(),
			"message": string(data),
		})
	})

	// 设置连接回调
	hub.OnConnect(func(conn *zws.Conn) {
		// 自动加入默认房间
		hub.JoinRoom("lobby", conn)
	})

	// 注册 WebSocket 路由
	zws.WS(engine, "/ws", hub, func(wsCtx *zws.WebSocketContext) {
		wsCtx.Log.Info("Client connected:", wsCtx.Conn().ID())

		// 发送欢迎消息
		wsCtx.JSON(map[string]string{
			"type":    "welcome",
			"message": "Welcome to the chat room!",
		})
	})

	// 启动服务器
	engine.SetAddr(":8080")
	znet.Run()
}
