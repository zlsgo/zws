package main

import (
	"fmt"

	"github.com/sohaha/zlsgo/znet"
	"github.com/zlsgo/zws"
)

func main() {
	// 创建 znet 引擎
	engine := znet.New()

	// 创建 WebSocket Hub
	hub := zws.NewHub(&zws.ServerConfig{
		AllowedOrigins: []string{"http://localhost:8080"},
	})

	// 设置消息处理
	hub.OnMessage(func(conn *zws.Conn, data []byte) {
		// 回显收到的消息
		// conn.JSON(map[string]string{
		// 	"echo": string(data),
		// })
		conn.Send(append([]byte("Client: "), data...))
	})

	// 注册 WebSocket 路由
	zws.WS(engine, "/ws", hub, func(wsCtx *zws.WebSocketContext) {
		fmt.Println("Client connected:", wsCtx.Conn().ID())
	})

	// 设置断开消息
	hub.OnDisconnect(func(c *zws.Conn) {
		fmt.Println("Client disconnect:", c.ID())
	})

	// 启动服务器
	engine.SetAddr(":8080")
	znet.Run()
}
