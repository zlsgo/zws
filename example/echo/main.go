package main

import (
	"fmt"

	"github.com/sohaha/zlsgo/znet"
	"github.com/sohaha/zws"
)

func main() {
	// 创建 znet 引擎
	engine := znet.New()

	// 创建 WebSocket 服务端
	server := zws.NewServer(&zws.ServerConfig{
		AllowedOrigins: []string{"http://localhost:8080"},
	})

	// 设置消息处理
	server.OnMessage(func(conn *zws.Conn, data []byte) {
		// 回显收到的消息
		// conn.JSON(map[string]string{
		// 	"echo": string(data),
		// })
		conn.Send(append([]byte("Client: "), data...))
	})

	// 注册 WebSocket 路由
	zws.WS(engine, "/ws", server, func(wsCtx *zws.WebSocketContext) {
		fmt.Println("Client connected:", wsCtx.Conn().ID())
	})

	// 设置断开消息
	server.OnDisconnect(func(c *zws.Conn) {
		fmt.Println("Client disconnect:", c.ID())
	})

	// 启动服务器
	engine.SetAddr(":8080")
	znet.Run()
}
