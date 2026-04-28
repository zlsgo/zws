// lowlevel 示例：展示如何使用 zws 暴露的底层 WebSocket API
//
// 运行方式：
//   go run ./example/lowlevel
//
// 此示例展示：
// - 使用底层消息类型（MessageText、MessageBinary）
// - 使用底层状态码（StatusCode）
// - 直接读写消息
// - 自定义关闭状态码
package main

import (
	"log"

	"github.com/sohaha/zlsgo/znet"
	"github.com/zlsgo/zws"
)

func main() {
	engine := znet.New()

	hub := zws.NewHub(&zws.ServerConfig{
		AllowedOrigins: []string{"*"},
	})

	// 设置消息处理器
	hub.OnMessage(func(conn *zws.Conn, data []byte) {
		log.Printf("收到消息: %s", string(data))

		// 使用底层 API 直接读取消息类型
		// 这绕过了默认的读取循环
		if conn.RawConn() != nil {
			// 发送二进制消息示例
			err := conn.WriteMessage(zws.MessageBinary, []byte("binary response"))
			if err != nil {
				log.Printf("发送二进制消息失败: %v", err)
			}
		}

		// 发送文本消息
		_ = conn.JSON(map[string]any{
			"type": "echo",
			"data": string(data),
		})
	})

	hub.OnDisconnect(func(conn *zws.Conn) {
		log.Printf("客户端断开连接: %s", conn.ID())
	})

	hub.OnError(func(conn *zws.Conn, err error) {
		// 使用 CloseStatus 提取关闭状态码
		if statusCode := zws.CloseStatus(err); statusCode != -1 {
			log.Printf("连接关闭，状态码: %d (%s)", statusCode, statusCode)
		} else {
			log.Printf("错误: %v", err)
		}
	})

	// 注册 WebSocket 路由
	zws.WS(engine, "/ws", hub, func(wsCtx *zws.WebSocketContext) {
		wsCtx.Log.Info("客户端连接:", wsCtx.Conn().ID())

		// 发送欢迎消息
		_ = wsCtx.JSON(map[string]string{
			"type":    "welcome",
			"message": "连接成功",
		})
	})

	engine.SetAddr(":8080")

	log.Println("服务器启动在 :8080")
	log.Println("可以使用 wscat 或其他 WebSocket 客户端连接 ws://localhost:8080/ws")

	znet.Run()
}
