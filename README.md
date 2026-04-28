# zws

> 面向 zlsgo 生态的 WebSocket 封装库，提供服务端路由接入、连接管理、房间广播和 Go 客户端。

[![Go Version](https://img.shields.io/badge/Go-1.18+-00ADD8?style=flat&logo=go)](https://golang.org)

`zws` 建立在 `znet` 和 `nhooyr.io/websocket` 之上，目标是给 zlsgo 项目提供一套足够轻、足够直接的 WebSocket 使用方式：

- 服务端通过 `zws.WS(...)` 直接挂到 `znet.Engine`
- 连接生命周期通过 `OnConnect` / `OnMessage` / `OnDisconnect` / `OnError` 管理
- 内置 `Hub`、`Room`、广播和房间消息分发
- 提供 `client` 子包用于 Go 客户端连接和收发消息
- 支持自定义消息编解码器，内置 `JSONCodec` 和 `RawCodec`

## 安装

```bash
go get github.com/zlsgo/zws
```

## 快速开始

### 服务端

```go
package main

import (
	"github.com/sohaha/zlsgo/znet"
	"github.com/zlsgo/zws"
)

func main() {
	engine := znet.New()

	hub := zws.NewHub(&zws.ServerConfig{
		AllowedOrigins: []string{"*"},
	})

	hub.OnMessage(func(conn *zws.Conn, data []byte) {
		_ = conn.JSON(map[string]any{
			"client_id": conn.ID(),
			"echo":      string(data),
		})
	})

	zws.WS(engine, "/ws", hub, func(wsCtx *zws.WebSocketContext) {
		wsCtx.Log.Info("client connected:", wsCtx.Conn().ID())
	})

	engine.SetAddr(":8080")
	znet.Run()
}
```

### Go 客户端

```go
package main

import (
	"log"

	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zws/client"
)

func main() {
	c, err := client.NewClient("ws://localhost:8080/ws", nil)
	if err != nil {
		log.Fatal(err)
	}

	c.OnMessage(func(_ *client.Client, data []byte) {
		log.Printf("recv: %s", data)
	})

	c.OnConnect(func(c *client.Client) {
		_ = c.JSON(ztype.Map{
			"type":    "hello",
			"message": "world",
		})
	})

	if err := c.Connect(); err != nil {
		log.Fatal(err)
	}

	select {}
}
```

### 运行仓库示例

仓库已经带了两个完整示例：

- [example/echo](./example/echo)：最小回显服务
- [example/chat](./example/chat)：带广播和房间入口的聊天示例

运行方式：

```bash
go run ./example/echo
```

另开一个终端启动客户端：

```bash
go run ./example/echo/client
```

聊天室示例同理：

```bash
go run ./example/chat
go run ./example/chat/client
```

## 核心概念

### Server

服务端入口。常用方法：

```go
hub := zws.NewHub(config)

hub.OnConnect(func(conn *zws.Conn) {})
hub.OnMessage(func(conn *zws.Conn, data []byte) {})
hub.OnDisconnect(func(conn *zws.Conn) {})
hub.OnError(func(conn *zws.Conn, err error) {})

cfg := hub.Config()
```

### znet 路由接入

`zws.WS` 会在 `znet.Engine` 上注册一个 `GET` 路由，完成升级、连接注册和读写循环启动。

```go
zws.WS(engine, "/ws", hub, func(wsCtx *zws.WebSocketContext) {
	wsCtx.JoinRoom("lobby")
	_ = wsCtx.JSON(map[string]string{"type": "welcome"})
})
```

### Conn

服务端连接对象，封装 `websocket.Conn` 的常用操作：

```go
conn.ID()
conn.Send(data)
conn.JSON(v)
conn.Set("user_id", 1001)
value, ok := conn.Get("user_id")
conn.Close()
```

### Hub 与 Room

`Hub` 负责维护所有连接和房间。连接关闭时会自动从所属房间移除。

```go
hub.Register(conn)
hub.Unregister(conn)
hub.Get(id)
hub.Count()

hub.Broadcast(data)
hub.BroadcastJSON(v)
hub.Send(id, data)
hub.SendJSON(id, v)

hub.JoinRoom("lobby", conn)
hub.LeaveRoom("lobby", conn)
hub.SendToRoom("lobby", data)
hub.SendJSONToRoom("lobby", v)
hub.GetRoomSize("lobby")
```

### WebSocketContext

`WebSocketContext` 内嵌 `*znet.Context`，同时暴露当前连接和 Hub：

```go
wsCtx.Conn()
wsCtx.Hub()
wsCtx.Send(data)
wsCtx.JSON(v)
wsCtx.JoinRoom("lobby")
wsCtx.LeaveRoom("lobby")
wsCtx.Emit("lobby", v)
wsCtx.Broadcast(v)
wsCtx.GetRoomSize("lobby")
```

### client.Client

Go 客户端位于 `github.com/zlsgo/zws/client`：

```go
c, err := client.NewClient(url, config)

c.SetHeader("Origin", "http://localhost:8080")

c.OnConnect(func(c *client.Client) {})
c.OnMessage(func(c *client.Client, data []byte) {})
c.OnDisconnect(func(c *client.Client) {})
c.OnError(func(c *client.Client, err error) {})

_ = c.Connect()
_ = c.Send([]byte("hello"))
_ = c.JSON(ztype.Map{"type": "ping"})
_ = c.Close()
```

## 配置

### BaseConfig

`ServerConfig` 和 `ClientConfig` 共享以下基础字段：

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `Codec` | `zws.JSONCodec{}` | 消息编解码器 |
| `PingInterval` | `30 * time.Second` | 心跳间隔；设为 `0` 表示禁用 |
| `PingWait` | `60 * time.Second` | 单次 Ping 等待超时 |
| `PingMessage` | `nil` | 自定义心跳负载；当前主流程不会自动发送该字段 |
| `MaxMessageSize` | `10 << 20` | 最大消息大小，服务端升级时会应用到读限制 |

### ServerConfig

```go
config := &zws.ServerConfig{
	BaseConfig: zws.BaseConfig{
		Codec:          zws.JSONCodec{},
		PingInterval:   30 * time.Second,
		PingWait:       60 * time.Second,
		MaxMessageSize: 10 << 20,
	},
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	AllowedOrigins:  []string{"https://example.com"},
}
```

字段说明：

- `AllowedOrigins`：默认空切片，表示拒绝所有来源。
- `ReadBufferSize`：当前会被归一化为默认值，但尚未传递给底层 `Accept` 逻辑。
- `WriteBufferSize`：当前会被归一化为默认值，但尚未用于底层写缓冲控制。

### ClientConfig

```go
config := &zws.ClientConfig{
	BaseConfig: zws.BaseConfig{
		Codec:          zws.JSONCodec{},
		PingInterval:   30 * time.Second,
		PingWait:       60 * time.Second,
		MaxMessageSize: 10 << 20,
	},
	HandshakeTimeout:     45 * time.Second,
	Reconnect:            false,
	ReconnectDelay:       5 * time.Second,
	MaxReconnectAttempts: -1,
}
```

字段说明：

- `HandshakeTimeout`：客户端握手超时。
- `Reconnect` / `ReconnectDelay` / `MaxReconnectAttempts`：当前仅作为配置字段保留，`Client.Connect()` 还没有自动重连实现。
- `MaxMessageSize`：当前客户端读循环没有把该字段下推到底层连接。

## 编解码器

### 内置实现

- `zws.JSONCodec`：默认实现，面向结构化消息。
- `zws.RawCodec`：只接受 `[]byte` 编码，解码目标必须是 `*[]byte`。

### 自定义编解码器

```go
type MyCodec struct{}

func (MyCodec) Encode(v any) ([]byte, error) {
	return []byte("custom"), nil
}

func (MyCodec) Decode(r io.Reader, v any) error {
	return nil
}

hub := zws.NewHub(&zws.ServerConfig{
	BaseConfig: zws.BaseConfig{
		Codec: MyCodec{},
	},
	AllowedOrigins: []string{"*"},
})
```

## 底层 API

`zws` 重新导出了 `nhooyr.io/websocket` 的核心类型和常量，你不需要直接引入底层包即可使用完整的 WebSocket 协议功能。

### 消息类型

```go
type MessageType int

const (
	MessageText   MessageType = iota + 1 // 文本消息
	MessageBinary                          // 二进制消息
)
```

### 状态码

```go
type StatusCode int

// 常用状态码
const (
	StatusNormalClosure           StatusCode = 1000 // 正常关闭
	StatusGoingAway               StatusCode = 1001 // 端点离开
	StatusProtocolError           StatusCode = 1002 // 协议错误
	StatusUnsupportedData         StatusCode = 1003 // 不支持的数据类型
	StatusInvalidFramePayloadData StatusCode = 1007 // 无效帧数据
	StatusPolicyViolation         StatusCode = 1008 // 策略违规
	StatusMessageTooBig           StatusCode = 1009 // 消息过大
	StatusInternalError           StatusCode = 1011 // 内部错误
)
```

### 辅助函数

```go
// 从错误中提取关闭状态码
func CloseStatus(err error) StatusCode
```

### Conn 底层方法

```go
conn.RawConn() *websocket.Conn                    // 获取底层连接
conn.ReadMessage() (MessageType, []byte, error)   // 直接读取消息
conn.WriteMessage(typ MessageType, data []byte)   // 直接写入消息
conn.Ping() error                                  // 发送 Ping
conn.CloseWithStatus(code StatusCode, reason)     // 带状态码关闭
```

### 使用示例

```go
hub.OnMessage(func(conn *zws.Conn, data []byte) {
	// 发送二进制消息
	err := conn.WriteMessage(zws.MessageBinary, []byte("binary data"))
	if err != nil {
		// 使用状态码关闭连接
		_ = conn.CloseWithStatus(zws.StatusInvalidFramePayloadData, "invalid data")
	}
})

hub.OnError(func(conn *zws.Conn, err error) {
	// 提取关闭状态码
	if statusCode := zws.CloseStatus(err); statusCode != -1 {
		log.Printf("连接关闭，状态码: %d", statusCode)
	}
})
```

完整示例见 [example/lowlevel](./example/lowlevel)。
