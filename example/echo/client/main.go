package main

import (
	"fmt"
	"log"
	"time"

	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zws/client"
)

func main() {
	// 创建客户端
	c, err := client.NewClient("ws://localhost:8080/ws", nil)
	if err != nil {
		log.Fatal(err)
	}

	// 设置消息处理
	c.OnMessage(func(c *client.Client, data []byte) {
		fmt.Printf("Received: %s\n", string(data))
	})

	c.OnConnect(func(c *client.Client) {
		fmt.Println("Connected to server")
		// 发送测试消息
		c.JSON(ztype.Map{"hello": "world", "time": time.Now().Unix()})
	})

	c.OnDisconnect(func(c *client.Client) {
		fmt.Println("Disconnected from server")
	})

	// CLI 客户端不会像浏览器一样自动附带 Origin，示例服务端限制了来源校验。
	c.SetHeader("Origin", "http://localhost:8080")

	// 连接到服务器
	if err := c.Connect(); err != nil {
		log.Fatal(err)
	}

	// 保持连接
	select {}
}
