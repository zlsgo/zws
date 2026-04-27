package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

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
		fmt.Printf("\nReceived: %s\n> ", string(data))
	})

	c.OnConnect(func(c *client.Client) {
		fmt.Println("Connected to chat server")
		fmt.Println("Type your messages (press Ctrl+C to exit):")
	})

	c.OnDisconnect(func(c *client.Client) {
		fmt.Println("\nDisconnected from server")
		os.Exit(0)
	})

	// CLI 客户端不会像浏览器一样自动附带 Origin，示例服务端限制了来源校验。
	c.SetHeader("Origin", "http://localhost:8080")

	// 连接到服务器
	if err := c.Connect(); err != nil {
		log.Fatal(err)
	}

	// 启动输入读取
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		print("> ")
		for scanner.Scan() {
			text := scanner.Text()
			if strings.TrimSpace(text) != "" {
				c.JSON(ztype.Map{
					"type":    "message",
					"content": text,
				})
			}
			print("> ")
		}
	}()

	// 保持连接
	select {}
}
