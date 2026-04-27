package client

import (
	"fmt"
	"time"
)

// clientHub 客户端消息通道管理
type clientHub struct {
	outbox chan []byte
}

// newClientHub 创建客户端 Hub
func newClientHub() *clientHub {
	return &clientHub{
		outbox: make(chan []byte, 256),
	}
}

// send 发送消息，带超时机制
func (h *clientHub) send(data []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("send failed: channel closed")
		}
	}()

	// 设置 5 秒超时，防止永久阻塞
	sendTimeout := 5 * time.Second

	select {
	case h.outbox <- data:
		return nil
	case <-time.After(sendTimeout):
		return fmt.Errorf("send timeout: channel full")
	}
}

// close 关闭 Hub
func (h *clientHub) close() {
	// outbox 由 Client 的 context 驱动生命周期，不关闭 channel 以避免
	// Close 与并发 Send 之间发生 send-on-closed-channel panic。
}
