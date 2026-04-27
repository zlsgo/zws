package zws

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"
)

// MessageCodec 定义消息编解码器接口
// 用于自定义 WebSocket 消息的序列化和反序列化方式
type MessageCodec interface {
	// Encode 将消息对象编码为字节流
	Encode(v any) ([]byte, error)

	// Decode 将字节流解码为消息对象
	Decode(r io.Reader, v any) error
}

var (
	// JSON 编码器 buffer 对象池
	jsonEncoderPool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, 1024))
		},
	}
)

// JSONCodec JSON 编解码器，默认使用
type JSONCodec struct{}

// Encode 实现 MessageCodec 接口，使用对象池优化
func (JSONCodec) Encode(v any) ([]byte, error) {
	buf := jsonEncoderPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		jsonEncoderPool.Put(buf)
	}()

	err := json.NewEncoder(buf).Encode(v)
	if err != nil {
		return nil, err
	}
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// Decode 实现 MessageCodec 接口
func (JSONCodec) Decode(r io.Reader, v any) error {
	return json.NewDecoder(r).Decode(v)
}

// RawCodec 原始字节编解码器，不做任何转换
type RawCodec struct{}

// Encode 实现 MessageCodec 接口
func (RawCodec) Encode(v any) ([]byte, error) {
	if data, ok := v.([]byte); ok {
		return data, nil
	}
	return nil, ErrInvalidRawMessage
}

// Decode 实现 MessageCodec 接口
func (RawCodec) Decode(r io.Reader, v any) error {
	if data, ok := v.(*[]byte); ok {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		*data = b
		return nil
	}
	return ErrInvalidRawMessage
}
