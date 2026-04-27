package zws

import (
	"bytes"
	"testing"
)

func TestJSONCodec(t *testing.T) {
	codec := JSONCodec{}

	// 测试编码
	data := map[string]string{"hello": "world"}
	encoded, err := codec.Encode(data)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// 测试解码
	var decoded map[string]string
	err = codec.Decode(bytes.NewReader(encoded), &decoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded["hello"] != "world" {
		t.Errorf("Expected 'world', got '%s'", decoded["hello"])
	}
}

func TestRawCodec(t *testing.T) {
	codec := RawCodec{}

	// 测试编码
	data := []byte("hello world")
	encoded, err := codec.Encode(data)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if string(encoded) != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", string(encoded))
	}

	// 测试解码
	var decoded []byte
	err = codec.Decode(bytes.NewReader(encoded), &decoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if string(decoded) != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", string(decoded))
	}
}

func TestRawCodecInvalidType(t *testing.T) {
	codec := RawCodec{}

	// 测试无效类型
	_, err := codec.Encode("string")
	if err != ErrInvalidRawMessage {
		t.Errorf("Expected ErrInvalidRawMessage, got %v", err)
	}
}
