package zws

import "testing"

func TestDefaultServerConfig(t *testing.T) {
	config := DefaultServerConfig()

	if config.Codec == nil {
		t.Error("Expected codec to be set")
	}

	if config.PingInterval == 0 {
		t.Error("Expected PingInterval to be set")
	}

	if config.MaxMessageSize == 0 {
		t.Error("Expected MaxMessageSize to be set")
	}
}

func TestDefaultClientConfig(t *testing.T) {
	config := DefaultClientConfig()

	if config.Codec == nil {
		t.Error("Expected codec to be set")
	}

	if config.PingInterval == 0 {
		t.Error("Expected PingInterval to be set")
	}

	if config.HandshakeTimeout == 0 {
		t.Error("Expected HandshakeTimeout to be set")
	}
}

func TestServerConfigNormalization(t *testing.T) {
	config := normalizeServerConfig(&ServerConfig{
		AllowedOrigins: []string{"https://example.com"},
	})

	if config.Codec == nil {
		t.Fatal("Expected codec to be filled")
	}
	if config.MaxMessageSize == 0 {
		t.Fatal("Expected MaxMessageSize to be filled")
	}
	if len(config.AllowedOrigins) != 1 || config.AllowedOrigins[0] != "https://example.com" {
		t.Fatalf("AllowedOrigins should be preserved, got %#v", config.AllowedOrigins)
	}
	if config.PingInterval != 0 {
		t.Fatalf("Explicit zero PingInterval should disable heartbeat, got %v", config.PingInterval)
	}
}

func TestClientConfigNormalization(t *testing.T) {
	config := NormalizeClientConfig(&ClientConfig{})

	if config.Codec == nil {
		t.Fatal("Expected codec to be filled")
	}
	if config.HandshakeTimeout == 0 {
		t.Fatal("Expected HandshakeTimeout to be filled")
	}
	if config.MaxReconnectAttempts != -1 {
		t.Fatalf("Expected MaxReconnectAttempts -1, got %d", config.MaxReconnectAttempts)
	}
}
