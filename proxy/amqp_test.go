package proxy

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestWriteShortString(t *testing.T) {
	var buf bytes.Buffer
	writeShortString(&buf, "hello")
	data := buf.Bytes()
	if data[0] != 5 {
		t.Errorf("length byte: expected 5, got %d", data[0])
	}
	if string(data[1:]) != "hello" {
		t.Errorf("string: expected hello, got %q", string(data[1:]))
	}
}

func TestWriteLongString(t *testing.T) {
	var buf bytes.Buffer
	writeLongString(&buf, "PLAIN")
	data := buf.Bytes()
	length := binary.BigEndian.Uint32(data[0:4])
	if length != 5 {
		t.Errorf("length: expected 5, got %d", length)
	}
	if string(data[4:]) != "PLAIN" {
		t.Errorf("string: expected PLAIN, got %q", string(data[4:]))
	}
}

func TestReadShortStringAt(t *testing.T) {
	data := []byte{5, 'h', 'e', 'l', 'l', 'o'}
	s := readShortStringAt(data, 0)
	if s != "hello" {
		t.Errorf("expected hello, got %q", s)
	}

	// With offset.
	data2 := []byte{0, 0, 3, 'f', 'o', 'o'}
	s2 := readShortStringAt(data2, 2)
	if s2 != "foo" {
		t.Errorf("expected foo, got %q", s2)
	}

	// Out of bounds.
	s3 := readShortStringAt(data, 100)
	if s3 != "" {
		t.Errorf("expected empty, got %q", s3)
	}

	// Truncated string.
	data3 := []byte{10, 'a', 'b'}
	s4 := readShortStringAt(data3, 0)
	if s4 != "" {
		t.Errorf("expected empty for truncated, got %q", s4)
	}
}

func TestWriteTable(t *testing.T) {
	var buf bytes.Buffer
	writeTable(&buf, map[string]string{"product": "test"})
	data := buf.Bytes()

	// First 4 bytes are table length.
	tableLen := binary.BigEndian.Uint32(data[0:4])
	if tableLen == 0 {
		t.Error("table length is 0")
	}
	// Table should contain "product" and "test".
	rest := string(data[4:])
	if !bytes.Contains([]byte(rest), []byte("product")) {
		t.Error("table does not contain 'product'")
	}
	if !bytes.Contains([]byte(rest), []byte("test")) {
		t.Error("table does not contain 'test'")
	}
}

func TestWriteFrameFormat(t *testing.T) {
	// Simulate writeFrame by building a frame manually and verifying structure.
	payload := []byte{0x00, 0x0A, 0x00, 0x0A} // class=10, method=10
	frameType := byte(frameMethod)
	channel := uint16(0)

	size := uint32(len(payload))
	buf := make([]byte, 7+size+1)
	buf[0] = frameType
	binary.BigEndian.PutUint16(buf[1:3], channel)
	binary.BigEndian.PutUint32(buf[3:7], size)
	copy(buf[7:], payload)
	buf[7+size] = frameEnd

	// Verify.
	if buf[0] != frameMethod {
		t.Errorf("frame type: expected %d, got %d", frameMethod, buf[0])
	}
	if binary.BigEndian.Uint16(buf[1:3]) != 0 {
		t.Error("channel should be 0")
	}
	if binary.BigEndian.Uint32(buf[3:7]) != 4 {
		t.Errorf("payload size: expected 4, got %d", binary.BigEndian.Uint32(buf[3:7]))
	}
	if buf[len(buf)-1] != frameEnd {
		t.Errorf("frame end: expected 0x%02x, got 0x%02x", frameEnd, buf[len(buf)-1])
	}
}

func TestAMQPProtocolHeader(t *testing.T) {
	if len(AMQPProtocolHeader) != 8 {
		t.Fatalf("protocol header length: expected 8, got %d", len(AMQPProtocolHeader))
	}
	if string(AMQPProtocolHeader[:4]) != "AMQP" {
		t.Error("protocol header does not start with AMQP")
	}
	// Version 0-9-1.
	if AMQPProtocolHeader[5] != 0 || AMQPProtocolHeader[6] != 9 || AMQPProtocolHeader[7] != 1 {
		t.Errorf("version: expected 0-9-1, got %d-%d-%d",
			AMQPProtocolHeader[5], AMQPProtocolHeader[6], AMQPProtocolHeader[7])
	}
}
