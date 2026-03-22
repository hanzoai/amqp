package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
)

// AMQP 0-9-1 frame types.
const (
	frameMethod    = 1
	frameHeader    = 2
	frameBody      = 3
	frameHeartbeat = 8
	frameEnd       = 0xCE
)

// AMQP 0-9-1 class/method IDs.
const (
	classConnection = 10
	classChannel    = 20
	classExchange   = 40
	classQueue      = 50
	classBasic      = 60

	methodConnectionStart   = 10
	methodConnectionStartOK = 11
	methodConnectionTune    = 30
	methodConnectionTuneOK  = 31
	methodConnectionOpen    = 40
	methodConnectionOpenOK  = 41
	methodConnectionClose   = 50
	methodConnectionCloseOK = 51

	methodChannelOpen   = 10
	methodChannelOpenOK = 11
	methodChannelClose  = 40
	methodChannelCloseOK = 41

	methodExchangeDeclare   = 10
	methodExchangeDeclareOK = 11

	methodQueueDeclare   = 10
	methodQueueDeclareOK = 11
	methodQueueBind      = 20
	methodQueueBindOK    = 21

	methodBasicQos      = 10
	methodBasicQosOK    = 11
	methodBasicConsume   = 20
	methodBasicConsumeOK = 21
	methodBasicPublish   = 40
	methodBasicDeliver   = 60
	methodBasicAck       = 80
)

// AMQPProtocolHeader is "AMQP\x00\x00\x09\x01".
var AMQPProtocolHeader = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

// Session represents one AMQP client connection.
type Session struct {
	conn     net.Conn
	pubsub   *PubSub
	mu       sync.Mutex
	channels map[uint16]*amqpChannel
	closed   bool

	// deliveryTag is a monotonic counter for basic.deliver frames.
	deliveryTag uint64
}

type amqpChannel struct {
	consumers map[string]string // consumerTag -> NATS subject
}

// HandleConnection handles the full lifecycle of an AMQP client connection.
func HandleConnection(conn net.Conn, pubsub *PubSub) {
	s := &Session{
		conn:     conn,
		pubsub:   pubsub,
		channels: make(map[uint16]*amqpChannel),
	}
	defer s.close()

	if err := s.handshake(); err != nil {
		Log("ERROR", "handshake failed from %s: %v", conn.RemoteAddr(), err)
		return
	}

	Log("INFO", "AMQP session established: %s", conn.RemoteAddr())

	for {
		frameType, channel, payload, err := s.readFrame()
		if err != nil {
			if err != io.EOF {
				Log("ERROR", "read frame: %v", err)
			}
			return
		}

		if err := s.handleFrame(frameType, channel, payload); err != nil {
			Log("ERROR", "handle frame: %v", err)
			return
		}
	}
}

func (s *Session) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.conn.Close()
	}
}

// handshake performs the AMQP 0-9-1 connection handshake.
func (s *Session) handshake() error {
	// Read protocol header.
	header := make([]byte, 8)
	if _, err := io.ReadFull(s.conn, header); err != nil {
		return fmt.Errorf("read protocol header: %w", err)
	}
	if !bytes.Equal(header, AMQPProtocolHeader) {
		return fmt.Errorf("invalid protocol header: %x", header)
	}

	// Send Connection.Start.
	if err := s.sendConnectionStart(); err != nil {
		return err
	}

	// Read Connection.Start-OK.
	_, _, _, err := s.readFrame()
	if err != nil {
		return fmt.Errorf("read connection.start-ok: %w", err)
	}

	// Send Connection.Tune.
	if err := s.sendConnectionTune(); err != nil {
		return err
	}

	// Read Connection.Tune-OK.
	_, _, _, err = s.readFrame()
	if err != nil {
		return fmt.Errorf("read connection.tune-ok: %w", err)
	}

	// Read Connection.Open.
	_, _, _, err = s.readFrame()
	if err != nil {
		return fmt.Errorf("read connection.open: %w", err)
	}

	// Send Connection.Open-OK.
	return s.sendConnectionOpenOK()
}

func (s *Session) sendConnectionStart() error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(classConnection))
	binary.Write(&buf, binary.BigEndian, uint16(methodConnectionStart))
	buf.WriteByte(0) // version-major
	buf.WriteByte(9) // version-minor
	// Server properties (empty table).
	writeTable(&buf, map[string]string{
		"product": "hanzo-amqp",
		"version": "1.0.0",
	})
	// Mechanisms.
	writeLongString(&buf, "PLAIN")
	// Locales.
	writeLongString(&buf, "en_US")
	return s.writeFrame(frameMethod, 0, buf.Bytes())
}

func (s *Session) sendConnectionTune() error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(classConnection))
	binary.Write(&buf, binary.BigEndian, uint16(methodConnectionTune))
	binary.Write(&buf, binary.BigEndian, uint16(2047))  // channel-max
	binary.Write(&buf, binary.BigEndian, uint32(131072)) // frame-max
	binary.Write(&buf, binary.BigEndian, uint16(60))     // heartbeat
	return s.writeFrame(frameMethod, 0, buf.Bytes())
}

func (s *Session) sendConnectionOpenOK() error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(classConnection))
	binary.Write(&buf, binary.BigEndian, uint16(methodConnectionOpenOK))
	writeShortString(&buf, "") // reserved
	return s.writeFrame(frameMethod, 0, buf.Bytes())
}

// handleFrame dispatches a received frame.
func (s *Session) handleFrame(frameType byte, channel uint16, payload []byte) error {
	switch frameType {
	case frameMethod:
		return s.handleMethod(channel, payload)
	case frameHeader:
		// Content header -- part of publish flow, we handle in method dispatch.
		return nil
	case frameBody:
		// Body frame -- we handle this as part of the publish flow.
		return nil
	case frameHeartbeat:
		// Respond with heartbeat.
		return s.writeFrame(frameHeartbeat, 0, nil)
	default:
		return fmt.Errorf("unknown frame type: %d", frameType)
	}
}

// handleMethod dispatches AMQP method frames.
func (s *Session) handleMethod(channel uint16, payload []byte) error {
	if len(payload) < 4 {
		return fmt.Errorf("method frame too short")
	}
	classID := binary.BigEndian.Uint16(payload[0:2])
	methodID := binary.BigEndian.Uint16(payload[2:4])
	body := payload[4:]

	switch classID {
	case classConnection:
		return s.handleConnectionMethod(methodID, body)
	case classChannel:
		return s.handleChannelMethod(channel, methodID)
	case classExchange:
		return s.handleExchangeMethod(channel, methodID, body)
	case classQueue:
		return s.handleQueueMethod(channel, methodID, body)
	case classBasic:
		return s.handleBasicMethod(channel, methodID, body)
	default:
		Log("WARN", "unhandled class %d method %d", classID, methodID)
		return nil
	}
}

func (s *Session) handleConnectionMethod(methodID uint16, body []byte) error {
	switch methodID {
	case methodConnectionClose:
		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint16(classConnection))
		binary.Write(&buf, binary.BigEndian, uint16(methodConnectionCloseOK))
		s.writeFrame(frameMethod, 0, buf.Bytes())
		return io.EOF // Signal to close
	default:
		return nil
	}
}

func (s *Session) handleChannelMethod(channel uint16, methodID uint16) error {
	switch methodID {
	case methodChannelOpen:
		s.mu.Lock()
		s.channels[channel] = &amqpChannel{
			consumers: make(map[string]string),
		}
		s.mu.Unlock()

		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint16(classChannel))
		binary.Write(&buf, binary.BigEndian, uint16(methodChannelOpenOK))
		binary.Write(&buf, binary.BigEndian, uint32(0)) // reserved
		return s.writeFrame(frameMethod, channel, buf.Bytes())
	case methodChannelClose:
		s.mu.Lock()
		delete(s.channels, channel)
		s.mu.Unlock()

		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint16(classChannel))
		binary.Write(&buf, binary.BigEndian, uint16(methodChannelCloseOK))
		return s.writeFrame(frameMethod, channel, buf.Bytes())
	default:
		return nil
	}
}

func (s *Session) handleExchangeMethod(channel uint16, methodID uint16, body []byte) error {
	switch methodID {
	case methodExchangeDeclare:
		exchangeName := readShortStringAt(body, 2) // skip 2 reserved bytes
		Log("INFO", "exchange.declare: %s", exchangeName)

		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint16(classExchange))
		binary.Write(&buf, binary.BigEndian, uint16(methodExchangeDeclareOK))
		return s.writeFrame(frameMethod, channel, buf.Bytes())
	default:
		return nil
	}
}

func (s *Session) handleQueueMethod(channel uint16, methodID uint16, body []byte) error {
	switch methodID {
	case methodQueueDeclare:
		queueName := readShortStringAt(body, 2) // skip 2 reserved bytes
		Log("INFO", "queue.declare: %s", queueName)

		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint16(classQueue))
		binary.Write(&buf, binary.BigEndian, uint16(methodQueueDeclareOK))
		writeShortString(&buf, queueName)
		binary.Write(&buf, binary.BigEndian, uint32(0)) // message count
		binary.Write(&buf, binary.BigEndian, uint32(0)) // consumer count
		return s.writeFrame(frameMethod, channel, buf.Bytes())
	case methodQueueBind:
		// Just ACK it.
		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint16(classQueue))
		binary.Write(&buf, binary.BigEndian, uint16(methodQueueBindOK))
		return s.writeFrame(frameMethod, channel, buf.Bytes())
	default:
		return nil
	}
}

func (s *Session) handleBasicMethod(channel uint16, methodID uint16, body []byte) error {
	switch methodID {
	case methodBasicQos:
		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint16(classBasic))
		binary.Write(&buf, binary.BigEndian, uint16(methodBasicQosOK))
		return s.writeFrame(frameMethod, channel, buf.Bytes())

	case methodBasicConsume:
		return s.handleBasicConsume(channel, body)

	case methodBasicPublish:
		return s.handleBasicPublish(channel, body)

	case methodBasicAck:
		// Client ACKing a delivered message. Nothing to do.
		return nil

	default:
		return nil
	}
}

// handleBasicPublish handles AMQP basic.publish by reading the subsequent
// content-header and content-body frames, then forwarding to NATS.
func (s *Session) handleBasicPublish(channel uint16, body []byte) error {
	// Parse the publish method arguments.
	// Skip 2 reserved bytes, then read exchange name and routing key.
	offset := 2
	exchange := readShortStringAt(body, offset)
	offset += 1 + len(exchange)
	routingKey := readShortStringAt(body, offset)

	// Determine the target -- use exchange name first, fall back to routing key.
	target := exchange
	if target == "" {
		target = routingKey
	}

	mapping := MappingByAMQP(target)
	if mapping == nil {
		// Try partial match on routing key.
		for _, m := range DefaultMappings() {
			if strings.Contains(routingKey, m.AMQPName) || strings.Contains(exchange, m.AMQPName) {
				mapping = &m
				break
			}
		}
	}
	if mapping == nil {
		Log("WARN", "no mapping for exchange=%q routing_key=%q, dropping", exchange, routingKey)
		return nil
	}

	// Read content-header frame.
	_, _, headerPayload, err := s.readFrame()
	if err != nil {
		return fmt.Errorf("read content header: %w", err)
	}

	// Parse body size from content-header (offset 4 for class+weight, then 8 bytes for size).
	var bodySize uint64
	if len(headerPayload) >= 12 {
		bodySize = binary.BigEndian.Uint64(headerPayload[4:12])
	}

	// Read content-body frame(s).
	var messageBody []byte
	for uint64(len(messageBody)) < bodySize {
		_, _, bodyPayload, err := s.readFrame()
		if err != nil {
			return fmt.Errorf("read content body: %w", err)
		}
		messageBody = append(messageBody, bodyPayload...)
	}

	// Forward to NATS.
	if err := s.pubsub.Publish(mapping.NATSSubject, messageBody); err != nil {
		Log("ERROR", "NATS publish to %s: %v", mapping.NATSSubject, err)
		return err
	}
	Log("DEBUG", "forwarded %d bytes: AMQP %s -> NATS %s", len(messageBody), target, mapping.NATSSubject)
	return nil
}

// handleBasicConsume sets up a NATS subscription that delivers messages back
// to the AMQP client via basic.deliver frames.
func (s *Session) handleBasicConsume(channel uint16, body []byte) error {
	// Parse: skip 2 reserved bytes, read queue name and consumer tag.
	offset := 2
	queueName := readShortStringAt(body, offset)
	offset += 1 + len(queueName)
	consumerTag := readShortStringAt(body, offset)

	if consumerTag == "" {
		consumerTag = fmt.Sprintf("ctag-%s-%d", queueName, channel)
	}

	mapping := MappingByAMQP(queueName)
	if mapping == nil {
		Log("WARN", "no mapping for consume queue=%q", queueName)
		// Still send consume-ok to avoid blocking the client.
		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint16(classBasic))
		binary.Write(&buf, binary.BigEndian, uint16(methodBasicConsumeOK))
		writeShortString(&buf, consumerTag)
		return s.writeFrame(frameMethod, channel, buf.Bytes())
	}

	// Send consume-ok.
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(classBasic))
	binary.Write(&buf, binary.BigEndian, uint16(methodBasicConsumeOK))
	writeShortString(&buf, consumerTag)
	if err := s.writeFrame(frameMethod, channel, buf.Bytes()); err != nil {
		return err
	}

	// Store consumer.
	s.mu.Lock()
	if ch, ok := s.channels[channel]; ok {
		ch.consumers[consumerTag] = mapping.NATSSubject
	}
	s.mu.Unlock()

	// Subscribe on NATS and deliver to client.
	_, err := s.pubsub.Subscribe(mapping.NATSStream, mapping.NATSSubject, func(data []byte) {
		s.mu.Lock()
		s.deliveryTag++
		tag := s.deliveryTag
		s.mu.Unlock()

		if err := s.sendBasicDeliver(channel, consumerTag, tag, mapping.AMQPName, data); err != nil {
			Log("ERROR", "deliver to AMQP client: %v", err)
		}
	})
	if err != nil {
		return fmt.Errorf("NATS subscribe %s: %w", mapping.NATSSubject, err)
	}

	Log("INFO", "consumer %s on %s -> NATS %s", consumerTag, queueName, mapping.NATSSubject)
	return nil
}

// sendBasicDeliver sends a basic.deliver + content-header + content-body to the AMQP client.
func (s *Session) sendBasicDeliver(channel uint16, consumerTag string, deliveryTag uint64, exchange string, body []byte) error {
	// Method frame: basic.deliver.
	var method bytes.Buffer
	binary.Write(&method, binary.BigEndian, uint16(classBasic))
	binary.Write(&method, binary.BigEndian, uint16(methodBasicDeliver))
	writeShortString(&method, consumerTag)
	binary.Write(&method, binary.BigEndian, deliveryTag)
	method.WriteByte(0) // redelivered = false
	writeShortString(&method, exchange)
	writeShortString(&method, exchange) // routing key = exchange name

	if err := s.writeFrame(frameMethod, channel, method.Bytes()); err != nil {
		return err
	}

	// Content header frame.
	var header bytes.Buffer
	binary.Write(&header, binary.BigEndian, uint16(classBasic)) // class
	binary.Write(&header, binary.BigEndian, uint16(0))          // weight (unused)
	binary.Write(&header, binary.BigEndian, uint64(len(body)))  // body size
	binary.Write(&header, binary.BigEndian, uint16(0))          // property flags (none)

	if err := s.writeFrame(frameHeader, channel, header.Bytes()); err != nil {
		return err
	}

	// Content body frame.
	return s.writeFrame(frameBody, channel, body)
}

// readFrame reads a single AMQP frame from the wire.
func (s *Session) readFrame() (frameType byte, channel uint16, payload []byte, err error) {
	// Frame header: type(1) + channel(2) + size(4) = 7 bytes.
	header := make([]byte, 7)
	if _, err = io.ReadFull(s.conn, header); err != nil {
		return 0, 0, nil, err
	}

	frameType = header[0]
	channel = binary.BigEndian.Uint16(header[1:3])
	size := binary.BigEndian.Uint32(header[3:7])

	// Read payload + frame-end byte.
	data := make([]byte, size+1)
	if _, err = io.ReadFull(s.conn, data); err != nil {
		return 0, 0, nil, fmt.Errorf("read frame payload: %w", err)
	}

	if data[size] != frameEnd {
		return 0, 0, nil, fmt.Errorf("frame-end marker missing (got 0x%02x)", data[size])
	}

	return frameType, channel, data[:size], nil
}

// writeFrame writes a single AMQP frame to the wire.
func (s *Session) writeFrame(frameType byte, channel uint16, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	size := uint32(len(payload))
	buf := make([]byte, 7+size+1)
	buf[0] = frameType
	binary.BigEndian.PutUint16(buf[1:3], channel)
	binary.BigEndian.PutUint32(buf[3:7], size)
	copy(buf[7:], payload)
	buf[7+size] = frameEnd

	_, err := s.conn.Write(buf)
	return err
}

// --- Wire format helpers ---

func writeShortString(buf *bytes.Buffer, s string) {
	buf.WriteByte(byte(len(s)))
	buf.WriteString(s)
}

func writeLongString(buf *bytes.Buffer, s string) {
	binary.Write(buf, binary.BigEndian, uint32(len(s)))
	buf.WriteString(s)
}

func writeTable(buf *bytes.Buffer, m map[string]string) {
	var inner bytes.Buffer
	for k, v := range m {
		writeShortString(&inner, k)
		inner.WriteByte('S') // type = long-string
		writeLongString(&inner, v)
	}
	binary.Write(buf, binary.BigEndian, uint32(inner.Len()))
	buf.Write(inner.Bytes())
}

func readShortStringAt(data []byte, offset int) string {
	if offset >= len(data) {
		return ""
	}
	length := int(data[offset])
	offset++
	if offset+length > len(data) {
		return ""
	}
	return string(data[offset : offset+length])
}
