package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hanzoai/amqp/wire"
)

// Class ids (§1.2).
const (
	classConnection = 10
	classChannel    = 20
	classExchange   = 40
	classQueue      = 50
	classBasic      = 60
	classTx         = 90
	classConfirm    = 85
)

// Method ids, by class.
const (
	connStart   = 10
	connStartOK = 11
	connTune    = 30
	connTuneOK  = 31
	connOpen    = 40
	connOpenOK  = 41
	connClose   = 50
	connCloseOK = 51

	chanOpen    = 10
	chanOpenOK  = 11
	chanFlow    = 20
	chanFlowOK  = 21
	chanClose   = 40
	chanCloseOK = 41

	exDeclare   = 10
	exDeclareOK = 11
	exDelete    = 20
	exDeleteOK  = 21
	exBind      = 30
	exBindOK    = 31
	exUnbind    = 40
	exUnbindOK  = 51

	qDeclare   = 10
	qDeclareOK = 11
	qBind      = 20
	qBindOK    = 21
	qPurge     = 30
	qPurgeOK   = 31
	qDelete    = 40
	qDeleteOK  = 41
	qUnbind    = 50
	qUnbindOK  = 51

	basicQos       = 10
	basicQosOK     = 11
	basicConsume   = 20
	basicConsumeOK = 21
	basicCancel    = 30
	basicCancelOK  = 31
	basicPublish   = 40
	basicReturn    = 50
	basicDeliver   = 60
	basicGet       = 70
	basicGetOK     = 71
	basicGetEmpty  = 72
	basicAck       = 80
	basicReject    = 90
	basicRecoverA  = 100
	basicRecover   = 110
	basicRecoverOK = 111
	basicNack      = 120

	confirmSelect   = 10
	confirmSelectOK = 11
)

// Reply codes (§4.3). Only the ones this gateway actually returns.
const (
	replySuccess       = 200
	noRoute            = 312
	accessRefused      = 403
	notFound           = 404
	preconditionFailed = 406
	frameError         = 501
	syntaxError        = 502
	commandInvalid     = 503
	channelError       = 504
	unexpectedFrame    = 505
	notAllowed         = 530
	notImplemented     = 540
	internalError      = 541
)

// fault is an AMQP exception: a reply code, prose, and the method that caused
// it. Conn says whether it kills the connection or only the channel (§1.5.2) —
// scope is a property of the fault, so the code that raises one states it and
// nothing downstream has to guess.
type fault struct {
	Code   uint16
	Text   string
	Class  uint16
	Method uint16
	Conn   bool
}

func (f fault) Error() string {
	return fmt.Sprintf("amqp %d %s (class %d method %d)", f.Code, f.Text, f.Class, f.Method)
}

func chanFault(code, class, method uint16, format string, a ...any) fault {
	return fault{Code: code, Text: fmt.Sprintf(format, a...), Class: class, Method: method}
}

func connFault(code, class, method uint16, format string, a ...any) fault {
	return fault{Code: code, Text: fmt.Sprintf(format, a...), Class: class, Method: method, Conn: true}
}

// absent is the answer to every method this gateway does not implement. It is
// the whole reason a client can trust the ones it does: an unknown method is
// told so on its channel, naming itself, rather than being acknowledged or
// silently dropped.
func absent(class, method uint16, why string) fault {
	return chanFault(notImplemented, class, method, "NOT_IMPLEMENTED - %s", why)
}

// maxChannels is the channel-max offered in connection.tune. A channel is a
// map entry and a mutex, so the number is about bounding one peer's appetite,
// not about a cost per channel.
const maxChannels = 2047

// errBye ends the read loop after the client's connection.close was answered.
var errBye = errors.New("client closed the connection")

// session is one AMQP connection: its channels, its socket, and the negotiated
// limits both ends agreed to.
type session struct {
	b    *Broker
	conn net.Conn
	r    *wire.Reader
	w    *wire.Writer

	heartbeat time.Duration
	maxChan   uint16

	mu      sync.Mutex
	chans   map[uint16]*channel
	closing map[uint16]bool // sent channel.close, awaiting the peer's close-ok
	owned   map[string]bool // queues that die with this connection
	done    chan struct{}
	once    sync.Once
}

func newSession(b *Broker, conn net.Conn) *session {
	return &session{
		b:       b,
		conn:    conn,
		r:       wire.NewReader(conn, b.cfg.FrameMax),
		w:       wire.NewWriter(conn, b.cfg.FrameMax),
		chans:   map[uint16]*channel{},
		closing: map[uint16]bool{},
		owned:   map[string]bool{},
		done:    make(chan struct{}),
	}
}

func (s *session) close() {
	s.once.Do(func() {
		close(s.done)
		s.conn.Close()

		s.mu.Lock()
		chans := make([]*channel, 0, len(s.chans))
		for _, c := range s.chans {
			chans = append(chans, c)
		}
		s.chans = map[uint16]*channel{}
		owned := make([]string, 0, len(s.owned))
		for q := range s.owned {
			owned = append(owned, q)
		}
		s.mu.Unlock()

		for _, c := range chans {
			c.shutdown()
		}
		// Exclusive and auto-delete queues live exactly as long as the
		// connection that asked for them (§1.7.2.1).
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, q := range owned {
			_ = s.b.dropConsumer(ctx, q)
			_ = s.b.top.DropQueue(ctx, q)
		}
	})
}

func (s *session) run() {
	if err := s.handshake(); err != nil {
		logf("handshake from %s: %v", s.conn.RemoteAddr(), err)
		return
	}
	go s.beats()

	for {
		if s.heartbeat > 0 {
			// A peer that stops talking has to be noticed; two missed
			// intervals is the spec's own threshold (§4.2.7).
			_ = s.conn.SetReadDeadline(time.Now().Add(2 * s.heartbeat))
		}
		f, err := s.r.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				logf("read from %s: %v", s.conn.RemoteAddr(), err)
			}
			return
		}
		if err := s.frame(f); err != nil {
			var flt fault
			switch {
			case errors.Is(err, errBye):
				return
			case errors.As(err, &flt) && !flt.Conn:
				s.killChannel(f.Channel, flt)
			case errors.As(err, &flt):
				s.killConn(flt)
				return
			default:
				s.killConn(fault{Code: internalError, Text: err.Error(), Conn: true})
				return
			}
		}
	}
}

// beats keeps the connection warm. Both peers send their own; a heartbeat is a
// statement that this end is alive, never a request for a reply.
func (s *session) beats() {
	if s.heartbeat <= 0 {
		return
	}
	t := time.NewTicker(s.heartbeat / 2)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			if err := s.w.Beat(); err != nil {
				return
			}
		}
	}
}

// handshake runs the four exchanges that open a connection: protocol header,
// start/start-ok, tune/tune-ok, open/open-ok (§2.2.4).
func (s *session) handshake() error {
	if err := s.r.Header(); err != nil {
		// A peer that spoke a different protocol gets the version we do speak,
		// which is what tells its client library to give up cleanly.
		_ = s.w.Send(wire.Frame{Type: wire.Method, Payload: wire.Protocol})
		return err
	}

	start := wire.NewMethod(classConnection, connStart).
		Octet(0).Octet(9).
		Table(wire.Table{
			"product":      s.b.cfg.Product,
			"version":      s.b.cfg.Version,
			"platform":     "Hanzo PubSub",
			"capabilities": wire.Table{"publisher_confirms": true, "basic.nack": true, "consumer_cancel_notify": true},
		}).
		LongStr("PLAIN").
		LongStr("en_US")
	if err := s.w.Send(start.Frame(0)); err != nil {
		return err
	}

	// start-ok: client-properties, mechanism, response, locale. The response
	// is a SASL PLAIN payload; the bus behind this gateway is reached over the
	// loopback or a credentials file, so there is no second credential here to
	// check and inventing one would be a second identity for the same access.
	if _, err := s.expect(classConnection, connStartOK); err != nil {
		return err
	}

	tune := wire.NewMethod(classConnection, connTune).
		Short(maxChannels).     // channel-max
		Long(s.b.cfg.FrameMax). // frame-max
		Short(uint16(s.b.cfg.Heartbeat / time.Second))
	if err := s.w.Send(tune.Frame(0)); err != nil {
		return err
	}

	a, err := s.expect(classConnection, connTuneOK)
	if err != nil {
		return err
	}
	s.maxChan = a.Short()
	frameMax := a.Long()
	s.heartbeat = time.Duration(a.Short()) * time.Second
	if err := a.Err(); err != nil {
		return err
	}
	if s.maxChan == 0 || s.maxChan > maxChannels {
		s.maxChan = maxChannels
	}
	if frameMax < wire.MinFrame || frameMax > s.b.cfg.FrameMax {
		frameMax = s.b.cfg.FrameMax
	}
	s.w.SetMax(frameMax)

	if _, err := s.expect(classConnection, connOpen); err != nil {
		return err
	}
	return s.w.Send(wire.NewMethod(classConnection, connOpenOK).ShortStr("").Frame(0))
}

// expect reads the next frame and insists it is the method named. Anything
// else during the handshake is a protocol error, not something to skip past.
func (s *session) expect(class, method uint16) (*wire.Args, error) {
	_ = s.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	f, err := s.r.Read()
	if err != nil {
		return nil, err
	}
	if f.Type != wire.Method {
		return nil, fmt.Errorf("frame type %d during handshake, want a method", f.Type)
	}
	gotClass, gotMethod, err := wire.ClassMethod(f.Payload)
	if err != nil {
		return nil, err
	}
	if gotClass != class || gotMethod != method {
		return nil, fmt.Errorf("method %d.%d during handshake, want %d.%d", gotClass, gotMethod, class, method)
	}
	return wire.NewArgs(f.Payload), nil
}

// frame routes one frame to the connection or to a channel.
func (s *session) frame(f wire.Frame) error {
	if f.Type == wire.Heartbeat {
		return nil // the read deadline already noted that the peer is alive
	}
	if f.Channel == 0 {
		if f.Type != wire.Method {
			return connFault(unexpectedFrame, 0, 0, "frame type %d on channel 0, which carries methods only", f.Type)
		}
		return s.connection(f.Payload)
	}
	if s.maxChan > 0 && f.Channel > s.maxChan {
		return connFault(channelError, 0, 0, "channel %d is above the negotiated channel-max %d", f.Channel, s.maxChan)
	}

	class, method, err := wire.ClassMethod(f.Payload)
	if f.Type == wire.Method && err != nil {
		return connFault(frameError, 0, 0, "method frame is %d octets", len(f.Payload))
	}

	// A channel this end closed is silent until the peer's close-ok arrives.
	// The peer had frames in flight when the exception went out; answering
	// each with another exception would escalate one channel's mistake into
	// the whole connection (§1.5.2).
	s.mu.Lock()
	shut := s.closing[f.Channel]
	if shut && f.Type == wire.Method && class == classChannel && method == chanCloseOK {
		delete(s.closing, f.Channel)
	}
	s.mu.Unlock()
	if shut {
		return nil
	}

	// channel.open is the one method allowed on a channel that is not open.
	if f.Type == wire.Method && class == classChannel && method == chanOpen {
		return s.open(f.Channel, f.Payload)
	}

	s.mu.Lock()
	c := s.chans[f.Channel]
	s.mu.Unlock()
	if c == nil {
		return connFault(channelError, class, method, "channel %d is not open", f.Channel)
	}
	return c.frame(f, class, method)
}

func (s *session) open(id uint16, payload []byte) error {
	s.mu.Lock()
	if _, dup := s.chans[id]; dup {
		s.mu.Unlock()
		return connFault(channelError, classChannel, chanOpen, "channel %d is already open", id)
	}
	s.chans[id] = newChannel(s, id)
	s.mu.Unlock()
	return s.w.Send(wire.NewMethod(classChannel, chanOpenOK).LongStr("").Frame(id))
}

// connection handles the methods that belong to the connection itself.
func (s *session) connection(payload []byte) error {
	class, method, err := wire.ClassMethod(payload)
	if err != nil {
		return connFault(frameError, 0, 0, "method frame is %d octets", len(payload))
	}
	if class != classConnection {
		return connFault(commandInvalid, class, method, "class %d is not valid on channel 0", class)
	}
	switch method {
	case connClose:
		if err := s.w.Send(wire.NewMethod(classConnection, connCloseOK).Frame(0)); err != nil {
			return err
		}
		return errBye
	case connCloseOK:
		return errBye
	default:
		return connFault(commandInvalid, class, method,
			"connection method %d is not valid once the connection is open", method)
	}
}

// killChannel closes one channel with an exception and leaves the connection up.
func (s *session) killChannel(id uint16, f fault) {
	s.mu.Lock()
	c := s.chans[id]
	delete(s.chans, id)
	s.closing[id] = true
	s.mu.Unlock()
	if c != nil {
		c.shutdown()
	}
	_ = s.w.Send(wire.NewMethod(classChannel, chanClose).
		Short(f.Code).ShortStr(f.Text).Short(f.Class).Short(f.Method).Frame(id))
}

// killConn closes the whole connection with an exception, then waits briefly
// for the peer's close-ok so its client library reports the reason rather than
// a reset socket.
func (s *session) killConn(f fault) {
	logf("closing %s: %v", s.conn.RemoteAddr(), f)
	if err := s.w.Send(wire.NewMethod(classConnection, connClose).
		Short(f.Code).ShortStr(f.Text).Short(f.Class).Short(f.Method).Frame(0)); err != nil {
		return
	}
	_ = s.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		fr, err := s.r.Read()
		if err != nil {
			return
		}
		if fr.Type == wire.Method {
			if _, m, err := wire.ClassMethod(fr.Payload); err == nil && m == connCloseOK {
				return
			}
		}
	}
}

// forget drops a channel the client closed cleanly.
func (s *session) forget(id uint16) {
	s.mu.Lock()
	delete(s.chans, id)
	delete(s.closing, id)
	s.mu.Unlock()
}

// own marks a queue as dying with this connection.
func (s *session) own(name string) {
	s.mu.Lock()
	s.owned[name] = true
	s.mu.Unlock()
}

func (s *session) disown(name string) {
	s.mu.Lock()
	delete(s.owned, name)
	s.mu.Unlock()
}
