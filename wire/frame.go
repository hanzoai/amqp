// Package wire is the AMQP 0-9-1 octet layer: frames on and off a socket, and
// the typed values a frame body carries. It knows the FORMAT and nothing about
// what any method means — that is package protocol's job.
//
// Two rules here are load-bearing rather than decorative:
//
// A Reader refuses a frame larger than the frame-max the server advertised.
// The size field is 32 bits, so a client that sends 0xFFFFFFFF asks for a 4 GB
// allocation; a broker that trusts it is one packet away from being killed by
// a stranger. §4.2.3 says the peer may not exceed the offered maximum, so
// refusing is the specified behaviour as well as the safe one.
//
// A Writer sends a whole frame SEQUENCE under one lock. A content delivery is
// three or more frames — method, header, body… — and AMQP requires them
// consecutive on a channel. Two goroutines each writing three frames without
// that lock produce a stream that is well-formed at the socket and nonsense at
// the channel.
package wire

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// Frame types (§4.2.3) and the octet that ends every frame.
const (
	Method    = 1
	Header    = 2
	Body      = 3
	Heartbeat = 8
	End       = 0xCE
)

// Protocol is the header a client sends before anything else: "AMQP" 0 0 9 1.
var Protocol = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

// MinFrame is the smallest frame-max the spec permits either peer to name
// (§4.2.3). A tune-ok asking for less is refused rather than honoured.
const MinFrame = 4096

// Frame is one AMQP frame: what kind, which channel, and the payload between
// the 7-byte header and the end octet.
type Frame struct {
	Type    byte
	Channel uint16
	Payload []byte
}

// Reader reads frames, refusing any payload longer than max.
type Reader struct {
	r   *bufio.Reader
	max uint32
	hdr [7]byte
}

// NewReader reads frames from r, accepting payloads up to max octets.
func NewReader(r io.Reader, max uint32) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 64<<10), max: max}
}

// Read returns the next frame. io.EOF at a frame boundary is a clean close.
func (rd *Reader) Read() (Frame, error) {
	if _, err := io.ReadFull(rd.r, rd.hdr[:]); err != nil {
		return Frame{}, err
	}
	size := binary.BigEndian.Uint32(rd.hdr[3:7])
	if size > rd.max {
		return Frame{}, fmt.Errorf("frame payload %d octets exceeds frame-max %d", size, rd.max)
	}
	payload := make([]byte, size+1) // payload + the end octet
	if _, err := io.ReadFull(rd.r, payload); err != nil {
		return Frame{}, fmt.Errorf("read frame payload: %w", err)
	}
	if payload[size] != End {
		return Frame{}, fmt.Errorf("frame-end octet is 0x%02x, want 0x%02x", payload[size], End)
	}
	return Frame{
		Type:    rd.hdr[0],
		Channel: binary.BigEndian.Uint16(rd.hdr[1:3]),
		Payload: payload[:size],
	}, nil
}

// Header reads the 8-octet protocol header a client opens with and reports
// whether it is the 0-9-1 one.
func (rd *Reader) Header() error {
	var h [8]byte
	if _, err := io.ReadFull(rd.r, h[:]); err != nil {
		return fmt.Errorf("read protocol header: %w", err)
	}
	if string(h[:]) != string(Protocol) {
		return fmt.Errorf("protocol header %x is not AMQP 0-9-1", h)
	}
	return nil
}

// Writer writes frames. Every method sends its frames as ONE unit.
type Writer struct {
	mu  sync.Mutex
	w   *bufio.Writer
	max uint32
}

// NewWriter writes frames to w, splitting content bodies to fit max octets.
func NewWriter(w io.Writer, max uint32) *Writer {
	return &Writer{w: bufio.NewWriterSize(w, 64<<10), max: max}
}

// SetMax adopts the frame-max the client named in connection.tune-ok. Bodies
// written after this are split to fit it.
func (w *Writer) SetMax(max uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.max = max
}

// Max reports the frame-max bodies are currently split to.
func (w *Writer) Max() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.max
}

// Send writes frames consecutively and flushes. Nothing else reaches the socket
// in between.
func (w *Writer) Send(frames ...Frame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.send(frames...)
}

// Content writes a method frame, the content header for body, and as many body
// frames as frame-max requires — one unit, so two deliveries on one channel
// cannot interleave.
func (w *Writer) Content(ch uint16, class uint16, method []byte, p Props, body []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	frames := make([]Frame, 0, 4)
	frames = append(frames,
		Frame{Type: Method, Channel: ch, Payload: method},
		HeaderFrame(ch, class, uint64(len(body)), p),
	)
	// -8: the 7-octet frame header plus the end octet ride inside frame-max.
	chunk := int(w.max) - 8
	if chunk < 1 {
		chunk = MinFrame - 8
	}
	for off := 0; off < len(body); off += chunk {
		end := min(off+chunk, len(body))
		frames = append(frames, Frame{Type: Body, Channel: ch, Payload: body[off:end]})
	}
	return w.send(frames...)
}

// Beat writes a heartbeat on channel 0.
func (w *Writer) Beat() error {
	return w.Send(Frame{Type: Heartbeat})
}

func (w *Writer) send(frames ...Frame) error {
	var hdr [7]byte
	for _, f := range frames {
		hdr[0] = f.Type
		binary.BigEndian.PutUint16(hdr[1:3], f.Channel)
		binary.BigEndian.PutUint32(hdr[3:7], uint32(len(f.Payload)))
		if _, err := w.w.Write(hdr[:]); err != nil {
			return err
		}
		if _, err := w.w.Write(f.Payload); err != nil {
			return err
		}
		if err := w.w.WriteByte(End); err != nil {
			return err
		}
	}
	return w.w.Flush()
}
