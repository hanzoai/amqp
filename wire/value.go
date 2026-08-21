package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// ErrSyntax is what every malformed value collapses to. A cursor that hits it
// keeps returning zeros, so a parser can read straight through and check once
// at the end instead of after every field.
var ErrSyntax = errors.New("wire: malformed value")

// Table is an AMQP field table: string keys over the typed values below. The
// octet codes are the 0-9-1 errata set that RabbitMQ and every client in the
// wild implement, not the smaller set in the original PDF.
type Table map[string]any

// Decimal is the 'D' field type: value scaled by 10^-Scale.
type Decimal struct {
	Scale uint8
	Value int32
}

// Props is the basic-class content header: the fourteen properties a message
// carries beside its body, in the order their presence bits appear.
type Props struct {
	ContentType     string
	ContentEncoding string
	Headers         Table
	DeliveryMode    uint8
	Priority        uint8
	CorrelationID   string
	ReplyTo         string
	Expiration      string
	MessageID       string
	Timestamp       uint64
	Type            string
	UserID          string
	AppID           string
}

// Presence bits, most significant first (§4.2.6.1).
const (
	flagContentType     = 0x8000
	flagContentEncoding = 0x4000
	flagHeaders         = 0x2000
	flagDeliveryMode    = 0x1000
	flagPriority        = 0x0800
	flagCorrelationID   = 0x0400
	flagReplyTo         = 0x0200
	flagExpiration      = 0x0100
	flagMessageID       = 0x0080
	flagTimestamp       = 0x0040
	flagType            = 0x0020
	flagUserID          = 0x0010
	flagAppID           = 0x0008
	flagMore            = 0x0001 // another flags short follows
)

// ---------------------------------------------------------------------------
// Building
// ---------------------------------------------------------------------------

// Buf builds a method payload. Every writer appends; nothing can fail, so the
// caller reads like the spec's field list.
type Buf struct {
	b    []byte
	bits byte // packed booleans awaiting a flush
	n    uint // how many are packed
}

// NewMethod starts a method payload with its class and method id.
func NewMethod(class, method uint16) *Buf {
	b := &Buf{b: make([]byte, 0, 64)}
	return b.Short(class).Short(method)
}

// Bytes returns the payload, flushing any trailing packed bits.
func (b *Buf) Bytes() []byte {
	b.flush()
	return b.b
}

// Frame wraps the payload as a method frame for ch.
func (b *Buf) Frame(ch uint16) Frame {
	return Frame{Type: Method, Channel: ch, Payload: b.Bytes()}
}

// Octet appends one byte.
func (b *Buf) Octet(v byte) *Buf { b.flush(); b.b = append(b.b, v); return b }

// Short appends a 16-bit integer.
func (b *Buf) Short(v uint16) *Buf {
	b.flush()
	b.b = binary.BigEndian.AppendUint16(b.b, v)
	return b
}

// Long appends a 32-bit integer.
func (b *Buf) Long(v uint32) *Buf {
	b.flush()
	b.b = binary.BigEndian.AppendUint32(b.b, v)
	return b
}

// LongLong appends a 64-bit integer.
func (b *Buf) LongLong(v uint64) *Buf {
	b.flush()
	b.b = binary.BigEndian.AppendUint64(b.b, v)
	return b
}

// Bit appends a boolean. Consecutive bits pack into one octet, least
// significant first, and are flushed by the next non-bit field (§4.2.5.2).
func (b *Buf) Bit(v bool) *Buf {
	if b.n == 8 {
		b.flush()
	}
	if v {
		b.bits |= 1 << b.n
	}
	b.n++
	return b
}

// ShortStr appends a short string. Longer than 255 octets is truncated: the
// length field is one byte, so there is no encoding that could carry it, and
// every such field in 0-9-1 is a name the peer chose.
func (b *Buf) ShortStr(s string) *Buf {
	b.flush()
	if len(s) > math.MaxUint8 {
		s = s[:math.MaxUint8]
	}
	b.b = append(b.b, byte(len(s)))
	b.b = append(b.b, s...)
	return b
}

// LongStr appends a long string.
func (b *Buf) LongStr(s string) *Buf {
	b.flush()
	b.b = binary.BigEndian.AppendUint32(b.b, uint32(len(s)))
	b.b = append(b.b, s...)
	return b
}

// Table appends a field table.
func (b *Buf) Table(t Table) *Buf {
	b.flush()
	at := len(b.b)
	b.b = binary.BigEndian.AppendUint32(b.b, 0)
	for k, v := range t {
		if len(k) > math.MaxUint8 {
			continue
		}
		b.b = append(b.b, byte(len(k)))
		b.b = append(b.b, k...)
		b.field(v)
	}
	binary.BigEndian.PutUint32(b.b[at:at+4], uint32(len(b.b)-at-4))
	return b
}

func (b *Buf) flush() {
	if b.n > 0 {
		b.b = append(b.b, b.bits)
		b.bits, b.n = 0, 0
	}
}

func (b *Buf) field(v any) {
	switch v := v.(type) {
	case bool:
		b.b = append(b.b, 't', 0)
		if v {
			b.b[len(b.b)-1] = 1
		}
	case byte:
		b.b = append(b.b, 'B', v)
	case int8:
		b.b = append(b.b, 'b', byte(v))
	case int16:
		b.b = append(b.b, 's')
		b.b = binary.BigEndian.AppendUint16(b.b, uint16(v))
	case uint16:
		b.b = append(b.b, 'u')
		b.b = binary.BigEndian.AppendUint16(b.b, v)
	case int:
		b.b = append(b.b, 'I')
		b.b = binary.BigEndian.AppendUint32(b.b, uint32(int32(v)))
	case int32:
		b.b = append(b.b, 'I')
		b.b = binary.BigEndian.AppendUint32(b.b, uint32(v))
	case uint32:
		b.b = append(b.b, 'i')
		b.b = binary.BigEndian.AppendUint32(b.b, v)
	case int64:
		b.b = append(b.b, 'l')
		b.b = binary.BigEndian.AppendUint64(b.b, uint64(v))
	case float32:
		b.b = append(b.b, 'f')
		b.b = binary.BigEndian.AppendUint32(b.b, math.Float32bits(v))
	case float64:
		b.b = append(b.b, 'd')
		b.b = binary.BigEndian.AppendUint64(b.b, math.Float64bits(v))
	case Decimal:
		b.b = append(b.b, 'D', v.Scale)
		b.b = binary.BigEndian.AppendUint32(b.b, uint32(v.Value))
	case string:
		b.b = append(b.b, 'S')
		b.b = binary.BigEndian.AppendUint32(b.b, uint32(len(v)))
		b.b = append(b.b, v...)
	case []byte:
		b.b = append(b.b, 'x')
		b.b = binary.BigEndian.AppendUint32(b.b, uint32(len(v)))
		b.b = append(b.b, v...)
	case Stamp:
		b.b = append(b.b, 'T')
		b.b = binary.BigEndian.AppendUint64(b.b, uint64(v))
	case []any:
		b.b = append(b.b, 'A')
		at := len(b.b)
		b.b = binary.BigEndian.AppendUint32(b.b, 0)
		for _, e := range v {
			b.field(e)
		}
		binary.BigEndian.PutUint32(b.b[at:at+4], uint32(len(b.b)-at-4))
	case Table:
		b.b = append(b.b, 'F')
		b.Table(v)
	case nil:
		b.b = append(b.b, 'V')
	default:
		// A value we cannot spell is written as void rather than dropped, so
		// the key still appears and the table stays parseable.
		b.b = append(b.b, 'V')
	}
}

// Stamp is the 'T' field type: seconds since the epoch. It is its own type so
// a table round-trips through Buf and Args unchanged, with no dependency on
// time.Time's monotonic clock or location.
type Stamp uint64

// HeaderFrame builds a content header frame: class, weight, body size, and the
// property section (§4.2.6).
func HeaderFrame(ch uint16, class uint16, size uint64, p Props) Frame {
	b := &Buf{b: make([]byte, 0, 96)}
	b.Short(class).Short(0).LongLong(size)
	b.b = append(b.b, EncodeProps(p)...)
	return Frame{Type: Header, Channel: ch, Payload: b.Bytes()}
}

// EncodeProps writes the presence bits and the properties present. It is the
// section of a content header after the body size — and also what rides on the
// bus, so a message's properties have ONE encoding whichever side reads them.
func EncodeProps(p Props) []byte {
	b := &Buf{b: make([]byte, 0, 80)}

	var flags uint16
	set := func(f uint16, present bool) {
		if present {
			flags |= f
		}
	}
	set(flagContentType, p.ContentType != "")
	set(flagContentEncoding, p.ContentEncoding != "")
	set(flagHeaders, len(p.Headers) > 0)
	set(flagDeliveryMode, p.DeliveryMode > 0)
	set(flagPriority, p.Priority > 0)
	set(flagCorrelationID, p.CorrelationID != "")
	set(flagReplyTo, p.ReplyTo != "")
	set(flagExpiration, p.Expiration != "")
	set(flagMessageID, p.MessageID != "")
	set(flagTimestamp, p.Timestamp > 0)
	set(flagType, p.Type != "")
	set(flagUserID, p.UserID != "")
	set(flagAppID, p.AppID != "")
	b.Short(flags)

	if flags&flagContentType != 0 {
		b.ShortStr(p.ContentType)
	}
	if flags&flagContentEncoding != 0 {
		b.ShortStr(p.ContentEncoding)
	}
	if flags&flagHeaders != 0 {
		b.Table(p.Headers)
	}
	if flags&flagDeliveryMode != 0 {
		b.Octet(p.DeliveryMode)
	}
	if flags&flagPriority != 0 {
		b.Octet(p.Priority)
	}
	if flags&flagCorrelationID != 0 {
		b.ShortStr(p.CorrelationID)
	}
	if flags&flagReplyTo != 0 {
		b.ShortStr(p.ReplyTo)
	}
	if flags&flagExpiration != 0 {
		b.ShortStr(p.Expiration)
	}
	if flags&flagMessageID != 0 {
		b.ShortStr(p.MessageID)
	}
	if flags&flagTimestamp != 0 {
		b.LongLong(p.Timestamp)
	}
	if flags&flagType != 0 {
		b.ShortStr(p.Type)
	}
	if flags&flagUserID != 0 {
		b.ShortStr(p.UserID)
	}
	if flags&flagAppID != 0 {
		b.ShortStr(p.AppID)
	}
	return b.Bytes()
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

// Args is a cursor over a frame payload. It records the first failure and
// returns zeros afterwards, so a parser reads its fields in a straight line
// and asks Err once — the shape that made bufio.Scanner readable.
type Args struct {
	b    []byte
	i    int
	err  error
	bits byte
	n    uint
	got  bool // bits holds an unconsumed octet
}

// NewArgs reads fields from a method payload, past the class and method ids.
func NewArgs(payload []byte) *Args {
	a := &Args{b: payload}
	if len(payload) < 4 {
		a.err = ErrSyntax
		return a
	}
	a.i = 4
	return a
}

// ClassMethod reports the ids at the head of a method payload.
func ClassMethod(payload []byte) (class, method uint16, err error) {
	if len(payload) < 4 {
		return 0, 0, ErrSyntax
	}
	return binary.BigEndian.Uint16(payload[0:2]), binary.BigEndian.Uint16(payload[2:4]), nil
}

// Err reports the first malformation seen, if any.
func (a *Args) Err() error { return a.err }

func (a *Args) take(n int) []byte {
	a.got = false
	if a.err != nil {
		return nil
	}
	if a.i+n > len(a.b) {
		a.err = ErrSyntax
		return nil
	}
	v := a.b[a.i : a.i+n]
	a.i += n
	return v
}

// Octet reads one byte.
func (a *Args) Octet() byte {
	v := a.take(1)
	if v == nil {
		return 0
	}
	return v[0]
}

// Short reads a 16-bit integer.
func (a *Args) Short() uint16 {
	v := a.take(2)
	if v == nil {
		return 0
	}
	return binary.BigEndian.Uint16(v)
}

// Long reads a 32-bit integer.
func (a *Args) Long() uint32 {
	v := a.take(4)
	if v == nil {
		return 0
	}
	return binary.BigEndian.Uint32(v)
}

// LongLong reads a 64-bit integer.
func (a *Args) LongLong() uint64 {
	v := a.take(8)
	if v == nil {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

// Bit reads a boolean. Consecutive bits come from one octet, least significant
// first; the first non-bit field starts a new octet.
func (a *Args) Bit() bool {
	if !a.got || a.n == 8 {
		if a.err != nil {
			return false
		}
		if a.i >= len(a.b) {
			a.err = ErrSyntax
			return false
		}
		a.bits, a.n = a.b[a.i], 0
		a.i++
		a.got = true
	}
	v := a.bits&(1<<a.n) != 0
	a.n++
	return v
}

// ShortStr reads a short string.
func (a *Args) ShortStr() string {
	n := a.take(1)
	if n == nil {
		return ""
	}
	v := a.take(int(n[0]))
	if v == nil {
		return ""
	}
	return string(v)
}

// LongStr reads a long string.
func (a *Args) LongStr() string {
	n := a.take(4)
	if n == nil {
		return ""
	}
	size := binary.BigEndian.Uint32(n)
	if size > uint32(len(a.b)) {
		a.err = ErrSyntax
		return ""
	}
	v := a.take(int(size))
	if v == nil {
		return ""
	}
	return string(v)
}

// Table reads a field table. An unrecognised type code is a syntax error and
// not a skip: the codes carry no length, so there is nothing to skip past and
// guessing would desynchronise every field after it.
func (a *Args) Table() Table {
	n := a.take(4)
	if n == nil {
		return nil
	}
	size := int(binary.BigEndian.Uint32(n))
	body := a.take(size)
	if body == nil {
		return nil
	}
	inner := &Args{b: body}
	t := Table{}
	for inner.err == nil && inner.i < len(inner.b) {
		k := inner.ShortStr()
		v := inner.field()
		if inner.err != nil {
			break
		}
		t[k] = v
	}
	if inner.err != nil {
		a.err = inner.err
		return nil
	}
	return t
}

func (a *Args) field() any {
	t := a.take(1)
	if t == nil {
		return nil
	}
	switch t[0] {
	case 't':
		return a.Octet() != 0
	case 'B':
		return a.Octet()
	case 'b':
		return int8(a.Octet())
	case 's':
		return int16(a.Short())
	case 'u':
		return a.Short()
	case 'I':
		return int32(a.Long())
	case 'i':
		return a.Long()
	case 'l':
		return int64(a.LongLong())
	case 'f':
		return math.Float32frombits(a.Long())
	case 'd':
		return math.Float64frombits(a.LongLong())
	case 'D':
		return Decimal{Scale: a.Octet(), Value: int32(a.Long())}
	case 'S':
		return a.LongStr()
	case 'x':
		return []byte(a.LongStr())
	case 'T':
		return Stamp(a.LongLong())
	case 'F':
		return a.Table()
	case 'A':
		n := a.take(4)
		if n == nil {
			return nil
		}
		body := a.take(int(binary.BigEndian.Uint32(n)))
		if body == nil {
			return nil
		}
		inner := &Args{b: body}
		var out []any
		for inner.err == nil && inner.i < len(inner.b) {
			out = append(out, inner.field())
		}
		if inner.err != nil {
			a.err = inner.err
		}
		return out
	case 'V':
		return nil
	default:
		a.err = fmt.Errorf("%w: field type %q", ErrSyntax, t[0])
		return nil
	}
}

// ParseHeader reads a content header frame: the class it belongs to, the total
// body size to expect, and the properties present.
func ParseHeader(payload []byte) (class uint16, size uint64, p Props, err error) {
	a := &Args{b: payload}
	class = a.Short()
	a.Short() // weight, unused since 0-9
	size = a.LongLong()
	if a.err != nil {
		return 0, 0, Props{}, a.err
	}
	p, err = decode(a)
	return class, size, p, err
}

// DecodeProps reads what EncodeProps wrote.
func DecodeProps(b []byte) (Props, error) { return decode(&Args{b: b}) }

func decode(a *Args) (p Props, err error) {
	flags := a.Short()
	// Bit 0 says another flags short follows. basic has fourteen properties so
	// one short always suffices, but a peer may still spell it long-form.
	for flags&flagMore != 0 {
		flags = a.Short()
		if a.err != nil {
			break
		}
	}

	if flags&flagContentType != 0 {
		p.ContentType = a.ShortStr()
	}
	if flags&flagContentEncoding != 0 {
		p.ContentEncoding = a.ShortStr()
	}
	if flags&flagHeaders != 0 {
		p.Headers = a.Table()
	}
	if flags&flagDeliveryMode != 0 {
		p.DeliveryMode = a.Octet()
	}
	if flags&flagPriority != 0 {
		p.Priority = a.Octet()
	}
	if flags&flagCorrelationID != 0 {
		p.CorrelationID = a.ShortStr()
	}
	if flags&flagReplyTo != 0 {
		p.ReplyTo = a.ShortStr()
	}
	if flags&flagExpiration != 0 {
		p.Expiration = a.ShortStr()
	}
	if flags&flagMessageID != 0 {
		p.MessageID = a.ShortStr()
	}
	if flags&flagTimestamp != 0 {
		p.Timestamp = a.LongLong()
	}
	if flags&flagType != 0 {
		p.Type = a.ShortStr()
	}
	if flags&flagUserID != 0 {
		p.UserID = a.ShortStr()
	}
	if flags&flagAppID != 0 {
		p.AppID = a.ShortStr()
	}
	return p, a.err
}
