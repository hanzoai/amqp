package wire

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

// TestTableRoundTrip covers every field type the errata set defines. A table
// is the one structure a peer can put anything in, so a type this codec cannot
// spell is a message it cannot read.
func TestTableRoundTrip(t *testing.T) {
	in := Table{
		"bool":    true,
		"uint8":   byte(200),
		"int8":    int8(-8),
		"int16":   int16(-1600),
		"uint16":  uint16(1600),
		"int32":   int32(-32000),
		"uint32":  uint32(32000),
		"int64":   int64(-64000000000),
		"float32": float32(1.5),
		"float64": float64(-2.25),
		"decimal": Decimal{Scale: 2, Value: 1234},
		"string":  "a string with \x00 and \xff in it",
		"bytes":   []byte{0, 1, 2, 255},
		"stamp":   Stamp(1700000000),
		"array":   []any{int32(1), "two", true},
		"table":   Table{"nested": int32(9)},
		"void":    nil,
	}
	b := NewMethod(1, 2).Table(in)
	got := NewArgs(b.Bytes()).Table()
	if err := NewArgs(b.Bytes()).Err(); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("table did not survive:\n in = %#v\nout = %#v", in, got)
	}
}

// TestPropsRoundTrip: the content header and the bus carry the same octets, so
// one codec has to answer for both.
func TestPropsRoundTrip(t *testing.T) {
	in := Props{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		Headers:         Table{"n": int32(1)},
		DeliveryMode:    2,
		Priority:        5,
		CorrelationID:   "c",
		ReplyTo:         "r",
		Expiration:      "1000",
		MessageID:       "m",
		Timestamp:       1700000000,
		Type:            "t",
		UserID:          "u",
		AppID:           "a",
	}
	got, err := DecodeProps(EncodeProps(in))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("properties did not survive:\n in = %#v\nout = %#v", in, got)
	}

	// Absent is absent: nothing set means no presence bits and nothing after
	// them, which is the two-octet header a body-only publish sends.
	empty := EncodeProps(Props{})
	if len(empty) != 2 || empty[0] != 0 || empty[1] != 0 {
		t.Fatalf("empty properties encoded as %x, want 0000", empty)
	}
}

// TestBitsPack: consecutive booleans share an octet, least significant first,
// and the next non-bit field starts a fresh one (§4.2.5.2). Getting this wrong
// shifts every field after it.
func TestBitsPack(t *testing.T) {
	b := NewMethod(1, 2).Bit(true).Bit(false).Bit(true).ShortStr("x").Bit(true)
	a := NewArgs(b.Bytes())
	for i, want := range []bool{true, false, true} {
		if got := a.Bit(); got != want {
			t.Fatalf("bit %d = %v, want %v", i, got, want)
		}
	}
	if s := a.ShortStr(); s != "x" {
		t.Fatalf("string after bits = %q, want x", s)
	}
	if !a.Bit() {
		t.Fatal("the bit after a string did not read back")
	}
	if err := a.Err(); err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Nine bits need two octets.
	nine := &Buf{}
	for range 9 {
		nine.Bit(true)
	}
	if got := len(nine.Bytes()); got != 2 {
		t.Fatalf("nine bits took %d octets, want 2", got)
	}
}

// TestArgsStopsAtTheEnd: a truncated frame reads as an error, never as zeros a
// caller might mistake for a real value.
func TestArgsStopsAtTheEnd(t *testing.T) {
	a := NewArgs([]byte{0, 10, 0, 20, 5}) // class, method, then a lone octet
	a.LongLong()
	if a.Err() == nil {
		t.Fatal("reading 8 octets out of 1 reported no error")
	}
	if v := a.Short(); v != 0 {
		t.Fatalf("a field read after a failure returned %d, want the zero value", v)
	}

	bad := NewArgs(append([]byte{0, 10, 0, 20}, 0, 0, 0, 4, 1, 'k', 'Z', 0))
	bad.Table() // 'Z' is not a field type
	if !errors.Is(bad.Err(), ErrSyntax) {
		t.Fatalf("an unknown field type gave %v, want ErrSyntax", bad.Err())
	}
}

// TestReaderRefusesAnOversizedFrame is the guard that keeps a stranger from
// asking for a 4 GiB allocation in seven octets.
func TestReaderRefusesAnOversizedFrame(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{Method, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF})
	if _, err := NewReader(&buf, 4096).Read(); err == nil {
		t.Fatal("a frame claiming 4 GiB was accepted")
	}

	// The refusal is on the header. Nothing was read past it, so a payload
	// that large never had to exist.
	var ok bytes.Buffer
	ok.Write([]byte{Method, 0, 1, 0, 0, 0, 3})
	ok.Write([]byte{1, 2, 3, End})
	f, err := NewReader(&ok, 4096).Read()
	if err != nil {
		t.Fatalf("a frame within frame-max: %v", err)
	}
	if f.Channel != 1 || !bytes.Equal(f.Payload, []byte{1, 2, 3}) {
		t.Fatalf("frame = %+v, want channel 1 payload 010203", f)
	}
}

// TestReaderRefusesAMissingEndOctet: the end octet is the frame's own claim to
// be well formed, and a stream that has lost it cannot be resynchronised.
func TestReaderRefusesAMissingEndOctet(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{Method, 0, 0, 0, 0, 0, 1})
	buf.Write([]byte{7, 0x00}) // payload, then the wrong end octet
	if _, err := NewReader(&buf, 4096).Read(); err == nil {
		t.Fatal("a frame with no end octet was accepted")
	}
}

// TestContentSplitsToFitFrameMax: a body larger than one frame goes out as
// several, each within the limit, and the header still declares the whole.
func TestContentSplitsToFitFrameMax(t *testing.T) {
	var sock bytes.Buffer
	w := NewWriter(&sock, MinFrame)
	body := bytes.Repeat([]byte{'x'}, MinFrame*3)
	method := NewMethod(60, 60).ShortStr("tag").Bytes()
	if err := w.Content(1, 60, method, Props{ContentType: "text/plain"}, body); err != nil {
		t.Fatalf("content: %v", err)
	}

	r := NewReader(&sock, MinFrame)
	f, err := r.Read()
	if err != nil || f.Type != Method {
		t.Fatalf("first frame = %+v, %v; want a method", f, err)
	}
	f, err = r.Read()
	if err != nil || f.Type != Header {
		t.Fatalf("second frame = %+v, %v; want a header", f, err)
	}
	class, size, props, err := ParseHeader(f.Payload)
	if err != nil || class != 60 || size != uint64(len(body)) {
		t.Fatalf("header: class %d size %d err %v", class, size, err)
	}
	if props.ContentType != "text/plain" {
		t.Fatalf("content-type = %q", props.ContentType)
	}

	var got []byte
	for {
		f, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("body frame: %v", err)
		}
		if f.Type != Body {
			t.Fatalf("frame type %d after the header, want a body", f.Type)
		}
		if len(f.Payload) > MinFrame-8 {
			t.Fatalf("body frame is %d octets, over the %d limit", len(f.Payload), MinFrame-8)
		}
		got = append(got, f.Payload...)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("reassembled %d octets, sent %d", len(got), len(body))
	}
}
