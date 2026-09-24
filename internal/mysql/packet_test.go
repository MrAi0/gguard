package mysql

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func buildPacket(seq byte, payload []byte) []byte {
	n := len(payload)
	pkt := []byte{byte(n), byte(n >> 8), byte(n >> 16), seq}
	return append(pkt, payload...)
}

func TestPayloadLen(t *testing.T) {
	cases := []struct {
		header [HeaderLen]byte
		want   int
	}{
		{[HeaderLen]byte{0x00, 0x00, 0x00}, 0},
		{[HeaderLen]byte{0x09, 0x00, 0x00}, 9},
		{[HeaderLen]byte{0xff, 0x00, 0x00}, 255},
		{[HeaderLen]byte{0x00, 0x01, 0x00}, 256},
		{[HeaderLen]byte{0x2c, 0x01, 0x00}, 300},
		{[HeaderLen]byte{0x00, 0x00, 0x01}, 65536},
		{[HeaderLen]byte{0xff, 0xff, 0xff}, 16777215},
		// the sequence id byte must not leak into the length
		{[HeaderLen]byte{0x01, 0x00, 0x00, 0xff}, 1},
	}
	for _, c := range cases {
		if got := payloadLen(c.header); got != c.want {
			t.Errorf("% x: got %d, want %d", c.header, got, c.want)
		}
	}
}

func TestReadPacket(t *testing.T) {
	raw := buildPacket(5, []byte("hello"))

	pkt, err := ReadPacket(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pkt, raw) {
		t.Errorf("got % x, want % x", pkt, raw)
	}
	if pkt.Seq() != 5 {
		t.Errorf("seq: got %d, want 5", pkt.Seq())
	}
	if string(pkt.Payload()) != "hello" {
		t.Errorf("payload: got %q, want %q", pkt.Payload(), "hello")
	}
}

func TestReadPacket_Truncated(t *testing.T) {
	full := buildPacket(0, []byte("SELECT 1"))

	cases := []struct {
		name string
		in   []byte
		want error
	}{
		{"empty stream", nil, io.EOF},
		{"cut mid-header", full[:2], io.ErrUnexpectedEOF},
		{"cut right after header", full[:HeaderLen], io.ErrUnexpectedEOF},
		{"cut mid-payload", full[:8], io.ErrUnexpectedEOF},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pkt, err := ReadPacket(bytes.NewReader(c.in))
			if !errors.Is(err, c.want) {
				t.Errorf("want %v, got %v", c.want, err)
			}
			if pkt != nil {
				t.Errorf("returned partial packet % x", pkt)
			}
		})
	}
}

func TestParseCommand(t *testing.T) {
	cases := []struct {
		name    string
		pkt     []byte
		wantCmd Command
		wantArg string
		wantOK  bool
	}{
		{"query", buildPacket(0, []byte("\x03SELECT 1")), ComQuery, "SELECT 1", true},
		{"prepare", buildPacket(0, []byte("\x16SELECT ?")), ComStmtPrepare, "SELECT ?", true},
		{"quit has no argument", buildPacket(0, []byte{0x01}), ComQuit, "", true},
		{"non-zero seq is not a command", buildPacket(1, []byte("\x03SELECT 1")), 0, "", false},
		{"empty payload", buildPacket(0, nil), 0, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cmd, arg, ok := ParseCommand(Packet(c.pkt))
			if ok != c.wantOK || cmd != c.wantCmd || string(arg) != c.wantArg {
				t.Errorf("got (%v, %q, %v), want (%v, %q, %v)",
					cmd, arg, ok, c.wantCmd, c.wantArg, c.wantOK)
			}
		})
	}
}

func TestCommandString(t *testing.T) {
	if got := ComQuery.String(); got != "COM_QUERY" {
		t.Errorf("got %q", got)
	}
	if got := Command(0xaa).String(); got != "COM_UNKNOWN(0xaa)" {
		t.Errorf("got %q", got)
	}
}
