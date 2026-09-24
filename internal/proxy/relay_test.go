package proxy

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/MrAi0/goguard/internal/mysql"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// buildPacket frames a payload the way MySQL does: 3-byte little-endian
// length, 1-byte sequence id, then the payload itself.
func buildPacket(seq byte, payload []byte) []byte {
	n := len(payload)
	pkt := []byte{byte(n), byte(n >> 8), byte(n >> 16), seq}
	return append(pkt, payload...)
}

// comQuery builds a COM_QUERY packet (command byte 0x03 + SQL text).
func comQuery(seq byte, sql string) []byte {
	return buildPacket(seq, append([]byte{0x03}, sql...))
}

// dribbleReader hands out at most n bytes per Read, simulating a fragmented
// TCP stream where packet boundaries never line up with read boundaries.
type dribbleReader struct {
	data []byte
	pos  int
	n    int
}

func (d *dribbleReader) Read(p []byte) (int, error) {
	if d.pos >= len(d.data) {
		return 0, io.EOF
	}
	max := d.n
	if len(p) < max {
		max = len(p)
	}
	end := d.pos + max
	if end > len(d.data) {
		end = len(d.data)
	}
	n := copy(p, d.data[d.pos:end])
	d.pos += n
	return n, nil
}

// failingWriter fails on the Nth write, to check error propagation.
type failingWriter struct {
	failOn int
	count  int
}

var errWriteFailed = errors.New("write failed")

func (f *failingWriter) Write(p []byte) (int, error) {
	f.count++
	if f.count == f.failOn {
		return 0, errWriteFailed
	}
	return len(p), nil
}

// STEP 1 (length decode) lives in internal/mysql as TestPayloadLen.

// ---------------------------------------------------------------------------
// STEP 2 - one packet in, one identical packet out
// ---------------------------------------------------------------------------

func TestStep2_SinglePacket(t *testing.T) {
	in := comQuery(0, "SELECT 1")

	var out bytes.Buffer
	err := relay(&out, bytes.NewReader(in), nil)

	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF at clean end of stream, got %v", err)
	}
	if !bytes.Equal(out.Bytes(), in) {
		t.Errorf("forwarded bytes differ:\n got % x\nwant % x", out.Bytes(), in)
	}
}

// ---------------------------------------------------------------------------
// STEP 3 - many packets back to back stay in sync
// ---------------------------------------------------------------------------

func TestStep3_MultiplePackets(t *testing.T) {
	var in []byte
	in = append(in, buildPacket(1, []byte{0x01})...)                   // column count
	in = append(in, buildPacket(2, []byte("def\x00\x00\x00\x011"))...) // column def
	in = append(in, buildPacket(3, []byte{0xfe, 0, 0, 0x02, 0})...)    // EOF
	in = append(in, buildPacket(4, []byte{0x01, '1'})...)              // row
	in = append(in, buildPacket(5, []byte{0xfe, 0, 0, 0x02, 0})...)    // EOF

	var out bytes.Buffer
	err := relay(&out, bytes.NewReader(in), nil)

	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if !bytes.Equal(out.Bytes(), in) {
		t.Errorf("stream desynchronized:\n got % x\nwant % x", out.Bytes(), in)
	}
}

// ---------------------------------------------------------------------------
// STEP 4 - fragmented delivery is reassembled correctly
// ---------------------------------------------------------------------------

func TestStep4_FragmentedStream(t *testing.T) {
	in := append(comQuery(0, "SELECT 1"), comQuery(0, "SELECT 2")...)

	// Try every fragment size from 1 byte upward. Size 1 and 3 are the nasty
	// ones: they split the header itself.
	for chunk := 1; chunk <= 20; chunk++ {
		var out bytes.Buffer
		err := relay(&out, &dribbleReader{data: in, n: chunk}, nil)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("chunk=%d: expected io.EOF, got %v", chunk, err)
		}
		if !bytes.Equal(out.Bytes(), in) {
			t.Errorf("chunk=%d: reassembly failed:\n got % x\nwant % x",
				chunk, out.Bytes(), in)
		}
	}
}

// ---------------------------------------------------------------------------
// STEP 5 - a zero-length payload is legal and must not hang
// ---------------------------------------------------------------------------

func TestStep5_ZeroLengthPacket(t *testing.T) {
	in := buildPacket(7, nil) // 00 00 00 07, no payload

	var out bytes.Buffer
	err := relay(&out, bytes.NewReader(in), nil)

	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if !bytes.Equal(out.Bytes(), in) {
		t.Errorf("got % x, want % x", out.Bytes(), in)
	}
}

// ---------------------------------------------------------------------------
// STEP 6 - truncated input reports the right error
// ---------------------------------------------------------------------------

func TestStep6_TruncatedInput(t *testing.T) {
	full := comQuery(0, "SELECT 1")

	t.Run("clean close between packets", func(t *testing.T) {
		var out bytes.Buffer
		err := relay(&out, bytes.NewReader(full), nil)
		if !errors.Is(err, io.EOF) {
			t.Errorf("want io.EOF, got %v", err)
		}
	})

	t.Run("cut mid-header", func(t *testing.T) {
		var out bytes.Buffer
		err := relay(&out, bytes.NewReader(full[:2]), nil)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("want io.ErrUnexpectedEOF, got %v", err)
		}
	})

	t.Run("cut right after header", func(t *testing.T) {
		var out bytes.Buffer
		err := relay(&out, bytes.NewReader(full[:4]), nil)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("want io.ErrUnexpectedEOF, got %v", err)
		}
	})

	t.Run("cut mid-payload", func(t *testing.T) {
		var out bytes.Buffer
		err := relay(&out, bytes.NewReader(full[:8]), nil)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("want io.ErrUnexpectedEOF, got %v", err)
		}
		// the partial packet must NOT have been forwarded
		if out.Len() != 0 {
			t.Errorf("forwarded %d bytes of an incomplete packet", out.Len())
		}
	})
}

// ---------------------------------------------------------------------------
// STEP 7 - a write error stops the loop and is returned
// ---------------------------------------------------------------------------

func TestStep7_WriteErrorPropagates(t *testing.T) {
	in := append(comQuery(0, "SELECT 1"), comQuery(0, "SELECT 2")...)

	w := &failingWriter{failOn: 2} // let the first packet through, fail the second
	err := relay(w, bytes.NewReader(in), nil)

	if !errors.Is(err, errWriteFailed) {
		t.Fatalf("want errWriteFailed, got %v", err)
	}
	if w.count != 2 {
		t.Errorf("expected to stop after 2 writes, got %d", w.count)
	}
}

// ---------------------------------------------------------------------------
// STEP 8 - over a real TCP connection
// ---------------------------------------------------------------------------

func TestStep8_OverRealTCP(t *testing.T) {
	// A fake "database" that accepts one connection and echoes an OK packet.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverGot := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 64)
		n, _ := conn.Read(buf)
		serverGot <- buf[:n]
	}()

	// The proxy side: read framed packets from a client, forward to the server.
	dbConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer dbConn.Close()

	clientBytes := comQuery(0, "SELECT 1")
	if err := relay(dbConn, bytes.NewReader(clientBytes), nil); !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF, got %v", err)
	}

	got := <-serverGot
	if !bytes.Equal(got, clientBytes) {
		t.Errorf("server received % x, want % x", got, clientBytes)
	}
}

// ---------------------------------------------------------------------------
// STEP 9 - the inspect hook sees every packet, in order, before it's forwarded
// ---------------------------------------------------------------------------

func TestStep9_InspectHook(t *testing.T) {
	first, second := comQuery(0, "SELECT 1"), comQuery(0, "SELECT 2")
	in := append(append([]byte{}, first...), second...)

	var out bytes.Buffer
	var seen [][]byte
	err := relay(&out, bytes.NewReader(in), func(pkt mysql.Packet) {
		// copy, so the check below doesn't depend on relay reusing buffers
		seen = append(seen, append([]byte{}, pkt...))
		if !bytes.Equal(out.Bytes(), bytes.Join(seen[:len(seen)-1], nil)) {
			t.Errorf("packet %d was forwarded before inspect ran", len(seen))
		}
	})

	if !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF, got %v", err)
	}
	if len(seen) != 2 || !bytes.Equal(seen[0], first) || !bytes.Equal(seen[1], second) {
		t.Errorf("inspect saw % x", seen)
	}
}
