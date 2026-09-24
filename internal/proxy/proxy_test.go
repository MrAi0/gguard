package proxy

import (
	"bytes"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MrAi0/goguard/internal/mysql"
)

// syncBuffer is a bytes.Buffer safe to share between the logger and the test.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLog(t *testing.T) *syncBuffer {
	t.Helper()
	out := &syncBuffer{}
	prevOut, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(out)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	return out
}

func readPacket(t *testing.T, conn net.Conn) mysql.Packet {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := mysql.ReadPacket(conn)
	if err != nil {
		t.Fatalf("reading packet: %v", err)
	}
	return pkt
}

// startProxy runs a proxy in front of a fake MySQL server. The fake server
// sends a greeting (seq 0, which must not be mistaken for a client command),
// then sends each packet it receives back on serverGot.
func startProxy(t *testing.T, cfg Config) (proxyAddr string, serverGot <-chan mysql.Packet) {
	t.Helper()

	dbLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dbLn.Close() })

	got := make(chan mysql.Packet, 8)
	go func() {
		conn, err := dbLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write(buildPacket(0, []byte("\x0afake-greeting")))
		for {
			pkt, err := mysql.ReadPacket(conn)
			if err != nil {
				return
			}
			got <- pkt
		}
	}()

	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { proxyLn.Close() })

	cfg.UpstreamAddr = dbLn.Addr().String()
	go New(cfg).Serve(proxyLn)

	return proxyLn.Addr().String(), got
}

func TestProxy_LogsClientQueries(t *testing.T) {
	logs := captureLog(t)
	addr, serverGot := startProxy(t, Config{LogQueries: true})

	client, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	readPacket(t, client) // greeting

	auth := buildPacket(1, []byte("\x03not-a-query-auth-response"))
	query := comQuery(0, "SELECT * FROM users WHERE id = 1")
	ping := buildPacket(0, []byte{byte(mysql.ComPing)})
	for _, pkt := range [][]byte{auth, query, ping} {
		if _, err := client.Write(pkt); err != nil {
			t.Fatal(err)
		}
	}

	for _, want := range [][]byte{auth, query, ping} {
		select {
		case pkt := <-serverGot:
			if !bytes.Equal(pkt, want) {
				t.Errorf("server received % x, want % x", pkt, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for packet at server")
		}
	}

	// The packets have been forwarded, so their log lines are already written.
	out := logs.String()
	for _, want := range []string{
		`COM_QUERY "SELECT * FROM users WHERE id = 1"`,
		"COM_PING",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %q:\n%s", want, out)
		}
	}
	for _, unwanted := range []string{"not-a-query", "fake-greeting", "seq="} {
		if strings.Contains(out, unwanted) {
			t.Errorf("log should not contain %q:\n%s", unwanted, out)
		}
	}
}

func TestProxy_QueryLoggingDisabled(t *testing.T) {
	logs := captureLog(t)
	addr, serverGot := startProxy(t, Config{LogQueries: false})

	client, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	readPacket(t, client)
	client.Write(comQuery(0, "SELECT secret"))

	select {
	case <-serverGot:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for packet at server")
	}

	if out := logs.String(); strings.Contains(out, "SELECT secret") {
		t.Errorf("query logged with LogQueries off:\n%s", out)
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate([]byte("abc"), 3); got != "abc" {
		t.Errorf("got %q", got)
	}
	if got := truncate([]byte("abcdef"), 3); got != "abc..." {
		t.Errorf("got %q", got)
	}
}
