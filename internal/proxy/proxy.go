// Package proxy accepts MySQL client connections and relays them, packet by
// packet, to an upstream MySQL server.
package proxy

import (
	"errors"
	"log"
	"net"

	"github.com/MrAi0/goguard/internal/mysql"
)

// maxLoggedQueryLen caps how much SQL text goes into one log line so a bulk
// INSERT doesn't flood the log.
const maxLoggedQueryLen = 1024

// Config controls where the proxy forwards to and what it logs.
type Config struct {
	// UpstreamAddr is the MySQL server every client connection is forwarded to.
	UpstreamAddr string

	// LogQueries logs every command a client sends, including the SQL text
	// of COM_QUERY and COM_STMT_PREPARE.
	LogQueries bool

	// LogPackets logs the direction, sequence id and length of every packet.
	LogPackets bool
}

type Proxy struct {
	cfg Config
}

// New creates a proxy instance.
func New(cfg Config) *Proxy {
	return &Proxy{cfg: cfg}
}

// Serve accepts connections on ln and relays each one to the upstream server.
// It returns once ln is closed.
func (p *Proxy) Serve(ln net.Listener) {
	for {
		clientConn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		go p.handleConnection(clientConn)
	}
}

func (p *Proxy) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	client := clientConn.RemoteAddr().String()
	log.Printf("Accepted new connection %s", client)

	dbConn, err := net.Dial("tcp", p.cfg.UpstreamAddr)
	if err != nil {
		log.Printf("Failed to connect to database %s: %v", p.cfg.UpstreamAddr, err)
		return
	}

	defer dbConn.Close()

	done := make(chan struct{}, 2)

	go func() {
		err := relay(dbConn, clientConn, p.inspector(client, "client->db", p.cfg.LogQueries))
		logRelayExit("client->db", err)
		done <- struct{}{}
	}()

	go func() {
		err := relay(clientConn, dbConn, p.inspector(client, "db->client", false))
		logRelayExit("db->client", err)
		done <- struct{}{}
	}()

	<-done
	log.Printf("Connection closed for client %s", client)
}

// inspector builds the per-packet hook for one relay direction, or returns nil
// when there is nothing to log for it.
func (p *Proxy) inspector(client, direction string, logCommands bool) func(mysql.Packet) {
	if !p.cfg.LogPackets && !logCommands {
		return nil
	}

	return func(pkt mysql.Packet) {
		if p.cfg.LogPackets {
			log.Printf("[%s] %s seq=%d len=%d", client, direction, pkt.Seq(), len(pkt.Payload()))
		}
		if logCommands {
			logCommand(client, pkt)
		}
	}
}

func logCommand(client string, pkt mysql.Packet) {
	cmd, arg, ok := mysql.ParseCommand(pkt)
	if !ok {
		return
	}

	switch cmd {
	case mysql.ComQuery, mysql.ComStmtPrepare, mysql.ComInitDB:
		log.Printf("[%s] %s %q", client, cmd, truncate(arg, maxLoggedQueryLen))
	default:
		log.Printf("[%s] %s", client, cmd)
	}
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
