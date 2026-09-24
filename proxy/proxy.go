package proxy

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
)

type Proxy struct {
	localAddr  string
	remoteAddr string
}

// Create new proxy instance
func New(localAddr, remoteAddr string) *Proxy {
	return &Proxy{
		localAddr:  localAddr,
		remoteAddr: remoteAddr,
	}
}

func (p *Proxy) Start() {
	listener, err := net.Listen("tcp", p.localAddr)
	if err != nil {
		log.Fatalf("Failed to start listener on %s: %v", p.localAddr, err)
	}

	defer listener.Close()

	for {
		clientConn, err := listener.Accept()
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

	log.Printf("Accepted new connection %s", clientConn.RemoteAddr())

	dbConn, err := net.Dial("tcp", p.remoteAddr)

	if err != nil {
		log.Printf("Failed to connect to database %s: %v", p.remoteAddr, err)
		return
	}

	defer dbConn.Close()

	done := make(chan struct{}, 2)

	go func() {
		if err := pipePackets(dbConn, clientConn, "client->db"); err != nil {
			logPipeExit("client->db", err)
		}
		done <- struct{}{}
	}()

	go func() {
		if err := pipePackets(clientConn, dbConn, "db->client"); err != nil {
			logPipeExit("db->client",err)
		} 
		done <- struct{}{}
	}()

	<-done
	log.Printf("Connection closed for client %s", clientConn.RemoteAddr())

}


// mysqlPacketHeaderLen is the fixed header on every MySQL wire packet:
// 3 bytes of payload length (little-endian) followed by a 1 byte sequence id.
const mysqlPacketHeaderLen = 4




func pipePackets(dst io.Writer, src io.Reader, direction string) error{
	buffered := bufio.NewReader(src)
	header := make([]byte, mysqlPacketHeaderLen)

	for {
		if _,err := io.ReadFull(buffered, header); err != nil{
			return err
		}

		length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		seq := header[3]


		packet := make([]byte, mysqlPacketHeaderLen+length)
		copy(packet, header)

		if _, err := io.ReadFull(buffered, packet[mysqlPacketHeaderLen:]); err != nil {
			return err
		}

		log.Printf("%s seq=%d len=%d", direction, seq, length)

		if _, err := dst.Write(packet); err != nil {
			return err
		}
	}
}

func logPipeExit(direction string, err error) {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return 
	}
	log.Printf("%s relay stopped: %v", direction, err)
}