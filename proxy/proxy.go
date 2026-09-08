package proxy

import (
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
			log.Fatalf("Failed to accept connection: %v", err)
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
	}

	defer dbConn.Close()

	done := make(chan struct{}, 2)

	go func() {
		io.Copy(dbConn, clientConn)
		done <- struct{}{}
	}()

	go func() {
		io.Copy(clientConn, dbConn)
		done <- struct{}{}
	}()

	<-done
	log.Printf("Connection closed for client %s", clientConn.RemoteAddr())

}
